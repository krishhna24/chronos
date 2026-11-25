package pebblestore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

var dt0 = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

func podKey(ns, name string) model.ObjectKey {
	return model.ObjectKey{Resource: "v1/pods", Namespace: ns, Name: name}
}

func recEntry(k model.ObjectKey, typ model.ChangeType, at time.Time, body string) store.Entry {
	return store.Entry{Record: &model.Record{ObservedAt: at, Key: k, UID: "u-" + k.Name, ResourceVersion: "1", Type: typ, Origin: model.OriginWatch, Object: []byte(body)}}
}

func covEntry(res string, typ model.CoverageType, at time.Time) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: at, Resource: res, Type: typ, Reason: "test"}}
}

func mustWrite(t testing.TB, s store.Store, es ...store.Entry) {
	t.Helper()
	if err := s.Write(context.Background(), es); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// dump renders everything a view exposes, for before/after comparisons.
func dump(t testing.TB, s store.Store) string {
	t.Helper()
	ctx := context.Background()
	v, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	var b strings.Builder
	fmt.Fprintf(&b, "horizon %s\n", v.Horizon())
	res, err := v.Resources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		cs, err := v.Coverage(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cs {
			fmt.Fprintf(&b, "cov %d %s %s %s %s\n", c.Seq, c.Resource, c.Type, c.At.Format(time.RFC3339Nano), c.Reason)
		}
	}
	err = v.Changes(ctx, store.ChangesOptions{To: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)}, func(r model.Record) error {
		fmt.Fprintf(&b, "rec %d %s %s %s %s %s %s %q\n", r.Seq, r.ObservedAt.Format(time.RFC3339Nano), r.Key, r.UID, r.ResourceVersion, r.Type, r.Origin, r.Object)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestRestartDurability(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	mustWrite(t, s,
		covEntry("v1/pods", model.CoverageStart, dt0),
		recEntry(podKey("prod", "a"), model.ChangeAdded, dt0.Add(time.Second), "a1"),
		recEntry(podKey("prod", "b"), model.ChangeAdded, dt0.Add(2*time.Second), "b1"),
	)
	mustWrite(t, s, recEntry(podKey("prod", "b"), model.ChangeDeleted, dt0.Add(3*time.Second), "b1"))
	mustWrite(t, s, recEntry(podKey("prod", "a"), model.ChangeModified, dt0.Add(20*time.Second), "a2"))
	if err := s.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(ctx, dt0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Last record far in the future of the next caller time, to check that
	// clamping continues from the restored clock.
	last := recEntry(podKey("dev", "c"), model.ChangeAdded, dt0.Add(time.Hour), "c1")
	mustWrite(t, s, last, covEntry("apps/v1/deployments", model.CoverageStart, dt0))
	hb := dt0.Add(30 * time.Minute)
	if err := s.SetHeartbeat(ctx, hb); err != nil {
		t.Fatal(err)
	}
	before := dump(t, s)
	lastSeq := s.seq
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)
	defer func() { _ = s.Close() }()
	if after := dump(t, s); after != before {
		t.Fatalf("state changed across restart:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if got, err := s.Heartbeat(ctx); err != nil || !got.Equal(hb) {
		t.Errorf("heartbeat after restart = %s, %v; want %s", got, err, hb)
	}
	if s.lastManifest.IsZero() {
		t.Error("manifest time not restored")
	}
	// Seq and time continue: a record with an older caller time is clamped
	// past the last assigned time and gets the next Seq.
	e := recEntry(podKey("prod", "a"), model.ChangeModified, dt0, "a3")
	mustWrite(t, s, e)
	if e.Record.Seq != lastSeq+1 {
		t.Errorf("seq after restart = %d, want %d", e.Record.Seq, lastSeq+1)
	}
	// The Start for deployments at dt0 was clamped past "last".
	if want := last.Record.ObservedAt.Add(2 * time.Nanosecond); !e.Record.ObservedAt.Equal(want) {
		t.Errorf("time after restart = %s, want %s", e.Record.ObservedAt, want)
	}
	// Coverage bookkeeping survived: Start after Start is rejected, End
	// before the last record is rejected, a valid End is accepted.
	if err := s.Write(ctx, []store.Entry{covEntry("v1/pods", model.CoverageStart, dt0.Add(2*time.Hour))}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("Start after Start after restart: %v", err)
	}
	if err := s.Write(ctx, []store.Entry{covEntry("v1/pods", model.CoverageEnd, dt0.Add(time.Minute))}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("End before last record after restart: %v", err)
	}
	mustWrite(t, s, covEntry("v1/pods", model.CoverageEnd, dt0.Add(2*time.Hour)))
	// LatestRecordTime survived the restart and the compaction that
	// removed b's records: it is the newest pods record ever written.
	lv, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := lv.LatestRecordTime(ctx, "v1/pods"); err != nil || !got.Equal(e.Record.ObservedAt) {
		t.Errorf("LatestRecordTime after restart = %s, %v; want %s", got, err, e.Record.ObservedAt)
	}
	_ = lv.Close()
	// The horizon survived and an equal Compact is a no-op.
	v, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	if !v.Horizon().Equal(dt0.Add(10 * time.Second)) {
		t.Errorf("horizon after restart = %s", v.Horizon())
	}
}

func TestOpenRejectsUnknownFormat(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	if err := s.db.Set(keyFormat, seal(keyFormat, []byte{FormatVersion + 1}), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
	if err == nil || !strings.Contains(err.Error(), "unsupported store format version 2") {
		t.Fatalf("Open with format 2: err=%v", err)
	}
}

func TestOpenRejectsDataWithoutFormat(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	mustWrite(t, s, recEntry(podKey("ns", "a"), model.ChangeAdded, dt0, "x"))
	if err := s.db.Delete(keyFormat, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1}); err == nil || !strings.Contains(err.Error(), "no format version") {
		t.Fatalf("Open without format key: err=%v", err)
	}
}

// Crash durability: a child process (this test binary re-executed) writes
// batches and acknowledges each one on stdout only after Write returned.
// The parent SIGKILLs it mid-stream, reopens the store and checks that
// every acknowledged batch is complete and no batch is partially visible.

const (
	crashEnvDir   = "CHRONOS_CRASH_CHILD_DIR"
	crashEnvStart = "CHRONOS_CRASH_CHILD_START"
	crashBatch    = 7 // records per batch
)

func crashKey(batch, j int) model.ObjectKey {
	return model.ObjectKey{Resource: "v1/pods", Namespace: fmt.Sprintf("ns%d", j%3), Name: fmt.Sprintf("b%06d-%d", batch, j)}
}

func TestCrashChild(t *testing.T) {
	dir := os.Getenv(crashEnvDir)
	if dir == "" {
		t.Skip("helper process for TestCrashDurability")
	}
	start, _ := strconv.Atoi(os.Getenv(crashEnvStart))
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: 5 * time.Millisecond})
	if err != nil {
		fmt.Println("open error", err)
		os.Exit(2)
	}
	ctx := context.Background()
	body := strings.Repeat("x", 2000)
	for i := start; ; i++ {
		es := make([]store.Entry, crashBatch)
		for j := range es {
			es[j] = recEntry(crashKey(i, j), model.ChangeAdded, time.Now(), fmt.Sprintf(`{"batch":%d,"j":%d,"pad":%q}`, i, j, body))
		}
		if err := s.Write(ctx, es); err != nil {
			fmt.Println("write error", err)
			os.Exit(2)
		}
		hb := es[crashBatch-1].Record.ObservedAt
		if err := s.SetHeartbeat(ctx, hb); err != nil {
			fmt.Println("heartbeat error", err)
			os.Exit(2)
		}
		if i%5 == 0 {
			if err := s.Compact(ctx, es[0].Record.ObservedAt.Add(-time.Hour)); err != nil {
				fmt.Println("compact error", err)
				os.Exit(2)
			}
		}
		// os.Stdout is unbuffered: the line is in the pipe before the next
		// batch starts.
		fmt.Printf("ack %d %d\n", i, hb.UnixNano())
	}
}

func TestCrashDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	r := newRand(42)
	next := 0
	for round := range 4 {
		killAfter := 10 + r.IntN(60)
		cmd := exec.Command(exe, "-test.run=^TestCrashChild$", "-test.v=false") //nolint:gosec // re-executes this test binary
		cmd.Env = append(os.Environ(), crashEnvDir+"="+dir, crashEnvStart+"="+strconv.Itoa(next))
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lastAck, acks := -1, 0
		var lastHB int64
		killed := false
		sc := bufio.NewScanner(out)
		// Read every ack until the pipe closes: the child keeps committing
		// and acknowledging while we sleep before the kill, and those acks
		// are already in the pipe. Stopping at the kill would make durable,
		// acknowledged batches look like they were never acknowledged.
		for sc.Scan() {
			var i int
			var hb int64
			if _, err := fmt.Sscanf(sc.Text(), "ack %d %d", &i, &hb); err != nil {
				t.Logf("child: %s", sc.Text())
				continue
			}
			lastAck, lastHB = i, hb
			if acks++; acks == killAfter && !killed {
				// Let the child get into the middle of its next commit.
				time.Sleep(time.Duration(r.IntN(3000)) * time.Microsecond)
				if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				killed = true
			}
		}
		_ = cmd.Wait()
		if !killed {
			t.Fatalf("round %d: child exited before being killed (%d acks)", round, acks)
		}
		if lastAck < next {
			t.Fatalf("round %d: child acknowledged nothing", round)
		}

		s := openTest(t, dir)
		v, err := s.View(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Every batch is all-or-nothing; acknowledged ones are all there.
		seen := map[int]int{}
		var prevSeq uint64
		err = v.Changes(context.Background(), store.ChangesOptions{To: time.Now().Add(time.Hour)}, func(rec model.Record) error {
			var b, j int
			if _, err := fmt.Sscanf(rec.Key.Name, "b%06d-%d", &b, &j); err != nil {
				return err
			}
			if rec.Seq <= prevSeq {
				return fmt.Errorf("seq %d after %d", rec.Seq, prevSeq)
			}
			prevSeq = rec.Seq
			seen[b]++
			return nil
		})
		if err != nil {
			t.Fatalf("round %d: Changes: %v", round, err)
		}
		maxBatch := -1
		for b, n := range seen {
			if n != crashBatch {
				t.Errorf("round %d: batch %d partially visible: %d of %d records", round, b, n, crashBatch)
			}
			maxBatch = max(maxBatch, b)
		}
		for b := 0; b <= lastAck; b++ {
			if seen[b] != crashBatch {
				t.Errorf("round %d: acknowledged batch %d has %d of %d records", round, b, seen[b], crashBatch)
			}
		}
		if maxBatch > lastAck+1 {
			t.Errorf("round %d: batch %d visible but child only reached %d", round, maxBatch, lastAck+1)
		}
		hb, err := s.Heartbeat(context.Background())
		if err != nil || hb.UnixNano() < lastHB {
			t.Errorf("round %d: heartbeat %d < acknowledged %d (%v)", round, hb.UnixNano(), lastHB, err)
		}
		if s.seq != prevSeq {
			t.Errorf("round %d: restored seq %d, last record seq %d", round, s.seq, prevSeq)
		}
		t.Logf("round %d: killed after %d acks (last acked batch %d, highest visible %d, unacked-but-durable %v)",
			round, acks, lastAck, maxBatch, maxBatch > lastAck)
		_ = v.Close()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		next = maxBatch + 1
	}
}
