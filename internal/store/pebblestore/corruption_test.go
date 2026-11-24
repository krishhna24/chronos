package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// rawFlip flips one byte of the stored value of key (a test hook writing
// the raw key directly, bypassing the store).
func rawFlip(t *testing.T, s *Store, key []byte, offset int) {
	t.Helper()
	val, closer, err := s.db.Get(key)
	if err != nil {
		t.Fatalf("raw get %s: %v", describeKey(key), err)
	}
	bad := slices.Clone(val)
	_ = closer.Close()
	if offset < 0 {
		offset += len(bad)
	}
	bad[offset] ^= 0x01
	if err := s.db.Set(key, bad, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptVersionIsReported(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	defer func() { _ = s.Close() }()
	a := podKey("prod", "a")
	e1 := recEntry(a, model.ChangeAdded, dt0, `{"v":1}`)
	e2 := recEntry(a, model.ChangeModified, dt0.Add(time.Second), `{"v":2}`)
	mustWrite(t, s, e1, e2, recEntry(podKey("prod", "b"), model.ChangeAdded, dt0.Add(2*time.Second), "b"))

	for _, off := range []int{-1, 0, 2, valueHeader + 1} { // payload, codec, crc, seq
		t.Run(fmt.Sprintf("offset=%d", off), func(t *testing.T) {
			vk := versionKey(a, e2.Record.ObservedAt)
			orig, closer, err := s.db.Get(vk)
			if err != nil {
				t.Fatal(err)
			}
			orig = slices.Clone(orig)
			_ = closer.Close()
			rawFlip(t, s, vk, off)
			defer func() {
				if err := s.db.Set(vk, orig, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}()

			v, err := s.View(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = v.Close() }()
			checkCorrupt := func(op string, err error) {
				t.Helper()
				if !errors.Is(err, store.ErrCorrupt) {
					t.Errorf("%s: err=%v, want ErrCorrupt", op, err)
					return
				}
				if !strings.Contains(err.Error(), a.String()) {
					t.Errorf("%s: error does not name the key: %v", op, err)
				}
			}
			r, ok, err := v.Get(ctx, a, dt0.Add(time.Hour))
			checkCorrupt("Get", err)
			if ok || r.Object != nil {
				t.Errorf("Get returned data with the error: ok=%v %q", ok, r.Object)
			}
			checkCorrupt("List", v.List(ctx, store.ListOptions{Resource: "v1/pods", At: dt0.Add(time.Hour)}, func(model.Record) error { return nil }))
			checkCorrupt("History", v.History(ctx, a, time.Time{}, dt0.Add(time.Hour), func(model.Record) error { return nil }))
			checkCorrupt("Changes", v.Changes(ctx, store.ChangesOptions{To: dt0.Add(time.Hour)}, func(model.Record) error { return nil }))
			// Unaffected versions still read fine.
			if r, ok, err := v.Get(ctx, a, dt0); err != nil || !ok || string(r.Object) != `{"v":1}` {
				t.Errorf("older version: %q ok=%v err=%v", r.Object, ok, err)
			}
		})
	}
}

func TestCorruptMetaFailsOpen(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	mustWrite(t, s, recEntry(podKey("ns", "a"), model.ChangeAdded, dt0, "x"))
	rawFlip(t, s, keyState, -1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
	if !errors.Is(err, store.ErrCorrupt) || !strings.Contains(err.Error(), "meta[state]") {
		t.Fatalf("Open with corrupt state: %v", err)
	}
}

// listAll returns List results rendered for comparison.
func listAll(t *testing.T, s *Store, opts store.ListOptions) string {
	t.Helper()
	v, err := s.View(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	var b bytes.Buffer
	if err := v.List(context.Background(), opts, func(r model.Record) error {
		fmt.Fprintf(&b, "%d %s %s %s\n", r.Seq, r.Key, r.Type, r.Object)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	return b.String()
}

func TestCorruptManifestFallsBackToScan(t *testing.T) {
	ctx := context.Background()
	type damage struct {
		name string
		do   func(t *testing.T, s *Store, at time.Time)
	}
	damages := []damage{
		{"header-byte", func(t *testing.T, s *Store, at time.Time) { rawFlip(t, s, manifestKey(at), -1) }},
		{"chunk-byte", func(t *testing.T, s *Store, at time.Time) { rawFlip(t, s, chunkKey(at, "v1/pods", "prod"), -3) }},
		{"chunk-missing", func(t *testing.T, s *Store, at time.Time) {
			if err := s.db.Delete(chunkKey(at, "v1/pods", "prod"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		}},
		{"chunk-truncated", func(t *testing.T, s *Store, at time.Time) {
			k := chunkKey(at, "v1/pods", "prod")
			val, closer, err := s.db.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			short := slices.Clone(val[:len(val)/2])
			_ = closer.Close()
			if err := s.db.Set(k, short, pebble.Sync); err != nil {
				t.Fatal(err)
			}
		}},
		{"chunk-resealed-with-wrong-count", func(t *testing.T, s *Store, at time.Time) {
			// A well-formed chunk that disagrees with the header.
			k := chunkKey(at, "v1/pods", "prod")
			payload := encodeChunk([]manifestEntry{{name: "a", at: at}})
			if err := s.db.Set(k, seal(k, payload), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, d := range damages {
		t.Run(d.name, func(t *testing.T) {
			s := openTest(t, t.TempDir())
			defer func() { _ = s.Close() }()
			for i := range 20 {
				ns := []string{"prod", "prod-2", "dev"}[i%3]
				mustWrite(t, s, recEntry(podKey(ns, fmt.Sprintf("p%02d", i)), model.ChangeAdded, dt0.Add(time.Duration(i)*time.Second), fmt.Sprint(i)))
			}
			mustWrite(t, s, recEntry(podKey("prod", "p00"), model.ChangeDeleted, dt0.Add(time.Minute), "0"))
			if err := s.Snapshot(ctx); err != nil {
				t.Fatal(err)
			}
			at := s.lastManifest
			mustWrite(t, s, recEntry(podKey("prod", "p03"), model.ChangeModified, dt0.Add(2*time.Minute), "3b"))

			queries := []store.ListOptions{
				{Resource: "v1/pods", At: dt0.Add(time.Hour)},
				{Resource: "v1/pods", Namespace: "prod", At: dt0.Add(time.Hour)},
				{Resource: "v1/pods", Namespace: "prod", At: at},
			}
			var want []string
			for _, q := range queries {
				want = append(want, listAll(t, s, q))
			}
			if s.stats.manifest.Load() != int64(len(queries)) {
				t.Fatalf("manifest path not used before damage: %d", s.stats.manifest.Load())
			}
			d.do(t, s, at)
			for i, q := range queries {
				if got := listAll(t, s, q); got != want[i] {
					t.Errorf("List(%+v) after damage:\n%s\nwant:\n%s", q, got, want[i])
				}
			}
			if s.stats.fallbacks.Load() == 0 {
				t.Error("damage not detected (no fallback recorded)")
			}
		})
	}
}

func TestContextCancellation(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer func() { _ = s.Close() }()
	var es []store.Entry
	for i := range 2000 {
		es = append(es, recEntry(podKey("ns", fmt.Sprintf("p%05d", i)), model.ChangeAdded, dt0.Add(time.Duration(i)*time.Millisecond), "x"))
	}
	mustWrite(t, s, es...)
	v, err := s.View(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()

	for name, run := range map[string]func(ctx context.Context, fn func(model.Record) error) error{
		"List": func(ctx context.Context, fn func(model.Record) error) error {
			return v.List(ctx, store.ListOptions{Resource: "v1/pods", At: dt0.Add(time.Hour)}, fn)
		},
		"Changes": func(ctx context.Context, fn func(model.Record) error) error {
			return v.Changes(ctx, store.ChangesOptions{To: dt0.Add(time.Hour)}, fn)
		},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		n := 0
		err := run(ctx, func(model.Record) error {
			if n++; n == 10 {
				cancel()
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err=%v, want context.Canceled", name, err)
		}
		if n >= 2000 {
			t.Errorf("%s: iteration ran to completion (%d) despite cancellation", name, n)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Compact(ctx, dt0.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Errorf("Compact with canceled ctx: %v", err)
	}
	if err := s.Write(ctx, es[:1]); !errors.Is(err, context.Canceled) {
		t.Errorf("Write with canceled ctx: %v", err)
	}
}

func TestClosedStoreAndViews(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	mustWrite(t, s, recEntry(podKey("ns", "a"), model.ChangeAdded, dt0, "x"))
	v, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, _, err := v.Get(ctx, podKey("ns", "a"), dt0); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Get on view of closed store: %v", err)
	}
	if err := v.Close(); err != nil {
		t.Errorf("view Close after store Close: %v", err)
	}
	for name, err := range map[string]error{
		"Compact":      s.Compact(ctx, dt0),
		"Snapshot":     s.Snapshot(ctx),
		"SetHeartbeat": s.SetHeartbeat(ctx, dt0),
	} {
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("%s after Close: %v", name, err)
		}
	}
}
