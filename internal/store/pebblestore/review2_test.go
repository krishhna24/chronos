package pebblestore

// Regression tests for the round-2 review findings (R2-n).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// R2-1: a corrupted SSTable block must make reads return ErrCorrupt
// naming the file, not panic, and the store must still close cleanly.
func TestReview2SSTCorruptionReturnsErrCorrupt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openTest(t, dir)
	r := newRand(1)
	var batch []store.Entry
	for i := range 3000 {
		body := make([]byte, 2048)
		for j := range body {
			body[j] = byte('a' + r.IntN(26))
		}
		batch = append(batch, recEntry(podKey("ns", fmt.Sprintf("p%05d", i)), model.ChangeAdded, dt0.Add(time.Duration(i)*time.Second), string(body)))
		if len(batch) == 100 {
			mustWrite(t, s, batch...)
			batch = nil
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(ssts) == 0 {
		t.Fatal("no sst")
	}
	f := ssts[0]
	b, err := os.ReadFile(f) //nolint:gosec // test-owned temp dir
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)*6/10] ^= 0xFF
	if err := os.WriteFile(f, b, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v, err := s2.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check := func(op string, err error) {
		t.Helper()
		if !errors.Is(err, store.ErrCorrupt) {
			t.Errorf("%s: err=%v, want ErrCorrupt", op, err)
		} else if num := strings.TrimSuffix(filepath.Base(f), ".sst"); !strings.Contains(err.Error(), "file "+num) {
			t.Errorf("%s: error does not name table %s: %v", op, num, err)
		}
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("read panicked: %v", p)
			}
		}()
		all := dt0.Add(24 * time.Hour)
		check("List", v.List(ctx, store.ListOptions{Resource: "v1/pods", At: all}, func(model.Record) error { return nil }))
		check("Changes", v.Changes(ctx, store.ChangesOptions{To: all}, func(model.Record) error { return nil }))
	}()
	if err := v.Close(); err != nil {
		t.Errorf("view Close: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Errorf("Store.Close after corrupt reads: %v", err)
	}
}

// r2HangFS blocks WAL syncs while armed.
type r2HangFS struct{ syncFailFS }

type r2HangFile struct{ syncFailFile }

func (f r2HangFile) Sync() error {
	for f.armed.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	return f.File.Sync()
}

func (f r2HangFile) SyncData() error { return f.Sync() }

func (f r2HangFile) SyncTo(int64) (bool, error) {
	if err := f.Sync(); err != nil {
		return false, err
	}
	return true, nil
}

func (f r2HangFS) wrapH(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return file, err
	}
	return r2HangFile{syncFailFile{File: file, armed: f.armed}}, nil
}

func (f r2HangFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, c)
	return f.wrapH(name, file, err)
}

func (f r2HangFS) ReuseForWrite(o, n string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(o, n, c)
	return f.wrapH(n, file, err)
}

// R2-2: Compact (and Write) must return when their context ends while the
// commit is stuck in Pebble; the outcome is unknown, so the store poisons
// itself and the error wraps ErrClosed.
func TestReview2CompactHonoursCtxDuringHungCommit(t *testing.T) {
	var armed atomic.Bool
	s, err := Open(t.TempDir(), Options{Logger: testLogger(), SnapshotInterval: -1, CloseTimeout: time.Second,
		fs: r2HangFS{syncFailFS{FS: vfs.Default, armed: &armed}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		mustWrite(t, s, recEntry(podKey("ns", "a"), model.ChangeModified, dt0.Add(time.Duration(i)*time.Second), "x"))
	}
	armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Compact(ctx, dt0.Add(time.Hour)) }()
	select {
	case err := <-done:
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("Compact after ctx expiry: err=%v, want ErrClosed (outcome unknown)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Compact still blocked 2.7s after its ctx expired")
	}
	if err := s.Write(context.Background(), []store.Entry{recEntry(podKey("ns", "b"), model.ChangeAdded, dt0.Add(time.Hour), "y")}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Write after an abandoned commit: err=%v, want ErrClosed (poisoned)", err)
	}
	armed.Store(false)
	_ = s.Close()
}

func TestReview2WriteHonoursCtxDuringHungCommit(t *testing.T) {
	var armed atomic.Bool
	s, err := Open(t.TempDir(), Options{Logger: testLogger(), SnapshotInterval: -1, CloseTimeout: time.Second,
		fs: r2HangFS{syncFailFS{FS: vfs.Default, armed: &armed}}})
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Write(ctx, []store.Entry{recEntry(podKey("ns", "a"), model.ChangeAdded, dt0, "x")}) }()
	returned := false
	select {
	case err := <-done:
		returned = true
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("Write with hung fsync and expired ctx: err=%v, want ErrClosed (outcome unknown)", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Write still blocked 2.8s after its ctx expired")
	}
	armed.Store(false)
	_ = s.Close()
	if !returned {
		if err := <-done; err != nil {
			t.Logf("Write released by Close: %v", err)
		}
	}
}

// R2-5: a second Close after a timed-out Close must report the same
// failure, not nil, because Pebble is still open.
func TestReview2CloseAfterTimedOutCloseRepeatsError(t *testing.T) {
	ctx := context.Background()
	var armed atomic.Bool
	hfs := &hangFS{FS: vfs.Default, armed: &armed, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, err := Open(t.TempDir(), Options{Logger: testLogger(), SnapshotInterval: -1, CloseTimeout: 100 * time.Millisecond, fs: hfs})
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	go func() { _ = s.Write(ctx, []store.Entry{recEntry(podKey("ns", "a"), model.ChangeAdded, dt0, "x")}) }()
	<-hfs.entered
	first := s.Close()
	second := s.Close()
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Errorf("Close results: first=%v second=%v, want the same timeout error twice", first, second)
	}
	armed.Store(false)
	close(hfs.release)
	s.ops.Wait()
	_ = s.db.Close()
}

// Adversarial keys (bytes 0x01 and 0xFF, very long names, resources
// that are prefixes of each other) through both List paths and After.
func TestReview2AdversarialKeysDifferential(t *testing.T) {
	ctx := context.Background()
	names := []string{"a", "a\x01", "a\xff", "a\xff\xff", "\x01", "\xff", "ab", "a-b", strings.Repeat("z", 5000), strings.Repeat("\xff", 300)}
	nss := []string{"", "\x01", "a", "a\x01", "\xff", "prod", "prod-2"}
	ress := []string{"v1/pods", "v1/pods\x01", "v1/pods\xff", "\xff"}
	ps := openTest(t, t.TempDir())
	defer func() { _ = ps.Close() }()
	ms := memstore.New()
	i := 0
	total := len(names) * len(nss) * len(ress)
	for _, res := range ress {
		for _, ns := range nss {
			for _, n := range names {
				typ := model.ChangeAdded
				if i%5 == 0 {
					typ = model.ChangeDeleted
				}
				e1 := store.Entry{Record: &model.Record{ObservedAt: dt0.Add(time.Duration(i) * time.Millisecond), Key: model.ObjectKey{Resource: res, Namespace: ns, Name: n}, Type: typ, Origin: model.OriginWatch, Object: []byte(fmt.Sprint(i))}}
				r2 := *e1.Record
				e2 := store.Entry{Record: &r2}
				if err := ps.Write(ctx, []store.Entry{e1}); err != nil {
					t.Fatal(err)
				}
				if err := ms.Write(ctx, []store.Entry{e2}); err != nil {
					t.Fatal(err)
				}
				i++
				if i == total/2 {
					if err := ps.Snapshot(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	list := func(s store.Store, o store.ListOptions) []string {
		v, err := s.View(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = v.Close() }()
		var out []string
		if err := v.List(ctx, o, func(r model.Record) error {
			out = append(out, fmt.Sprintf("%q/%q/%q=%s", r.Key.Resource, r.Key.Namespace, r.Key.Name, r.Object))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	afters := []*model.ObjectKey{nil}
	for _, n2 := range nss {
		for _, nm := range names {
			afters = append(afters, &model.ObjectKey{Namespace: n2, Name: nm})
		}
	}
	checks := 0
	for _, res := range ress {
		for _, ns := range append([]string{""}, nss...) {
			for _, after := range afters {
				for _, when := range []time.Time{dt0.Add(time.Hour), dt0.Add(time.Duration(total/3) * time.Millisecond)} {
					o := store.ListOptions{Resource: res, Namespace: ns, At: when, After: after}
					a, b := list(ps, o), list(ms, o)
					checks++
					if fmt.Sprint(a) != fmt.Sprint(b) {
						t.Fatalf("divergence for %+v after=%v:\npebble %v\nmem    %v", o, after, a, b)
					}
				}
			}
		}
	}
	if ps.stats.manifest.Load() == 0 {
		t.Error("manifest path not exercised")
	}
	t.Logf("%d List comparisons equal; manifest=%d scan=%d", checks, ps.stats.manifest.Load(), ps.stats.scan.Load())
}
