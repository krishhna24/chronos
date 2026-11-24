package pebblestore

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// syncFailFS fails Sync/SyncData/SyncTo on WAL files while armed,
// simulating an fsync error after Pebble has applied a batch to its
// memtable and written it to the WAL: the outcome is ambiguous.
type syncFailFS struct {
	vfs.FS
	armed *atomic.Bool
}

type syncFailFile struct {
	vfs.File
	armed *atomic.Bool
}

var errInjectedSync = errors.New("injected WAL fsync failure")

func (f syncFailFS) wrap(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return file, err
	}
	return syncFailFile{File: file, armed: f.armed}, nil
}

func (f syncFailFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, c)
	return f.wrap(name, file, err)
}

func (f syncFailFS) ReuseForWrite(oldname, newname string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, c)
	return f.wrap(newname, file, err)
}

func (f syncFailFile) Sync() error {
	if f.armed.Load() {
		return errInjectedSync
	}
	return f.File.Sync()
}

func (f syncFailFile) SyncData() error {
	if f.armed.Load() {
		return errInjectedSync
	}
	return f.File.SyncData()
}

func (f syncFailFile) SyncTo(n int64) (bool, error) {
	if f.armed.Load() {
		return false, errInjectedSync
	}
	return f.File.SyncTo(n)
}

func TestAmbiguousCommitFailurePoisonsStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var armed atomic.Bool
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, fs: syncFailFS{FS: vfs.Default, armed: &armed}})
	if err != nil {
		t.Fatal(err)
	}
	ok := recEntry(podKey("ns", "ok"), model.ChangeAdded, dt0, "ok")
	mustWrite(t, s, ok)

	armed.Store(true)
	err = s.Write(ctx, []store.Entry{recEntry(podKey("ns", "ambiguous"), model.ChangeAdded, dt0.Add(time.Second), "?")})
	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Write during fsync failure: err=%v, want ErrClosed", err)
	}
	if !strings.Contains(err.Error(), "poisoned") || !strings.Contains(err.Error(), errInjectedSync.Error()) {
		t.Errorf("error does not explain the poisoning: %v", err)
	}
	armed.Store(false)

	// Even with the disk healthy again, the store refuses every mutation,
	// so the caller's retry cannot apply the batch a second time.
	for name, err := range map[string]error{
		"Write":        s.Write(ctx, []store.Entry{recEntry(podKey("ns", "retry"), model.ChangeAdded, dt0.Add(2*time.Second), "r")}),
		"SetHeartbeat": s.SetHeartbeat(ctx, dt0.Add(time.Hour)),
		"Compact":      s.Compact(ctx, dt0.Add(time.Hour)),
		"Snapshot":     s.Snapshot(ctx),
	} {
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("%s after poisoning: err=%v, want ErrClosed", name, err)
		}
	}
	// Reads keep working on the poisoned store.
	if pv, err := s.View(ctx); err != nil {
		t.Errorf("View after poisoning: %v", err)
	} else {
		if _, found, err := pv.Get(ctx, podKey("ns", "ok"), dt0.Add(time.Hour)); err != nil || !found {
			t.Errorf("Get after poisoning: found=%v err=%v", found, err)
		}
		_ = pv.Close()
	}
	t.Logf("Close of poisoned store: %v", s.Close())

	// After reopening, the acknowledged batch is there, the retry is not,
	// and the ambiguous batch is either fully present or absent.
	s = openTest(t, dir)
	defer func() { _ = s.Close() }()
	v, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	if _, found, err := v.Get(ctx, podKey("ns", "ok"), dt0.Add(time.Hour)); err != nil || !found {
		t.Errorf("acknowledged record lost: found=%v err=%v", found, err)
	}
	if _, found, err := v.Get(ctx, podKey("ns", "retry"), dt0.Add(time.Hour)); err != nil || found {
		t.Errorf("write after poisoning was applied: found=%v err=%v", found, err)
	}
	_, amb, err := v.Get(ctx, podKey("ns", "ambiguous"), dt0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ambiguous batch durable after reopen: %v", amb)
	mustWrite(t, s, recEntry(podKey("ns", "after"), model.ChangeAdded, dt0.Add(3*time.Second), "a"))
}
