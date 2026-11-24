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

// hangFS blocks WAL syncs while armed, until release is closed: a disk
// that stops answering.
type hangFS struct {
	vfs.FS
	armed   *atomic.Bool
	entered chan struct{} // receives once per blocked sync
	release chan struct{}
}

type hangFile struct {
	vfs.File
	fs *hangFS
}

func (f *hangFS) wrap(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return file, err
	}
	return hangFile{File: file, fs: f}, nil
}

func (f *hangFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, c)
	return f.wrap(name, file, err)
}

func (f *hangFS) ReuseForWrite(oldname, newname string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, c)
	return f.wrap(newname, file, err)
}

func (f hangFile) block() {
	if f.fs.armed.Load() {
		select {
		case f.fs.entered <- struct{}{}:
		default:
		}
		<-f.fs.release
	}
}

func (f hangFile) Sync() error                  { f.block(); return f.File.Sync() }
func (f hangFile) SyncData() error              { f.block(); return f.File.SyncData() }
func (f hangFile) SyncTo(n int64) (bool, error) { f.block(); return f.File.SyncTo(n) }

func TestCloseReleasesStuckWrite(t *testing.T) {
	ctx := context.Background()
	var armed atomic.Bool
	hfs := &hangFS{FS: vfs.Default, armed: &armed, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, err := Open(t.TempDir(), Options{Logger: testLogger(), SnapshotInterval: -1, CloseTimeout: 300 * time.Millisecond, fs: hfs})
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, s, recEntry(podKey("ns", "before"), model.ChangeAdded, dt0, "x"))

	armed.Store(true)
	stuck := make(chan error, 1)
	go func() {
		stuck <- s.Write(ctx, []store.Entry{recEntry(podKey("ns", "stuck"), model.ChangeAdded, dt0.Add(time.Second), "x")})
	}()
	select {
	case <-hfs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never reached the WAL sync")
	}
	// A second writer queues behind the stuck one for the writer slot.
	queued := make(chan error, 1)
	go func() {
		queued <- s.Write(ctx, []store.Entry{recEntry(podKey("ns", "queued"), model.ChangeAdded, dt0.Add(2*time.Second), "x")})
	}()
	time.Sleep(50 * time.Millisecond)

	closed := make(chan error, 1)
	start := time.Now()
	go func() { closed <- s.Close() }()
	for name, ch := range map[string]chan error{"stuck": stuck, "queued": queued} {
		select {
		case err := <-ch:
			if !errors.Is(err, store.ErrClosed) {
				t.Errorf("%s Write after Close: err=%v, want ErrClosed", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Write still blocked after Close", name)
		}
	}
	select {
	case err := <-closed:
		if err == nil || !strings.Contains(err.Error(), "did not finish") {
			t.Errorf("Close with a hung commit: err=%v, want timeout error", err)
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Errorf("Close took %s, want about the 300ms timeout", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked on the stuck writer")
	}
	if err := s.Write(ctx, []store.Entry{recEntry(podKey("ns", "later"), model.ChangeAdded, dt0.Add(3*time.Second), "x")}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("Write after Close: %v", err)
	}
	// Unblock the disk so the test can clean up; Pebble was left open.
	armed.Store(false)
	close(hfs.release)
	s.ops.Wait()
	_ = s.db.Close()
}

func TestCloseWaitsForCommitThatFinishes(t *testing.T) {
	// With a slow (not hung) disk, Close waits for the commit and then
	// closes cleanly; the Write itself returns ErrClosed (outcome unknown
	// to it) but the batch is durable after reopen.
	ctx := context.Background()
	dir := t.TempDir()
	var armed atomic.Bool
	hfs := &hangFS{FS: vfs.Default, armed: &armed, entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, CloseTimeout: 5 * time.Second, fs: hfs})
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	res := make(chan error, 1)
	go func() {
		res <- s.Write(ctx, []store.Entry{recEntry(podKey("ns", "slow"), model.ChangeAdded, dt0, "x")})
	}()
	<-hfs.entered
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	if err := <-res; !errors.Is(err, store.ErrClosed) {
		t.Errorf("Write: err=%v, want ErrClosed", err)
	}
	time.Sleep(100 * time.Millisecond)
	armed.Store(false)
	close(hfs.release)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	s = openTest(t, dir)
	defer func() { _ = s.Close() }()
	v, err := s.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	_, found, err := v.Get(ctx, podKey("ns", "slow"), dt0.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("batch whose Write returned ErrClosed is durable: %v", found)
}
