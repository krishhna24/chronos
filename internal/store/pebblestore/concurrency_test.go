package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// TestConcurrentWriteViewCompact runs writers, readers, compaction and the
// background snapshot job at once (meant for -race). Readers check
// snapshot isolation: repeating a read on one view while the store
// changes underneath must return the same answer, and Seq order must hold.
func TestConcurrentWriteViewCompact(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir(), Options{Logger: testLogger(), SnapshotInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		lastTime atomic.Int64 // UnixNano of the latest assigned record time
		writes   atomic.Int64
		reads    atomic.Int64
		compacts atomic.Int64
	)
	errc := make(chan error, 16)
	fail := func(err error) {
		select {
		case errc <- err:
		default:
		}
		stop.Store(true)
	}

	for w := range 2 {
		wg.Go(func() {
			r := newRand(uint64(100 + w))
			for i := 0; !stop.Load(); i++ {
				var es []store.Entry
				for j := range 1 + r.IntN(8) {
					k := podKey(fmt.Sprintf("ns%d", r.IntN(4)), fmt.Sprintf("p%d", r.IntN(50)))
					typ := model.ChangeType(1 + r.IntN(3))
					es = append(es, recEntry(k, typ, time.Now(), fmt.Sprintf("w%d-%d-%d", w, i, j)))
				}
				if err := s.Write(ctx, es); err != nil {
					fail(fmt.Errorf("Write: %w", err))
					return
				}
				lastTime.Store(es[len(es)-1].Record.ObservedAt.UnixNano())
				writes.Add(1)
			}
		})
	}
	wg.Go(func() {
		for !stop.Load() {
			lt := lastTime.Load()
			if lt == 0 {
				continue
			}
			// Compact up to slightly behind the writers.
			if err := s.Compact(ctx, time.Unix(0, lt).Add(-5*time.Millisecond)); err != nil {
				fail(fmt.Errorf("Compact: %w", err))
				return
			}
			compacts.Add(1)
			time.Sleep(3 * time.Millisecond)
		}
	})
	for rd := range 3 {
		wg.Go(func() {
			r := newRand(uint64(200 + rd))
			for !stop.Load() {
				if err := readerPass(ctx, s, r.IntN(4)); err != nil {
					fail(err)
					return
				}
				reads.Add(1)
			}
		})
	}

	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
	t.Logf("writes %d, reader passes %d, compactions %d, manifest lists %d, scan lists %d",
		writes.Load(), reads.Load(), compacts.Load(), s.stats.manifest.Load(), s.stats.scan.Load())
	if writes.Load() == 0 || reads.Load() == 0 || compacts.Load() == 0 {
		t.Fatal("a worker made no progress")
	}
}

func render(rs []model.Record) string {
	out := ""
	for _, r := range rs {
		out += fmt.Sprintf("%d %s %s %s|", r.Seq, r.Key, r.Type, r.Object)
	}
	return out
}

func readerPass(ctx context.Context, s *Store, nsIdx int) error {
	v, err := s.View(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = v.Close() }()
	at := time.Now()
	opts := store.ListOptions{Resource: "v1/pods", Namespace: fmt.Sprintf("ns%d", nsIdx), At: at}
	if nsIdx == 0 {
		opts.Namespace = ""
	}
	first, err := collect(func(fn func(model.Record) error) error { return v.List(ctx, opts, fn) })
	if err != nil {
		return fmt.Errorf("List: %w", err)
	}
	var changes []model.Record
	var prev uint64
	if err := v.Changes(ctx, store.ChangesOptions{From: at.Add(-time.Second), To: at}, func(r model.Record) error {
		if r.Seq <= prev {
			return errors.New("Changes out of Seq order")
		}
		prev = r.Seq
		changes = append(changes, r)
		return nil
	}); err != nil {
		return fmt.Errorf("Changes: %w", err)
	}
	for _, r := range first {
		got, ok, err := v.Get(ctx, r.Key, at)
		if err != nil || !ok || got.Seq != r.Seq {
			return fmt.Errorf("Get(%s) = seq %d ok=%v, List said seq %d: %w", r.Key, got.Seq, ok, r.Seq, err)
		}
	}
	time.Sleep(time.Millisecond) // let writers and compaction move on
	again, err := collect(func(fn func(model.Record) error) error { return v.List(ctx, opts, fn) })
	if err != nil {
		return fmt.Errorf("List again: %w", err)
	}
	if render(first) != render(again) {
		return fmt.Errorf("view changed under concurrent writes/compaction:\n%s\n%s", render(first), render(again))
	}
	// The manifest and scan paths agree on the same snapshot.
	scan, err := collect(func(fn func(model.Record) error) error { return v.(*view).listScan(ctx, opts, fn) })
	if err != nil {
		return fmt.Errorf("listScan: %w", err)
	}
	if render(scan) != render(first) {
		return fmt.Errorf("List and scan path disagree:\n%s\n%s", render(first), render(scan))
	}
	return nil
}
