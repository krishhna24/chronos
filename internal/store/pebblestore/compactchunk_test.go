package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

var errSimulatedCrash = errors.New("simulated crash between compaction chunks")

// TestCompactCrashBetweenChunks stops a multi-chunk compaction after its
// second chunk (as a crash would: every chunk is a synced commit), reopens
// the store and checks that every answer at t >= horizon is already the
// final one, then lets the next Compact finish the run and compares the
// whole store with memstore.
func TestCompactCrashBetweenChunks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stop := false
	open := func() *Store {
		s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, compactChunkDeletes: 10,
			compactChunkHook: func(chunk int) error {
				if stop && chunk == 2 {
					return errSimulatedCrash
				}
				return nil
			}})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ps, ms := open(), memstore.New()
	both := func(es ...store.Entry) {
		t.Helper()
		if err := ms.Write(ctx, cloneEntries(es)); err != nil {
			t.Fatal(err)
		}
		if err := ps.Write(ctx, es); err != nil {
			t.Fatal(err)
		}
	}
	both(covEntry("v1/pods", model.CoverageStart, dt0))
	r := newRand(3)
	at := dt0
	for round := range 6 {
		for k := range 40 {
			at = at.Add(time.Second)
			typ := model.ChangeModified
			switch {
			case round == 0:
				typ = model.ChangeAdded
			case r.IntN(4) == 0:
				typ = model.ChangeDeleted
			}
			both(recEntry(podKey(fmt.Sprintf("ns%d", k%3), fmt.Sprintf("p%02d", k)), typ, at, fmt.Sprintf("r%d", round)))
		}
		if round == 2 {
			if err := ps.Snapshot(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	h := dt0.Add(150 * time.Second) // inside round 3
	end := at.Add(time.Hour)
	if err := ms.Compact(ctx, h); err != nil {
		t.Fatal(err)
	}
	stop = true
	if err := ps.Compact(ctx, h); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Compact: err=%v, want simulated crash", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	stop = false

	ps = open()
	defer func() { _ = ps.Close() }()
	if ps.pending.IsZero() {
		t.Fatal("pending compaction marker not restored")
	}
	if err := ps.Snapshot(ctx); err != nil || !ps.lastManifest.Before(h) {
		t.Errorf("Snapshot during pending compaction wrote a manifest (err=%v, last=%s)", err, ps.lastManifest)
	}
	pv, err := ps.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mv, err := ms.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !pv.Horizon().Equal(h) {
		t.Fatalf("horizon after interrupted compaction = %s, want %s", pv.Horizon(), h)
	}
	// A delete record older than the horizon may linger until its chunk
	// commits; everything else must already be final.
	lingering := func(r model.Record) bool { return r.Type == model.ChangeDeleted && r.ObservedAt.Before(h) }
	removedNow := 0
	for _, when := range []time.Time{h, h.Add(30 * time.Second), end} {
		for k := range 40 {
			key := podKey(fmt.Sprintf("ns%d", k%3), fmt.Sprintf("p%02d", k))
			mr, mok, err1 := mv.Get(ctx, key, when)
			pr, pok, err2 := pv.Get(ctx, key, when)
			if err := errors.Join(err1, err2); err != nil {
				t.Fatal(err)
			}
			switch {
			case mok && (!pok || diffRecord(mr, pr) != ""):
				t.Errorf("Get(%s, %s): mem %+v, peb ok=%v %+v", key, when, mr, pok, pr)
			case !mok && pok && !lingering(pr):
				t.Errorf("Get(%s, %s): mem none, peb %+v", key, when, pr)
			case !mok && !pok:
				removedNow++
			}
		}
		for _, path := range []string{"default", "scan"} {
			opts := store.ListOptions{Resource: "v1/pods", At: when}
			m, err1 := collect(func(fn func(model.Record) error) error { return mv.List(ctx, opts, fn) })
			p, err2 := collect(func(fn func(model.Record) error) error {
				if path == "scan" {
					return pv.(*view).listScan(ctx, opts, fn)
				}
				return pv.List(ctx, opts, fn)
			})
			if err := errors.Join(err1, err2); err != nil {
				t.Fatal(err)
			}
			var pf []model.Record
			for _, r := range p {
				if !lingering(r) {
					pf = append(pf, r)
				}
			}
			if summarize(m) != summarize(pf) {
				t.Errorf("List(%s, %s) after interrupted compaction:\nmem %s\npeb %s", path, when, summarize(m), summarize(pf))
			}
		}
	}
	_ = pv.Close()
	_ = mv.Close()
	t.Logf("deleted keys already removed by the committed chunks (over 3 probe times): %d", removedNow)

	// The next Compact (here with an older horizon) finishes the run.
	if err := ps.Compact(ctx, dt0); err != nil {
		t.Fatal(err)
	}
	if !ps.pending.IsZero() {
		t.Error("pending marker still set after resumed compaction")
	}
	if got, want := dump(t, ps), dump(t, ms); got != want {
		t.Errorf("store after resumed compaction differs from memstore:\npeb:\n%s\nmem:\n%s", got, want)
	}
	if err := ps.Snapshot(ctx); err != nil || ps.lastManifest.Before(h) {
		t.Errorf("Snapshot after compaction finished: err=%v last=%s", err, ps.lastManifest)
	}
}
