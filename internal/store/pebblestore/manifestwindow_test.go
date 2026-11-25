package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// TestManifestPathIsLazyAndBounded checks that a paged List on the
// manifest path reads only the namespace chunks it reaches, that a large
// window of changes since the manifest sends List to the scan path, and
// that paged walks agree with the scan path in both regimes.
func TestManifestPathIsLazyAndBounded(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, t.TempDir())
	defer func() { _ = s.Close() }()
	at := dt0
	var batch []store.Entry
	put := func(k model.ObjectKey, typ model.ChangeType, body string) {
		at = at.Add(time.Millisecond)
		batch = append(batch, recEntry(k, typ, at, body))
		if len(batch) == 500 {
			mustWrite(t, s, batch...)
			batch = nil
		}
	}
	flush := func() {
		if len(batch) > 0 {
			mustWrite(t, s, batch...)
			batch = nil
		}
	}
	for i := range 4000 {
		put(podKey(fmt.Sprintf("ns%02d", i%20), fmt.Sprintf("p%05d", i)), model.ChangeAdded, "v0")
	}
	flush()
	if err := s.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}

	walk := func(v store.View, scan bool, ns string, k int) (pages []string) {
		opts := store.ListOptions{Resource: "v1/pods", Namespace: ns, At: at.Add(time.Hour)}
		for {
			var page []model.Record
			run := v.List
			if scan {
				run = v.(*view).listScan
			}
			err := run(ctx, opts, func(r model.Record) error {
				r.Object = append([]byte(nil), r.Object...)
				page = append(page, r)
				if len(page) == k {
					return errPageFull
				}
				return nil
			})
			if err != nil && !errors.Is(err, errPageFull) {
				t.Fatal(err)
			}
			pages = append(pages, summarize(page))
			if len(page) < k {
				return pages
			}
			last := page[len(page)-1].Key
			opts.After = &last
		}
	}
	compare := func(label string) {
		v, err := s.View(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = v.Close() }()
		for _, ns := range []string{"", "ns07"} {
			for _, k := range []int{1, 37, 500} {
				a, b := walk(v, false, ns, k), walk(v, true, ns, k)
				if fmt.Sprint(a) != fmt.Sprint(b) {
					t.Fatalf("%s: ns=%q k=%d: default and scan walks differ", label, ns, k)
				}
			}
		}
	}

	// Small window: manifest path.
	for i := range 100 {
		put(podKey(fmt.Sprintf("ns%02d", i%20), fmt.Sprintf("p%05d", i*7)), model.ChangeModified, "v1")
	}
	flush()
	m0 := s.stats.manifest.Load()
	compare("small window")
	if s.stats.manifest.Load() == m0 || s.stats.windowSkips.Load() != 0 {
		t.Errorf("small window: manifest lists %d -> %d, window skips %d", m0, s.stats.manifest.Load(), s.stats.windowSkips.Load())
	}
	// A first page reads one chunk, not all twenty.
	gets := countChunkReads(t, s, store.ListOptions{Resource: "v1/pods", At: at.Add(time.Hour)}, 10)
	if gets != 1 {
		t.Errorf("first 10-item page read %d manifest chunks, want 1", gets)
	}

	// Large window (> max(1024, entries/4)): scan path.
	for i := range 3000 {
		put(podKey(fmt.Sprintf("ns%02d", i%20), fmt.Sprintf("p%05d", (i*13)%4000)), model.ChangeModified, "v2")
		if i%11 == 0 {
			put(podKey(fmt.Sprintf("ns%02d", i%20), fmt.Sprintf("new%05d", i)), model.ChangeAdded, "n")
		}
	}
	flush()
	compare("large window")
	if s.stats.windowSkips.Load() == 0 {
		t.Error("large window did not fall back to the scan path")
	}
}

// countChunkReads lists one page and reports how many namespace chunks
// the manifest stream read.
func countChunkReads(t *testing.T, s *Store, opts store.ListOptions, k int) int {
	t.Helper()
	v, err := s.View(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	before := s.stats.chunkReads.Load()
	n := 0
	err = v.List(context.Background(), opts, func(model.Record) error {
		if n++; n == k {
			return errPageFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errPageFull) {
		t.Fatal(err)
	}
	return int(s.stats.chunkReads.Load() - before)
}
