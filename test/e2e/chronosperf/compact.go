//go:build e2e

package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/pebblestore"
)

func dirSize(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

type probe struct {
	key model.ObjectKey
	at  time.Time
	seq uint64
	ok  bool
}

// runCompact opens a COPY of a Chronos data directory (never the live
// one: Pebble is single-process), records reference answers at instants
// at or after the horizon, then runs Compact(horizon) while reader
// goroutines issue Get and List against fresh Views. It reports reader
// latency before and during compaction, the compaction time, and checks
// that every reference answer is unchanged afterwards (the Compact
// contract: Get at any t >= horizon is unchanged).
func runCompact(args []string) error {
	fls := flag.NewFlagSet("compact", flag.ContinueOnError)
	dir := fls.String("data-dir", "", "copy of a pebble data dir (required)")
	horizonS := fls.String("horizon", "", "RFC 3339 horizon (default: midpoint between first and last record)")
	resource := fls.String("resource", resConfigMaps, "resource sampled by readers")
	readers := fls.Int("readers", 4, "concurrent readers")
	nprobe := fls.Int("probes", 2000, "reference answers checked after compaction")
	warm := fls.Duration("baseline", 5*time.Second, "reader-only period before compaction")
	if err := fls.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-data-dir is required")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	st, err := pebblestore.Open(*dir, pebblestore.Options{Logger: log, SnapshotInterval: -1})
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	v, err := st.View(ctx)
	if err != nil {
		return err
	}
	var keys []model.ObjectKey
	var first, last time.Time
	err = v.Changes(ctx, store.ChangesOptions{From: time.Unix(0, 0), To: farFuture()}, func(r model.Record) error {
		if first.IsZero() {
			first = r.ObservedAt
		}
		last = r.ObservedAt
		return nil
	})
	if err != nil {
		return err
	}
	if err := v.List(ctx, store.ListOptions{Resource: *resource, At: farFuture()}, func(r model.Record) error {
		keys = append(keys, r.Key)
		return nil
	}); err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("no keys of %s", *resource)
	}
	horizon := first.Add(last.Sub(first) / 2)
	if *horizonS != "" {
		if horizon, err = time.Parse(time.RFC3339Nano, *horizonS); err != nil {
			return err
		}
	}
	probes := make([]probe, *nprobe)
	for i := range probes {
		p := &probes[i]
		p.key = keys[rand.IntN(len(keys))] // #nosec G404 -- sampling
		// Uniform in [horizon, last], plus the horizon itself.
		p.at = horizon.Add(time.Duration(rand.Int64N(int64(last.Sub(horizon)) + 1))) // #nosec G404
		if i%10 == 0 {
			p.at = horizon
		}
		rec, ok, err := v.Get(ctx, p.key, p.at)
		if err != nil {
			return err
		}
		p.seq, p.ok = rec.Seq, ok
	}
	covBefore, err := v.Coverage(ctx, *resource)
	if err != nil {
		return err
	}
	_ = v.Close()
	sizeBefore := dirSize(*dir)
	fmt.Printf("compact: %s keys=%d records %s .. %s horizon=%s size=%d B\n", *resource, len(keys),
		first.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano), horizon.Format(time.RFC3339Nano), sizeBefore)

	// Readers: a fresh View per operation, as the HTTP server does.
	var phase atomic.Int32 // 0 baseline, 1 compacting, 2 after
	var mu sync.Mutex
	lat := map[string][][]time.Duration{"get": make([][]time.Duration, 3), "list-ns": make([][]time.Duration, 3)}
	var readErr atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range *readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[rand.IntN(len(keys))]                                             // #nosec G404
				at := horizon.Add(time.Duration(rand.Int64N(int64(last.Sub(horizon)) + 1))) // #nosec G404
				op := "get"
				t := time.Now()
				ph := phase.Load()
				vv, err := st.View(ctx)
				if err == nil {
					if rand.IntN(10) == 0 { // #nosec G404
						op = "list-ns"
						n := 0
						err = vv.List(ctx, store.ListOptions{Resource: *resource, Namespace: k.Namespace, At: at}, func(model.Record) error {
							n++
							return nil
						})
					} else {
						_, _, err = vv.Get(ctx, k, at)
					}
					_ = vv.Close()
				}
				if err != nil {
					readErr.Store(err)
					continue
				}
				d := time.Since(t)
				if phase.Load() != ph {
					continue // straddled a phase change
				}
				mu.Lock()
				lat[op][ph] = append(lat[op][ph], d)
				mu.Unlock()
			}
		})
	}
	time.Sleep(*warm)
	phase.Store(1)
	t0 := time.Now()
	cerr := st.Compact(ctx, horizon)
	took := time.Since(t0)
	phase.Store(2)
	time.Sleep(*warm)
	close(stop)
	wg.Wait()
	if cerr != nil {
		return fmt.Errorf("compact: %w", cerr)
	}
	fmt.Printf("compact: Compact(horizon) took %s with %d concurrent readers\n", took.Round(time.Microsecond), *readers)
	for _, op := range []string{"get", "list-ns"} {
		for ph, name := range []string{"before", "during", "after"} {
			fmt.Printf("compact: reader %-7s %-6s %s\n", op, name, summarize(lat[op][ph]))
		}
	}
	if e := readErr.Load(); e != nil {
		return fmt.Errorf("reader error: %v", e)
	}

	// Contract check.
	v, err = st.View(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = v.Close() }()
	bad := 0
	for _, p := range probes {
		rec, ok, err := v.Get(ctx, p.key, p.at)
		if err != nil {
			return err
		}
		if ok != p.ok || rec.Seq != p.seq {
			bad++
			if bad <= 5 {
				fmt.Printf("compact: MISMATCH %s at %s: before seq=%d ok=%v, after seq=%d ok=%v\n",
					p.key, p.at.Format(time.RFC3339Nano), p.seq, p.ok, rec.Seq, ok)
			}
		}
	}
	covAfter, err := v.Coverage(ctx, *resource)
	if err != nil {
		return err
	}
	nAfter := 0
	if err := v.Changes(ctx, store.ChangesOptions{From: time.Unix(0, 0), To: farFuture()}, func(model.Record) error {
		nAfter++
		return nil
	}); err != nil {
		return err
	}
	if err := st.Flush(); err != nil {
		return err
	}
	if err := st.CompactLSM(ctx); err != nil {
		return err
	}
	fmt.Printf("compact: horizon now %s; records left %d; coverage events %d -> %d; probes checked %d, mismatches %d; size %d B -> %d B (after LSM compaction)\n",
		v.Horizon().Format(time.RFC3339Nano), nAfter, len(covBefore), len(covAfter), len(probes), bad, sizeBefore, dirSize(*dir))
	if bad > 0 {
		return fmt.Errorf("%d answers at t >= horizon changed", bad)
	}
	return nil
}

func farFuture() time.Time { return time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC) }
