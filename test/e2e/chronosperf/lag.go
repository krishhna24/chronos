//go:build e2e

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/krishhna24/chronos/internal/api"
)

type objID struct{ resource, namespace, name string }

func readStormCSV(path string) ([]write, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-supplied input path
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	var out []write
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		w, err := parseWrite(rec)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
}

// runLag joins a storm CSV with Chronos's history of every touched
// object. For each successful write it finds the record with the same
// resourceVersion and classifies it:
//
//   - watch:    recorded from the watch; lag = ObservedAt - client time
//   - resync:   recorded by a relist (the write happened during a gap)
//   - gap-miss: no record, and the write was not inside one coverage
//     interval (legitimate: overwritten during a gap, ADR 0004)
//   - edge:     no record, the write is inside coverage but within
//     -tolerance of the interval's End (watch delivery lag at shutdown)
//   - LOST:     no record although coverage claims Chronos observed the
//     write: a defect (fabricated continuity)
func runLag(args []string) error {
	fs := flag.NewFlagSet("lag", flag.ContinueOnError)
	server := fs.String("server", "http://127.0.0.1:18080", "Chronos base URL")
	in := fs.String("in", "storm.csv", "storm CSV")
	conc := fs.Int("concurrency", 4, "concurrent history requests (server allows 8 heavy requests)")
	tol := fs.Duration("tolerance", time.Second, "watch delivery tolerance at an interval End")
	showLost := fs.Int("show", 10, "print at most this many LOST writes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	writes, err := readStormCSV(*in)
	if err != nil {
		return err
	}
	c := newChronosClient(*server)
	ctx := context.Background()
	rr, err := c.resources(ctx)
	if err != nil {
		return err
	}

	byKey := map[objID][]write{}
	failedWrites := 0
	for _, w := range writes {
		if w.err != "" {
			failedWrites++
			continue
		}
		k := objID{w.resource, w.namespace, w.name}
		byKey[k] = append(byKey[k], w)
	}
	keys := make([]objID, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	hist := make(map[objID]api.HistoryResponse, len(keys))
	var mu sync.Mutex
	var firstErr error
	work := make(chan objID)
	var wg sync.WaitGroup
	for range *conc {
		wg.Go(func() {
			for k := range work {
				var h api.HistoryResponse
				var err error
				for attempt := 0; attempt < 20; attempt++ {
					h, err = c.history(ctx, k.resource, k.namespace, k.name)
					var he *httpError
					if errors.As(err, &he) && he.status == 503 {
						time.Sleep(200 * time.Millisecond)
						continue
					}
					break
				}
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("history %v: %w", k, err)
				}
				hist[k] = h
				mu.Unlock()
			}
		})
	}
	start := time.Now()
	for _, k := range keys {
		work <- k
	}
	close(work)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	fetch := time.Since(start)

	var lag0, lag1 []time.Duration
	var nWatch, nResync, nGapMiss, nEdge, nLost, nNeg, nOrder, nTrunc int
	var lost []write
	var worst []struct {
		w   write
		lag time.Duration
	}
	for k, ws := range byKey {
		h := hist[k]
		if h.Truncated {
			nTrunc++
		}
		byRV := map[string]api.Version{}
		for _, v := range h.Versions {
			if v.Type != "Deleted" {
				byRV[v.ResourceVersion] = v
			}
		}
		cov := coverageOf(rr, k.resource)
		sort.Slice(ws, func(i, j int) bool { return ws[i].t0.Before(ws[j].t0) })
		var prevT1, prevObs time.Time
		for _, w := range ws {
			v, ok := byRV[w.rv]
			switch {
			case ok && v.Origin == "watch":
				nWatch++
				d0, d1 := v.ObservedAt.Sub(w.t0), v.ObservedAt.Sub(w.t1)
				lag0, lag1 = append(lag0, d0), append(lag1, d1)
				if d0 < 0 {
					nNeg++
				}
				// A write that completed before this one started must have
				// been recorded earlier.
				if !prevT1.IsZero() && prevT1.Before(w.t0) && !prevObs.Before(v.ObservedAt) {
					nOrder++
				}
				prevT1, prevObs = w.t1, v.ObservedAt
				worst = append(worst, struct {
					w   write
					lag time.Duration
				}{w, d0})
			case ok:
				nResync++
			case !observedThroughout(cov, w.t0, w.t1):
				nGapMiss++
			case !observedThroughout(cov, w.t0, w.t1.Add(*tol)):
				nEdge++
			default:
				nLost++
				lost = append(lost, w)
			}
		}
	}
	fmt.Printf("lag: %d writes (%d failed at the apiserver) on %d objects; history fetched in %s; truncated histories: %d\n",
		len(writes), failedWrites, len(keys), fetch.Round(time.Millisecond), nTrunc)
	fmt.Printf("lag: recorded by watch=%d resync=%d | not recorded: gap-miss=%d edge=%d LOST=%d\n",
		nWatch, nResync, nGapMiss, nEdge, nLost)
	fmt.Printf("lag: ordering violations=%d, ObservedAt before client send=%d\n", nOrder, nNeg)
	fmt.Printf("lag: ObservedAt - client send     %s\n", summarize(lag0))
	fmt.Printf("lag: ObservedAt - client response %s\n", summarize(lag1))
	sort.Slice(worst, func(i, j int) bool { return worst[i].lag > worst[j].lag })
	for i := range min(5, len(worst)) {
		w := worst[i]
		fmt.Printf("lag: worst #%d %s %s/%s rv=%s sent=%s lag=%s\n", i+1, w.w.resource, w.w.namespace, w.w.name,
			w.w.rv, w.w.t0.Format(time.RFC3339Nano), w.lag)
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i].t0.Before(lost[j].t0) })
	for i := range min(*showLost, len(lost)) {
		w := lost[i]
		fmt.Printf("lag: LOST %s %s/%s rv=%s sent=%s done=%s\n", w.resource, w.namespace, w.name, w.rv,
			w.t0.Format(time.RFC3339Nano), w.t1.Format(time.RFC3339Nano))
	}
	for _, id := range []string{resConfigMaps, resDeployments} {
		for _, iv := range coverageOf(rr, id) {
			end := "open"
			if iv.End != nil {
				end = iv.End.Format(time.RFC3339Nano) + " (" + iv.EndReason + ")"
			}
			fmt.Printf("coverage %s: %s .. %s\n", id, iv.Start.Format(time.RFC3339Nano), end)
		}
	}
	if nLost > 0 || nOrder > 0 {
		return fmt.Errorf("%d writes lost inside observed coverage, %d ordering violations", nLost, nOrder)
	}
	return nil
}
