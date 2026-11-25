//go:build e2e

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/krishhna24/chronos/internal/api"
)

// runBench measures API latency (wall time of the HTTP round trip on
// loopback, body fully read and decoded) at the current dataset.
func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	server := fs.String("server", "http://127.0.0.1:18080", "Chronos base URL")
	prefix := fs.String("prefix", "perf", "namespace prefix")
	nns := fs.Int("namespaces", 10, "perf namespaces")
	ncm := fs.Int("configmaps", 7000, "perf ConfigMaps")
	iters := fs.Int("iterations", 200, "requests per scenario")
	fullIters := fs.Int("full-iterations", 5, "complete paged listings per full-list scenario")
	conc := fs.Int("concurrency", 1, "concurrent clients per scenario")
	pastAgo := fs.Duration("past", 2*time.Minute, "how far back the 'past' queries go")
	only := fs.String("only", "", "comma-separated scenario names to run (default all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c := newChronosClient(*server)
	ctx := context.Background()
	rr, err := c.resources(ctx)
	if err != nil {
		return err
	}
	now := rr.Now
	past := now.Add(-*pastAgo)
	rfc := func(t time.Time) string { return t.Format(time.RFC3339Nano) }
	randCM := func() (string, string) {
		i := rand.IntN(*ncm) // #nosec G404 -- load generation
		return perfNamespace(*prefix, i%*nns), fmt.Sprintf("cm-%05d", i)
	}
	type scenario struct {
		name string
		n    int
		do   func() (int, error) // returns bytes
	}
	listAll := func(resource, ns string, limit int, at time.Time) (int, int, error) {
		q := url.Values{"resource": {resource}, "limit": {fmt.Sprint(limit)}, "at": {rfc(at)}}
		if ns != "" {
			q.Set("namespace", ns)
		}
		total, items := 0, 0
		for {
			var lr api.ListResponse
			n, err := c.get(ctx, "/api/v1/list", q, &lr)
			total += n
			if err != nil {
				return total, items, err
			}
			items += len(lr.Items)
			if lr.Continue == "" {
				return total, items, nil
			}
			q.Set("continue", lr.Continue)
		}
	}
	var fullItems sync.Map
	scenarios := []scenario{
		{"state-now", *iters, func() (int, error) {
			ns, n := randCM()
			return c.get(ctx, "/api/v1/state", url.Values{"resource": {resConfigMaps}, "namespace": {ns}, "name": {n}}, &api.ObjectState{})
		}},
		{"state-past", *iters, func() (int, error) {
			ns, n := randCM()
			return c.get(ctx, "/api/v1/state", url.Values{"resource": {resConfigMaps}, "namespace": {ns}, "name": {n}, "at": {rfc(past)}}, &api.ObjectState{})
		}},
		{"history", *iters, func() (int, error) {
			ns, n := randCM()
			return c.get(ctx, "/api/v1/history", url.Values{"resource": {resConfigMaps}, "namespace": {ns}, "name": {n}}, &api.HistoryResponse{})
		}},
		{"diff", *iters, func() (int, error) {
			ns, n := randCM()
			return c.get(ctx, "/api/v1/diff", url.Values{"resource": {resConfigMaps}, "namespace": {ns}, "name": {n},
				"from": {rfc(past)}, "to": {rfc(now)}}, &api.DiffResponse{})
		}},
		{"list-ns-page500-cm", *iters, func() (int, error) {
			ns, _ := randCM()
			return c.get(ctx, "/api/v1/list", url.Values{"resource": {resConfigMaps}, "namespace": {ns}, "limit": {"500"}}, &api.ListResponse{})
		}},
		{"list-all-page500-cm", *iters, func() (int, error) {
			return c.get(ctx, "/api/v1/list", url.Values{"resource": {resConfigMaps}, "limit": {"500"}}, &api.ListResponse{})
		}},
		{"list-all-page500-pods", *iters, func() (int, error) {
			return c.get(ctx, "/api/v1/list", url.Values{"resource": {"v1/pods"}, "limit": {"500"}}, &api.ListResponse{})
		}},
		{"list-all-page500-cm-past", *iters, func() (int, error) {
			return c.get(ctx, "/api/v1/list", url.Values{"resource": {resConfigMaps}, "limit": {"500"}, "at": {rfc(past)}}, &api.ListResponse{})
		}},
		{"list-full-cm-paged500", *fullIters, func() (int, error) {
			b, n, err := listAll(resConfigMaps, "", 500, now)
			fullItems.Store("list-full-cm-paged500", n)
			return b, err
		}},
		{"list-full-pods-paged500", *fullIters, func() (int, error) {
			b, n, err := listAll("v1/pods", "", 500, now)
			fullItems.Store("list-full-pods-paged500", n)
			return b, err
		}},
		{"changes-1m", *iters / 4, func() (int, error) {
			return c.get(ctx, "/api/v1/changes", url.Values{"from": {rfc(now.Add(-time.Minute))}, "to": {rfc(now)}, "limit": {"1000"}}, &api.ChangesResponse{})
		}},
	}
	want := map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		if s = strings.TrimSpace(s); s != "" {
			want[s] = true
		}
	}
	fmt.Printf("bench: server now=%s past=%s concurrency=%d\n", rfc(now), rfc(past), *conc)
	for _, s := range scenarios {
		if len(want) > 0 && !want[s.name] {
			continue
		}
		var mu sync.Mutex
		var lat []time.Duration
		var bytes, busy, errs int
		var firstErr error
		jobs := make(chan struct{})
		var wg sync.WaitGroup
		for range *conc {
			wg.Go(func() {
				for range jobs {
					t := time.Now()
					n, err := s.do()
					d := time.Since(t)
					mu.Lock()
					var he *httpError
					switch {
					case errors.As(err, &he) && he.status == 503:
						busy++
					case err != nil:
						errs++
						if firstErr == nil {
							firstErr = err
						}
					default:
						lat = append(lat, d)
						bytes += n
					}
					mu.Unlock()
				}
			})
		}
		for range s.n {
			jobs <- struct{}{}
		}
		close(jobs)
		wg.Wait()
		avg := 0
		if len(lat) > 0 {
			avg = bytes / len(lat)
		}
		extra := ""
		if v, ok := fullItems.Load(s.name); ok {
			extra = fmt.Sprintf(" items=%d", v)
		}
		fmt.Printf("bench %-26s %s avgBytes=%d 503=%d errors=%d%s\n", s.name, summarize(lat), avg, busy, errs, extra)
		if firstErr != nil {
			fmt.Printf("bench %-26s first error: %v\n", s.name, firstErr)
		}
	}
	return nil
}
