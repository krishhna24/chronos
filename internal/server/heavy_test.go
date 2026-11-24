package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/store"
)

// gatedStore blocks the first n View calls until release is closed, so a
// test can hold heavy requests in flight.
type gatedStore struct {
	store.Store
	n       int32
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newGatedStore(s store.Store, n int) *gatedStore {
	return &gatedStore{Store: s, n: int32(n), entered: make(chan struct{}, n), release: make(chan struct{})}
}

func (g *gatedStore) View(ctx context.Context) (store.View, error) {
	if g.calls.Add(1) <= g.n {
		g.entered <- struct{}{}
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.Store.View(ctx)
}

func TestHeavyRequestBound(t *testing.T) {
	g := newGatedStore(newStore(t), 2)
	srv := newTestServer(t, g, func(o *Options) { o.MaxHeavyRequests = 2 })

	var wg sync.WaitGroup
	codes := make([]int, 2)
	for i, path := range []string{"/api/v1/list?resource=v1/pods", "/api/v1/history?resource=v1/pods&namespace=ns&name=a"} {
		wg.Go(func() { codes[i] = srv.get(t, path, nil).Code })
	}
	for range 2 {
		select {
		case <-g.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("heavy requests did not start")
		}
	}

	// Saturated: every heavy endpoint is refused at once, without queueing.
	for _, target := range []string{
		"/api/v1/list?resource=v1/pods",
		"/api/v1/history?resource=v1/pods&namespace=ns&name=a",
		"/api/v1/changes?from=" + url.QueryEscape(ts(0)) + "&to=" + url.QueryEscape(ts(90)),
		"/api/v1/diff?resource=v1/pods&namespace=ns&name=a&from=" + url.QueryEscape(ts(15)) + "&to=" + url.QueryEscape(ts(25)),
	} {
		start := time.Now()
		w := srv.do(t, http.MethodGet, target, nil)
		e := wantError(t, w, http.StatusServiceUnavailable, "unavailable")
		if w.Header().Get("Retry-After") == "" {
			t.Errorf("%s: no Retry-After", target)
		}
		if !strings.Contains(e.Message, "busy") {
			t.Errorf("%s: message %q", target, e.Message)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("%s: refusal took %s (queued?)", target, d)
		}
	}
	// Light endpoints are not bounded.
	wantStatus(t, srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a", "at", ts(25))), http.StatusOK)
	wantStatus(t, srv.get(t, "/healthz", nil), http.StatusOK)

	close(g.release)
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusOK {
			t.Errorf("held request %d finished with %d", i, c)
		}
	}
	// Slots are released.
	for range 3 {
		wantStatus(t, srv.get(t, "/api/v1/list", q("resource", pods)), http.StatusOK)
	}
}

func TestHeavyRequestBoundDisabled(t *testing.T) {
	g := newGatedStore(newStore(t), 3)
	srv := newTestServer(t, g, func(o *Options) { o.MaxHeavyRequests = -1 })
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() { srv.get(t, "/api/v1/list?resource=v1/pods", nil) })
	}
	for range 3 {
		select {
		case <-g.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("unbounded server refused concurrent requests")
		}
	}
	close(g.release)
	wg.Wait()
}
