package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// bigPodStore is the reviewer's repro: 10k pods with 8 KB of annotations.
func bigPodStore(tb testing.TB) (*memstore.Store, time.Time) {
	tb.Helper()
	st := memstore.New()
	tb.Cleanup(func() { _ = st.Close() })
	t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	pad := strings.Repeat("x", 8000)
	es := []store.Entry{{Coverage: &model.CoverageEvent{At: t0, Resource: pods, Type: model.CoverageStart}}}
	for i := range 10000 {
		obj := fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p%05d","namespace":"ns","uid":"u%d","annotations":{"pad":"%s"}}}`, i, i, pad)
		es = append(es, store.Entry{Record: &model.Record{
			ObservedAt: t0.Add(time.Second), Key: model.ObjectKey{Resource: pods, Namespace: "ns", Name: fmt.Sprintf("p%05d", i)},
			UID: fmt.Sprintf("u%d", i), Type: model.ChangeAdded, Origin: model.OriginWatch, Object: []byte(obj),
		}})
	}
	if err := st.Write(context.Background(), es); err != nil {
		tb.Fatal(err)
	}
	return st, t0
}

// Regression for the review finding that an unparameterized /list of 10k x
// 8 KB pods produced an 84 MB body and ~600 MB of allocations. The default
// page must now be bounded by the default limit.
func TestListResponseSizeBounded(t *testing.T) {
	st, t0 := bigPodStore(t)
	h := New(Options{Engine: query.New(st, testingclock.NewFakePassiveClock(t0.Add(time.Hour)))})
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/list?resource=v1/pods", nil))
	runtime.ReadMemStats(&m1)
	body := float64(rr.Body.Len()) / 1e6
	alloc := float64(m1.TotalAlloc-m0.TotalAlloc) / 1e6
	t.Logf("status=%d body=%.1f MB, allocated during request=%.1f MB", rr.Code, body, alloc)
	wantStatus(t, rr, http.StatusOK)
	r := decode[api.ListResponse](t, rr)
	if len(r.Items) != query.ListDefaultLimit || r.Continue == "" {
		t.Fatalf("items = %d, continue %q", len(r.Items), r.Continue)
	}
	// 500 x ~8 KB = ~4 MB of objects; allow encoding overhead.
	if body > 6 {
		t.Errorf("body = %.1f MB, want <= 6 MB", body)
	}
	// Allocations were ~600 MB before paging. The bound is loose because
	// TotalAlloc is process-wide.
	if alloc > 60 {
		t.Errorf("allocated %.1f MB during one page, want <= 60 MB", alloc)
	}
}

// BenchmarkListPageHTTP reports per-request cost of one /list page of the
// reviewer's 10k x 8 KB pods, end to end through the handler (engine,
// JSON encoding, response buffering). memstore's View copy and key sort
// are included; they are not pebblestore costs.
func BenchmarkListPageHTTP(b *testing.B) {
	st, t0 := bigPodStore(b)
	h := New(Options{Engine: query.New(st, testingclock.NewFakePassiveClock(t0.Add(time.Hour)))})
	atParam := url.QueryEscape(t0.Add(time.Minute).Format(time.RFC3339Nano))
	var tokenPage10 string
	{
		next := ""
		for range 9 {
			rr := httptest.NewRecorder()
			target := "/api/v1/list?resource=v1/pods&limit=500&at=" + atParam
			if next != "" {
				target += "&continue=" + url.QueryEscape(next)
			}
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
			r := decode[api.ListResponse](b, rr)
			next = r.Continue
		}
		tokenPage10 = next
	}
	for _, bc := range []struct{ name, query string }{
		{"default-page", "resource=v1/pods&at=" + atParam},
		{"page-10", "resource=v1/pods&limit=500&at=" + atParam + "&continue=" + url.QueryEscape(tokenPage10)},
		{"limit-5000", "resource=v1/pods&limit=5000&at=" + atParam},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			var size int
			for b.Loop() {
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/list?"+bc.query, nil))
				if rr.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rr.Code, rr.Body.String())
				}
				size = rr.Body.Len()
			}
			b.ReportMetric(float64(size)/1e6, "body-MB")
		})
	}
}
