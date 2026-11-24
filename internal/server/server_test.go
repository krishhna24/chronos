package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

const pods = "v1/pods"

var t0 = time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func ts(sec int) string { return at(sec).Format(time.RFC3339Nano) }

func podObj(name, uid, image string) []byte {
	return []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"ns","name":"` + name +
		`","uid":"` + uid + `","labels":{"app":"web"}},"spec":{"containers":[{"name":"app","image":"` + image + `"}]}}`)
}

func rec(name, uid string, typ model.ChangeType, origin model.Origin, t time.Time, obj []byte) store.Entry {
	return store.Entry{Record: &model.Record{
		ObservedAt: t, Key: model.ObjectKey{Resource: pods, Namespace: "ns", Name: name},
		UID: uid, ResourceVersion: "1", Type: typ, Origin: origin, Object: obj,
	}}
}

func cov(typ model.CoverageType, t time.Time, reason string) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: t, Resource: pods, Type: typ, Reason: reason}}
}

// newStore writes (seconds after t0): 10 baseline a + Start, 20 watch
// Modified a, 30 watch Added b, 40 End, 60 resync Modified a + Start,
// 70 watch Deleted b. The clock reads t0+100.
func newStore(t *testing.T) *memstore.Store {
	t.Helper()
	s := memstore.New()
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	for _, batch := range [][]store.Entry{
		{rec("a", "a1", model.ChangeAdded, model.OriginBaseline, at(10), podObj("a", "a1", "img:v1")), cov(model.CoverageStart, at(10), "initial list")},
		{rec("a", "a1", model.ChangeModified, model.OriginWatch, at(20), podObj("a", "a1", "img:v2"))},
		{rec("b", "b1", model.ChangeAdded, model.OriginWatch, at(30), podObj("b", "b1", "img:v1"))},
		{cov(model.CoverageEnd, at(40), "watch expired")},
		{rec("a", "a1", model.ChangeModified, model.OriginResync, at(60), podObj("a", "a1", "img:v3")), cov(model.CoverageStart, at(60), "relist")},
		{rec("b", "b1", model.ChangeDeleted, model.OriginWatch, at(70), podObj("b", "b1", "img:v1"))},
	} {
		if err := s.Write(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type testServer struct {
	h    http.Handler
	logs *syncBuffer
}

func newTestServer(t *testing.T, s store.Store, mod func(*Options)) *testServer {
	t.Helper()
	logs := &syncBuffer{}
	o := Options{
		Engine: query.New(s, testingclock.NewFakePassiveClock(at(100))),
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mod != nil {
		mod(&o)
	}
	return &testServer{h: New(o), logs: logs}
}

func (ts *testServer) do(t *testing.T, method, target string, hdr http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, vs := range hdr {
		req.Header[k] = vs
	}
	w := httptest.NewRecorder()
	ts.h.ServeHTTP(w, req)
	return w
}

func (ts *testServer) get(t *testing.T, path string, q url.Values) *httptest.ResponseRecorder {
	t.Helper()
	target := path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	return ts.do(t, http.MethodGet, target, nil)
}

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T from %q: %v", v, w.Body.String(), err)
	}
	return v
}

func wantStatus(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d; body %s", w.Code, status, w.Body.String())
	}
}

func wantError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) api.Error {
	t.Helper()
	wantStatus(t, w, status)
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q, want JSON", ct)
	}
	e := decode[api.Error](t, w)
	if e.Code != code {
		t.Errorf("error code = %q, want %q (message %q)", e.Code, code, e.Message)
	}
	return e
}

func q(kv ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		v.Add(kv[i], kv[i+1])
	}
	return v
}

func TestEndpointsOK(t *testing.T) {
	srv := newTestServer(t, newStore(t), nil)

	t.Run("resources", func(t *testing.T) {
		w := srv.get(t, "/api/v1/resources", nil)
		wantStatus(t, w, http.StatusOK)
		r := decode[api.ResourcesResponse](t, w)
		if !r.Now.Equal(at(100)) || r.Horizon != nil || len(r.Resources) == 0 {
			t.Fatalf("resources = %+v", r)
		}
		for _, res := range r.Resources {
			if res.ID == pods && len(res.Coverage) != 2 {
				t.Errorf("pods coverage = %+v", res.Coverage)
			}
		}
	})
	t.Run("state present", func(t *testing.T) {
		w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a", "at", ts(25)))
		wantStatus(t, w, http.StatusOK)
		st := decode[api.ObjectState](t, w)
		if st.Existence != api.ExistencePresent || st.Coverage.State != api.CoverageObserved || !strings.Contains(string(st.Object), "img:v2") {
			t.Errorf("state = %+v", st)
		}
	})
	t.Run("state default at is now", func(t *testing.T) {
		w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a"))
		wantStatus(t, w, http.StatusOK)
		if st := decode[api.ObjectState](t, w); !st.At.Equal(at(100)) {
			t.Errorf("at = %s", st.At)
		}
	})
	t.Run("state gap", func(t *testing.T) {
		w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a", "at", ts(50)))
		wantStatus(t, w, http.StatusOK)
		st := decode[api.ObjectState](t, w)
		if st.Existence != api.ExistenceUnknown || st.Coverage.State != api.CoverageGap || st.Coverage.Reason != "watch expired" {
			t.Errorf("state = %+v", st)
		}
	})
	t.Run("state missing object is 200 absent", func(t *testing.T) {
		w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "nope", "at", ts(25)))
		wantStatus(t, w, http.StatusOK)
		if st := decode[api.ObjectState](t, w); st.Existence != api.ExistenceAbsent {
			t.Errorf("existence = %s", st.Existence)
		}
	})
	t.Run("state with offset time", func(t *testing.T) {
		w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a",
			"at", at(25).In(time.FixedZone("x", 2*3600)).Format(time.RFC3339)))
		wantStatus(t, w, http.StatusOK)
	})
	t.Run("list", func(t *testing.T) {
		w := srv.get(t, "/api/v1/list", q("resource", pods, "at", ts(80), "includeDeleted", "true", "labelSelector", "app=web"))
		wantStatus(t, w, http.StatusOK)
		l := decode[api.ListResponse](t, w)
		if len(l.Items) != 2 || l.Items[0].Key.Name != "a" || l.Items[1].Existence != api.ExistenceAbsent {
			t.Errorf("list = %+v", l)
		}
	})
	t.Run("list empty is [] not null", func(t *testing.T) {
		w := srv.get(t, "/api/v1/list", q("resource", pods, "at", ts(5)))
		wantStatus(t, w, http.StatusOK)
		if !strings.Contains(w.Body.String(), `"items":[]`) {
			t.Errorf("body = %s", w.Body.String())
		}
	})
	t.Run("diff", func(t *testing.T) {
		w := srv.get(t, "/api/v1/diff", q("resource", pods, "namespace", "ns", "name", "a", "from", ts(15), "to", ts(65)))
		wantStatus(t, w, http.StatusOK)
		d := decode[api.DiffResponse](t, w)
		if d.Complete || d.Recreated || !strings.Contains(d.Diff, "+  - image: img:v3") {
			t.Errorf("diff = %+v", d)
		}
	})
	t.Run("history", func(t *testing.T) {
		w := srv.get(t, "/api/v1/history", q("resource", pods, "namespace", "ns", "name", "a", "limit", "2"))
		wantStatus(t, w, http.StatusOK)
		h := decode[api.HistoryResponse](t, w)
		if len(h.Versions) != 2 || !h.Truncated || len(h.Coverage) != 2 {
			t.Errorf("history = %+v", h)
		}
	})
	t.Run("changes", func(t *testing.T) {
		w := srv.get(t, "/api/v1/changes", q("from", ts(0), "to", ts(100), "resource", pods, "limit", "3"))
		wantStatus(t, w, http.StatusOK)
		c := decode[api.ChangesResponse](t, w)
		if len(c.Changes) != 3 || !c.Truncated || c.Changes[0].Key.Name != "a" {
			t.Errorf("changes = %+v", c)
		}
	})
	t.Run("object", func(t *testing.T) {
		w := srv.get(t, "/api/v1/object", q("resource", pods, "namespace", "ns", "name", "a", "seq", "3"))
		wantStatus(t, w, http.StatusOK)
		st := decode[api.ObjectState](t, w)
		if st.Version == nil || st.Version.Seq != 3 || !strings.Contains(string(st.Object), "img:v2") {
			t.Errorf("object = %+v", st)
		}
	})
	t.Run("HEAD", func(t *testing.T) {
		w := srv.do(t, http.MethodHead, "/api/v1/resources", nil)
		wantStatus(t, w, http.StatusOK)
	})
}

func TestParameterValidation(t *testing.T) {
	srv := newTestServer(t, newStore(t), nil)
	key := []string{"resource", pods, "namespace", "ns", "name", "a"}
	with := func(extra ...string) url.Values { return q(append(append([]string{}, key...), extra...)...) }
	tests := []struct {
		name string
		path string
		q    url.Values
	}{
		{"state: missing resource", "/api/v1/state", q("namespace", "ns", "name", "a")},
		{"state: missing name", "/api/v1/state", q("resource", pods, "namespace", "ns")},
		{"state: namespaced without namespace", "/api/v1/state", q("resource", pods, "name", "a")},
		{"state: unknown resource", "/api/v1/state", q("resource", "v1/widgets", "namespace", "ns", "name", "a")},
		{"state: alias is not canonical", "/api/v1/state", q("resource", "po", "namespace", "ns", "name", "a")},
		{"state: bad time", "/api/v1/state", with("at", "yesterday")},
		{"state: relative time", "/api/v1/state", with("at", "-20m")},
		{"state: time without zone", "/api/v1/state", with("at", "2026-11-24T14:00:00")},
		{"state: future", "/api/v1/state", with("at", ts(200))},
		{"state: repeated param", "/api/v1/state", with("at", ts(20), "at", ts(30))},
		{"list: missing resource", "/api/v1/list", q()},
		{"list: bad selector", "/api/v1/list", q("resource", pods, "labelSelector", "app in (")},
		{"list: bad bool", "/api/v1/list", q("resource", pods, "includeDeleted", "maybe")},
		{"diff: missing from", "/api/v1/diff", with("to", ts(20))},
		{"diff: missing to", "/api/v1/diff", with("from", ts(20))},
		{"diff: from == to", "/api/v1/diff", with("from", ts(20), "to", ts(20))},
		{"diff: from > to", "/api/v1/diff", with("from", ts(30), "to", ts(20))},
		{"diff: namespaced without namespace", "/api/v1/diff", q("resource", pods, "name", "a", "from", ts(10), "to", ts(20))},
		{"history: limit 0", "/api/v1/history", with("limit", "0")},
		{"history: negative limit", "/api/v1/history", with("limit", "-1")},
		{"history: limit too large", "/api/v1/history", with("limit", "10001")},
		{"history: limit not a number", "/api/v1/history", with("limit", "ten")},
		{"history: from >= to", "/api/v1/history", with("from", ts(30), "to", ts(20))},
		{"history: namespaced without namespace", "/api/v1/history", q("resource", pods, "name", "a")},
		{"changes: missing from", "/api/v1/changes", q("to", ts(20))},
		{"changes: missing to", "/api/v1/changes", q("from", ts(20))},
		{"changes: unknown resource", "/api/v1/changes", q("from", ts(10), "to", ts(20), "resource", "nope")},
		{"changes: limit too large", "/api/v1/changes", q("from", ts(10), "to", ts(20), "limit", "10001")},
		{"object: missing seq", "/api/v1/object", with()},
		{"object: seq 0", "/api/v1/object", with("seq", "0")},
		{"object: seq not a number", "/api/v1/object", with("seq", "x")},
		{"object: seq of another key", "/api/v1/object", with("seq", "5")},
		{"object: unknown seq", "/api/v1/object", with("seq", "999")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := wantError(t, srv.get(t, tc.path, tc.q), http.StatusBadRequest, "bad_request")
			if e.Message == "" {
				t.Error("empty error message")
			}
		})
	}
}

func TestRoutingErrors(t *testing.T) {
	srv := newTestServer(t, newStore(t), nil)
	wantError(t, srv.get(t, "/api/v1/nope", nil), http.StatusNotFound, "not_found")
	wantError(t, srv.get(t, "/api/v2/state", nil), http.StatusNotFound, "not_found")
	wantError(t, srv.get(t, "/", nil), http.StatusNotFound, "not_found") // no Web configured

	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions} {
		w := srv.do(t, m, "/api/v1/state", nil)
		wantError(t, w, http.StatusMethodNotAllowed, "bad_request")
		if got := w.Header().Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s: Allow = %q", m, got)
		}
		if w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("%s: CORS header sent", m)
		}
	}
}

func TestHealthAndReadiness(t *testing.T) {
	var mu sync.Mutex
	ready := errors.New("store opening")
	srv := newTestServer(t, newStore(t), func(o *Options) {
		o.Readiness = func() error { mu.Lock(); defer mu.Unlock(); return ready }
	})

	w := srv.get(t, "/healthz", nil)
	wantStatus(t, w, http.StatusOK)
	if w.Body.String() != "ok\n" {
		t.Errorf("healthz body = %q", w.Body.String())
	}
	e := wantError(t, srv.get(t, "/readyz", nil), http.StatusServiceUnavailable, "unavailable")
	if strings.Contains(e.Message, "store opening") {
		t.Error("readiness error text leaked to client")
	}
	wantError(t, srv.get(t, "/api/v1/resources", nil), http.StatusServiceUnavailable, "unavailable")

	mu.Lock()
	ready = nil
	mu.Unlock()
	wantStatus(t, srv.get(t, "/readyz", nil), http.StatusOK)
	wantStatus(t, srv.get(t, "/api/v1/resources", nil), http.StatusOK)
}

func TestClosedStoreIsUnavailable(t *testing.T) {
	s := newStore(t)
	srv := newTestServer(t, s, nil)
	_ = s.Close()
	wantError(t, srv.get(t, "/api/v1/resources", nil), http.StatusServiceUnavailable, "unavailable")
}

// faultStore makes View misbehave.
type faultStore struct {
	store.Store
	view func(ctx context.Context) (store.View, error)
}

func (f *faultStore) View(ctx context.Context) (store.View, error) { return f.view(ctx) }

func TestPanicRecovered(t *testing.T) {
	fs := &faultStore{Store: newStore(t), view: func(context.Context) (store.View, error) {
		panic("secret internal detail /var/lib/chronos")
	}}
	srv := newTestServer(t, fs, nil)
	w := srv.get(t, "/api/v1/resources", nil)
	e := wantError(t, w, http.StatusInternalServerError, "internal")
	if strings.Contains(w.Body.String(), "secret") || strings.Contains(e.Message, "/var/lib") {
		t.Errorf("panic detail leaked: %s", w.Body.String())
	}
	if w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("security headers missing on 500")
	}
	logs := srv.logs.String()
	if !strings.Contains(logs, "panic in handler") || !strings.Contains(logs, `"status":500`) {
		t.Errorf("logs = %s", logs)
	}
}

func TestInternalErrorNotLeaked(t *testing.T) {
	s := newStore(t)
	bad := rec("bad", "x", model.ChangeAdded, model.OriginWatch, at(80), []byte("{corrupt /var/lib/chronos"))
	if err := s.Write(context.Background(), []store.Entry{bad}); err != nil {
		t.Fatal(err)
	}
	srv := newTestServer(t, s, nil)
	w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "bad", "at", ts(90)))
	e := wantError(t, w, http.StatusInternalServerError, "internal")
	if e.Message != "internal error" {
		t.Errorf("message = %q", e.Message)
	}
	if !strings.Contains(srv.logs.String(), "api request failed") {
		t.Error("internal error not logged")
	}
}

func TestRequestTimeout(t *testing.T) {
	fs := &faultStore{Store: newStore(t), view: func(ctx context.Context) (store.View, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	srv := newTestServer(t, fs, func(o *Options) { o.RequestTimeout = 20 * time.Millisecond })
	start := time.Now()
	e := wantError(t, srv.get(t, "/api/v1/resources", nil), http.StatusServiceUnavailable, "unavailable")
	if e.Message != "request timed out" {
		t.Errorf("message = %q", e.Message)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("timeout took %s", d)
	}
}

func TestSecurityHeaders(t *testing.T) {
	srv := newTestServer(t, newStore(t), func(o *Options) { o.Web = testWeb() })
	for _, path := range []string{"/api/v1/resources", "/api/v1/nope", "/healthz", "/", "/assets/app-1234.js"} {
		w := srv.get(t, path, nil)
		h := w.Header()
		for k, want := range map[string]string{
			"X-Content-Type-Options": "nosniff",
			"X-Frame-Options":        "DENY",
			"Referrer-Policy":        "no-referrer",
		} {
			if got := h.Get(k); got != want {
				t.Errorf("%s: %s = %q, want %q", path, k, got, want)
			}
		}
		csp := h.Get("Content-Security-Policy")
		if !strings.Contains(csp, "frame-ancestors 'none'") || strings.Contains(csp, "unsafe-") {
			t.Errorf("%s: CSP = %q", path, csp)
		}
		if strings.HasPrefix(path, "/api/") && !strings.HasPrefix(csp, "default-src 'none'") {
			t.Errorf("%s: API CSP = %q", path, csp)
		}
		if h.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("%s: CORS header present", path)
		}
	}
}

func testWeb() fs.FS {
	return fstest.MapFS{
		"index.html":          {Data: []byte("<!doctype html><title>Chronos</title>")},
		"assets/app-1234.js":  {Data: []byte("console.log(1)")},
		"assets/app-1234.css": {Data: []byte("body{}")},
		"favicon.svg":         {Data: []byte("<svg/>")},
		"assets/sub":          {Mode: fs.ModeDir},
	}
}

func TestSPA(t *testing.T) {
	srv := newTestServer(t, newStore(t), func(o *Options) { o.Web = testWeb() })
	html := http.Header{"Accept": {"text/html,application/xhtml+xml"}}
	tests := []struct {
		name, method, path string
		hdr                http.Header
		status             int
		ctype, cache, body string
	}{
		{"root", "GET", "/", nil, 200, "text/html; charset=utf-8", "no-cache", "<title>Chronos"},
		{"index.html", "GET", "/index.html", nil, 200, "text/html; charset=utf-8", "no-cache", "<title>Chronos"},
		{"hashed js", "GET", "/assets/app-1234.js", nil, 200, "text/javascript; charset=utf-8", "public, max-age=31536000, immutable", "console.log"},
		{"hashed css", "GET", "/assets/app-1234.css", nil, 200, "text/css; charset=utf-8", "public, max-age=31536000, immutable", "body{}"},
		{"svg", "GET", "/favicon.svg", nil, 200, "image/svg+xml", "no-cache", "<svg/>"},
		{"client route falls back", "GET", "/objects/v1/pods/ns/my.pod", html, 200, "text/html; charset=utf-8", "no-cache", "<title>Chronos"},
		{"directory falls back", "GET", "/assets/sub", html, 200, "text/html; charset=utf-8", "no-cache", "<title>Chronos"},
		{"missing asset is 404", "GET", "/assets/missing.js", nil, 404, "application/json; charset=utf-8", "no-store", "not_found"},
		{"HEAD", "HEAD", "/", nil, 200, "text/html; charset=utf-8", "no-cache", ""},
		{"api is never SPA", "GET", "/api/v1/nope", html, 404, "application/json; charset=utf-8", "no-store", "not_found"},
		{"POST", "POST", "/", nil, 405, "application/json; charset=utf-8", "no-store", "bad_request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := srv.do(t, tc.method, tc.path, tc.hdr)
			wantStatus(t, w, tc.status)
			if got := w.Header().Get("Content-Type"); got != tc.ctype {
				t.Errorf("content type = %q, want %q", got, tc.ctype)
			}
			if got := w.Header().Get("Cache-Control"); got != tc.cache {
				t.Errorf("cache control = %q, want %q", got, tc.cache)
			}
			if !strings.Contains(w.Body.String(), tc.body) {
				t.Errorf("body = %q, want it to contain %q", w.Body.String(), tc.body)
			}
		})
	}
}

// The mux cleans paths before routing; the SPA handler must not rely on it.
func TestSPATraversalWithoutMux(t *testing.T) {
	h := spa(testWeb())
	for _, p := range []string{"/../../etc/passwd", "/assets/../../index.html", "/a/../../../x"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.URL.Path = p
		req.Header.Set("Accept", "text/html")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "<title>Chronos") {
			t.Errorf("%s: status %d body %q", p, w.Code, w.Body.String())
		}
	}
}

func TestAccessLogOmitsContents(t *testing.T) {
	srv := newTestServer(t, newStore(t), nil)
	w := srv.get(t, "/api/v1/state", q("resource", pods, "namespace", "ns", "name", "a", "at", ts(25)))
	wantStatus(t, w, http.StatusOK)
	logs := srv.logs.String()
	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(logs)), &entry); err != nil {
		t.Fatalf("want exactly one JSON log line, got %q: %v", logs, err)
	}
	for k, want := range map[string]any{
		"msg": "http request", "method": "GET", "path": "/api/v1/state",
		"route": "/api/v1/state", "status": float64(200),
	} {
		if entry[k] != want {
			t.Errorf("log %s = %v, want %v", k, entry[k], want)
		}
	}
	for _, leak := range []string{"img:v2", "namespace=", "a1"} {
		if strings.Contains(logs, leak) {
			t.Errorf("access log contains %q: %s", leak, logs)
		}
	}
}

func TestNewRequiresEngine(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New without Engine did not panic")
		}
	}()
	New(Options{})
}

func TestNewHTTPServerTimeouts(t *testing.T) {
	s := NewHTTPServer(":0", http.NotFoundHandler())
	if s.ReadHeaderTimeout == 0 || s.WriteTimeout <= DefaultRequestTimeout || s.IdleTimeout == 0 {
		t.Errorf("timeouts not set: %+v", s)
	}
}
