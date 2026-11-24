package query

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// pagingEngine holds 3 namespaces x 9 pods (seconds after t0):
//
//	10   baseline Added for all, Start
//	20   every third pod deleted (watch)
//	40   End
//	60   resync Start (no records)
//
// Labels alternate app=web / app=db, so selectors and deletes interleave
// with page boundaries.
func pagingEngine(t testing.TB) (*Engine, *memstore.Store) {
	t.Helper()
	ctx := context.Background()
	mem := memstore.New()
	t.Cleanup(func() { _ = mem.Close() })
	var batch, dels []store.Entry
	for n := range 3 {
		ns := fmt.Sprintf("ns-%d", n)
		for i := range 9 {
			name := fmt.Sprintf("p%d", i)
			app := "web"
			if i%2 == 1 {
				app = "db"
			}
			uid := ns + "-" + name
			obj := podJSON(ns, name, uid, map[string]string{"app": app}, "img:v1")
			batch = append(batch, recEntry(podKey(ns, name), uid, model.ChangeAdded, model.OriginBaseline, at(10), obj))
			if i%3 == 0 {
				dels = append(dels, recEntry(podKey(ns, name), uid, model.ChangeDeleted, model.OriginWatch, at(20), obj))
			}
		}
	}
	batch = append(batch, covEntry(model.CoverageStart, at(10), "initial list"))
	for _, es := range [][]store.Entry{batch, dels, {covEntry(model.CoverageEnd, at(40), "watch expired")}, {covEntry(model.CoverageStart, at(60), "relist")}} {
		if err := mem.Write(ctx, es); err != nil {
			t.Fatal(err)
		}
	}
	return New(mem, testingclock.NewFakePassiveClock(at(100))), mem
}

// listAll pages through req with the given limit and returns the
// concatenated items and the number of pages.
func listAll(t *testing.T, e *Engine, req ListRequest, limit int) ([]api.ObjectState, int) {
	t.Helper()
	req.Limit = limit
	var items []api.ObjectState
	pages := 0
	for {
		r, err := e.List(context.Background(), req)
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		pages++
		if len(r.Items) > limit {
			t.Fatalf("page %d has %d items, limit %d", pages, len(r.Items), limit)
		}
		if r.Continue != "" && len(r.Items) != limit {
			t.Fatalf("page %d: continue set on a short page (%d items)", pages, len(r.Items))
		}
		items = append(items, r.Items...)
		if r.Continue == "" {
			return items, pages
		}
		if pages > 1000 {
			t.Fatal("paging does not terminate")
		}
		req.Continue = r.Continue
		req.At = r.At
	}
}

func TestListPagesConcatenateToUnpaged(t *testing.T) {
	e, _ := pagingEngine(t)
	ctx := context.Background()
	for _, at := range []time.Time{at(30), at(50)} { // observed, gap
		for _, ns := range []string{"", "ns-1"} {
			for _, sel := range []string{"", "app=web", "app=db", "app=none"} {
				for _, del := range []bool{false, true} {
					req := ListRequest{Resource: pods, Namespace: ns, At: at, LabelSelector: sel, IncludeDeleted: del}
					full := req
					full.Limit = ListMaxLimit
					want, err := e.List(ctx, full)
					if err != nil {
						t.Fatal(err)
					}
					if want.Continue != "" {
						t.Fatal("unpaged listing has a continue token")
					}
					for _, limit := range []int{1, 2, 3, 4, 7, len(want.Items), 100} {
						if limit == 0 {
							continue
						}
						name := fmt.Sprintf("at=%s/ns=%q/sel=%q/del=%v/limit=%d", at.Format("05"), ns, sel, del, limit)
						t.Run(name, func(t *testing.T) {
							got, pages := listAll(t, e, req, limit)
							if len(got) == 0 {
								got = []api.ObjectState{}
							}
							if !reflect.DeepEqual(got, want.Items) {
								t.Fatalf("paged (%d pages) != unpaged:\n got %v\nwant %v", pages, keysOf(got), keysOf(want.Items))
							}
							wantPages := max(1, (len(want.Items)+limit-1)/limit)
							if pages != wantPages {
								t.Errorf("pages = %d, want %d (no trailing empty page)", pages, wantPages)
							}
						})
					}
				}
			}
		}
	}
}

func keysOf(items []api.ObjectState) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.Key.Namespace+"/"+it.Key.Name)
	}
	return out
}

func TestListDefaultLimit(t *testing.T) {
	e, mem := pagingEngine(t)
	ctx := context.Background()
	var es []store.Entry
	for i := range ListDefaultLimit + 1 {
		name := fmt.Sprintf("bulk-%04d", i)
		es = append(es, recEntry(podKey("ns-9", name), name, model.ChangeAdded, model.OriginWatch, at(70),
			podJSON("ns-9", name, name, nil, "img")))
	}
	if err := mem.Write(ctx, es); err != nil {
		t.Fatal(err)
	}
	r, err := e.List(ctx, ListRequest{Resource: pods, Namespace: "ns-9", At: at(80)})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Items) != ListDefaultLimit || r.Continue == "" {
		t.Errorf("items = %d continue=%q, want %d and a token", len(r.Items), r.Continue, ListDefaultLimit)
	}
}

// A page must stop at the first item it will not return, so later keys are
// never decoded: here a corrupt object lies beyond the first page.
func TestListPageStopsEarly(t *testing.T) {
	e, mem := pagingEngine(t)
	ctx := context.Background()
	bad := recEntry(podKey("zz", "bad"), "bad", model.ChangeAdded, model.OriginWatch, at(70), []byte("{corrupt"))
	if err := mem.Write(ctx, []store.Entry{bad}); err != nil {
		t.Fatal(err)
	}
	r, err := e.List(ctx, ListRequest{Resource: pods, At: at(80), Limit: 1})
	if err != nil {
		t.Fatalf("first page touched a later key: %v", err)
	}
	if len(r.Items) != 1 || r.Continue == "" {
		t.Fatalf("page = %v continue=%q", keysOf(r.Items), r.Continue)
	}
	if _, err := e.List(ctx, ListRequest{Resource: pods, At: at(80), Limit: ListMaxLimit}); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("full listing err = %v, want ErrCorrupt", err)
	}
}

func TestListContinueValidation(t *testing.T) {
	e, _ := pagingEngine(t)
	ctx := context.Background()
	base := ListRequest{Resource: pods, At: at(30), LabelSelector: "app=web", Limit: 2}
	first, err := e.List(ctx, base)
	if err != nil || first.Continue == "" {
		t.Fatalf("first page: %v, continue %q", err, first.Continue)
	}
	tok := first.Continue
	valid := base
	valid.Continue = tok
	if _, err := e.List(ctx, valid); err != nil {
		t.Fatalf("valid continuation: %v", err)
	}
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	goodJSON := func(extra string) string {
		return enc(`{"v":1,"r":"v1/pods","at":"` + at(30).Format(time.RFC3339Nano) + `","sel":"app=web","kn":"p0"` + extra + `}`)
	}
	tests := []struct {
		name string
		mod  func(*ListRequest)
	}{
		{"different resource", func(r *ListRequest) { r.Resource = "v1/services" }},
		{"different namespace", func(r *ListRequest) { r.Namespace = "ns-1" }},
		{"different at", func(r *ListRequest) { r.At = at(31) }},
		{"missing at", func(r *ListRequest) { r.At = time.Time{} }},
		{"different selector", func(r *ListRequest) { r.LabelSelector = "app=db" }},
		{"different includeDeleted", func(r *ListRequest) { r.IncludeDeleted = true }},
		{"not base64", func(r *ListRequest) { r.Continue = "!!!" }},
		{"padded base64", func(r *ListRequest) { r.Continue = tok + "==" }},
		{"truncated", func(r *ListRequest) { r.Continue = tok[:len(tok)/2] }},
		{"not JSON", func(r *ListRequest) { r.Continue = enc("hello") }},
		{"JSON null", func(r *ListRequest) { r.Continue = enc("null") }},
		{"JSON array", func(r *ListRequest) { r.Continue = enc("[1,2]") }},
		{"unknown field", func(r *ListRequest) { r.Continue = goodJSON(`,"x":1`) }},
		{"trailing data", func(r *ListRequest) { r.Continue = goodJSON("") + enc("{}") }},
		{"wrong version", func(r *ListRequest) { r.Continue = enc(`{"v":2,"r":"v1/pods","kn":"a"}`) }},
		{"empty name", func(r *ListRequest) {
			r.Continue = enc(`{"v":1,"r":"v1/pods","at":"` + at(30).Format(time.RFC3339Nano) + `","sel":"app=web"}`)
		}},
		{"bad time", func(r *ListRequest) { r.Continue = enc(`{"v":1,"r":"v1/pods","at":"yesterday","kn":"a"}`) }},
		{"too long", func(r *ListRequest) { r.Continue = strings.Repeat("A", maxTokenLen+1) }},
		{"limit too large", func(r *ListRequest) { r.Limit = ListMaxLimit + 1 }},
		{"negative limit", func(r *ListRequest) { r.Limit = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mod(&req)
			if _, err := e.List(ctx, req); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
	// A hand-made token that is well formed is honored: forging one only
	// moves the cursor.
	req := valid
	req.Continue = goodJSON("")
	r, err := e.List(ctx, req)
	if err != nil {
		t.Fatalf("well-formed token: %v", err)
	}
	if len(r.Items) == 0 || r.Items[0].Key.Name != "p2" {
		t.Errorf("after p0 (ns \"\"), got %v", keysOf(r.Items))
	}
}

func FuzzListContinue(f *testing.F) {
	e, _ := pagingEngine(f)
	r, err := e.List(context.Background(), ListRequest{Resource: pods, At: at(30), Limit: 2})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(r.Continue)
	f.Add("")
	f.Add("e30")
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"r":"v1/pods","kn":"\u0000"}`)))
	f.Fuzz(func(t *testing.T, tok string) {
		_, err := e.List(context.Background(), ListRequest{Resource: pods, At: at(30), Limit: 2, Continue: tok})
		if err != nil && !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("token %q: err = %v, want nil or ErrInvalidArgument", tok, err)
		}
	})
}

// reversedStore lists keys in the wrong order, as a buggy store might.
type reversedStore struct{ store.Store }

func (s reversedStore) View(ctx context.Context) (store.View, error) {
	v, err := s.Store.View(ctx)
	return reversedView{v}, err
}

type reversedView struct{ store.View }

func (v reversedView) List(ctx context.Context, o store.ListOptions, fn func(model.Record) error) error {
	var rs []model.Record
	if err := v.View.List(ctx, o, func(r model.Record) error { rs = append(rs, r); return nil }); err != nil {
		return err
	}
	for i := len(rs) - 1; i >= 0; i-- {
		if err := fn(rs[i]); err != nil {
			return err
		}
	}
	return nil
}

func TestListRejectsUnorderedStore(t *testing.T) {
	_, mem := pagingEngine(t)
	e := New(reversedStore{mem}, testingclock.NewFakePassiveClock(at(100)))
	_, err := e.List(context.Background(), ListRequest{Resource: pods, At: at(30)})
	if err == nil || errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "order") {
		t.Fatalf("err = %v, want an internal ordering error", err)
	}
}
