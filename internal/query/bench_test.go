package query

import (
	"context"
	"fmt"
	"testing"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// These benchmarks measure engine overhead (coverage classification, JSON
// validation and copying, label matching, sorting) on top of memstore. They
// say nothing about storage latency: memstore is an in-memory map with
// per-View map copies, so its cost profile is unlike pebblestore's.

const benchObjects = 10000

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	ctx := context.Background()
	mem := memstore.New()
	b.Cleanup(func() { _ = mem.Close() })
	var batch []store.Entry
	for i := range benchObjects {
		ns := fmt.Sprintf("ns-%02d", i%20)
		name := fmt.Sprintf("pod-%05d", i)
		app := "web"
		if i%2 == 1 {
			app = "db"
		}
		uid := fmt.Sprintf("uid-%d", i)
		batch = append(batch, recEntry(podKey(ns, name), uid, model.ChangeAdded, model.OriginBaseline, at(10),
			podJSON(ns, name, uid, map[string]string{"app": app}, "img:v1")))
	}
	batch = append(batch, covEntry(model.CoverageStart, at(10), "initial list"))
	if err := mem.Write(ctx, batch); err != nil {
		b.Fatal(err)
	}
	// One modification per object, so every key has two records.
	batch = batch[:0]
	for i := range benchObjects {
		ns := fmt.Sprintf("ns-%02d", i%20)
		name := fmt.Sprintf("pod-%05d", i)
		uid := fmt.Sprintf("uid-%d", i)
		batch = append(batch, recEntry(podKey(ns, name), uid, model.ChangeModified, model.OriginWatch, at(20),
			podJSON(ns, name, uid, map[string]string{"app": "web"}, "img:v2")))
	}
	if err := mem.Write(ctx, batch); err != nil {
		b.Fatal(err)
	}
	return New(mem, testingclock.NewFakePassiveClock(at(100)))
}

func BenchmarkState(b *testing.B) {
	e := benchEngine(b)
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := podKey(fmt.Sprintf("ns-%02d", i%20), fmt.Sprintf("pod-%05d", i%benchObjects))
		if _, err := e.State(ctx, k, at(50)); err != nil {
			b.Fatal(err)
		}
		i += 7919
	}
}

// BenchmarkList measures single pages (the unit a request now costs) and a
// full paged walk. "page-10" starts after 4500 keys: View.List has no start
// key, so its cost includes visiting (not decoding) every earlier key.
func BenchmarkList(b *testing.B) {
	e := benchEngine(b)
	ctx := context.Background()
	tokenAt := func(req ListRequest, page int) string {
		for range page - 1 {
			r, err := e.List(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			req.Continue = r.Continue
		}
		return req.Continue
	}
	all := ListRequest{Resource: pods, At: at(50)}
	sel := ListRequest{Resource: pods, At: at(50), LabelSelector: "app=web"}
	for _, bc := range []struct {
		name string
		req  ListRequest
		want int
	}{
		{"page-1", all, ListDefaultLimit},
		{"page-10", ListRequest{Resource: pods, At: at(50), Continue: tokenAt(all, 10)}, ListDefaultLimit},
		{"page-1-one-namespace", ListRequest{Resource: pods, Namespace: "ns-03", At: at(50)}, benchObjects / 20},
		{"page-1-selector", sel, ListDefaultLimit},
		{"page-1-limit-5000", ListRequest{Resource: pods, At: at(50), Limit: ListMaxLimit}, ListMaxLimit},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r, err := e.List(ctx, bc.req)
				if err != nil {
					b.Fatal(err)
				}
				if len(r.Items) != bc.want {
					b.Fatalf("items = %d, want %d", len(r.Items), bc.want)
				}
			}
		})
	}
	b.Run("all-pages", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			req, n := all, 0
			for {
				r, err := e.List(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				n += len(r.Items)
				if r.Continue == "" {
					break
				}
				req.Continue = r.Continue
			}
			if n != benchObjects {
				b.Fatalf("items = %d", n)
			}
		}
	})
}

// BenchmarkMemstoreViewOnly is the floor under BenchmarkState: memstore's
// View copies its per-key map, so most of State's cost on memstore is the
// store, not the engine.
func BenchmarkMemstoreViewOnly(b *testing.B) {
	e := benchEngine(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		v, err := e.store.View(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = v.Close()
	}
}
