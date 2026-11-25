package query

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/pebblestore"
)

// The pebblestore benchmarks reproduce the docs/performance.md list
// comparison (a 500-item ConfigMap page, all namespaces vs one namespace)
// on a store shaped like the perf dataset: 7,000 ~3 KB ConfigMaps in
// perf-0..perf-9, a snapshot manifest, then an update storm written after
// the manifest so List has to merge the time-index window.

const (
	perfConfigMaps = 7000
	perfNamespaces = 10
)

func configMapJSON(r *rand.Rand, ns, name string, rev int) []byte {
	buf := make([]byte, 1400)
	for i := range buf {
		buf[i] = byte(r.Uint32())
	}
	obj := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"namespace": ns, "name": name, "uid": ns + "/" + name,
			"labels":      map[string]string{"app": "perf"},
			"annotations": map[string]string{"rev": fmt.Sprint(rev)},
		},
		"data": map[string]string{"config.ini": hex.EncodeToString(buf)},
	}
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func perfPebbleEngine(b *testing.B, stormWrites int, settle, manifest bool) *Engine {
	b.Helper()
	ctx := context.Background()
	st, err := pebblestore.Open(b.TempDir(), pebblestore.Options{
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		SnapshotInterval: -1,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	r := rand.New(rand.NewPCG(7, 7))
	const cms = "v1/configmaps"
	key := func(i int) model.ObjectKey {
		return model.ObjectKey{Resource: cms, Namespace: fmt.Sprintf("perf-%d", i%perfNamespaces), Name: fmt.Sprintf("cm-%05d", i)}
	}
	t := at(10)
	var batch []store.Entry
	for i := range perfConfigMaps {
		k := key(i)
		batch = append(batch, store.Entry{Record: &model.Record{ObservedAt: t, Key: k, UID: k.String(), ResourceVersion: "1",
			Type: model.ChangeAdded, Origin: model.OriginBaseline, Object: configMapJSON(r, k.Namespace, k.Name, 0)}})
		if len(batch) == 500 {
			if err := st.Write(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	batch = append(batch, store.Entry{Coverage: &model.CoverageEvent{At: t, Resource: cms, Type: model.CoverageStart, Reason: "list"}})
	if err := st.Write(ctx, batch); err != nil {
		b.Fatal(err)
	}
	if manifest {
		if err := st.Snapshot(ctx); err != nil {
			b.Fatal(err)
		}
	}
	batch = batch[:0]
	for w := range stormWrites {
		k := key(r.IntN(perfConfigMaps))
		batch = append(batch, store.Entry{Record: &model.Record{ObservedAt: at(20), Key: k, UID: k.String(), ResourceVersion: fmt.Sprint(w + 2),
			Type: model.ChangeModified, Origin: model.OriginWatch, Object: configMapJSON(r, k.Namespace, k.Name, w+1)}})
		if len(batch) == 10 || w == stormWrites-1 {
			if err := st.Write(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if settle {
		// Without this, Pebble's background compactions of the bulk load
		// run during the measurement and dominate it.
		if err := st.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := st.CompactLSM(ctx); err != nil {
			b.Fatal(err)
		}
	}
	return New(st, testingclock.NewFakePassiveClock(at(1000)), WithMaxClockSkew(time.Hour))
}

// BenchmarkPebbleListPage compares a fixed all-namespaces first page, a
// fixed single-namespace page, and the access pattern chronosperf's
// list-ns scenario actually uses: a random namespace per request. The
// last is compared with all-namespaces pages that also rotate (random
// continue positions), so both touch the same amount of distinct data.
func BenchmarkPebbleListPage(b *testing.B) {
	for _, cfg := range []struct {
		storm    int
		manifest bool
	}{{0, false}, {0, true}, {30000, false}, {30000, true}} {
		storm := cfg.storm
		e := perfPebbleEngine(b, storm, true, cfg.manifest)
		ctx := context.Background()
		list := func(b *testing.B, req ListRequest) {
			req.Resource, req.At, req.Limit = "v1/configmaps", at(900), 500
			r, err := e.List(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if len(r.Items) == 0 {
				b.Fatal("empty page")
			}
		}
		// Continue tokens for every 500-item page of the full listing.
		var tokens []string
		req := ListRequest{Resource: "v1/configmaps", At: at(900), Limit: 500}
		for {
			r, err := e.List(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if r.Continue == "" {
				break
			}
			tokens = append(tokens, r.Continue)
			req.Continue = r.Continue
		}
		cases := []struct {
			name string
			req  func(i int) ListRequest
		}{
			{"all-ns-first-page", func(int) ListRequest { return ListRequest{} }},
			{"one-ns-fixed", func(int) ListRequest { return ListRequest{Namespace: "perf-3"} }},
			{"one-ns-random", func(i int) ListRequest {
				return ListRequest{Namespace: fmt.Sprintf("perf-%d", (i*7)%perfNamespaces)}
			}},
			{"all-ns-random-page", func(i int) ListRequest { return ListRequest{Continue: tokens[(i*7)%len(tokens)]} }},
		}
		for _, c := range cases {
			b.Run(fmt.Sprintf("storm=%d/manifest=%v/%s", storm, cfg.manifest, c.name), func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					list(b, c.req(i))
					i++
				}
			})
		}
	}
}
