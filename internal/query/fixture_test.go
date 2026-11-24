package query

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

const pods = "v1/pods"

var t0 = time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

func podKey(ns, name string) model.ObjectKey {
	return model.ObjectKey{Resource: pods, Namespace: ns, Name: name}
}

func podJSON(ns, name, uid string, labels map[string]string, image string) []byte {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"namespace": ns, "name": name, "uid": uid, "labels": labels,
		},
		"spec": map[string]any{
			"containers": []any{map[string]any{"name": "app", "image": image}},
		},
	}
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return b
}

func recEntry(k model.ObjectKey, uid string, typ model.ChangeType, origin model.Origin, t time.Time, obj []byte) store.Entry {
	return store.Entry{Record: &model.Record{
		ObservedAt: t, Key: k, UID: uid, ResourceVersion: "1",
		Type: typ, Origin: origin, Object: obj,
	}}
}

func covEntry(typ model.CoverageType, t time.Time, reason string) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: t, Resource: pods, Type: typ, Reason: reason}}
}

// fixture is the shared timeline used by the engine tests (seconds after t0):
//
//	10   batch: baseline Added ns/a (uid a1, app=web), ns/b (uid b1, app=db), Start "initial list"
//	20   watch Modified ns/a (image v2)
//	30   watch Deleted ns/b
//	40   End "watch expired"
//	60   batch: resync Modified ns/a (image v3), resync Added other/c, Start "relist"
//	70   watch Deleted ns/a (uid a1)
//	71   watch Added ns/a (uid a2, image v4)
//	now = 100
//
// Records in a batch share a requested time, so the store clamps them (and
// the Start after them) to successive nanoseconds; tests that probe exact
// boundaries use the times written back into the entries.
type fixture struct {
	store   *countingStore
	mem     *memstore.Store
	clock   *testingclock.FakePassiveClock
	engine  *Engine
	start1  time.Time // first Start.At
	end1    time.Time // End.At
	start2  time.Time // second Start.At
	resyncA model.Record
	resyncC model.Record
	modA    model.Record
	baseA   model.Record
	delB    model.Record
	delA    model.Record
	addA2   model.Record
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	mem := memstore.New()
	t.Cleanup(func() { _ = mem.Close() })
	f := &fixture{mem: mem, store: &countingStore{Store: mem}, clock: testingclock.NewFakePassiveClock(at(100))}
	f.engine = New(f.store, f.clock)

	write := func(es ...store.Entry) {
		t.Helper()
		if err := mem.Write(ctx, es); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	web := map[string]string{"app": "web", "tier": "front"}
	db := map[string]string{"app": "db"}

	b1 := []store.Entry{
		recEntry(podKey("ns", "a"), "a1", model.ChangeAdded, model.OriginBaseline, at(10), podJSON("ns", "a", "a1", web, "img:v1")),
		recEntry(podKey("ns", "b"), "b1", model.ChangeAdded, model.OriginBaseline, at(10), podJSON("ns", "b", "b1", db, "db:v1")),
		covEntry(model.CoverageStart, at(10), "initial list"),
	}
	write(b1...)
	f.baseA, f.start1 = *b1[0].Record, b1[2].Coverage.At

	mod := recEntry(podKey("ns", "a"), "a1", model.ChangeModified, model.OriginWatch, at(20), podJSON("ns", "a", "a1", web, "img:v2"))
	write(mod)
	f.modA = *mod.Record
	delB := recEntry(podKey("ns", "b"), "b1", model.ChangeDeleted, model.OriginWatch, at(30), podJSON("ns", "b", "b1", db, "db:v1"))
	write(delB)
	f.delB = *delB.Record
	end := covEntry(model.CoverageEnd, at(40), "watch expired")
	write(end)
	f.end1 = end.Coverage.At

	b2 := []store.Entry{
		recEntry(podKey("ns", "a"), "a1", model.ChangeModified, model.OriginResync, at(60), podJSON("ns", "a", "a1", web, "img:v3")),
		recEntry(podKey("other", "c"), "c1", model.ChangeAdded, model.OriginResync, at(60), podJSON("other", "c", "c1", web, "img:v1")),
		covEntry(model.CoverageStart, at(60), "relist"),
	}
	write(b2...)
	f.resyncA, f.resyncC, f.start2 = *b2[0].Record, *b2[1].Record, b2[2].Coverage.At

	delA := recEntry(podKey("ns", "a"), "a1", model.ChangeDeleted, model.OriginWatch, at(70), podJSON("ns", "a", "a1", web, "img:v3"))
	write(delA)
	f.delA = *delA.Record
	addA2 := recEntry(podKey("ns", "a"), "a2", model.ChangeAdded, model.OriginWatch, at(71), podJSON("ns", "a", "a2", web, "img:v4"))
	write(addA2)
	f.addA2 = *addA2.Record
	return f
}

// countingStore records how many views were opened and closed, so tests
// can assert that every engine call uses one view and releases it.
type countingStore struct {
	store.Store
	opened, closed atomic.Int64
}

func (s *countingStore) View(ctx context.Context) (store.View, error) {
	v, err := s.Store.View(ctx)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &countingView{View: v, s: s}, nil
}

type countingView struct {
	store.View
	s *countingStore
}

func (v *countingView) Close() error {
	v.s.closed.Add(1)
	return v.View.Close()
}

// oneView asserts that fn opened exactly one view and closed it.
func (f *fixture) oneView(t *testing.T, fn func()) {
	t.Helper()
	o, c := f.store.opened.Load(), f.store.closed.Load()
	fn()
	if d := f.store.opened.Load() - o; d != 1 {
		t.Errorf("views opened = %d, want 1", d)
	}
	if d := f.store.closed.Load() - c; d != 1 {
		t.Errorf("views closed = %d, want 1", d)
	}
}
