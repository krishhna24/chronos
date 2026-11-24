package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

var t0 = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

func mustType(id string) resources.Type {
	t, ok := resources.ByID(id)
	if !ok {
		panic(id)
	}
	return t
}

var (
	configmaps = mustType("v1/configmaps")
	secretsRT  = mustType("v1/secrets")
)

// fakeServer is an in-memory API server for one resource type. Tests
// mutate its state and push watch events explicitly, so they can also
// change state "inside a gap" without telling the watcher.
type fakeServer struct {
	rt        resources.Type
	watchList bool // support streaming lists (SendInitialEvents)

	mu      sync.Mutex
	objs    map[string]*unstructured.Unstructured
	rv      int
	cur     *watch.RaceFreeFakeWatcher
	nextErr error // returned by the next Watch call

	watchers chan *watch.RaceFreeFakeWatcher
	lists    atomic.Int32
	watches  atomic.Int32
}

func newServer(rt resources.Type, objs ...*unstructured.Unstructured) *fakeServer {
	s := &fakeServer{rt: rt, objs: map[string]*unstructured.Unstructured{}, watchers: make(chan *watch.RaceFreeFakeWatcher, 64)}
	for _, o := range objs {
		s.set(o)
	}
	return s
}

func (s *fakeServer) set(o *unstructured.Unstructured) *unstructured.Unstructured {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rv++
	o = o.DeepCopy()
	o.SetResourceVersion(strconv.Itoa(s.rv))
	s.objs[o.GetNamespace()+"/"+o.GetName()] = o
	return o.DeepCopy()
}

func (s *fakeServer) del(ns, name string) *unstructured.Unstructured {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rv++
	o := s.objs[ns+"/"+name]
	delete(s.objs, ns+"/"+name)
	o = o.DeepCopy()
	o.SetResourceVersion(strconv.Itoa(s.rv))
	return o
}

func (s *fakeServer) IsWatchListSemanticsUnSupported() bool { return !s.watchList }

func (s *fakeServer) snapshot() ([]*unstructured.Unstructured, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objs))
	for k := range s.objs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*unstructured.Unstructured, 0, len(keys))
	for _, k := range keys {
		out = append(out, s.objs[k].DeepCopy())
	}
	return out, strconv.Itoa(s.rv)
}

func (s *fakeServer) List(metav1.ListOptions) (runtime.Object, error) {
	s.lists.Add(1)
	items, rv := s.snapshot()
	if s.rt.MetadataOnly {
		l := &metav1.PartialObjectMetadataList{ListMeta: metav1.ListMeta{ResourceVersion: rv}}
		for _, o := range items {
			l.Items = append(l.Items, *toPartial(o))
		}
		return l, nil
	}
	l := &unstructured.UnstructuredList{Object: map[string]any{
		"apiVersion": "v1", "kind": s.rt.Kind + "List",
		"metadata": map[string]any{"resourceVersion": rv},
	}}
	for _, o := range items {
		l.Items = append(l.Items, *o)
	}
	return l, nil
}

func (s *fakeServer) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	s.watches.Add(1)
	s.mu.Lock()
	if err := s.nextErr; err != nil {
		s.nextErr = nil
		s.mu.Unlock()
		return nil, err
	}
	w := watch.NewRaceFreeFake()
	s.cur = w
	s.mu.Unlock()
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
		items, rv := s.snapshot()
		for _, o := range items {
			w.Add(s.wire(o))
		}
		bm := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": s.rt.Kind,
			"metadata": map[string]any{
				"resourceVersion": rv,
				"annotations":     map[string]any{metav1.InitialEventsAnnotationKey: "true"},
			},
		}}
		w.Action(watch.Bookmark, bm)
	}
	s.watchers <- w
	return w, nil
}

// wire converts to the type the reflector expects for this resource.
func (s *fakeServer) wire(o *unstructured.Unstructured) runtime.Object {
	if s.rt.MetadataOnly {
		return toPartial(o)
	}
	return o
}

func toPartial(o *unstructured.Unstructured) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"},
		ObjectMeta: metav1.ObjectMeta{
			Name: o.GetName(), Namespace: o.GetNamespace(), UID: o.GetUID(),
			ResourceVersion: o.GetResourceVersion(), Labels: o.GetLabels(), Annotations: o.GetAnnotations(),
		},
	}
}

// watcher waits for the next watch stream the reflector opens.
func (s *fakeServer) watcher(t *testing.T) *watch.RaceFreeFakeWatcher {
	t.Helper()
	select {
	case w := <-s.watchers:
		return w
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: reflector did not open a watch", s.rt.ID())
		return nil
	}
}

func cm(ns, name, uid, value string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": ns, "name": name, "uid": uid},
		"data":     map[string]any{"value": value},
	}}
}

// --- harness ------------------------------------------------------------------

type harness struct {
	t      *testing.T
	st     store.Store
	mem    *memstore.Store
	clk    *testingclock.FakeClock
	c      *Collector
	cancel context.CancelFunc
	done   chan error
}

type opt func(*Config)

func testLogger() *slog.Logger {
	if os.Getenv("CHRONOS_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func start(t *testing.T, st store.Store, clk *testingclock.FakeClock, servers []*fakeServer, opts ...opt) *harness {
	t.Helper()
	byID := map[string]*fakeServer{}
	var rts []resources.Type
	for _, s := range servers {
		byID[s.rt.ID()] = s
		rts = append(rts, s.rt)
	}
	cfg := Config{
		Store:         st,
		Resources:     rts,
		Clock:         clk,
		Logger:        testLogger(),
		FlushInterval: -1,
		ListerWatcherFor: func(rt resources.Type) (cache.ListerWatcher, error) {
			s, ok := byID[rt.ID()]
			if !ok {
				return nil, fmt.Errorf("no server for %s", rt.ID())
			}
			return s, nil
		},
		ReflectorBackoff: &wait.Backoff{Duration: 5 * time.Millisecond, Factor: 1, Steps: 1 << 30, Cap: 5 * time.Millisecond},
	}
	for _, o := range opts {
		o(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, st: st, clk: clk, c: c, cancel: cancel, done: make(chan error, 1)}
	h.mem, _ = st.(*memstore.Store)
	go func() { h.done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("collector did not stop")
		}
	})
	return h
}

func (h *harness) stop() error {
	h.t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		h.done <- err // let Cleanup observe it too
		return err
	case <-time.After(10 * time.Second):
		h.t.Fatal("collector did not stop")
		return nil
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func coverageOf(t *testing.T, st store.Store, id string) []model.CoverageEvent {
	t.Helper()
	v, err := st.View(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	c, err := v.Coverage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func recordsOf(t *testing.T, st store.Store, id string) []model.Record {
	t.Helper()
	v, err := st.View(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	var out []model.Record
	err = v.Changes(context.Background(), store.ChangesOptions{From: time.Time{}, To: farFuture, Resource: id}, func(r model.Record) error {
		r.Object = append([]byte(nil), r.Object...)
		out = append(out, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// journal is a compact, comparable rendering of records and coverage in
// Seq order: "Start", "End", or "<Type>/<origin> <ns>/<name> <uid>".
func journal(t *testing.T, st store.Store, id string) []string {
	t.Helper()
	type ev struct {
		seq uint64
		s   string
	}
	var evs []ev
	for _, r := range recordsOf(t, st, id) {
		evs = append(evs, ev{r.Seq, fmt.Sprintf("%s/%s %s/%s %s", r.Type, r.Origin, r.Key.Namespace, r.Key.Name, r.UID)})
	}
	for _, c := range coverageOf(t, st, id) {
		evs = append(evs, ev{c.Seq, c.Type.String()})
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].seq < evs[j].seq })
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.s
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func waitJournal(t *testing.T, st store.Store, id string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := journal(t, st, id)
		if equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("journal of %s:\n got  %q\n want %q", id, got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func status(c *Collector, id string) ResourceStatus {
	for _, s := range c.Status() {
		if s.Resource == id {
			return s
		}
	}
	return ResourceStatus{}
}

// failingStore wraps a store and fails or blocks writes on demand.
type failingStore struct {
	store.Store
	fail   atomic.Bool
	gate   chan struct{} // if non-nil, writes wait for it to close
	writes atomic.Int32
	mu     sync.Mutex
}

var errDisk = errors.New("disk on fire")

func (f *failingStore) Write(ctx context.Context, es []store.Entry) error {
	f.writes.Add(1)
	f.mu.Lock()
	g := f.gate
	f.mu.Unlock()
	if g != nil {
		select {
		case <-g:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.fail.Load() {
		return errDisk
	}
	return f.Store.Write(ctx, es)
}

func uid(s string) types.UID { return types.UID(s) }

type resourcesType = resources.Type
