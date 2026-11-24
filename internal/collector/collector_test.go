package collector

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/normalize"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

const cmID = "v1/configmaps"

func TestInitialSyncWritesBaselineThenStart(t *testing.T) {
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"), cm("ns", "b", "ub", "1"))
	h := start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})

	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Start")
	eventually(t, "synced status", func() bool { s := status(h.c, cmID); return s.Synced && s.CoverageOpen })

	recs := recordsOf(t, st, cmID)
	if recs[0].ResourceVersion == "" || !bytes.Contains(recs[0].Object, []byte(`"value":"1"`)) {
		t.Fatalf("record content: %+v %s", recs[0], recs[0].Object)
	}
	cov := coverageOf(t, st, cmID)
	if !cov[0].At.After(recs[1].ObservedAt) || cov[0].Reason != reasonInitialList {
		t.Fatalf("Start must follow the baseline records: %+v", cov)
	}
}

func TestWatchEvents(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, st, clk, []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")

	t1 := t0.Add(time.Second)
	clk.SetTime(t1)
	w.Add(srv.set(cm("ns", "b", "ub", "1")))
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))

	// No-op update: only resourceVersion and managedFields change.
	noop := cm("ns", "a", "ua", "2")
	noop.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate}})
	w.Modify(srv.set(noop))

	w.Delete(srv.del("ns", "b"))
	// Delete and recreate with the same name: new UID.
	w.Delete(srv.del("ns", "a"))
	w.Add(srv.set(cm("ns", "a", "ua2", "3")))
	// A new UID without a delete (should not happen on a continuous watch)
	// is recorded as Deleted + Added.
	w.Modify(srv.set(cm("ns", "a", "ua3", "4")))

	waitJournal(t, st, cmID,
		"Added/baseline ns/a ua", "Start",
		"Added/watch ns/b ub",
		"Modified/watch ns/a ua",
		"Deleted/watch ns/b ub",
		"Deleted/watch ns/a ua",
		"Added/watch ns/a ua2",
		"Deleted/watch ns/a ua2",
		"Added/watch ns/a ua3",
	)
	recs := recordsOf(t, st, cmID)
	for _, r := range recs[1:] {
		if r.ObservedAt.Before(t1) || r.ObservedAt.After(t1.Add(time.Microsecond)) {
			t.Errorf("ObservedAt %v not from the injected clock (%v)", r.ObservedAt, t1)
		}
	}
	// Delete records carry the final / last known object.
	if del := recs[3]; !bytes.Contains(del.Object, []byte(`"name":"b"`)) {
		t.Errorf("delete of b carries no object: %s", del.Object)
	}
	if del := recs[6]; !bytes.Contains(del.Object, []byte(`"uid":"ua2"`)) || !bytes.Contains(del.Object, []byte(`"value":"3"`)) {
		t.Errorf("implicit delete must carry the last stored state of ua2: %s", del.Object)
	}
	if s := status(h.c, cmID); !s.LastEventAt.Equal(t1) {
		t.Errorf("LastEventAt = %v, want %v", s.LastEventAt, t1)
	}
}

func TestTombstoneDeleteUsesLastStoredObject(t *testing.T) {
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")

	ss := sourceStore{h.c.res[0].src}
	if err := ss.Delete(cache.DeletedFinalStateUnknown{Key: "ns/a"}); err != nil {
		t.Fatal(err)
	}
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Deleted/watch ns/a ua")
	if del := recordsOf(t, st, cmID)[1]; !bytes.Contains(del.Object, []byte(`"value":"1"`)) {
		t.Fatalf("tombstone delete must carry last stored object: %s", del.Object)
	}
}

func TestCleanWatchCloseResumesWithoutGap(t *testing.T) {
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")

	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua")
	time.Sleep(1100 * time.Millisecond) // avoid the reflector's VeryShortWatchError path
	w.Stop()                            // server closes the stream normally (timeout)
	w2 := srv.watcher(t)
	w2.Modify(srv.set(cm("ns", "a", "ua", "3")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua", "Modified/watch ns/a ua")
	if n := srv.lists.Load(); n != 1 {
		t.Fatalf("resumed watch must not relist: %d lists", n)
	}
}

func TestRelistAfterGoneWritesEndResyncStart(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps,
		cm("ns", "a", "ua", "1"), cm("ns", "b", "ub", "1"), cm("ns", "c", "uc", "1"),
		cm("ns", "e", "ue", "1"), cm("ns", "f", "uf", "1"))
	start(t, st, clk, []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Added/baseline ns/c uc",
		"Added/baseline ns/e ue", "Added/baseline ns/f uf", "Start")

	t1 := t0.Add(time.Second)
	clk.SetTime(t1)
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Added/baseline ns/c uc",
		"Added/baseline ns/e ue", "Added/baseline ns/f uf", "Start", "Modified/watch ns/a ua")

	// The gap: changes the watcher never reports.
	t2 := t0.Add(time.Minute)
	clk.SetTime(t2)
	srv.set(cm("ns", "b", "ub", "2")) // modified
	srv.del("ns", "c")                // deleted
	srv.set(cm("ns", "d", "ud", "1")) // added
	srv.del("ns", "e")                // recreated with a new UID
	srv.set(cm("ns", "e", "ue2", "1"))
	// f unchanged.
	w.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old resource version"})

	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Added/baseline ns/c uc",
		"Added/baseline ns/e ue", "Added/baseline ns/f uf", "Start", "Modified/watch ns/a ua",
		"End",
		"Modified/resync ns/b ub",
		"Deleted/resync ns/c uc",
		"Added/resync ns/d ud",
		"Deleted/resync ns/e ue",
		"Added/resync ns/e ue2",
		"Start")

	cov := coverageOf(t, st, cmID)
	recs := recordsOf(t, st, cmID)
	end := cov[1]
	// Continuity is known up to the last event received (t1), not the
	// relist time: everything in (t1, t2] is a gap.
	if !end.At.Equal(recs[5].ObservedAt) || end.Reason != reasonRelist {
		t.Fatalf("End = %+v, want At = last watch record time %v", end, recs[5].ObservedAt)
	}
	for _, r := range recs[6:] {
		if r.ObservedAt.Before(t2) {
			t.Errorf("resync record %s at %v, want relist time %v", r.Key, r.ObservedAt, t2)
		}
	}
	if !cov[2].At.After(recs[len(recs)-1].ObservedAt) {
		t.Fatalf("Start must follow resync records")
	}
	if del := recs[7]; !bytes.Contains(del.Object, []byte(`"name":"c"`)) {
		t.Errorf("resync delete carries last known object: %s", del.Object)
	}
	if srv.lists.Load() != 2 {
		t.Errorf("lists = %d, want 2", srv.lists.Load())
	}
}

func TestWatchCallFailureRelists(t *testing.T) {
	// A non-retriable error from the Watch call itself (as opposed to an
	// error event) makes ListAndWatch return; the reflector relists.
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	srv.mu.Lock()
	srv.nextErr = apierrors.NewResourceExpired("gone")
	srv.mu.Unlock()
	srv.set(cm("ns", "a", "ua", "2"))
	time.Sleep(1100 * time.Millisecond)
	w.Stop()
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "End", "Modified/resync ns/a ua", "Start")
}

func TestStreamingListReplacesOnce(t *testing.T) {
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"), cm("ns", "b", "ub", "1"))
	srv.watchList = true
	start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Start")
	// The same stream continues as the live watch.
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Start", "Modified/watch ns/a ua")
	if srv.lists.Load() != 0 {
		t.Fatalf("WatchList path must not LIST, got %d", srv.lists.Load())
	}
}

func TestRestartAfterCleanShutdown(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"), cm("ns", "b", "ub", "1"))
	h := start(t, st, clk, []*fakeServer{srv})
	srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Start")
	clk.SetTime(t0.Add(time.Second))
	if err := h.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cov := coverageOf(t, st, cmID)
	if len(cov) != 2 || cov[1].Type != model.CoverageEnd || cov[1].Reason != reasonShutdown || !cov[1].At.Equal(t0.Add(time.Second)) {
		t.Fatalf("shutdown coverage: %+v", cov)
	}

	// While Chronos is down: a changes, b is deleted, c appears.
	srv.set(cm("ns", "a", "ua", "2"))
	srv.del("ns", "b")
	srv.set(cm("ns", "c", "uc", "1"))
	clk.SetTime(t0.Add(time.Hour))
	start(t, st, clk, []*fakeServer{srv})
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Added/baseline ns/b ub", "Start", "End",
		"Modified/resync ns/a ua", "Deleted/resync ns/b ub", "Added/resync ns/c uc", "Start")
}

func TestRestartUnchangedWritesNoRecords(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, st, clk, []*fakeServer{srv})
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	start(t, st, clk, []*fakeServer{srv})
	// Hash rebuilt from the stored bytes matches: no spurious resync.
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "End", "Start")
}

func TestRestartAfterCrashEndsAtHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name      string
		heartbeat time.Time
		wantEnd   time.Time
	}{
		{"heartbeat after last record", t0.Add(5 * time.Second), t0.Add(5 * time.Second)},
		{"last record after heartbeat", t0.Add(500 * time.Millisecond), t0.Add(time.Second)},
		{"no heartbeat", time.Time{}, t0.Add(time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := memstore.New()
			ctx := context.Background()
			a, _ := normalize.Normalize(configmaps.ResourceType, cm("ns", "a", "ua", "1"), normalize.Options{})
			// A previous run: Start, one record, then a crash (no End).
			err := st.Write(ctx, []store.Entry{
				coverage(cmID, model.CoverageStart, t0, reasonInitialList),
				record(&resource{rt: configmaps, id: cmID}, a, model.ChangeAdded, model.OriginWatch, t0.Add(time.Second)),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !tc.heartbeat.IsZero() {
				if err := st.SetHeartbeat(ctx, tc.heartbeat); err != nil {
					t.Fatal(err)
				}
			}
			srv := newServer(configmaps, cm("ns", "a", "ua", "2"))
			start(t, st, testingclock.NewFakeClock(t0.Add(time.Hour)), []*fakeServer{srv})
			waitJournal(t, st, cmID, "Start", "Added/watch ns/a ua", "End", "Modified/resync ns/a ua", "Start")
			cov := coverageOf(t, st, cmID)
			if !cov[1].At.Equal(tc.wantEnd) || cov[1].Reason != reasonCrash {
				t.Fatalf("crash End = %+v, want At %v", cov[1], tc.wantEnd)
			}
		})
	}
}

func TestGracefulShutdownWritesEndForEveryResource(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	s1 := newServer(configmaps, cm("ns", "a", "ua", "1"))
	s2 := newServer(secretsRT, cm("ns", "s", "us", ""))
	h := start(t, st, clk, []*fakeServer{s1, s2})
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	waitJournal(t, st, "v1/secrets", "Added/baseline ns/s us", "Start")
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{cmID, "v1/secrets"} {
		cov := coverageOf(t, st, id)
		if last := cov[len(cov)-1]; last.Type != model.CoverageEnd || last.Reason != reasonShutdown {
			t.Errorf("%s: last coverage %+v, want End(shutdown)", id, last)
		}
		if s := status(h.c, id); s.CoverageOpen || s.Synced {
			t.Errorf("%s: status after shutdown %+v", id, s)
		}
	}
}

func TestHeartbeatOnlyWhileHealthy(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, clk, []*fakeServer{srv}, func(c *Config) { c.HeartbeatInterval = 10 * time.Second })
	srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	var want time.Time
	eventually(t, "heartbeat", func() bool {
		if !clk.HasWaiters() {
			return false
		}
		clk.Step(10 * time.Second)
		want = clk.Now()
		hb, _ := st.Heartbeat(context.Background())
		return !hb.IsZero()
	})
	hb, _ := st.Heartbeat(context.Background())
	if hb.Before(t0.Add(10*time.Second)) || hb.After(want) {
		t.Fatalf("heartbeat %v outside [%v, %v]", hb, t0.Add(10*time.Second), want)
	}
}

func TestSecretsUseMetadataClientAndNeverStoreData(t *testing.T) {
	scheme := k8sruntime.NewScheme()
	if err := metav1.AddMetaToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secretGVR := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	cmGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

	// The dynamic client would return the full Secret, data included. The
	// collector must never ask it for secrets.
	full := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "db", "namespace": "prod", "uid": "us"},
		"data":     map[string]any{"password": "c3VwZXItc2VjcmV0"}, //nolint:gosec // fake test value
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(),
		map[schema.GroupVersionResource]string{secretGVR: "SecretList", cmGVR: "ConfigMapList"},
		full, cm("prod", "cfg", "uc", "1"))

	md := metadatafake.NewSimpleMetadataClient(scheme, &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "prod", UID: uid("us"), ResourceVersion: "5",
			Annotations: map[string]string{
				normalize.LastAppliedAnnotation: `{"data":{"password":"c3VwZXItc2VjcmV0"}}`,
				"owner":                         "team-a",
			}},
	})

	st := memstore.New()
	c, err := New(Config{
		Dynamic: dyn, Metadata: md, Store: st,
		Resources:     []resourcesType{configmaps, secretsRT},
		Clock:         testingclock.NewFakeClock(t0),
		Logger:        testLogger(),
		FlushInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	waitJournal(t, st, "v1/secrets", "Added/baseline prod/db us", "Start")
	waitJournal(t, st, cmID, "Added/baseline prod/cfg uc", "Start")
	eventually(t, "secrets watch", func() bool {
		for _, a := range md.Actions() {
			if a.GetVerb() == "watch" && a.GetResource() == secretGVR {
				return true
			}
		}
		return false
	})

	// A delete through the metadata watch.
	if err := md.Resource(secretGVR).Namespace("prod").Delete(ctx, "db", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitJournal(t, st, "v1/secrets", "Added/baseline prod/db us", "Start", "Deleted/watch prod/db us")

	for _, r := range recordsOf(t, st, "v1/secrets") {
		for _, bad := range []string{"c3VwZXItc2VjcmV0", `"data"`, "last-applied"} {
			if bytes.Contains(r.Object, []byte(bad)) {
				t.Fatalf("secret record contains %q: %s", bad, r.Object)
			}
		}
		if !bytes.Contains(r.Object, []byte(`"kind":"Secret"`)) || !bytes.Contains(r.Object, []byte(`"owner":"team-a"`)) {
			t.Fatalf("secret record lost metadata: %s", r.Object)
		}
	}
	for _, a := range dyn.Actions() {
		if a.GetResource().Resource == "secrets" {
			t.Fatalf("dynamic client used for secrets: %v %v", a.GetVerb(), a.GetResource())
		}
	}
}

func TestWriteFailureEndsCoverageAndRelists(t *testing.T) {
	fs := &failingStore{Store: memstore.New()}
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, fs, clk, []*fakeServer{srv}, func(c *Config) { c.WriteRetryTimeout = time.Second })
	w := srv.watcher(t)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start")

	fs.fail.Store(true)
	clk.SetTime(t0.Add(time.Minute))
	writes := fs.writes.Load()
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	eventually(t, "first failed write", func() bool { return fs.writes.Load() > writes })
	// Drive the writer's retry backoff on the fake clock until it gives up
	// and has forced at least one relist.
	eventually(t, "forced relist", func() bool {
		clk.Step(time.Second)
		return srv.lists.Load() >= 2
	})
	fs.fail.Store(false)
	stopStepping := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopStepping:
				return
			case <-time.After(5 * time.Millisecond):
				clk.Step(time.Second)
			}
		}
	}()
	defer close(stopStepping)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start", "End", "Modified/resync ns/a ua", "Start")
	cov := coverageOf(t, fs, cmID)
	if cov[1].Reason != reasonWriteFailure || cov[1].At.After(t0.Add(time.Minute)) {
		t.Fatalf("End = %+v, want reason %q at or before the first lost change", cov[1], reasonWriteFailure)
	}
}

func TestBackpressureLosesNothing(t *testing.T) {
	fs := &failingStore{Store: memstore.New()}
	srv := newServer(configmaps, cm("ns", "a", "ua", "0"))
	start(t, fs, testingclock.NewFakeClock(t0), []*fakeServer{srv}, func(c *Config) {
		c.QueueSize = 2
		c.MaxBatchSize = 3
	})
	w := srv.watcher(t)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start")

	gate := make(chan struct{})
	fs.mu.Lock()
	fs.gate = gate
	fs.mu.Unlock()
	const n = 50
	for i := 1; i <= n; i++ {
		w.Modify(srv.set(cm("ns", "a", "ua", strings.Repeat("x", i))))
	}
	// The writer is stuck, the 2-slot queue is full, and the reflector is
	// blocked in Update with the rest still in the watch channel.
	time.Sleep(50 * time.Millisecond)
	if got := len(recordsOf(t, fs, cmID)); got != 1 {
		t.Fatalf("records written while store blocked: %d", got)
	}
	fs.mu.Lock()
	fs.gate = nil
	fs.mu.Unlock()
	close(gate)
	eventually(t, "all modifications", func() bool { return len(recordsOf(t, fs, cmID)) == n+1 })
	if cov := coverageOf(t, fs, cmID); len(cov) != 1 {
		t.Fatalf("backpressure must not end coverage: %+v", cov)
	}
}

func TestRunStopsAllGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= before })
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("want error without store")
	}
	if _, err := New(Config{Store: memstore.New()}); err == nil {
		t.Fatal("want error without resources")
	}
	if _, err := New(Config{Store: memstore.New(), Resources: []resourcesType{secretsRT}}); err == nil {
		t.Fatal("want error: metadata-only type without metadata client")
	}
	if _, err := New(Config{Store: memstore.New(), Resources: []resourcesType{configmaps, configmaps},
		ListerWatcherFor: func(resourcesType) (cache.ListerWatcher, error) { return newServer(configmaps), nil }}); err == nil {
		t.Fatal("want error for duplicate resource")
	}
}

var _ watch.Interface = (*proxy)(nil)
