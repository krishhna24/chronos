package collector

// Regression tests for the adversarial review of the collector
// (2026-11-25). Each reproduces one finding; most are ported from the
// reviewer's review_*_test.go with their logging turned into assertions.

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/normalize"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

type flakyLW struct {
	*fakeServer
	failList  atomic.Bool
	listCalls atomic.Int32
}

func (f *flakyLW) List(o metav1.ListOptions) (runtime.Object, error) {
	f.listCalls.Add(1)
	if f.failList.Load() {
		return nil, errors.New("apiserver: too many requests")
	}
	return f.fakeServer.List(o)
}

// Finding 1: graceful shutdown while the watch is lost and relists keep
// failing must not claim continuity up to the shutdown instant.
func TestRegressionShutdownDuringFailingRelist(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	f := &flakyLW{fakeServer: srv}
	h := start(t, st, clk, []*fakeServer{srv}, func(c *Config) {
		c.ListerWatcherFor = func(resources.Type) (cache.ListerWatcher, error) { return f, nil }
	})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")

	t1 := t0.Add(time.Second)
	clk.SetTime(t1)
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua")

	f.failList.Store(true)
	srv.mu.Lock()
	srv.nextErr = apierrors.NewResourceExpired("gone")
	srv.mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	w.Stop()
	eventually(t, "failing relists", func() bool { return f.listCalls.Load() >= 3 })

	clk.SetTime(t0.Add(10 * time.Minute))
	srv.set(cm("ns", "a", "ua", "3")) // changes while nobody watches

	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	cov := coverageOf(t, st, cmID)
	end := cov[len(cov)-1]
	if end.Type != model.CoverageEnd {
		t.Fatalf("no End: %+v", cov)
	}
	if end.At.After(t1.Add(2 * time.Second)) {
		t.Fatalf("End.At = %v, but continuity was lost at ~%v", end.At, t1)
	}
	s, err := query.New(st, clk).State(context.Background(), model.ObjectKey{Resource: cmID, Namespace: "ns", Name: "a"}, t0.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if s.Coverage.State != api.CoverageGap {
		t.Fatalf("state at t0+5m classified %s, want gap", s.Coverage.State)
	}
}

// Finding 2: under backpressure, events received but not accepted at
// shutdown are dropped; End(shutdown) must not cover them.
func TestRegressionShutdownUnderBackpressure(t *testing.T) {
	fs := &failingStore{Store: memstore.New()}
	srv := newServer(configmaps, cm("ns", "a", "ua", "0"))
	clk := testingclock.NewFakeClock(t0)
	h := start(t, fs, clk, []*fakeServer{srv}, func(c *Config) {
		c.QueueSize = 2
		c.MaxBatchSize = 3
	})
	w := srv.watcher(t)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start")
	gate := make(chan struct{})
	fs.mu.Lock()
	fs.gate = gate
	fs.mu.Unlock()
	const n = 20
	for i := 1; i <= n; i++ {
		w.Modify(srv.set(cm("ns", "a", "ua", strings.Repeat("x", i))))
	}
	time.Sleep(50 * time.Millisecond)
	clk.SetTime(t0.Add(time.Minute))
	h.cancel()
	time.Sleep(100 * time.Millisecond)
	fs.mu.Lock()
	fs.gate = nil
	fs.mu.Unlock()
	close(gate)
	if err := <-h.done; err != nil {
		t.Logf("Run: %v", err)
	}
	h.done <- nil
	recs := recordsOf(t, fs, cmID)
	cov := coverageOf(t, fs, cmID)
	last := recs[len(recs)-1]
	end := cov[len(cov)-1]
	if end.Type != model.CoverageEnd {
		t.Fatalf("no End: %+v", cov)
	}
	if len(recs)-1 < n && end.At.After(last.ObservedAt.Add(time.Nanosecond)) {
		t.Fatalf("%d delivered changes lost, but coverage claims continuity until %v (last record %v)",
			n-(len(recs)-1), end.At, last.ObservedAt)
	}
}

// Finding 3: crash with open coverage, compaction removed the newest
// (delete) records, no heartbeat. Recovery must still place a valid End.
func TestRegressionRecoverAfterCompaction(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	r := &resource{rt: configmaps, id: cmID}
	a, _ := normalize.Normalize(configmaps.ResourceType, cm("ns", "a", "ua", "1"), normalize.Options{})
	if err := st.Write(ctx, []store.Entry{
		coverage(cmID, model.CoverageStart, t0, reasonInitialList),
		record(r, a, model.ChangeAdded, model.OriginWatch, t0.Add(time.Second)),
		record(r, a, model.ChangeDeleted, model.OriginWatch, t0.Add(2*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Compact(ctx, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	srv := newServer(configmaps)
	h := start(t, st, testingclock.NewFakeClock(t0.Add(2*time.Hour)), []*fakeServer{srv})
	// Compaction keeps the open Start (the latest event before the horizon).
	waitJournal(t, st, cmID, "Start", "End", "Start")
	if err := h.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cov := coverageOf(t, st, cmID)
	if cov[1].Reason != reasonCrash || !cov[1].At.After(t0.Add(2*time.Second)) {
		t.Fatalf("crash End = %+v, want after the compacted delete at %v", cov[1], t0.Add(2*time.Second))
	}
}

// Finding 4: a clean close must not advance continuity past the last
// event: the server may have dropped buffered events when closing.
func TestRegressionCleanCloseKeepsLastEventBound(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, clk, []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")

	clk.SetTime(t0.Add(time.Minute))
	srv.set(cm("ns", "a", "ua", "2")) // committed, never delivered
	clk.SetTime(t0.Add(3 * time.Minute))
	srv.mu.Lock()
	srv.nextErr = apierrors.NewResourceExpired("gone")
	srv.mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	w.Stop() // clean close
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "End", "Modified/resync ns/a ua", "Start")
	cov := coverageOf(t, st, cmID)
	if cov[1].At.After(t0.Add(time.Minute)) {
		t.Fatalf("End at %v is after an undelivered change at %v", cov[1].At, t0.Add(time.Minute))
	}
}

// hangStore's Write blocks forever and ignores ctx (like an fsync).
type hangStore struct {
	store.Store
	hang    atomic.Bool
	release chan struct{}
}

func (h *hangStore) Write(ctx context.Context, es []store.Entry) error {
	if h.hang.Load() {
		<-h.release
		return errors.New("released")
	}
	return h.Store.Write(ctx, es)
}

// Finding 5: Run must return within ShutdownTimeout even if a store Write
// never returns.
func TestRegressionShutdownBoundedWithBlockedStore(t *testing.T) {
	hs := &hangStore{Store: memstore.New(), release: make(chan struct{})}
	defer close(hs.release) // let the stuck writer goroutine exit
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	c, err := New(Config{Store: hs, Resources: []resources.Type{configmaps}, Clock: clk, Logger: testLogger(), FlushInterval: -1,
		ShutdownTimeout:  200 * time.Millisecond,
		ListerWatcherFor: func(resources.Type) (cache.ListerWatcher, error) { return srv, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	w := srv.watcher(t)
	waitJournal(t, hs, cmID, "Added/baseline ns/a ua", "Start")
	hs.hang.Store(true)
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil although the final writes never completed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return 3s after cancel with ShutdownTimeout=200ms")
	}
}

// Finding 6: an item that waits longer than StallTimeout to be accepted
// means the reflector stopped reading its stream: coverage ends at the
// moment the item was received and the resource is relisted.
func TestRegressionStallEndsCoverage(t *testing.T) {
	fs := &failingStore{Store: memstore.New()}
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "0"))
	h := start(t, fs, clk, []*fakeServer{srv}, func(c *Config) {
		c.QueueSize = 1
		c.MaxBatchSize = 1
		c.StallTimeout = 5 * time.Second
	})
	w := srv.watcher(t)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start")
	gate := make(chan struct{})
	fs.mu.Lock()
	fs.gate = gate
	fs.mu.Unlock()
	t1 := t0.Add(time.Second)
	clk.SetTime(t1)
	for i := 1; i <= 3; i++ { // writer busy, queue full, third blocks
		w.Modify(srv.set(cm("ns", "a", "ua", strings.Repeat("x", i))))
	}
	src := h.c.res[0].src
	eventually(t, "reflector blocked on the queue", func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return src.waiting
	})
	clk.Step(6 * time.Second) // the stall outlasts StallTimeout
	// D2: the writer is stuck, so the End cannot be written yet, but the
	// status already reports the loss.
	eventually(t, "LostAt while the writer is stuck", func() bool {
		s := status(h.c, cmID)
		return s.LostAt != nil && s.LostAt.Equal(t1)
	})
	time.Sleep(20 * time.Millisecond)
	fs.mu.Lock()
	fs.gate = nil
	fs.mu.Unlock()
	close(gate)
	eventually(t, "relist after stall", func() bool {
		cov := coverageOf(t, fs, cmID)
		return len(cov) == 3
	})
	cov := coverageOf(t, fs, cmID)
	if cov[1].Type != model.CoverageEnd || cov[1].Reason != reasonStall || cov[1].At.After(t1.Add(time.Microsecond)) {
		t.Fatalf("End = %+v, want reason %q at ~%v (when the blocked item was received)", cov[1], reasonStall, t1)
	}
	for _, r := range recordsOf(t, fs, cmID) {
		if r.Origin == model.OriginWatch && r.ObservedAt.After(t1.Add(time.Microsecond)) {
			t.Fatalf("watch record stamped %v, after its receipt at %v", r.ObservedAt, t1)
		}
	}
}

// Finding 7: a live End must lie strictly after the last record so that
// record classifies as observed.
func TestRegressionEndAfterLastRecord(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, clk, []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	clk.SetTime(t0.Add(time.Second))
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua")
	clk.SetTime(t0.Add(time.Minute))
	w.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua", "End", "Start")
	rec := recordsOf(t, st, cmID)[1]
	s, err := query.New(st, clk).ObjectAtSeq(context.Background(), model.ObjectKey{Resource: cmID, Namespace: "ns", Name: "a"}, rec.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if s.Coverage.State != api.CoverageObserved {
		t.Fatalf("watch record at %v classified %s, want observed", rec.ObservedAt, s.Coverage.State)
	}
}

// Finding 8: a watch Delete whose UID differs from the known instance
// deletes the known instance (inferred: origin resync) and leaves the key
// absent.
func TestRegressionDeleteWithUnknownUID(t *testing.T) {
	st := memstore.New()
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	start(t, st, testingclock.NewFakeClock(t0), []*fakeServer{srv})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	w.Delete(cm("ns", "a", "ub", "9")) // a different instance than the one known
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Deleted/resync ns/a ua")
	del := recordsOf(t, st, cmID)[1]
	if !strings.Contains(string(del.Object), `"value":"1"`) {
		t.Fatalf("delete must carry the last stored state of ua: %s", del.Object)
	}
	// The key is absent afterwards: a later Add is a fresh Added.
	w.Add(srv.set(cm("ns", "a", "uc", "1")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Deleted/resync ns/a ua", "Added/watch ns/a uc")
}

// captureHandler records log records for assertions.
type captureHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func newCapture() (*slog.Logger, func() []slog.Record) {
	h := captureHandler{mu: &sync.Mutex{}, recs: &[]slog.Record{}}
	return slog.New(h), func() []slog.Record {
		h.mu.Lock()
		defer h.mu.Unlock()
		return slices.Clone(*h.recs)
	}
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r.Clone())
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

// listApplied returns the coverageEnded attribute of every "list applied"
// log record, in order.
func listApplied(recs []slog.Record) []bool {
	var out []bool
	for _, r := range recs {
		if r.Message != "list applied" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "coverageEnded" {
				out = append(out, a.Value.Bool())
			}
			return true
		})
	}
	return out
}

// D4: "list applied" must report coverageEnded only when the batch held
// an End. A batch of just [Start] (empty or unchanged LIST) does not.
func TestRegressionListAppliedCoverageEndedAttr(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps) // empty LIST: batch is [Start]
	log, logs := newCapture()
	h := start(t, st, clk, []*fakeServer{srv}, func(c *Config) { c.Logger = log })
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Start")
	eventually(t, "source healthy", h.c.res[0].src.healthy)
	w.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})
	waitJournal(t, st, cmID, "Start", "End", "Start") // relist: [End, Start]
	eventually(t, "two list applied logs", func() bool { return len(listApplied(logs())) == 2 })
	if got := listApplied(logs()); got[0] || !got[1] {
		t.Fatalf("coverageEnded = %v, want [false true]", got)
	}
}

// D1 (ADR 0007 §1), receipt path: the process freezes, then the watch
// delivers the backlog. The first event received after the freeze trips
// the watchdog before it is queued, so no late-stamped change is recorded
// as observed: coverage ends at the last on-time observation and the
// relist re-derives the backlog as resync.
func TestRegressionWatchdogFirstEventAfterFreeze(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, st, clk, []*fakeServer{srv}, func(c *Config) {
		c.WatchdogInterval = time.Hour // only receipts observe in this test
		c.HeartbeatInterval = time.Hour
		c.StallTimeout = 5 * time.Second
	})
	w := srv.watcher(t)
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	eventually(t, "source healthy", h.c.res[0].src.healthy)

	t1 := t0.Add(time.Second)
	clk.SetTime(t1)
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua")

	// Frozen for a minute while the object keeps changing; on resume the
	// backlog arrives.
	clk.SetTime(t0.Add(time.Minute))
	w.Modify(srv.set(cm("ns", "a", "ua", "3")))
	w.Modify(srv.set(cm("ns", "a", "ua", "4")))

	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start", "Modified/watch ns/a ua",
		"End", "Modified/resync ns/a ua", "Start")
	cov := coverageOf(t, st, cmID)
	if cov[1].Reason != reasonProcessStall || cov[1].At.After(t1.Add(time.Microsecond)) {
		t.Fatalf("End = %+v, want reason %q at the last on-time observation %v", cov[1], reasonProcessStall, t1)
	}
	recs := recordsOf(t, st, cmID)
	if !strings.Contains(string(recs[2].Object), `"value":"4"`) {
		t.Fatalf("resync must carry the state after the freeze: %s", recs[2].Object)
	}
	for _, r := range recs {
		if r.Origin == model.OriginWatch && r.ObservedAt.After(t1.Add(time.Microsecond)) {
			t.Fatalf("late watch record recorded as observed at %v", r.ObservedAt)
		}
	}
	eventually(t, "healthy again", h.c.res[0].src.healthy)
}

// D1, ticker path: a freeze with no traffic is detected by the 1 s tick
// (and the late heartbeat), ending every resource's coverage.
func TestRegressionWatchdogTickerDetectsFreeze(t *testing.T) {
	st := memstore.New()
	clk := testingclock.NewFakeClock(t0)
	s1 := newServer(configmaps, cm("ns", "a", "ua", "1"))
	s2 := newServer(secretsRT, cm("ns", "s", "us", ""))
	h := start(t, st, clk, []*fakeServer{s1, s2}, func(c *Config) {
		c.WatchdogInterval = time.Second
		c.StallTimeout = 5 * time.Second
	})
	waitJournal(t, st, cmID, "Added/baseline ns/a ua", "Start")
	waitJournal(t, st, "v1/secrets", "Added/baseline ns/s us", "Start")
	eventually(t, "sources healthy", func() bool { return h.c.res[0].src.healthy() && h.c.res[1].src.healthy() })

	clk.SetTime(t0.Add(2 * time.Minute)) // the process did not run for two minutes
	for _, id := range []string{cmID, "v1/secrets"} {
		eventually(t, id+" relisted after the freeze", func() bool { return len(coverageOf(t, st, id)) == 3 })
		cov := coverageOf(t, st, id)
		if cov[1].Type != model.CoverageEnd || cov[1].Reason != reasonProcessStall || cov[1].At.After(t0.Add(time.Second)) {
			t.Fatalf("%s: End = %+v, want %q before the freeze", id, cov[1], reasonProcessStall)
		}
	}
	if hb, _ := st.Heartbeat(context.Background()); hb.After(t0.Add(time.Second)) {
		t.Fatalf("heartbeat %v written for the frozen window", hb)
	}
}

// D2 (ADR 0007 §2): while the store cannot persist the End, the status
// exposes the in-memory loss (LostAt) and the write error; once the End
// is committed LostAt is cleared.
func TestRegressionStatusLostAtUntilEndDurable(t *testing.T) {
	fs := &failingStore{Store: memstore.New()}
	clk := testingclock.NewFakeClock(t0)
	srv := newServer(configmaps, cm("ns", "a", "ua", "1"))
	h := start(t, fs, clk, []*fakeServer{srv}, func(c *Config) { c.WriteRetryTimeout = time.Second })
	w := srv.watcher(t)
	waitJournal(t, fs, cmID, "Added/baseline ns/a ua", "Start")
	if s := status(h.c, cmID); s.LostAt != nil || s.LastWriteError != "" || s.LastWriteErrorAt != nil {
		t.Fatalf("healthy status: %+v", s)
	}

	fs.fail.Store(true)
	t1 := t0.Add(time.Minute)
	clk.SetTime(t1)
	writes := fs.writes.Load()
	w.Modify(srv.set(cm("ns", "a", "ua", "2")))
	eventually(t, "first failed write", func() bool { return fs.writes.Load() > writes })
	eventually(t, "write error in status", func() bool { return status(h.c, cmID).LastWriteError != "" })
	if s := status(h.c, cmID); !strings.Contains(s.LastWriteError, errDisk.Error()) || s.LastWriteErrorAt == nil {
		t.Fatalf("status: %+v", s)
	}
	// The writer gives up; coverage is lost in memory but the End cannot
	// be written while the store keeps failing.
	eventually(t, "LostAt set", func() bool {
		clk.Step(time.Second)
		return status(h.c, cmID).LostAt != nil
	})
	s := status(h.c, cmID)
	if s.LostAt.After(t1) || !s.CoverageOpen || s.Synced {
		t.Fatalf("status while End not durable: %+v (LostAt %v)", s, *s.LostAt)
	}
	if cov := coverageOf(t, fs, cmID); len(cov) != 1 {
		t.Fatalf("End must not be durable yet: %+v", cov)
	}

	fs.fail.Store(false)
	eventually(t, "End committed and LostAt cleared", func() bool {
		clk.Step(time.Second)
		s := status(h.c, cmID)
		return len(coverageOf(t, fs, cmID)) == 3 && s.LostAt == nil && s.Synced
	})
	if s := status(h.c, cmID); s.LastWriteError == "" {
		t.Fatal("LastWriteError must persist after recovery")
	}
}
