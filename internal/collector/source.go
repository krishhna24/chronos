package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/watchlist"
	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/normalize"
)

// source is the reflector-facing half of one resource type. It plays two
// roles for the same cache.Reflector:
//
//   - sourceLW wraps the real ListerWatcher. Every watch stream is passed
//     through a proxy so the source knows whether a stream is live and can
//     force a relist by closing the stream and failing the next Watch call
//     with 410 Expired.
//   - sourceStore is the reflector's store. It normalizes objects and puts
//     them on the writer queue; it never returns errors for Add/Update/
//     Delete, because the reflector only logs those (handleAnyWatch) and
//     the event would be lost silently.
//
// contUntil is the latest instant up to which the source knows it saw
// every change: the receipt time of the last accepted watch event, the
// last bookmark, or the last applied LIST. A stream that merely closes
// does not advance it: the apiserver may drop events buffered for a
// watcher when it terminates the watch, so only delivered events prove
// progress. It is frozen between the start of a LIST (or streaming list)
// and the Replace that applies it; its value at Replace time is the
// End.At of the gap the relist closes, and when the source is not healthy
// at shutdown it bounds the shutdown End.
type source struct {
	res   *resource
	inner cache.ListerWatcherWithContext
	// unsupportedWatchList mirrors the wrapped client, so the reflector's
	// WatchList decision is the one it would have made without us.
	unsupportedWatchList bool
	q                    *queue
	clock                clock.Clock
	stallTimeout         time.Duration
	norm                 normalize.Options
	log                  *slog.Logger

	ctx context.Context // set by Run before the reflector starts

	mu              sync.Mutex
	contUntil       time.Time
	awaitingReplace bool // a LIST / streaming list is in progress
	replaced        bool // at least one Replace in this process
	live            bool // a watch stream is open
	relist          bool // forced relist requested by the writer
	cur             *proxy
	lastEventAt     time.Time
	waiting         bool // an item is stamped but not yet accepted by the queue
	stalled         bool // an item waited > stallTimeout; cleared by relistDone
	dropped         bool // an item was received but never accepted (shutdown)
	healthyAtStop   bool
	acceptedEvents  uint64 // watch events accepted by the queue (tests, debugging)
	// relistGen counts forced-relist requests; listGen is its value when
	// the LIST in progress started. A Replace satisfies the pending
	// request only if its LIST started after it (listGen == relistGen).
	relistGen, listGen uint64
	// lossAt: continuity lost here (queue stall, process stall) and not
	// yet known to be durable as an End. Reported in ResourceStatus.LostAt.
	lossAt time.Time

	wd *watchdog // nil: disabled
}

func newSource(res *resource, lw cache.ListerWatcher, q *queue, c clock.Clock, stall time.Duration, norm normalize.Options, log *slog.Logger) *source {
	return &source{
		res:                  res,
		inner:                cache.ToListerWatcherWithContext(lw),
		unsupportedWatchList: watchlist.DoesClientNotSupportWatchListSemantics(lw),
		q:                    q,
		clock:                c,
		stallTimeout:         stall,
		norm:                 norm,
		log:                  log,
	}
}

func (s *source) rt() model.ResourceType { return s.res.rt.ResourceType }

// healthy reports whether the source is synced, has a live watch, and has
// no received item still waiting for queue space.
func (s *source) healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthyLocked() && !s.waiting
}

func (s *source) healthyLocked() bool {
	return s.replaced && s.live && !s.awaitingReplace && !s.relist && !s.stalled && !s.dropped
}

// stopping is called by Run just before the reflectors are stopped. It
// records whether continuity held at that moment.
func (s *source) stopping() {
	s.mu.Lock()
	s.healthyAtStop = s.healthyLocked()
	s.mu.Unlock()
}

// shutdownBound reports whether the shutdown End must be bounded by
// contUntil instead of the shutdown time, and returns that bound. That is
// the case when the source was not healthy when shutdown began (watch
// lost, relist in progress or failing, stalled), or when it dropped a
// received item during shutdown (its change is in no record, so coverage
// must not include it).
func (s *source) shutdownBound() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contUntil, !s.healthyAtStop || s.dropped
}

func (s *source) lastEvent() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEventAt
}

func (s *source) advance(t time.Time) {
	if !s.awaitingReplace && t.After(s.contUntil) {
		s.contUntil = t
	}
}

// requestRelist makes the reflector drop its watch and relist: the current
// stream is closed and the next Watch call fails with 410 Expired, which
// reflector.watch treats as non-retriable (ListAndWatch returns, and the
// next iteration lists).
func (s *source) requestRelist() {
	s.mu.Lock()
	s.relist = true
	s.relistGen++
	p := s.cur
	s.mu.Unlock()
	if p != nil {
		p.Stop()
	}
}

// startListLocked marks the start of a LIST or streaming list. A LIST
// that starts after a forced-relist request is that relist, so the
// request is consumed here. Leaving it for the next Watch call (as when
// the reflector relists on its own, e.g. after VeryShortWatchError,
// without calling Watch) would fail that Watch with 410 and force a
// second, spurious relist, with an extra End/Start pair, whenever the
// writer had not yet applied the first LIST.
func (s *source) startListLocked() {
	if !s.awaitingReplace {
		s.listGen = s.relistGen
		s.relist = false
	}
	s.awaitingReplace = true
}

// relistDone is called by the writer once a Replace whose LIST started at
// generation gen has been applied. A pending forced relist (or stall) is
// satisfied only by a LIST that started after it was requested; a LIST
// already in progress or queued when, say, the watchdog tripped does not
// count.
func (s *source) relistDone(gen uint64) {
	s.mu.Lock()
	if gen == s.relistGen {
		s.relist = false
		s.stalled = false
	}
	s.mu.Unlock()
}

// processStalled is called by the watchdog: continuity was lost at at.
func (s *source) processStalled(at time.Time) {
	s.mu.Lock()
	s.stalled = true
	s.setLossLocked(at)
	s.mu.Unlock()
	s.requestRelist()
}

func (s *source) setLossLocked(at time.Time) {
	if s.lossAt.IsZero() || at.Before(s.lossAt) {
		s.lossAt = at
	}
}

// loss returns the source-side continuity loss not yet ended in the store.
func (s *source) loss() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lossAt
}

// endCommitted is called once an End at endAt is durable: a loss at or
// before it is now recorded in the store.
func (s *source) endCommitted(endAt time.Time) {
	s.mu.Lock()
	if !s.lossAt.IsZero() && !s.lossAt.After(endAt) {
		s.lossAt = time.Time{}
	}
	s.mu.Unlock()
}

func (s *source) streamEnded(p *proxy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == p {
		s.cur = nil
		s.live = false
	}
}

// enqueue stamps it with the receipt time and waits for queue space. The
// stamp is taken under s.mu together with setting waiting, which is what
// the heartbeat relies on (see Collector.heartbeats).
func (s *source) enqueue(it item, stallable bool) bool {
	it.res = s.res
	now := s.clock.Now()
	s.wd.observe(now) // may trip the watchdog before this item is queued
	s.mu.Lock()
	it.at = now
	s.waiting = true
	s.mu.Unlock()
	var stall time.Duration
	if stallable {
		stall = s.stallTimeout
	}
	err := s.q.put(s.ctx, it, s.clock, stall, func() {
		s.log.Warn("writer queue full for too long; ending coverage and relisting",
			"resource", s.res.id, "waitingSince", it.at, "stallTimeout", s.stallTimeout)
		s.mu.Lock()
		s.stalled = true
		s.setLossLocked(it.at)
		s.mu.Unlock()
		s.requestRelist()
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiting = false
	if err != nil {
		// Only at shutdown. Recorded so the shutdown End does not cover it.
		s.dropped = true
		s.log.Debug("received item dropped at shutdown", "resource", s.res.id)
		return false
	}
	if it.kind == itemEvent {
		s.lastEventAt = it.at
		s.acceptedEvents++
	}
	s.advance(it.at)
	return true
}

func (s *source) normalize(obj any) (normalize.Result, bool) {
	ro, ok := obj.(runtime.Object)
	if !ok {
		s.log.Error("unexpected object type from reflector", "resource", s.res.id, "type", typeName(obj))
		return normalize.Result{}, false
	}
	r, err := normalize.Normalize(s.rt(), ro, s.norm)
	if err != nil {
		ns, name := objName(ro)
		s.log.Error("cannot normalize object; skipped", "resource", s.res.id, "namespace", ns, "name", name, "err", err)
		return normalize.Result{}, false
	}
	return r, true
}

// --- cache.ListerWatcher ---------------------------------------------------

type sourceLW struct{ s *source }

var (
	_ cache.ListerWatcher            = sourceLW{}
	_ cache.ListerWatcherWithContext = sourceLW{}
)

func (l sourceLW) List(opts metav1.ListOptions) (runtime.Object, error) {
	return l.ListWithContext(l.s.ctx, opts)
}

func (l sourceLW) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return l.WatchWithContext(l.s.ctx, opts)
}

// IsWatchListSemanticsUnSupported is consulted by NewReflectorWithOptions
// (util/watchlist.DoesClientNotSupportWatchListSemantics).
func (l sourceLW) IsWatchListSemanticsUnSupported() bool { return l.s.unsupportedWatchList }

func (l sourceLW) ListWithContext(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
	s := l.s
	s.mu.Lock()
	s.startListLocked()
	s.mu.Unlock()
	obj, err := s.inner.ListWithContext(ctx, opts)
	if err != nil && ctx.Err() == nil {
		s.log.Warn("list failed", "resource", s.res.id, "err", err)
	}
	return obj, err
}

func (l sourceLW) WatchWithContext(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	s := l.s
	s.mu.Lock()
	if s.relist {
		s.relist = false
		s.live = false
		s.mu.Unlock()
		s.log.Info("forcing relist", "resource", s.res.id)
		return nil, apierrors.NewResourceExpired("chronos: relist forced by collector")
	}
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
		// Streaming list (WatchList): the initial events go to the
		// reflector's temporary store and end in one Replace.
		s.startListLocked()
	}
	s.mu.Unlock()

	w, err := s.inner.WatchWithContext(ctx, opts)
	if err != nil {
		s.mu.Lock()
		s.live = false
		s.mu.Unlock()
		if ctx.Err() == nil {
			s.log.Warn("watch failed", "resource", s.res.id, "err", err)
		}
		return nil, err
	}
	p := &proxy{s: s, in: w, out: make(chan watch.Event), stop: make(chan struct{})}
	s.mu.Lock()
	s.cur = p
	s.live = true
	forced := s.relist
	s.mu.Unlock()
	go p.run()
	if forced {
		p.Stop()
	}
	return p, nil
}

// proxy forwards a watch stream and reports when it ended.
type proxy struct {
	s    *source
	in   watch.Interface
	out  chan watch.Event
	stop chan struct{}
	once sync.Once
}

func (p *proxy) ResultChan() <-chan watch.Event { return p.out }

func (p *proxy) Stop() {
	p.once.Do(func() {
		close(p.stop)
		p.in.Stop()
	})
}

func (p *proxy) run() {
	defer func() {
		// Record the end before the reflector can observe it.
		p.s.streamEnded(p)
		close(p.out)
	}()
	for {
		select {
		case <-p.stop:
			return
		case ev, ok := <-p.in.ResultChan():
			if !ok {
				return
			}
			select {
			case p.out <- ev:
			case <-p.stop:
				return
			}
		}
	}
}

// --- cache.ReflectorStore ----------------------------------------------------

type sourceStore struct{ s *source }

var (
	_ cache.ReflectorStore         = sourceStore{}
	_ cache.ReflectorBookmarkStore = sourceStore{}
)

func (st sourceStore) Add(obj any) error    { st.event(model.ChangeAdded, obj); return nil }
func (st sourceStore) Update(obj any) error { st.event(model.ChangeModified, obj); return nil }

func (st sourceStore) Delete(obj any) error {
	s := st.s
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		// Not produced by a bare Reflector (DeltaFIFO creates these), but
		// handled in case the store is ever fed by one: use the stale
		// object if present, else the writer uses the last stored state.
		if tomb.Obj != nil {
			obj = tomb.Obj
		} else {
			ns, name, err := cache.SplitMetaNamespaceKey(tomb.Key)
			if err != nil || name == "" {
				s.log.Error("tombstone with unusable key; skipped", "resource", s.res.id, "err", err)
				return nil
			}
			if !s.rt().Namespaced {
				ns = ""
			}
			s.enqueue(item{kind: itemEvent, change: model.ChangeDeleted, tomb: nsName{ns, name}}, true)
			return nil
		}
	}
	st.event(model.ChangeDeleted, obj)
	return nil
}

func (st sourceStore) event(change model.ChangeType, obj any) {
	r, ok := st.s.normalize(obj)
	if !ok {
		return
	}
	st.s.enqueue(item{kind: itemEvent, change: change, obj: r, hasObj: true}, true)
}

func (st sourceStore) Replace(list []any, _ string) error {
	s := st.s
	results := make([]normalize.Result, 0, len(list))
	for _, o := range list {
		if r, ok := s.normalize(o); ok {
			results = append(results, r)
		}
	}
	s.mu.Lock()
	lostAt := s.contUntil
	gen := s.listGen
	s.awaitingReplace = false
	s.replaced = true
	s.mu.Unlock()

	// A LIST is a snapshot, not a stream: waiting for space loses nothing,
	// so it is not subject to the stall rule.
	if !s.enqueue(item{kind: itemReplace, list: results, lostAt: lostAt, gen: gen}, false) {
		return s.ctx.Err() // shutting down; the reflector is exiting anyway
	}
	return nil
}

// Bookmark advances continuity: the server confirms nothing was missed up
// to now (bookmarks arrive about once a minute and just before the watch
// timeout; apiserver cacher defaultBookmarkFrequency).
func (st sourceStore) Bookmark(string) error {
	s := st.s
	s.mu.Lock()
	s.advance(s.clock.Now())
	s.mu.Unlock()
	return nil
}

// Resync is never called: the reflector runs with ResyncPeriod 0.
func (st sourceStore) Resync() error { return nil }

func objName(o runtime.Object) (ns, name string) {
	type named interface {
		GetNamespace() string
		GetName() string
	}
	if n, ok := o.(named); ok {
		return n.GetNamespace(), n.GetName()
	}
	return "", ""
}

func typeName(v any) string { return fmt.Sprintf("%T", v) }
