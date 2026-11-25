package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/normalize"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store"
)

// Defaults for zero Config fields.
const (
	DefaultHeartbeatInterval = 10 * time.Second
	DefaultMaxBatchSize      = 1000
	DefaultFlushInterval     = 50 * time.Millisecond
	DefaultQueueSize         = 1024
	DefaultWriteRetryTimeout = 30 * time.Second
	DefaultShutdownTimeout   = 30 * time.Second
	DefaultStallTimeout      = 5 * time.Second
	DefaultWatchdogInterval  = time.Second
)

// Config configures a Collector.
type Config struct {
	// Dynamic lists and watches full objects. Required unless every type
	// is MetadataOnly or ListerWatcherFor is set.
	Dynamic dynamic.Interface
	// Metadata lists and watches MetadataOnly types (Secrets). Required if
	// any such type is configured and ListerWatcherFor is not set.
	Metadata metadata.Interface
	Store    store.Store
	// Resources to collect. IDs must be unique.
	Resources []resources.Type

	// Clock stamps ObservedAt and drives heartbeats, batching and retry
	// backoff. Default: real clock. Reflector-internal timing (watch
	// timeouts, relist backoff) always uses the real clock.
	Clock  clock.WithTicker
	Logger *slog.Logger

	HeartbeatInterval time.Duration // default 10s
	Normalize         normalize.Options
	// MaxBatchSize bounds entries per store write; larger LISTs are
	// written in chunks (see handleReplace). Default 1000.
	MaxBatchSize int
	// FlushInterval is how long a non-full batch of watch events may wait
	// for more events. Default 50ms; negative disables waiting.
	FlushInterval time.Duration
	// QueueSize bounds items between reflectors and the writer. Default 1024.
	QueueSize int
	// WriteRetryTimeout bounds retries of a failing store write before the
	// affected resources lose coverage and are relisted. Default 30s.
	WriteRetryTimeout time.Duration
	// ShutdownTimeout bounds the final flush and End writes. Default 30s.
	ShutdownTimeout time.Duration
	// StallTimeout: an event that waits longer than this for queue space
	// ends the resource's coverage and forces a relist, and a gap longer
	// than this between watchdog observations (the process did not run)
	// ends every resource's coverage. Default 5s.
	StallTimeout time.Duration
	// WatchdogInterval is the stall watchdog's tick (ADR 0007 §1).
	// Default 1s; negative disables the watchdog.
	WatchdogInterval time.Duration

	// ListerWatcherFor, if set, supplies the list/watch source of every
	// resource instead of Dynamic/Metadata (tests, custom transports).
	// For MetadataOnly types it must produce *metav1.PartialObjectMetadata,
	// otherwise *unstructured.Unstructured.
	ListerWatcherFor func(resources.Type) (cache.ListerWatcher, error)
	// ReflectorBackoff overrides the reflector's relist backoff.
	ReflectorBackoff *wait.Backoff
}

// ResourceStatus is the collector's view of one resource type, for /readyz.
type ResourceStatus struct {
	Resource string `json:"resource"`
	// Synced: a complete LIST has been applied and coverage is continuous.
	Synced bool `json:"synced"`
	// LastEventAt: when the last change for this resource was received.
	LastEventAt time.Time `json:"lastEventAt"`
	// CoverageOpen: the store's latest coverage event is a Start.
	CoverageOpen bool `json:"coverageOpen"`
	// LostAt is set while continuity has been lost in memory (write
	// failure, queue or process stall) but the End recording it is not
	// yet durable, so the store still says observed from LostAt on. It is
	// cleared once that End (or a later LIST) is committed, and is nil
	// whenever the store's coverage is not open (ADR 0007 §2).
	LostAt *time.Time `json:"lostAt,omitempty"`
	// LastWriteError is the last store write error for a batch holding
	// entries of this resource, and LastWriteErrorAt its time. They are
	// not cleared by later successful writes.
	LastWriteError   string     `json:"lastWriteError,omitempty"`
	LastWriteErrorAt *time.Time `json:"lastWriteErrorAt,omitempty"`
}

// Collector records changes of the configured resource types into a store.
type Collector struct {
	cfg        Config
	log        *slog.Logger
	q          *queue
	w          *writer
	wd         *watchdog
	res        []*resource
	reflectors []*cache.Reflector
	started    atomic.Bool
}

// New validates cfg and builds a Collector. Nothing runs until Run.
func New(cfg Config) (*Collector, error) {
	if cfg.Store == nil {
		return nil, errors.New("collector: Store is required")
	}
	if len(cfg.Resources) == 0 {
		return nil, errors.New("collector: no resources configured")
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = DefaultMaxBatchSize
	}
	switch {
	case cfg.FlushInterval == 0:
		cfg.FlushInterval = DefaultFlushInterval
	case cfg.FlushInterval < 0:
		cfg.FlushInterval = 0
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.WriteRetryTimeout <= 0 {
		cfg.WriteRetryTimeout = DefaultWriteRetryTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = DefaultStallTimeout
	}
	if cfg.WatchdogInterval == 0 {
		cfg.WatchdogInterval = DefaultWatchdogInterval
	}

	log := cfg.Logger.With("component", "collector")
	c := &Collector{cfg: cfg, log: log, q: newQueue(cfg.QueueSize)}
	c.w = &writer{
		store:        cfg.Store,
		clock:        cfg.Clock,
		log:          log,
		q:            c.q,
		norm:         cfg.Normalize,
		maxBatch:     cfg.MaxBatchSize,
		linger:       cfg.FlushInterval,
		retryTimeout: cfg.WriteRetryTimeout,
		byID:         map[string]*resource{},
	}
	klogger := logr.FromSlogHandler(log.Handler())
	for _, rt := range cfg.Resources {
		id := rt.ID()
		if rt.Version == "" || rt.Resource == "" || rt.Kind == "" {
			return nil, fmt.Errorf("collector: resource type %q needs version, resource and kind", id)
		}
		if _, dup := c.w.byID[id]; dup {
			return nil, fmt.Errorf("collector: resource %s configured twice", id)
		}
		lw, err := c.listerWatcher(rt)
		if err != nil {
			return nil, err
		}
		r := &resource{rt: rt, id: id, known: map[nsName]objState{}}
		r.src = newSource(r, lw, c.q, cfg.Clock, cfg.StallTimeout, cfg.Normalize, log)
		var expected runtime.Object = &unstructured.Unstructured{}
		if rt.MetadataOnly {
			expected = &metav1.PartialObjectMetadata{}
		}
		refl := cache.NewReflectorWithOptions(sourceLW{r.src}, expected, sourceStore{r.src}, cache.ReflectorOptions{
			Name:            "chronos/" + id,
			TypeDescription: id,
			Clock:           clock.RealClock{},
			Backoff:         cfg.ReflectorBackoff,
			Logger:          &klogger,
		})
		c.w.byID[id] = r
		c.w.ordered = append(c.w.ordered, r)
		c.res = append(c.res, r)
		c.reflectors = append(c.reflectors, refl)
	}
	if cfg.WatchdogInterval > 0 {
		c.wd = &watchdog{clock: cfg.Clock, interval: cfg.WatchdogInterval, stallTimeout: cfg.StallTimeout, log: log, q: c.q}
		for _, r := range c.res {
			c.wd.sources = append(c.wd.sources, r.src)
			r.src.wd = c.wd
		}
	}
	return c, nil
}

func (c *Collector) listerWatcher(rt resources.Type) (cache.ListerWatcher, error) {
	if c.cfg.ListerWatcherFor != nil {
		lw, err := c.cfg.ListerWatcherFor(rt)
		if err != nil {
			return nil, fmt.Errorf("collector: lister-watcher for %s: %w", rt.ID(), err)
		}
		return lw, nil
	}
	gvr := schema.GroupVersionResource{Group: rt.Group, Version: rt.Version, Resource: rt.Resource}
	if rt.MetadataOnly {
		// ADR 0005: Secret contents never enter the process.
		if c.cfg.Metadata == nil {
			return nil, fmt.Errorf("collector: %s is metadata-only but no metadata client is configured", rt.ID())
		}
		ri := c.cfg.Metadata.Resource(gvr).Namespace(metav1.NamespaceAll)
		return cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
				return ri.List(ctx, o)
			},
			WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
				return ri.Watch(ctx, o)
			},
		}, c.cfg.Metadata), nil
	}
	if c.cfg.Dynamic == nil {
		return nil, fmt.Errorf("collector: no dynamic client configured for %s", rt.ID())
	}
	ri := c.cfg.Dynamic.Resource(gvr).Namespace(metav1.NamespaceAll)
	return cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
			return ri.List(ctx, o)
		},
		WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
			return ri.Watch(ctx, o)
		},
	}, c.cfg.Dynamic), nil
}

// Run collects until ctx is cancelled, then flushes, writes End(shutdown)
// for every open coverage interval, and returns nil. It returns early
// with an error if startup recovery fails or the store is closed. Run may
// be called once.
func (c *Collector) Run(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("collector: Run called more than once")
	}
	if err := c.w.recover(ctx); err != nil {
		return err
	}

	wctx, wcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer wcancel()
	werr := make(chan error, 1)
	go func() { werr <- c.w.run(wctx) }()

	// The reflectors must not see ctx's cancellation directly: shutdown
	// first snapshots each source's continuity (stopping) and only then
	// stops them. If they shared ctx, a reflector could tear down its
	// watch before the snapshot, and a healthy source would be recorded
	// as having lost continuity at its last event instead of ending at
	// the shutdown time.
	rctx, rcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer rcancel()
	kctx := klog.NewContext(rctx, logr.FromSlogHandler(c.log.Handler()))
	var wg sync.WaitGroup
	for i, r := range c.res {
		r.src.ctx = rctx
		refl := c.reflectors[i]
		wg.Go(func() { refl.RunWithContext(kctx) })
	}
	wg.Go(func() { c.heartbeats(rctx) })
	if c.wd != nil {
		wg.Go(func() { c.wd.run(rctx) })
	}
	c.log.Info("collector started", "resources", len(c.res))

	var fatal error
	select {
	case <-ctx.Done():
	case fatal = <-werr:
	}
	// Record which sources still had continuity before stopping them
	// (stopping closes their streams).
	for _, r := range c.res {
		r.src.stopping()
	}
	rcancel()
	wg.Wait()
	if fatal != nil {
		return fatal
	}

	// Every accepted item was received before this point, so the shutdown
	// item (stamped now) bounds them all.
	//
	// The final flush and End writes are bounded by ShutdownTimeout. If the
	// store's Write ignores cancellation and does not return, Run gives up
	// and returns an error while the writer goroutine is still inside
	// Write; the caller then closes the store, and Store.Close must make
	// that Write return. The next start treats the open coverage as a
	// crash.
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ShutdownTimeout)
	defer scancel()
	stop := context.AfterFunc(sctx, wcancel)
	defer stop()
	err := c.q.put(sctx, item{kind: itemShutdown, at: c.cfg.Clock.Now()}, c.cfg.Clock, 0, nil)
	if err == nil {
		select {
		case err = <-werr:
			if err == nil {
				c.log.Info("collector stopped")
				return nil
			}
			return err
		case <-sctx.Done():
			err = sctx.Err()
		}
	}
	c.log.Error("shutdown did not complete within the timeout; coverage left open (treated as a crash on next start)",
		"timeout", c.cfg.ShutdownTimeout, "err", err)
	return fmt.Errorf("collector: shutdown: final writes not completed within %s: %w", c.cfg.ShutdownTimeout, err)
}

func (c *Collector) heartbeats(ctx context.Context) {
	t := c.cfg.Clock.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		// at is read before the sources are checked. A source stamps its
		// items and marks them waiting under its own lock, so every item
		// received before at is either already in the queue (and so ahead
		// of this heartbeat) or still waiting, which makes the source
		// unhealthy and skips the heartbeat. Hence a written heartbeat
		// never covers a received-but-unwritten change.
		at := c.cfg.Clock.Now()
		c.wd.observe(at) // a heartbeat tick late by > StallTimeout is a stall too
		healthy := true
		for _, r := range c.res {
			if !r.src.healthy() {
				healthy = false
				break
			}
		}
		if healthy && !c.q.tryPut(item{kind: itemHeartbeat, at: at, healthy: true}) {
			c.log.Debug("heartbeat skipped: writer queue full")
		}
	}
}

// Status reports per-resource state in configuration order.
func (c *Collector) Status() []ResourceStatus {
	out := make([]ResourceStatus, 0, len(c.res))
	for _, r := range c.res {
		r.mu.Lock()
		s := ResourceStatus{Resource: r.id, Synced: r.synced && !r.lost, CoverageOpen: r.covOpen, LastWriteError: r.lastWriteErr}
		lost := r.unrecordedLossAt
		if !r.lastWriteErrAt.IsZero() {
			t := r.lastWriteErrAt
			s.LastWriteErrorAt = &t
		}
		r.mu.Unlock()
		s.LastEventAt = r.src.lastEvent()
		if sl := r.src.loss(); !sl.IsZero() && (lost.IsZero() || sl.Before(lost)) {
			lost = sl
		}
		if s.CoverageOpen && !lost.IsZero() {
			s.LostAt = &lost
			s.Synced = false
		}
		out = append(out, s)
	}
	return out
}
