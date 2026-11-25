// Command chronos runs the Chronos recorder: it collects Kubernetes
// resource changes into the historical store, enforces retention, and
// serves the query API and dashboard.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/collector"
	"github.com/krishhna24/chronos/internal/normalize"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/server"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/web"
)

var version = "dev"

type config struct {
	kubeconfig        string
	listen            string
	storage           string
	dataDir           string
	retention         time.Duration
	retentionInterval time.Duration
	resources         string
	redact            string
	secretAnnotations string
	stallTimeout      time.Duration
	degradedExitAfter time.Duration
	keepManagedFields bool
	logLevel          string
	logFormat         string
	qps               float64
	burst             int
	printVersion      bool
}

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("chronos", flag.ContinueOnError)
	fs.StringVar(&c.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "path to a kubeconfig; empty uses in-cluster config")
	fs.StringVar(&c.listen, "listen", ":8080", "HTTP listen address for the API and dashboard")
	fs.StringVar(&c.storage, "storage", "pebble", "storage backend: pebble or memory (memory is not durable; for development)")
	fs.StringVar(&c.dataDir, "data-dir", "/var/lib/chronos", "directory for the pebble store")
	fs.DurationVar(&c.retention, "retention", 7*24*time.Hour, "how long history is kept")
	fs.DurationVar(&c.retentionInterval, "retention-interval", 10*time.Minute, "how often retention runs")
	fs.StringVar(&c.resources, "resources", "", "comma-separated resource types to record (default: all supported)")
	fs.StringVar(&c.redact, "redact-annotations", "", "comma-separated annotation keys removed from every object before storing")
	fs.StringVar(&c.secretAnnotations, "secret-annotation-allowlist", "", "comma-separated annotation keys kept on secrets; all other secret annotations are dropped because they can carry credentials")
	fs.DurationVar(&c.stallTimeout, "stall-timeout", 5*time.Second, "if a received change waits longer than this for the store, coverage of its type ends and the type is relisted, so recorded times are never later than this")
	fs.DurationVar(&c.degradedExitAfter, "degraded-exit-after", 10*time.Minute, "after the store stops accepting writes (e.g. disk full), keep serving history read-only for this long, then exit so the pod restarts; 0 exits immediately")
	fs.BoolVar(&c.keepManagedFields, "keep-managed-fields", false, "store metadata.managedFields (large; never kept for secrets)")
	fs.StringVar(&c.logLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&c.logFormat, "log-format", "json", "json or text")
	fs.Float64Var(&c.qps, "kube-api-qps", 20, "client-side QPS limit for the Kubernetes API")
	fs.IntVar(&c.burst, "kube-api-burst", 40, "client-side burst limit for the Kubernetes API")
	fs.BoolVar(&c.printVersion, "version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if c.retention < time.Hour {
		return c, fmt.Errorf("--retention must be at least 1h, got %s", c.retention)
	}
	if c.degradedExitAfter < 0 {
		return c, fmt.Errorf("--degraded-exit-after must not be negative")
	}
	if c.stallTimeout <= 0 {
		return c, fmt.Errorf("--stall-timeout must be positive")
	}
	if c.retentionInterval < time.Minute {
		return c, fmt.Errorf("--retention-interval must be at least 1m, got %s", c.retentionInterval)
	}
	if c.qps <= 0 || c.burst <= 0 {
		return c, fmt.Errorf("--kube-api-qps and --kube-api-burst must be positive")
	}
	return c, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func selectResources(spec string) ([]resources.Type, error) {
	names := splitList(spec)
	if len(names) == 0 {
		return resources.Builtin, nil
	}
	seen := map[string]bool{}
	var out []resources.Type
	for _, n := range names {
		t, err := resources.Resolve(n)
		if err != nil {
			return nil, err
		}
		if !seen[t.ID()] {
			seen[t.ID()] = true
			out = append(out, t)
		}
	}
	return out, nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("--log-level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: l}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	}
	return nil, fmt.Errorf("--log-format must be json or text, got %q", format)
}

func restConfig(c config) (*rest.Config, error) {
	var rc *rest.Config
	var err error
	if c.kubeconfig == "" {
		rc, err = rest.InClusterConfig()
	} else {
		rc, err = clientcmd.BuildConfigFromFlags("", c.kubeconfig)
	}
	if err != nil {
		return nil, fmt.Errorf("kubernetes client config: %w", err)
	}
	rc.QPS, rc.Burst = float32(c.qps), c.burst
	rc.UserAgent = "chronos/" + version
	return rc, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "chronos:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	if cfg.printVersion {
		fmt.Println(version)
		return nil
	}
	log, err := newLogger(cfg.logLevel, cfg.logFormat)
	if err != nil {
		return err
	}
	types, err := selectResources(cfg.resources)
	if err != nil {
		return err
	}
	rc, err := restConfig(cfg)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	meta, err := metadata.NewForConfig(rc)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}

	st, err := openStore(cfg, log)
	if err != nil {
		return err
	}
	// The store is closed explicitly during shutdown (see below); this
	// covers the early-return paths before that.
	var closeOnce sync.Once
	closeStore := func() {
		closeOnce.Do(func() {
			if err := st.Close(); err != nil {
				log.Error("closing store", "err", err)
			}
		})
	}
	defer closeStore()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	clk := clock.RealClock{}
	col, err := collector.New(collector.Config{
		Dynamic:      dyn,
		Metadata:     meta,
		Store:        st,
		Resources:    types,
		Clock:        clk,
		Logger:       log,
		StallTimeout: cfg.stallTimeout,
		Normalize: normalize.Options{
			KeepManagedFields:               cfg.keepManagedFields,
			RedactAnnotations:               splitList(cfg.redact),
			MetadataOnlyAnnotationAllowlist: splitList(cfg.secretAnnotations),
		},
	})
	if err != nil {
		return err
	}

	// ADR 0007: a continuity loss the collector could not yet persist
	// (store unwritable, process stall) is shown as a gap immediately.
	degraded := &degradedState{}
	engine := query.New(st, clk, query.WithLiveCoverage(liveCoverage(col, degraded)))
	var webFS = web.FS()
	if !web.Built() {
		log.Warn("dashboard assets not built; serving placeholder page (run make web)")
	}
	handler := server.New(server.Options{
		Engine: engine,
		// Ready means the store can serve reads. Collector sync state is
		// deliberately not part of it: taking the only replica out of the
		// Service during a relist would cut users off exactly when they
		// need history, and gaps are already reported in every answer.
		Readiness: func() error { return storeReadable(st) },
		Web:       webFS,
		Logger:    log.With("component", "http"),
	})
	srv := server.NewHTTPServer(cfg.listen, handler)

	// Shutdown order matters: the collector and HTTP server must stop
	// before the store closes (they write and read it), but retention must
	// be waited for only after the close, because Store.Close is what
	// releases a compaction stuck on a hung disk.
	var wg, retention sync.WaitGroup
	errc := make(chan error, 3)

	wg.Go(func() {
		log.Info("starting collector", "resources", len(types), "version", version)
		if err := col.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errc <- fmt.Errorf("collector: %w", err)
		}
	})
	retention.Go(func() { retentionLoop(ctx, st, clk, cfg, log.With("component", "retention")) })
	wg.Go(func() {
		log.Info("serving HTTP", "addr", cfg.listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http server: %w", err)
		}
	})

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case runErr = <-errc:
		if cfg.degradedExitAfter > 0 && errors.Is(runErr, store.ErrClosed) && ctx.Err() == nil {
			// The store refused writes (poisoned after an ambiguous
			// commit failure, e.g. disk full). History up to the loss is
			// still valid, so keep serving it read-only, with everything
			// after the loss reported as a gap, then exit so Kubernetes
			// restarts us and recovery can be attempted.
			degraded.enter(ctx, st, types, log)
			log.Error("store unwritable; serving history read-only", "err", runErr, "exitAfter", cfg.degradedExitAfter)
			select {
			case <-ctx.Done():
				log.Info("shutting down")
			case <-clk.After(cfg.degradedExitAfter):
				log.Error("read-only period over, exiting so the pod restarts")
			case err := <-errc:
				log.Error("fatal error, shutting down", "err", err)
			}
		} else {
			log.Error("fatal error, shutting down", "err", runErr)
		}
	}
	stop()
	shutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	wg.Wait()
	closeStore()
	retention.Wait()
	return runErr
}

// degradedState records, per resource, when continuity was lost because
// the store stopped accepting writes. Set once; read by the overlay.
type degradedState struct {
	mu     sync.RWMutex
	lostAt map[string]time.Time
}

// enter computes each resource's loss bound the same way crash recovery
// does (ADR 0004): everything up to max(last heartbeat, latest record) is
// durable, so continuity is only claimed up to that point.
func (d *degradedState) enter(ctx context.Context, st store.Store, types []resources.Type, log *slog.Logger) {
	hb, err := st.Heartbeat(ctx)
	if err != nil {
		log.Error("reading heartbeat for read-only mode", "err", err)
	}
	m := map[string]time.Time{}
	v, err := st.View(ctx)
	if err != nil {
		// Without a view, fall back to the heartbeat for every resource;
		// zero time means "lost since the beginning", the safest answer.
		log.Error("opening view for read-only mode", "err", err)
	}
	for _, t := range types {
		at := hb
		if v != nil {
			if lr, err := v.LatestRecordTime(ctx, t.ID()); err == nil && lr.After(at) {
				at = lr
			}
		}
		m[t.ID()] = at
	}
	if v != nil {
		_ = v.Close()
	}
	d.mu.Lock()
	d.lostAt = m
	d.mu.Unlock()
}

func (d *degradedState) get(resource string) (time.Time, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.lostAt == nil {
		return time.Time{}, false
	}
	at, ok := d.lostAt[resource]
	if !ok {
		return time.Time{}, true
	}
	return at, true
}

// liveCoverage adapts the collector's in-memory loss state, and the
// read-only state after the store stopped accepting writes, to the query
// overlay. It can only turn observed answers into gaps; when both report a
// loss the earlier one wins.
func liveCoverage(col interface {
	Status() []collector.ResourceStatus
}, degraded *degradedState) query.LiveCoverage {
	return func(resource string) (time.Time, bool) {
		lost, ok := degraded.get(resource)
		for _, s := range col.Status() {
			if s.Resource == resource && s.LostAt != nil && (!ok || s.LostAt.Before(lost)) {
				lost, ok = *s.LostAt, true
			}
		}
		return lost, ok
	}
}

func storeReadable(st store.Store) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := st.View(ctx)
	if err != nil {
		return fmt.Errorf("store not readable: %w", err)
	}
	return v.Close()
}

func retentionLoop(ctx context.Context, st store.Store, clk clock.WithTicker, cfg config, log *slog.Logger) {
	t := clk.NewTicker(cfg.retentionInterval)
	defer t.Stop()
	for {
		horizon := clk.Now().Add(-cfg.retention)
		start := clk.Now()
		if err := st.Compact(ctx, horizon); err != nil && ctx.Err() == nil {
			log.Error("retention compaction failed", "horizon", horizon, "err", err)
		} else if err == nil {
			log.Debug("retention compaction done", "horizon", horizon, "took", clk.Since(start))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
	}
}
