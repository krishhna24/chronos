package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// watchdog detects that the process itself did not run (SIGSTOP, VM or
// container freeze, CPU starvation; ADR 0007 §1). It ticks every interval
// on the injected clock, and every receipt in a source also reports the
// time it observed. If two consecutive observations are more than
// stallTimeout apart, nothing received in or after that window can be
// trusted to carry its real time: every resource loses continuity at the
// last on-time observation and is relisted.
//
// Receipts report too so the first event delivered after a freeze trips
// the watchdog before it is queued (and possibly written) with a late
// stamp; the ticker alone would only notice on its next tick.
//
// It cannot detect a slow API server delivering events late while this
// process runs normally (watch delivery lag, ADR 0002).
type watchdog struct {
	clock        clock.WithTicker
	interval     time.Duration
	stallTimeout time.Duration
	log          *slog.Logger
	sources      []*source
	q            *queue

	mu   sync.Mutex
	ctx  context.Context
	last time.Time // last on-time observation; zero until run starts
}

// run ticks until ctx is done.
func (d *watchdog) run(ctx context.Context) {
	d.mu.Lock()
	d.ctx = ctx
	d.last = d.clock.Now()
	d.mu.Unlock()
	t := d.clock.NewTicker(d.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			d.observe(d.clock.Now())
		}
	}
}

// observe records that the process was running at now. Safe on a nil
// watchdog (disabled). Callers must not hold a source lock.
func (d *watchdog) observe(now time.Time) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.last.IsZero() || d.ctx == nil {
		return
	}
	gap := now.Sub(d.last)
	if gap <= d.stallTimeout {
		if gap > 0 {
			d.last = now
		}
		return
	}
	lastOnTime := d.last
	d.last = now
	d.log.Warn("process stall detected; ending coverage of every resource and relisting",
		"lastOnTime", lastOnTime, "resumedAt", now, "gap", gap, "stallTimeout", d.stallTimeout)
	for _, s := range d.sources {
		s.processStalled(lastOnTime)
	}
	// Held under d.mu so no receipt that observes after the trip can be
	// queued ahead of this item.
	if err := d.q.put(d.ctx, item{kind: itemProcessStall, at: now, lostAt: lastOnTime}, d.clock, 0, nil); err != nil {
		d.log.Debug("process stall item dropped at shutdown", "err", err)
	}
}
