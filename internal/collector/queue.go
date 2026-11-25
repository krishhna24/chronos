package collector

import (
	"context"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/normalize"
)

type itemKind uint8

const (
	itemEvent itemKind = iota + 1
	itemReplace
	itemHeartbeat
	itemShutdown
	itemProcessStall // watchdog: process did not run; lostAt = last on-time tick
)

// item is one unit of work for the writer. Items are processed strictly in
// queue order, which is also the order of their timestamps.
type item struct {
	kind itemKind
	res  *resource
	// at is when the item was received (stamped by the producer before it
	// waits for queue space).
	at time.Time
	// stalled: the item waited longer than Config.StallTimeout for space;
	// the reflector stopped reading its stream at at.
	stalled bool

	// itemEvent
	change model.ChangeType
	obj    normalize.Result
	hasObj bool   // false: tombstone without a final object
	tomb   nsName // key of an object-less tombstone

	// itemReplace
	list   []normalize.Result
	lostAt time.Time // source's continuity bound when the LIST was applied (itemProcessStall: last on-time tick)

	gen uint64 // itemReplace: source.listGen when the LIST started

	// itemHeartbeat
	healthy bool
}

// queue is a bounded FIFO between the reflector callbacks (many producers)
// and the writer (single consumer). Producers block while it is full, which
// in turn stops the reflector from reading its watch stream: backpressure
// reaches the API server connection instead of growing memory.
//
// Items arrive already stamped (at receipt, by the producer). Within one
// resource the queue order is the stamp order (one reflector goroutine
// per resource). Across resources an item that waited for space can be
// overtaken by a later-stamped one; the store's clamping absorbs that, and
// the heartbeat rule (see Collector.heartbeats) does not depend on it.
type queue struct {
	slots  chan struct{} // one token per queued item
	notify chan struct{} // capacity 1: "items available"

	mu    sync.Mutex
	items []item
}

func newQueue(size int) *queue {
	return &queue{
		slots:  make(chan struct{}, size),
		notify: make(chan struct{}, 1),
	}
}

// put blocks until there is room or ctx is done, then enqueues it. If
// stall > 0 and the wait exceeds it, onStall is called once (while still
// waiting) and the item is marked stalled.
func (q *queue) put(ctx context.Context, it item, c clock.Clock, stall time.Duration, onStall func()) error {
	select {
	case q.slots <- struct{}{}:
		q.append(it)
		return nil
	default:
	}
	var stallC <-chan time.Time
	if stall > 0 {
		t := c.NewTimer(stall)
		defer t.Stop()
		stallC = t.C()
	}
	for {
		select {
		case q.slots <- struct{}{}:
			q.append(it)
			return nil
		case <-stallC:
			stallC = nil
			it.stalled = true
			onStall()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryPut enqueues it only if there is room right now.
func (q *queue) tryPut(it item) bool {
	select {
	case q.slots <- struct{}{}:
	default:
		return false
	}
	q.append(it)
	return true
}

func (q *queue) append(it item) {
	q.mu.Lock()
	q.items = append(q.items, it)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// take removes up to maxItems items. If none are queued and block is
// false it returns nil at once; otherwise it waits until items arrive, ctx
// is done, or wait fires (a nil wait never fires).
func (q *queue) take(ctx context.Context, maxItems int, wait <-chan time.Time, block bool) []item {
	for {
		q.mu.Lock()
		if n := min(maxItems, len(q.items)); n > 0 {
			out := make([]item, n)
			copy(out, q.items)
			clear(q.items[:n]) // drop references so large LISTs can be collected
			q.items = q.items[n:]
			q.mu.Unlock()
			for range n {
				<-q.slots
			}
			return out
		}
		q.mu.Unlock()
		if !block {
			return nil
		}
		select {
		case <-q.notify:
		case <-wait: // nil channel: never
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}
