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
)

// item is one unit of work for the writer. Items are processed strictly in
// queue order, which is also the order of their timestamps.
type item struct {
	kind itemKind
	res  *resource
	// at is stamped by the queue when the item is accepted (see queue.put).
	at time.Time

	// itemEvent
	change model.ChangeType
	obj    normalize.Result
	hasObj bool   // false: tombstone without a final object
	tomb   nsName // key of an object-less tombstone

	// itemReplace
	list   []normalize.Result
	lostAt time.Time // source's continuity bound when the LIST was applied

	// itemHeartbeat
	healthy bool
}

// queue is a bounded FIFO between the reflector callbacks (many producers)
// and the writer (single consumer). Producers block while it is full, which
// in turn stops the reflector from reading its watch stream: backpressure
// reaches the API server connection instead of growing memory.
//
// Timestamps are assigned under the queue lock at the moment an item is
// accepted, so queue order and time order agree. That makes "every item
// stamped before t has been written" true once the writer has processed
// the heartbeat item stamped t, which is what makes a heartbeat safe to
// use as a crash bound.
type queue struct {
	clock  clock.PassiveClock
	slots  chan struct{} // one token per queued item
	notify chan struct{} // capacity 1: "items available"

	mu    sync.Mutex
	items []item
}

func newQueue(size int, c clock.PassiveClock) *queue {
	return &queue{
		clock:  c,
		slots:  make(chan struct{}, size),
		notify: make(chan struct{}, 1),
	}
}

// put blocks until there is room or ctx is done, then stamps and enqueues
// it. It returns the assigned time.
func (q *queue) put(ctx context.Context, it item) (time.Time, error) {
	select {
	case q.slots <- struct{}{}:
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}
	return q.append(it), nil
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

func (q *queue) append(it item) time.Time {
	q.mu.Lock()
	it.at = q.clock.Now()
	q.items = append(q.items, it)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return it.at
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
