package collector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/normalize"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store"
)

// farFuture is used as "latest" in View queries.
var farFuture = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// Coverage event reasons.
const (
	reasonInitialList  = "initial-list"
	reasonRelist       = "relist"
	reasonCrash        = "crash-recovery"
	reasonShutdown     = "shutdown"
	reasonWriteFailure = "write-failure"
)

type nsName struct{ ns, name string }

func compareNsName(a, b nsName) int {
	if c := cmp.Compare(a.ns, b.ns); c != 0 {
		return c
	}
	return cmp.Compare(a.name, b.name)
}

// objState is what change detection needs per live object. Full objects
// are not kept in memory; the last stored object is read back from the
// store on the rare paths that need it (resync deletes, UID changes,
// object-less tombstones).
type objState struct {
	uid  string
	hash normalize.Hash
}

// resource is the writer's state for one resource type. Fields below mu
// are written only by the writer goroutine; Status reads them under mu.
type resource struct {
	rt  resources.Type
	id  string
	src *source

	known map[nsName]objState // keys whose latest record is not a delete

	// Durable facts about the store, updated only after successful writes.
	hasHistory  bool      // at least one coverage event exists
	lastRecAt   time.Time // latest record time
	lastStartAt time.Time // latest Start time

	mu      sync.Mutex
	covOpen bool // latest coverage event is Start (durable)
	synced  bool // a LIST has been applied and nothing was lost since
	lost    bool // a write failed; coverage must end and a relist is pending
	lostAt  time.Time
}

func (r *resource) setState(f func()) {
	r.mu.Lock()
	f()
	r.mu.Unlock()
}

type writer struct {
	store        store.Store
	clock        clock.Clock
	log          *slog.Logger
	q            *queue
	norm         normalize.Options
	maxBatch     int
	linger       time.Duration
	retryTimeout time.Duration
	byID         map[string]*resource
	ordered      []*resource

	pending []store.Entry
}

// errFatal wraps errors that stop the collector (the store is closed).
type errFatal struct{ err error }

func (e errFatal) Error() string { return e.err.Error() }
func (e errFatal) Unwrap() error { return e.err }

// recover loads change-detection state from the store and closes coverage
// left open by a crash: End.At = max(heartbeat, latest record, latest
// Start) of that resource (ADR 0004).
func (w *writer) recover(ctx context.Context) error {
	v, err := w.store.View(ctx)
	if err != nil {
		return fmt.Errorf("collector: open view: %w", err)
	}
	defer func() { _ = v.Close() }()
	hb, err := w.store.Heartbeat(ctx)
	if err != nil {
		return fmt.Errorf("collector: read heartbeat: %w", err)
	}
	var ends []store.Entry
	for _, r := range w.ordered {
		covs, err := v.Coverage(ctx, r.id)
		if err != nil {
			return fmt.Errorf("collector: read coverage of %s: %w", r.id, err)
		}
		if n := len(covs); n > 0 {
			r.hasHistory = true
			if last := covs[n-1]; last.Type == model.CoverageStart {
				r.setState(func() { r.covOpen = true })
				r.lastStartAt = last.At
			}
		}
		if err := w.loadKnown(ctx, v, r); err != nil {
			return err
		}
		if r.covOpen {
			at := latest(hb, r.lastRecAt, r.lastStartAt)
			w.log.Warn("coverage left open by an unclean stop; closing it",
				"resource", r.id, "at", at, "heartbeat", hb)
			ends = append(ends, coverage(r.id, model.CoverageEnd, at, reasonCrash))
		}
	}
	if len(ends) > 0 {
		if err := w.write(ctx, ends); err != nil {
			return fmt.Errorf("collector: close crashed coverage: %w", err)
		}
	}
	return nil
}

// loadKnown rebuilds r.known and r.lastRecAt from the store.
func (w *writer) loadKnown(ctx context.Context, v store.View, r *resource) error {
	known := map[nsName]objState{}
	var lastRec time.Time
	err := v.List(ctx, store.ListOptions{Resource: r.id, At: farFuture}, func(rec model.Record) error {
		lastRec = latest(lastRec, rec.ObservedAt)
		if rec.Type == model.ChangeDeleted {
			return nil
		}
		n, err := normalize.NormalizeJSON(r.rt.ResourceType, rec.Object, w.norm)
		if err != nil {
			// Unreadable stored object: forget its hash so the next LIST
			// records it as Modified rather than failing the whole load.
			w.log.Error("stored object unreadable", "resource", r.id, "namespace", rec.Key.Namespace, "name", rec.Key.Name, "err", err)
		}
		known[nsName{rec.Key.Namespace, rec.Key.Name}] = objState{uid: rec.UID, hash: n.Hash}
		return nil
	})
	if err != nil {
		return fmt.Errorf("collector: load state of %s: %w", r.id, err)
	}
	r.known = known
	r.lastRecAt = latest(r.lastRecAt, lastRec)
	return nil
}

// run processes the queue until a shutdown item has been handled (nil), or
// a fatal error occurs.
func (w *writer) run(ctx context.Context) error {
	// lingerC bounds how long a non-empty batch waits for more events. It
	// is armed when the first entry becomes pending, not per take, so a
	// steady trickle of events cannot postpone the write indefinitely.
	var lingerC <-chan time.Time
	for {
		switch {
		case len(w.pending) == 0:
			lingerC = nil
		case w.linger > 0 && lingerC == nil:
			lingerC = w.clock.After(w.linger)
		}
		items := w.q.take(ctx, w.maxBatch, lingerC, len(w.pending) == 0 || w.linger > 0)
		if len(items) == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("collector: writer stopped: %w", err)
			}
			if err := w.flush(ctx); err != nil {
				return err
			}
			continue
		}
		for _, it := range items {
			var err error
			switch it.kind {
			case itemEvent:
				err = w.handleEvent(ctx, it)
			case itemReplace:
				err = w.handleReplace(ctx, it)
			case itemHeartbeat:
				err = w.handleHeartbeat(ctx, it)
			case itemShutdown:
				return w.handleShutdown(ctx, it)
			}
			if err != nil {
				return err
			}
			if len(w.pending) >= w.maxBatch {
				if err := w.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// flush writes the pending watch batch. A batch that cannot be written is
// not dropped silently: every resource in it loses coverage (End at the
// first unwritten change) and is relisted, which re-derives the lost
// changes as resync records.
func (w *writer) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	batch := w.pending
	w.pending = nil
	err := w.write(ctx, batch)
	if err == nil {
		return nil
	}
	var fe errFatal
	if errors.As(err, &fe) || ctx.Err() != nil {
		return err
	}
	w.declareLost(batch, err)
	return nil
}

func (w *writer) declareLost(batch []store.Entry, err error) {
	first := map[*resource]time.Time{}
	for _, e := range batch {
		var id string
		var at time.Time
		switch {
		case e.Record != nil:
			id, at = e.Record.Key.Resource, e.Record.ObservedAt.Add(-time.Nanosecond)
		case e.Coverage != nil:
			id, at = e.Coverage.Resource, e.Coverage.At
		}
		r := w.byID[id]
		if r == nil {
			continue
		}
		if t, ok := first[r]; !ok || at.Before(t) {
			first[r] = at
		}
	}
	for r, at := range first {
		w.log.Error("store write failed; ending coverage and forcing a relist",
			"resource", r.id, "lostAt", at, "entries", len(batch), "err", err)
		r.setState(func() {
			r.lost, r.synced = true, false
			if r.lostAt.IsZero() || at.Before(r.lostAt) {
				r.lostAt = at
			}
		})
		r.src.requestRelist()
	}
}

// write writes entries, retrying transient failures with backoff for up
// to retryTimeout. While it retries, the queue fills and the reflectors
// block. ErrInvalid is not retried (the batch is wrong, not the store);
// ErrClosed is fatal.
//
// Retrying assumes a failed Write applied nothing (see the report's
// proposed contract clarification).
func (w *writer) write(ctx context.Context, entries []store.Entry) error {
	deadline := w.clock.Now().Add(w.retryTimeout)
	delay := 50 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := w.store.Write(ctx, entries)
		if err == nil {
			w.applyWritten(entries)
			return nil
		}
		switch {
		case errors.Is(err, store.ErrClosed):
			return errFatal{fmt.Errorf("collector: store closed: %w", err)}
		case errors.Is(err, store.ErrInvalid):
			return fmt.Errorf("collector: store rejected batch: %w", err)
		case ctx.Err() != nil:
			return fmt.Errorf("collector: write: %w", ctx.Err())
		case !w.clock.Now().Before(deadline):
			return fmt.Errorf("collector: write failed after %d attempts: %w", attempt, err)
		}
		w.log.Warn("store write failed; retrying", "attempt", attempt, "entries", len(entries), "retryIn", delay, "err", err)
		select {
		case <-w.clock.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("collector: write: %w", ctx.Err())
		}
		delay = min(delay*2, 5*time.Second)
	}
}

// applyWritten updates durable bookkeeping from the times the store
// assigned (clamped) and wrote back into the entries.
func (w *writer) applyWritten(entries []store.Entry) {
	for _, e := range entries {
		switch {
		case e.Record != nil:
			if r := w.byID[e.Record.Key.Resource]; r != nil {
				r.lastRecAt = latest(r.lastRecAt, e.Record.ObservedAt)
			}
		case e.Coverage != nil:
			r := w.byID[e.Coverage.Resource]
			if r == nil {
				continue
			}
			r.hasHistory = true
			open := e.Coverage.Type == model.CoverageStart
			if open {
				r.lastStartAt = e.Coverage.At
			}
			r.setState(func() { r.covOpen = open })
		}
	}
}

func (w *writer) handleEvent(ctx context.Context, it item) error {
	r := it.res
	if !r.synced || r.lost {
		// Coverage is (being) ended and a relist is pending; the LIST will
		// re-derive this change as a resync record.
		w.log.Debug("event dropped while resource is not synced", "resource", r.id)
		return nil
	}
	k := it.tomb
	if it.hasObj {
		k = nsName{it.obj.Namespace, it.obj.Name}
	}
	prev, had := r.known[k]
	switch it.change {
	case model.ChangeAdded, model.ChangeModified:
		if had && prev.uid != it.obj.UID {
			// A new instance without a delete in between should not happen
			// on a continuous watch; record it the way a relist would.
			last, err := w.lastObjects(ctx, r, []nsName{k})
			if err != nil || r.lost {
				return err
			}
			w.pending = append(w.pending, deleteRecord(r, k, prev, last[k], model.OriginWatch, it.at))
			delete(r.known, k)
			had = false
		}
		if had && prev.hash == it.obj.Hash {
			w.log.Debug("unchanged object skipped", "resource", r.id, "namespace", k.ns, "name", k.name, "resourceVersion", it.obj.ResourceVersion)
			return nil
		}
		typ := model.ChangeModified
		if !had {
			typ = model.ChangeAdded
		}
		w.pending = append(w.pending, record(r, it.obj, typ, model.OriginWatch, it.at))
		r.known[k] = objState{uid: it.obj.UID, hash: it.obj.Hash}
	case model.ChangeDeleted:
		if !had {
			w.log.Debug("delete of an object not known to be present skipped", "resource", r.id, "namespace", k.ns, "name", k.name)
			return nil
		}
		if it.hasObj {
			// The watch delivered the final state (ADR 0003).
			w.pending = append(w.pending, record(r, it.obj, model.ChangeDeleted, model.OriginWatch, it.at))
		} else {
			last, err := w.lastObjects(ctx, r, []nsName{k})
			if err != nil || r.lost {
				return err
			}
			w.pending = append(w.pending, deleteRecord(r, k, prev, last[k], model.OriginWatch, it.at))
		}
		delete(r.known, k)
	}
	return nil
}

// handleReplace applies a complete LIST. The batch is
//
//	[End(lostAt)]  if coverage is open (relist after loss of continuity)
//	diff records   baseline (resource never had coverage) or resync
//	Start(at)
//
// End comes first because the store requires End.At >= the resource's
// latest record time, and the diff records are stamped with the relist
// time, which is later than lostAt. Start comes last so the store clamps
// it after every diff record: from Start.At on, the view is complete.
//
// If the batch exceeds maxBatch entries it is split; End travels in the
// first chunk and Start in the last, so a crash between chunks leaves the
// resource with coverage closed (End, or no events at all) and the
// records already written are simply part of the next relist's baseline.
// Start is never durable before every record of the LIST is.
func (w *writer) handleReplace(ctx context.Context, it item) error {
	r := it.res
	if err := w.flush(ctx); err != nil {
		return err
	}
	if r.lost {
		// Change detection state may contain changes that never reached
		// the store; diff against what the store actually holds.
		v, err := w.store.View(ctx)
		if err != nil {
			return w.replaceFailed(r, err)
		}
		err = w.loadKnown(ctx, v, r)
		_ = v.Close()
		if err != nil {
			return w.replaceFailed(r, err)
		}
	}

	listed := make(map[nsName]normalize.Result, len(it.list))
	for _, o := range it.list {
		listed[nsName{o.Namespace, o.Name}] = o
	}
	keys := make([]nsName, 0, len(listed)+len(r.known))
	for k := range listed {
		keys = append(keys, k)
	}
	var gone []nsName
	for k, prev := range r.known {
		cur, ok := listed[k]
		if !ok {
			keys = append(keys, k)
		}
		if !ok || cur.UID != prev.uid {
			gone = append(gone, k)
		}
	}
	slices.SortFunc(keys, compareNsName)
	last, err := w.lastObjects(ctx, r, gone)
	if err != nil {
		return err
	}

	origin, startReason := model.OriginResync, reasonRelist
	if !r.hasHistory {
		origin, startReason = model.OriginBaseline, reasonInitialList
	}
	entries := make([]store.Entry, 0, len(keys)+2)
	if r.covOpen {
		lostAt, reason := it.lostAt, reasonRelist
		if !r.lostAt.IsZero() && (lostAt.IsZero() || r.lostAt.Before(lostAt)) {
			lostAt, reason = r.lostAt, reasonWriteFailure
		}
		lostAt = latest(lostAt, r.lastRecAt, r.lastStartAt)
		entries = append(entries, coverage(r.id, model.CoverageEnd, lostAt, reason))
	}
	var added, modified, deleted int
	known := make(map[nsName]objState, len(listed))
	for _, k := range keys {
		cur, inList := listed[k]
		prev, had := r.known[k]
		if had && (!inList || cur.UID != prev.uid) {
			entries = append(entries, deleteRecord(r, k, prev, last[k], origin, it.at))
			deleted++
			had = false
		}
		if !inList {
			continue
		}
		known[k] = objState{uid: cur.UID, hash: cur.Hash}
		switch {
		case !had:
			entries = append(entries, record(r, cur, model.ChangeAdded, origin, it.at))
			added++
		case cur.Hash != prev.hash:
			entries = append(entries, record(r, cur, model.ChangeModified, origin, it.at))
			modified++
		}
	}
	entries = append(entries, coverage(r.id, model.CoverageStart, it.at, startReason))

	for chunk := range slices.Chunk(entries, w.maxBatch) {
		if err := w.write(ctx, chunk); err != nil {
			var fe errFatal
			if errors.As(err, &fe) || ctx.Err() != nil {
				return err
			}
			return w.replaceFailed(r, err)
		}
	}
	r.known = known
	r.setState(func() { r.synced, r.lost, r.lostAt = true, false, time.Time{} })
	r.src.relistDone()
	w.log.Info("list applied", "resource", r.id, "origin", origin.String(), "objects", len(listed),
		"added", added, "modified", modified, "deleted", deleted, "coverageEnded", len(entries) > 0 && entries[0].Coverage != nil)
	return nil
}

func (w *writer) replaceFailed(r *resource, err error) error {
	w.log.Error("cannot apply list; forcing another relist", "resource", r.id, "err", err)
	r.setState(func() {
		r.lost, r.synced = true, false
		if r.lostAt.IsZero() {
			r.lostAt = latest(r.lastRecAt, r.lastStartAt)
		}
	})
	r.src.requestRelist()
	return nil
}

// lastObjects returns the latest stored record of each key. Pending
// entries are flushed first so the store holds the latest state.
func (w *writer) lastObjects(ctx context.Context, r *resource, keys []nsName) (map[nsName]model.Record, error) {
	out := map[nsName]model.Record{}
	if len(keys) == 0 {
		return out, nil
	}
	if err := w.flush(ctx); err != nil {
		return nil, err
	}
	v, err := w.store.View(ctx)
	if err != nil {
		w.log.Error("cannot read last known objects; delete records will carry no object", "resource", r.id, "err", err)
		return out, nil
	}
	defer func() { _ = v.Close() }()
	for _, k := range keys {
		rec, ok, err := v.Get(ctx, model.ObjectKey{Resource: r.id, Namespace: k.ns, Name: k.name}, farFuture)
		if err != nil {
			w.log.Error("cannot read last known object", "resource", r.id, "namespace", k.ns, "name", k.name, "err", err)
			continue
		}
		if ok {
			rec.Object = slices.Clone(rec.Object)
			out[k] = rec
		}
	}
	return out, nil
}

func (w *writer) handleHeartbeat(ctx context.Context, it item) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if !it.healthy {
		return nil
	}
	for _, r := range w.ordered {
		if !r.synced || r.lost || !r.covOpen {
			return nil
		}
	}
	if err := w.store.SetHeartbeat(ctx, it.at); err != nil {
		if errors.Is(err, store.ErrClosed) {
			return errFatal{fmt.Errorf("collector: store closed: %w", err)}
		}
		w.log.Warn("heartbeat failed", "err", err)
	}
	return nil
}

// handleShutdown ends every open coverage interval. it.at was stamped
// after all reflectors stopped, so it is not earlier than any accepted
// event.
func (w *writer) handleShutdown(ctx context.Context, it item) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	var ends []store.Entry
	for _, r := range w.ordered {
		if r.covOpen {
			at := latest(it.at, r.lastRecAt, r.lastStartAt)
			if r.lost && !r.lostAt.IsZero() {
				at = latest(r.lostAt, r.lastRecAt, r.lastStartAt)
			}
			ends = append(ends, coverage(r.id, model.CoverageEnd, at, reasonShutdown))
		}
	}
	if len(ends) == 0 {
		return nil
	}
	if err := w.write(ctx, ends); err != nil {
		return fmt.Errorf("collector: end coverage at shutdown (the next start will treat it as a crash): %w", err)
	}
	for _, r := range w.ordered {
		r.setState(func() { r.synced = false })
	}
	return nil
}

func record(r *resource, o normalize.Result, typ model.ChangeType, origin model.Origin, at time.Time) store.Entry {
	return store.Entry{Record: &model.Record{
		ObservedAt:      at,
		Key:             o.Key(r.rt.ResourceType),
		UID:             o.UID,
		ResourceVersion: o.ResourceVersion,
		Type:            typ,
		Origin:          origin,
		Object:          o.Object,
	}}
}

// deleteRecord records the deletion of instance prev of key k, carrying
// the last state Chronos stored for it.
func deleteRecord(r *resource, k nsName, prev objState, last model.Record, origin model.Origin, at time.Time) store.Entry {
	rec := &model.Record{
		ObservedAt: at,
		Key:        model.ObjectKey{Resource: r.id, Namespace: k.ns, Name: k.name},
		UID:        prev.uid,
		Type:       model.ChangeDeleted,
		Origin:     origin,
	}
	if last.UID == prev.uid {
		rec.ResourceVersion, rec.Object = last.ResourceVersion, last.Object
	}
	return store.Entry{Record: rec}
}

func coverage(id string, typ model.CoverageType, at time.Time, reason string) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: at, Resource: id, Type: typ, Reason: reason}}
}

func latest(ts ...time.Time) time.Time {
	var m time.Time
	for _, t := range ts {
		if t.After(m) {
			m = t
		}
	}
	return m
}
