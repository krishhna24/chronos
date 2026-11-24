package pebblestore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Snapshot writes a snapshot manifest for the store's current state (ADR
// 0001): for every resource and namespace, the keys that have a retained
// record at S = the last assigned time, with the time of each key's latest
// record and whether it is a delete. It is a no-op when nothing was written
// since the previous manifest. The background job calls it every
// Options.SnapshotInterval; it is exported for tests, benchmarks and
// operators.
//
// Manifests include keys whose latest record is a delete because
// View.List must report them (see the package documentation).
func (s *Store) Snapshot(ctx context.Context) error {
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.ops.Done()
	ctx, cancel := s.opContext(ctx)
	defer cancel()
	s.maintMu.Lock()
	defer s.maintMu.Unlock()

	s.mu.Lock()
	at, lastSeq, horizon, prev := s.lastTime, s.seq, s.horizon, s.lastManifest
	resources := slices.Sorted(maps.Keys(s.lastRec))
	var snap *pebble.Snapshot
	if !at.IsZero() && at.After(prev) && !at.Before(horizon) {
		snap = s.db.NewSnapshot()
	}
	s.mu.Unlock()
	if snap == nil {
		return nil
	}
	defer func() { _ = snap.Close() }()

	start := time.Now()
	h := &manifestHeader{at: at, lastSeq: lastSeq, builtHorizon: horizon, resources: map[string][]nsCount{}}
	chunks := map[string]map[string][]manifestEntry{} // resource -> ns -> entries
	entries := 0
	for _, res := range resources {
		byNS := map[string][]manifestEntry{}
		var order []string
		err := scanLatest(ctx, snap, versionResourcePrefix(res), nil, at, func(key, val []byte) error {
			rec, err := decodeVersion(key, val)
			if err != nil {
				return err
			}
			ns := rec.Key.Namespace
			if _, ok := byNS[ns]; !ok {
				order = append(order, ns)
			}
			byNS[ns] = append(byNS[ns], manifestEntry{name: rec.Key.Name, at: rec.ObservedAt, deleted: rec.Type == model.ChangeDeleted})
			entries++
			return nil
		})
		if err != nil {
			return fmt.Errorf("pebblestore: snapshot %s: %w", res, err)
		}
		for _, ns := range order {
			h.resources[res] = append(h.resources[res], nsCount{ns: ns, count: len(byNS[ns])})
		}
		chunks[res] = byNS
	}

	b := s.db.NewBatch()
	owned := true // until handed to commitWait
	defer func() {
		if owned {
			_ = b.Close()
		}
	}()
	for _, res := range resources {
		for ns, es := range chunks[res] {
			k := chunkKey(at, res, ns)
			if err := b.Set(k, seal(k, encodeChunk(es)), nil); err != nil {
				return fmt.Errorf("pebblestore: snapshot: %w", err)
			}
		}
	}
	mk := manifestKey(at)
	if err := b.Set(mk, seal(mk, encodeManifestHeader(h, resources)), nil); err != nil {
		return fmt.Errorf("pebblestore: snapshot: %w", err)
	}
	size := b.Len()
	owned = false
	if err := s.commitWait(b, "snapshot manifest commit"); err != nil {
		return err
	}
	s.mu.Lock()
	s.lastManifest = at
	s.mu.Unlock()
	s.log.Info("snapshot manifest written",
		"at", at, "resources", len(resources), "entries", entries,
		"batchBytes", size, "duration", time.Since(start))
	return nil
}

func (s *Store) snapshotLoop(interval time.Duration) {
	defer close(s.bgDone)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-s.stopBG
		cancel()
	}()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stopBG:
			return
		case <-t.C:
			if err := s.Snapshot(ctx); err != nil && !errors.Is(err, store.ErrClosed) && !errors.Is(err, context.Canceled) {
				s.log.Error("snapshot manifest failed", "err", err)
			}
		}
	}
}

type listEntry struct {
	ns, name string
	at       time.Time
	deleted  bool
	fromMan  bool
}

func compareEntry(a, b listEntry) int {
	if c := cmp.Compare(a.ns, b.ns); c != 0 {
		return c
	}
	return cmp.Compare(a.name, b.name)
}

// loadManifest returns the entries of the newest usable manifest at or
// before opts.At, sorted by key. ok is false when there is no usable
// manifest (none, older than the horizon, or failing validation); the
// caller then uses the scan path.
func (v *view) loadManifest(ctx context.Context, opts store.ListOptions) (h *manifestHeader, es []listEntry, ok bool, err error) {
	it, err := v.snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: []byte{tabManifest},
		UpperBound: atOrBefore([]byte{tabManifest}, opts.At),
	})
	if err != nil {
		return nil, nil, false, err
	}
	if !it.Last() {
		return nil, nil, false, closeIter(it)
	}
	mk := slices.Clone(it.Key())
	val, err := iterValue(it)
	if err == nil {
		h, err = decodeManifestHeader(mk, val)
	}
	if cerr := closeIter(it); err == nil {
		err = cerr
	}
	if err != nil {
		v.s.stats.fallbacks.Add(1)
		v.s.log.Warn("snapshot manifest unusable, falling back to scan", "manifest", describeKey(mk), "err", err)
		return nil, nil, false, nil
	}
	if h.at.Before(v.horizon) {
		// Compact deletes these; never trust one that survived.
		return nil, nil, false, nil
	}
	for _, n := range h.resources[opts.Resource] {
		if opts.Namespace != "" && n.ns != opts.Namespace {
			continue
		}
		// Namespaces before After are skipped without reading their chunk.
		if opts.After != nil && n.ns < opts.After.Namespace {
			continue
		}
		ck := chunkKey(h.at, opts.Resource, n.ns)
		val, closer, err := v.snap.Get(ck)
		if errors.Is(err, pebble.ErrNotFound) {
			v.s.stats.fallbacks.Add(1)
			v.s.log.Warn("snapshot manifest chunk missing, falling back to scan", "chunk", describeKey(ck))
			return nil, nil, false, nil
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("pebblestore: read %s: %w", describeKey(ck), err)
		}
		chunk, err := decodeChunk(ck, val, n.count)
		_ = closer.Close()
		if err != nil {
			v.s.stats.fallbacks.Add(1)
			v.s.log.Warn("snapshot manifest chunk unusable, falling back to scan", "chunk", describeKey(ck), "err", err)
			return nil, nil, false, nil
		}
		for _, e := range chunk {
			if !afterKey(opts, n.ns, e.name) {
				continue
			}
			es = append(es, listEntry{ns: n.ns, name: e.name, at: e.at, deleted: e.deleted, fromMan: true})
		}
	}
	if !slices.IsSortedFunc(es, compareEntry) {
		v.s.stats.fallbacks.Add(1)
		v.s.log.Warn("snapshot manifest namespaces out of order, falling back to scan", "manifest", describeKey(mk))
		return nil, nil, false, nil
	}
	return h, es, true, nil
}

// listFromManifest serves List from the newest manifest at S <= opts.At
// plus the time index over (S, opts.At]. done is false when no usable
// manifest exists and nothing has been emitted, so the caller must fall
// back to the scan path.
func (v *view) listFromManifest(ctx context.Context, opts store.ListOptions, fn func(model.Record) error) (done bool, err error) {
	h, base, ok, err := v.loadManifest(ctx, opts)
	if err != nil {
		return true, err
	}
	if !ok {
		return false, nil
	}
	s := h.at
	// Entries that are deletes older than the view's horizon were removed
	// by a Compact that ran after the manifest was built (the store.go
	// contract drops a key whose latest record before the horizon is a
	// delete). If the manifest was built at the current horizon, its
	// entries already reflect that compaction and any such delete was
	// written afterwards, so it is real.
	compactedSince := v.horizon.After(h.builtHorizon)

	// Keys changed in (S, At], with their latest change in that window.
	window := map[model.ObjectKey]listEntry{}
	if opts.At.After(s) {
		it, err := v.snap.NewIterWithContext(ctx, &pebble.IterOptions{
			LowerBound: atOrBefore([]byte{tabIndex}, s),
			UpperBound: atOrBefore([]byte{tabIndex}, opts.At),
		})
		if err != nil {
			return true, err
		}
		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			if n++; n%ctxCheckEvery == 0 {
				if err := ctx.Err(); err != nil {
					_ = it.Close()
					return true, err
				}
			}
			val, err := iterValue(it)
			var ref indexRef
			if err == nil {
				ref, err = decodeIndexRef(it.Key(), val)
			}
			if err != nil {
				_ = it.Close()
				return true, err
			}
			if ref.key.Resource != opts.Resource || (opts.Namespace != "" && ref.key.Namespace != opts.Namespace) ||
				!afterKey(opts, ref.key.Namespace, ref.key.Name) {
				continue
			}
			window[ref.key] = listEntry{ns: ref.key.Namespace, name: ref.key.Name, at: ref.time, deleted: ref.typ == model.ChangeDeleted}
		}
		if err := closeIter(it); err != nil {
			return true, err
		}
	}
	changed := slices.SortedFunc(maps.Values(window), compareEntry)

	emit := func(e listEntry) error {
		if compactedSince && e.fromMan && e.deleted && e.at.Before(v.horizon) {
			return nil
		}
		k := model.ObjectKey{Resource: opts.Resource, Namespace: e.ns, Name: e.name}
		vk := versionKey(k, e.at)
		found, err := getVersion(v.snap, vk, fn)
		if err == nil && !found {
			err = corruptf(vk, "listed by snapshot manifest or time index but missing")
		}
		return err
	}
	i, j, n := 0, 0, 0
	for i < len(base) || j < len(changed) {
		if n++; n%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		var e listEntry
		switch {
		case j >= len(changed):
			e = base[i]
			i++
		case i >= len(base):
			e = changed[j]
			j++
		default:
			switch c := compareEntry(base[i], changed[j]); {
			case c < 0:
				e = base[i]
				i++
			case c > 0:
				e = changed[j]
				j++
			default: // changed after S: the window entry is newer
				e = changed[j]
				i++
				j++
			}
		}
		if err := emit(e); err != nil {
			return true, err
		}
	}
	return true, nil
}
