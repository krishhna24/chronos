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
	if !s.pending.IsZero() {
		// An interrupted compaction may still remove records a manifest
		// would name; wait until Compact has finished it.
		s.mu.Unlock()
		return nil
	}
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
	if err := s.commitWait(ctx, b, "snapshot manifest commit"); err != nil {
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

// Window limits for the manifest path. The time-index window (S, At] is
// scanned in full before the first record can be emitted, because its
// entries are ordered by time, not key. The manifest path is therefore
// only used when that window is small: at most defaultManifestWindowMin records,
// or a quarter of the manifest's entries for the resource. The window size
// is bounded without scanning it: every record and coverage event since S
// takes one Seq, so it is at most the view's last Seq minus the manifest's.
const defaultManifestWindowMin = 1024

// loadManifestHeader returns the newest usable manifest at or before
// opts.At, or nil when there is none, it is older than the horizon, or its
// header fails validation (the caller then uses the scan path).
func (v *view) loadManifestHeader(ctx context.Context, opts store.ListOptions) (*manifestHeader, error) {
	it, err := newIter(ctx, v.snap, &pebble.IterOptions{
		LowerBound: []byte{tabManifest},
		UpperBound: atOrBefore([]byte{tabManifest}, opts.At),
	})
	if err != nil {
		return nil, err
	}
	defer func(it *iter) { _ = it.Close() }(it)
	if !it.Last() {
		return nil, closeIter(it)
	}
	mk := slices.Clone(it.Key())
	val, err := iterValue(it)
	var h *manifestHeader
	if err == nil {
		h, err = decodeManifestHeader(mk, val)
	}
	if cerr := closeIter(it); err == nil {
		err = cerr
	}
	if err != nil {
		v.s.stats.fallbacks.Add(1)
		v.s.log.Warn("snapshot manifest unusable, falling back to scan", "manifest", describeKey(mk), "err", err)
		return nil, nil
	}
	if h.at.Before(v.horizon) {
		// Compact deletes these; never trust one that survived.
		return nil, nil
	}
	return h, nil
}

// manifestStream yields the entries of one resource's manifest chunks in
// key order, reading each namespace chunk only when the merge reaches it.
type manifestStream struct {
	v    *view
	h    *manifestHeader
	opts store.ListOptions
	nss  []nsCount // remaining namespaces, in order
	buf  []listEntry
}

func newManifestStream(v *view, h *manifestHeader, opts store.ListOptions) *manifestStream {
	var nss []nsCount
	for _, n := range h.resources[opts.Resource] {
		if opts.Namespace != "" && n.ns != opts.Namespace {
			continue
		}
		// Namespaces before After are skipped without reading their chunk.
		if opts.After != nil && n.ns < opts.After.Namespace {
			continue
		}
		nss = append(nss, n)
	}
	return &manifestStream{v: v, h: h, opts: opts, nss: nss}
}

// errChunkUnusable reports a manifest chunk that is missing or fails
// validation; the caller resumes on the scan path.
var errChunkUnusable = errors.New("snapshot manifest chunk unusable")

// peek returns the next entry without consuming it; ok is false at the end.
func (m *manifestStream) peek() (listEntry, bool, error) {
	for len(m.buf) == 0 {
		if len(m.nss) == 0 {
			return listEntry{}, false, nil
		}
		n := m.nss[0]
		m.nss = m.nss[1:]
		ck := chunkKey(m.h.at, m.opts.Resource, n.ns)
		m.v.s.stats.chunkReads.Add(1)
		val, closer, err := m.v.snap.Get(ck)
		if errors.Is(err, pebble.ErrNotFound) {
			m.v.s.log.Warn("snapshot manifest chunk missing, falling back to scan", "chunk", describeKey(ck))
			return listEntry{}, false, errChunkUnusable
		}
		if err != nil {
			return listEntry{}, false, fmt.Errorf("pebblestore: read %s: %w", describeKey(ck), mapPebbleErr(err))
		}
		chunk, err := decodeChunk(ck, val, n.count)
		_ = closer.Close()
		if err != nil {
			m.v.s.log.Warn("snapshot manifest chunk unusable, falling back to scan", "chunk", describeKey(ck), "err", err)
			return listEntry{}, false, errChunkUnusable
		}
		for _, e := range chunk {
			if afterKey(m.opts, n.ns, e.name) {
				m.buf = append(m.buf, listEntry{ns: n.ns, name: e.name, at: e.at, deleted: e.deleted, fromMan: true})
			}
		}
	}
	return m.buf[0], true, nil
}

func (m *manifestStream) next() { m.buf = m.buf[1:] }

// seqAt returns the Seq of the latest record at or before t (0: none):
// one seek on the time index. Seqs since a manifest's lastSeq bound the
// number of time-index entries in its window.
func (v *view) seqAt(ctx context.Context, t time.Time) (uint64, error) {
	it, err := newIter(ctx, v.snap, &pebble.IterOptions{
		LowerBound: []byte{tabIndex},
		UpperBound: atOrBefore([]byte{tabIndex}, t),
	})
	if err != nil {
		return 0, err
	}
	defer func(it *iter) { _ = it.Close() }(it)
	if !it.Last() {
		return 0, closeIter(it)
	}
	val, err := iterValue(it)
	var ref indexRef
	if err == nil {
		ref, err = decodeIndexRef(it.Key(), val)
	}
	if err != nil {
		return 0, err
	}
	return ref.seq, closeIter(it)
}

// listFromManifest serves List from the newest manifest at S <= opts.At
// plus the time index over (S, opts.At]. done is false when the scan path
// must serve the request instead (no usable manifest, or a window too
// large to be worth it); nothing has been emitted then. A chunk found
// unusable part-way through is handled here: the rest of the listing
// continues on the scan path after the last key emitted, so results stay
// identical and nothing is emitted twice.
func (v *view) listFromManifest(ctx context.Context, opts store.ListOptions, fn func(model.Record) error) (done bool, err error) {
	h, err := v.loadManifestHeader(ctx, opts)
	if err != nil || h == nil {
		return err != nil, err
	}
	entries := 0
	for _, n := range h.resources[opts.Resource] {
		entries += n.count
	}
	seqAt, err := v.seqAt(ctx, opts.At)
	if err != nil {
		return true, err
	}
	if window := seqAt - min(seqAt, h.lastSeq); window > uint64(max(v.s.manifestWindowMin, entries/4)) {
		v.s.stats.windowSkips.Add(1)
		return false, nil
	}
	s := h.at
	// Entries that are deletes older than the view's horizon may have been
	// removed by a Compact that ran after the manifest was built (the
	// store.go contract drops a key whose latest record before the horizon
	// is a delete), or by one still pending in this snapshot; such an entry
	// is skipped if its record is gone, as the scan would not find it.
	compactedSince := v.horizon.After(h.builtHorizon) || !v.pending.IsZero()

	// Keys of this resource changed in (S, At], latest change each. The
	// window is small (checked above).
	window := map[model.ObjectKey]listEntry{}
	if opts.At.After(s) {
		it, err := newIter(ctx, v.snap, &pebble.IterOptions{
			LowerBound: atOrBefore([]byte{tabIndex}, s),
			UpperBound: atOrBefore([]byte{tabIndex}, opts.At),
		})
		if err != nil {
			return true, err
		}
		defer func(it *iter) { _ = it.Close() }(it)
		for ok := it.First(); ok; ok = it.Next() {
			val, err := iterValue(it)
			var ref indexRef
			if err == nil {
				ref, err = decodeIndexRef(it.Key(), val)
			}
			if err != nil {
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

	var last *listEntry
	emit := func(e listEntry) error {
		k := model.ObjectKey{Resource: opts.Resource, Namespace: e.ns, Name: e.name}
		vk := versionKey(k, e.at)
		found, err := getVersion(v.snap, vk, fn)
		if err == nil && !found && compactedSince && e.fromMan && e.deleted && e.at.Before(v.horizon) {
			return nil // removed with its key by compaction
		}
		if err == nil && !found {
			err = corruptf(vk, "listed by snapshot manifest or time index but missing")
		}
		if err == nil {
			last = &e
		}
		return err
	}
	ms := newManifestStream(v, h, opts)
	for i := 1; i < len(ms.nss); i++ {
		if ms.nss[i-1].ns >= ms.nss[i].ns {
			v.s.stats.fallbacks.Add(1)
			v.s.log.Warn("snapshot manifest namespaces out of order, falling back to scan", "manifest", describeKey(manifestKey(h.at)))
			return false, nil
		}
	}
	j, n := 0, 0
	for {
		if n++; n%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		b, okB, err := ms.peek()
		if errors.Is(err, errChunkUnusable) {
			v.s.stats.fallbacks.Add(1)
			rest := opts
			if last != nil {
				rest.After = &model.ObjectKey{Namespace: last.ns, Name: last.name}
			}
			return true, v.listScan(ctx, rest, fn)
		}
		if err != nil {
			return true, err
		}
		okC := j < len(changed)
		var e listEntry
		switch {
		case !okB && !okC:
			return true, nil
		case !okC:
			e = b
			ms.next()
		case !okB:
			e = changed[j]
			j++
		default:
			switch c := compareEntry(b, changed[j]); {
			case c < 0:
				e = b
				ms.next()
			case c > 0:
				e = changed[j]
				j++
			default: // changed after S: the window entry is newer
				e = changed[j]
				ms.next()
				j++
			}
		}
		if err := emit(e); err != nil {
			return true, err
		}
	}
}
