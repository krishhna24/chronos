package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/krishhna24/chronos/internal/model"
)

// Default bounds on one compaction batch. A first retention run after a
// long outage can delete millions of records; committing them in bounded
// chunks keeps memory flat instead of building one batch for all of them.
const (
	defaultCompactChunkDeletes = 50_000
	defaultCompactChunkBytes   = 64 << 20
)

// Compact implements store.Store.
//
// Compaction runs in three steps, each committed with pebble.Sync:
//
//  1. Start batch: the new horizon H, a "compaction pending at H" marker
//     (meta "compacting"), and deletion of every snapshot manifest older
//     than H (they rely on time-index entries about to be removed).
//  2. Record chunks: a scan of the version table, object by object, that
//     deletes every version older than H except the newest one below H,
//     and that newest one too if it is a delete (store.go contract).
//     Deletions are committed in chunks of at most
//     defaultCompactChunkDeletes keys or defaultCompactChunkBytes.
//  3. Final batch: coverage events older than H except the latest, and
//     removal of the pending marker.
//
// Why the horizon goes first: the contract promises that state at every
// t >= horizon is unchanged by compaction. Each chunk deletes only
// versions older than H that are superseded by a newer version below H
// (never visible at t >= H), or the newest version below H when it is a
// delete (the contract removes that key entirely). So at every
// intermediate point, and after a crash between chunks, every answer at
// t >= H is already the final one, except that a key whose newest record
// below H is a delete may still be reported with that delete record until
// its chunk commits. Writing H last instead would make Views opened (or a
// restart) between chunks claim history is reconstructable from the old
// horizon while records in [old, H) are already gone.
//
// A crash leaves the marker; the next Compact (any horizon) finishes the
// pending run first, and snapshot manifests are not written while one is
// pending. Concurrent Writes are not blocked: each run behaves as if it
// happened at the instant of its snapshot, so a record committed after
// that instant is kept even if it is older than H, as memstore keeps a
// record written right after its Compact returns. (A resumed run takes a
// fresh snapshot and therefore also compacts such records; the contract
// says nothing about state below the horizon.)
func (s *Store) Compact(ctx context.Context, horizon time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.ops.Done()
	ctx, cancel := s.opContext(ctx)
	defer cancel()
	s.maintMu.Lock()
	defer s.maintMu.Unlock()
	horizon = normTime(horizon)

	s.mu.Lock()
	cur, pending := s.horizon, s.pending
	s.mu.Unlock()
	if !pending.IsZero() {
		if err := s.compactTo(ctx, pending, true); err != nil {
			return err
		}
		cur = pending
	}
	if !horizon.After(cur) {
		return nil
	}
	return s.compactTo(ctx, horizon, false)
}

// compactTo runs (or, with resume, finishes) a compaction to h.
func (s *Store) compactTo(ctx context.Context, h time.Time, resume bool) error {
	start := time.Now()
	var stats struct{ records, coverage, manifests, chunks int }

	// Step 1: horizon, pending marker, stale manifests.
	b := s.db.NewBatch()
	if err := s.addManifestDeletes(ctx, b, h, &stats.manifests); err != nil {
		_ = b.Close()
		return err
	}
	if err := errors.Join(
		b.Set(keyHorizon, seal(keyHorizon, encodeTimeValue(h)), nil),
		b.Set(keyCompacting, seal(keyCompacting, encodeTimeValue(h)), nil),
	); err != nil {
		_ = b.Close()
		return fmt.Errorf("pebblestore: compact: %w", err)
	}
	if err := s.commitWait(ctx, b, "compaction start commit"); err != nil {
		return err
	}
	s.mu.Lock()
	s.horizon, s.pending = h, h
	s.mu.Unlock()

	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	// Step 2: record chunks.
	maxDeletes, maxBytes := s.compactChunkDeletes, s.compactChunkBytes
	b = s.db.NewBatch()
	owned := true
	defer func() {
		if owned {
			_ = b.Close()
		}
	}()
	pendingDeletes := 0
	flush := func() error {
		if pendingDeletes == 0 {
			return nil
		}
		owned = false
		if err := s.commitWait(ctx, b, "compaction chunk commit"); err != nil {
			return err
		}
		stats.chunks++
		pendingDeletes = 0
		b, owned = s.db.NewBatch(), true
		if s.compactChunkHook != nil {
			return s.compactChunkHook(stats.chunks)
		}
		return nil
	}
	del := func(vk []byte, t time.Time) error {
		if err := errors.Join(b.Delete(vk, nil), b.Delete(indexKey(t), nil)); err != nil {
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		stats.records++
		if pendingDeletes++; pendingDeletes >= maxDeletes || b.Len() >= maxBytes {
			return flush()
		}
		return nil
	}
	if err := s.sweepVersions(ctx, snap, h, del); err != nil {
		return err
	}

	// Step 3: coverage and the end of the pending state, in the last batch.
	resources, err := readResources(ctx, snap)
	if err != nil {
		return fmt.Errorf("pebblestore: compact: %w", err)
	}
	for _, res := range resources {
		cs, err := readCoverage(ctx, snap, res)
		if err != nil {
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		// Mirrors memstore: keep from the first event at or after h, plus
		// the one before it.
		i := sort.Search(len(cs), func(i int) bool { return !cs[i].At.Before(h) })
		for _, c := range cs[:max(i-1, 0)] {
			if err := b.Delete(coverageKey(res, c.Seq), nil); err != nil {
				return fmt.Errorf("pebblestore: compact: %w", err)
			}
			stats.coverage++
		}
	}
	if err := b.Delete(keyCompacting, nil); err != nil {
		return fmt.Errorf("pebblestore: compact: %w", err)
	}
	owned = false
	if err := s.commitWait(ctx, b, "compaction final commit"); err != nil {
		return err
	}
	stats.chunks++
	s.mu.Lock()
	s.pending = time.Time{}
	s.mu.Unlock()
	s.log.Info("compacted",
		"horizon", h, "resumed", resume, "recordsRemoved", stats.records,
		"coverageRemoved", stats.coverage, "manifestsRemoved", stats.manifests,
		"batches", stats.chunks+1, "duration", time.Since(start))
	return nil
}

// sweepVersions walks the version table of snap object by object and
// calls del for every version the contract removes at horizon h. Memory
// is O(1) in the number of objects: versions of one object are adjacent
// and only the previous version below h is remembered.
func (s *Store) sweepVersions(ctx context.Context, snap pebbleReader, h time.Time, del func(vk []byte, t time.Time) error) error {
	it, err := newIter(ctx, snap, &pebble.IterOptions{LowerBound: []byte{tabVersion}, UpperBound: []byte{tabVersion + 1}})
	if err != nil {
		return err
	}
	defer func(it *iter) { _ = it.Close() }(it)
	hEnc := appendTime(nil, h)
	n := 0
	var prevKey []byte
	var prevAt time.Time
	for ok := it.First(); ok; {
		if n++; n%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		k := it.Key()
		if len(k) < 1+timeLen {
			return corruptf(k, "version key too short")
		}
		obj := slices.Clone(k[:len(k)-timeLen])
		prevKey = prevKey[:0]
		// Versions of obj below h, oldest first.
		for ok && len(it.Key()) == len(obj)+timeLen && bytes.HasPrefix(it.Key(), obj) &&
			bytes.Compare(it.Key()[len(obj):], hEnc) < 0 {
			t, err := decodeTime(it.Key()[len(obj):])
			if err != nil {
				return corruptf(it.Key(), "bad version time: %v", err)
			}
			if len(prevKey) > 0 {
				if err := del(prevKey, prevAt); err != nil {
					return err
				}
			}
			prevKey, prevAt = append(prevKey[:0], it.Key()...), t
			ok = it.Next()
		}
		if len(prevKey) > 0 {
			// The newest version below h survives unless it is a delete.
			// Its value must be read (and validated) to know the type.
			typ, err := s.versionType(snap, prevKey)
			if err != nil {
				return err
			}
			if typ == model.ChangeDeleted {
				if err := del(prevKey, prevAt); err != nil {
					return err
				}
			}
		}
		ok = it.SeekGE(prefixEnd(obj))
	}
	return closeIter(it)
}

func (s *Store) versionType(r pebbleReader, vk []byte) (model.ChangeType, error) {
	var typ model.ChangeType
	found, err := getVersion(r, vk, func(rec model.Record) error {
		typ = rec.Type
		return nil
	})
	if err == nil && !found {
		err = corruptf(vk, "version vanished from the compaction snapshot")
	}
	return typ, mapPebbleErr(err)
}

// addManifestDeletes adds deletion of every manifest older than h to b.
func (s *Store) addManifestDeletes(ctx context.Context, b *pebble.Batch, h time.Time, n *int) error {
	it, err := newIter(ctx, s.db, &pebble.IterOptions{
		LowerBound: []byte{tabManifest},
		UpperBound: manifestKey(h),
	})
	if err != nil {
		return err
	}
	defer func(it *iter) { _ = it.Close() }(it)
	for ok := it.First(); ok; ok = it.Next() {
		mk := slices.Clone(it.Key())
		if len(mk) != 1+timeLen {
			continue
		}
		cp := append([]byte{tabChunk}, mk[1:]...)
		if err := errors.Join(b.Delete(mk, nil), b.DeleteRange(cp, prefixEnd(cp), nil)); err != nil {
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		*n++
	}
	return closeIter(it)
}
