package pebblestore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Compact implements store.Store.
//
// It reads a Pebble snapshot, computes every deletion the store.go
// contract requires for records and coverage events older than horizon,
// drops snapshot manifests older than horizon, and commits all of it with
// the new horizon in one synced batch. Compact is therefore atomic: a View
// sees either the state before or after it, never a mix, and a crash
// leaves one or the other.
//
// Concurrent Writes are not blocked. Compact behaves as if it ran at the
// instant its snapshot was taken: a record committed after that instant is
// kept even if it is older than horizon, exactly as memstore keeps a
// record written right after its Compact returns. Deleted keys are point
// keys disjoint from anything a Write adds (times are unique), so the two
// never conflict.
func (s *Store) Compact(ctx context.Context, horizon time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.maintMu.Lock()
	defer s.maintMu.Unlock()
	horizon = normTime(horizon)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return store.ErrClosed
	}
	if !horizon.After(s.horizon) {
		s.mu.Unlock()
		return nil
	}
	snap := s.db.NewSnapshot()
	s.mu.Unlock()
	defer func() { _ = snap.Close() }()

	start := time.Now()
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	var stats struct{ records, coverage, manifests int }
	del := func(keys ...[]byte) error {
		for _, k := range keys {
			if err := b.Delete(k, nil); err != nil {
				return fmt.Errorf("pebblestore: compact: %w", err)
			}
		}
		return nil
	}

	// Records: walk the time index below the horizon. For each key, every
	// record older than the newest one below the horizon is dropped; the
	// newest is dropped too if it is a delete.
	type latest struct {
		at      time.Time
		deleted bool
	}
	last := map[model.ObjectKey]latest{}
	it, err := snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: []byte{tabIndex},
		UpperBound: indexKey(horizon),
	})
	if err != nil {
		return err
	}
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		if n++; n%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				_ = it.Close()
				return err
			}
		}
		val, err := iterValue(it)
		var ref indexRef
		if err == nil {
			ref, err = decodeIndexRef(it.Key(), val)
		}
		if err != nil {
			_ = it.Close()
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		if prev, ok := last[ref.key]; ok {
			if err := del(versionKey(ref.key, prev.at), indexKey(prev.at)); err != nil {
				_ = it.Close()
				return err
			}
			stats.records++
		}
		last[ref.key] = latest{at: ref.time, deleted: ref.typ == model.ChangeDeleted}
	}
	if err := closeIter(it); err != nil {
		return err
	}
	for k, l := range last {
		if l.deleted {
			if err := del(versionKey(k, l.at), indexKey(l.at)); err != nil {
				return err
			}
			stats.records++
		}
	}

	// Coverage: keep events from the first one at or after the horizon,
	// plus the one before it. The search mirrors memstore exactly.
	resources, err := readResources(ctx, snap)
	if err != nil {
		return fmt.Errorf("pebblestore: compact: %w", err)
	}
	for _, res := range resources {
		cs, err := readCoverage(ctx, snap, res)
		if err != nil {
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		i := sort.Search(len(cs), func(i int) bool { return !cs[i].At.Before(horizon) })
		for _, c := range cs[:max(i-1, 0)] {
			if err := del(coverageKey(res, c.Seq)); err != nil {
				return err
			}
			stats.coverage++
		}
	}

	// Manifests older than the horizon: the time index entries they rely on
	// for (S, T] are being removed, so they would give wrong answers.
	it, err = snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: []byte{tabManifest},
		UpperBound: manifestKey(horizon),
	})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		mk := slices.Clone(it.Key())
		if len(mk) != 1+timeLen {
			continue
		}
		cp := append([]byte{tabChunk}, mk[1:]...)
		if err := del(mk); err != nil {
			_ = it.Close()
			return err
		}
		if err := b.DeleteRange(cp, prefixEnd(cp), nil); err != nil {
			_ = it.Close()
			return fmt.Errorf("pebblestore: compact: %w", err)
		}
		stats.manifests++
	}
	if err := closeIter(it); err != nil {
		return err
	}

	if err := b.Set(keyHorizon, seal(keyHorizon, encodeTimeValue(horizon)), nil); err != nil {
		return fmt.Errorf("pebblestore: compact: %w", err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: commit compaction: %w", err)
	}
	s.mu.Lock()
	s.horizon = horizon
	s.mu.Unlock()
	s.log.Info("compacted",
		"horizon", horizon, "recordsRemoved", stats.records,
		"coverageRemoved", stats.coverage, "manifestsRemoved", stats.manifests,
		"keysBelowHorizon", len(last), "duration", time.Since(start))
	return nil
}
