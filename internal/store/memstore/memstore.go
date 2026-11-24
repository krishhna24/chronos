// Package memstore is an in-memory store.Store. It is the reference
// implementation of the store contract: simple enough to be obviously
// correct, used as the oracle in differential tests and as a fake in
// collector and query tests. It is not durable.
package memstore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

type state struct {
	log      []model.Record                     // all records, Seq order
	byKey    map[model.ObjectKey][]model.Record // per key, Seq order
	coverage map[string][]model.CoverageEvent   // per resource, Seq order
	horizon  time.Time
}

// Store is safe for concurrent use.
type Store struct {
	mu        sync.RWMutex
	st        state
	seq       uint64
	lastTime  time.Time
	lastRec   map[string]time.Time // per resource: latest record time
	heartbeat time.Time
	closed    bool
}

var _ store.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		st: state{
			byKey:    map[model.ObjectKey][]model.Record{},
			coverage: map[string][]model.CoverageEvent{},
		},
		lastRec: map[string]time.Time{},
	}
}

func (s *Store) Write(ctx context.Context, entries []store.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}

	// Validate against a scratch copy of the mutable bookkeeping so a
	// failing batch leaves no trace.
	seq, lastTime := s.seq, s.lastTime
	lastRec := map[string]time.Time{}
	lastCov := map[string]model.CoverageEvent{}
	recs := make([]model.Record, 0, len(entries))
	covs := make([]model.CoverageEvent, 0)
	getLastRec := func(r string) time.Time {
		if t, ok := lastRec[r]; ok {
			return t
		}
		return s.lastRec[r]
	}
	getLastCov := func(r string) (model.CoverageEvent, bool) {
		if c, ok := lastCov[r]; ok {
			return c, true
		}
		cs := s.st.coverage[r]
		if len(cs) == 0 {
			return model.CoverageEvent{}, false
		}
		return cs[len(cs)-1], true
	}
	clamp := func(t time.Time) time.Time {
		if !t.After(lastTime) {
			t = lastTime.Add(time.Nanosecond)
		}
		lastTime = t
		return t
	}

	for i, e := range entries {
		switch {
		case e.Record != nil && e.Coverage == nil:
			r := *e.Record
			if err := r.Key.Validate(); err != nil {
				return fmt.Errorf("%w: entry %d: %w", store.ErrInvalid, i, err)
			}
			if r.Type < model.ChangeAdded || r.Type > model.ChangeDeleted {
				return fmt.Errorf("%w: entry %d: bad change type %d", store.ErrInvalid, i, r.Type)
			}
			if r.Origin < model.OriginWatch || r.Origin > model.OriginResync {
				return fmt.Errorf("%w: entry %d: bad origin %d", store.ErrInvalid, i, r.Origin)
			}
			seq++
			r.Seq = seq
			r.ObservedAt = clamp(r.ObservedAt)
			r.Object = slices.Clone(r.Object)
			lastRec[r.Key.Resource] = r.ObservedAt
			recs = append(recs, r)
		case e.Coverage != nil && e.Record == nil:
			c := *e.Coverage
			if c.Resource == "" {
				return fmt.Errorf("%w: entry %d: coverage without resource", store.ErrInvalid, i)
			}
			prev, hasPrev := getLastCov(c.Resource)
			switch c.Type {
			case model.CoverageStart:
				if hasPrev && prev.Type == model.CoverageStart {
					return fmt.Errorf("%w: entry %d: Start after Start for %s", store.ErrInvalid, i, c.Resource)
				}
				c.At = clamp(c.At)
			case model.CoverageEnd:
				if !hasPrev || prev.Type != model.CoverageStart {
					return fmt.Errorf("%w: entry %d: End without open Start for %s", store.ErrInvalid, i, c.Resource)
				}
				if c.At.Before(prev.At) || c.At.Before(getLastRec(c.Resource)) {
					return fmt.Errorf("%w: entry %d: End at %s precedes earlier data for %s", store.ErrInvalid, i, c.At, c.Resource)
				}
			default:
				return fmt.Errorf("%w: entry %d: bad coverage type %d", store.ErrInvalid, i, c.Type)
			}
			seq++
			c.Seq = seq
			lastCov[c.Resource] = c
			covs = append(covs, c)
		default:
			return fmt.Errorf("%w: entry %d: exactly one of Record and Coverage must be set", store.ErrInvalid, i)
		}
	}

	// Commit.
	ri, ci := 0, 0
	for _, e := range entries {
		if e.Record != nil {
			r := recs[ri]
			ri++
			s.st.log = append(s.st.log, r)
			s.st.byKey[r.Key] = append(s.st.byKey[r.Key], r)
			e.Record.Seq, e.Record.ObservedAt = r.Seq, r.ObservedAt
		} else {
			c := covs[ci]
			ci++
			s.st.coverage[c.Resource] = append(s.st.coverage[c.Resource], c)
			e.Coverage.Seq, e.Coverage.At = c.Seq, c.At
		}
	}
	for r, t := range lastRec {
		s.lastRec[r] = t
	}
	s.seq, s.lastTime = seq, lastTime
	return nil
}

func (s *Store) View(ctx context.Context) (store.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, store.ErrClosed
	}
	// Slices are append-only between compactions and compaction builds new
	// ones, so copying headers yields an immutable snapshot.
	v := &view{st: state{
		log:      s.st.log[:len(s.st.log):len(s.st.log)],
		byKey:    make(map[model.ObjectKey][]model.Record, len(s.st.byKey)),
		coverage: make(map[string][]model.CoverageEvent, len(s.st.coverage)),
		horizon:  s.st.horizon,
	}}
	for k, rs := range s.st.byKey {
		v.st.byKey[k] = rs[:len(rs):len(rs)]
	}
	for k, cs := range s.st.coverage {
		v.st.coverage[k] = cs[:len(cs):len(cs)]
	}
	return v, nil
}

func (s *Store) SetHeartbeat(ctx context.Context, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}
	if t.After(s.heartbeat) {
		s.heartbeat = t
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return time.Time{}, store.ErrClosed
	}
	return s.heartbeat, nil
}

func (s *Store) Compact(ctx context.Context, horizon time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}
	if !horizon.After(s.st.horizon) {
		return nil
	}
	keep := map[uint64]bool{}
	byKey := make(map[model.ObjectKey][]model.Record, len(s.st.byKey))
	for k, rs := range s.st.byKey {
		i := sort.Search(len(rs), func(i int) bool { return !rs[i].ObservedAt.Before(horizon) })
		var out []model.Record
		if i > 0 && rs[i-1].Type != model.ChangeDeleted {
			out = append(out, rs[i-1])
		}
		out = append(out, rs[i:]...)
		if len(out) == 0 {
			continue
		}
		for _, r := range out {
			keep[r.Seq] = true
		}
		byKey[k] = out
	}
	var log []model.Record
	for _, r := range s.st.log {
		if keep[r.Seq] {
			log = append(log, r)
		}
	}
	coverage := make(map[string][]model.CoverageEvent, len(s.st.coverage))
	for res, cs := range s.st.coverage {
		i := sort.Search(len(cs), func(i int) bool { return !cs[i].At.Before(horizon) })
		start := max(i-1, 0)
		coverage[res] = slices.Clone(cs[start:])
	}
	s.st = state{log: log, byKey: byKey, coverage: coverage, horizon: horizon}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

type view struct {
	mu     sync.Mutex
	st     state
	closed bool
}

func (v *view) check(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return store.ErrClosed
	}
	return ctx.Err()
}

func latestAt(rs []model.Record, at time.Time) (model.Record, bool) {
	i := sort.Search(len(rs), func(i int) bool { return rs[i].ObservedAt.After(at) })
	if i == 0 {
		return model.Record{}, false
	}
	return rs[i-1], true
}

func (v *view) Get(ctx context.Context, key model.ObjectKey, at time.Time) (model.Record, bool, error) {
	if err := v.check(ctx); err != nil {
		return model.Record{}, false, err
	}
	r, ok := latestAt(v.st.byKey[key], at)
	return r, ok, nil
}

func (v *view) List(ctx context.Context, opts store.ListOptions, fn func(model.Record) error) error {
	if err := v.check(ctx); err != nil {
		return err
	}
	var keys []model.ObjectKey
	for k := range v.st.byKey {
		if k.Resource == opts.Resource && (opts.Namespace == "" || k.Namespace == opts.Namespace) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Namespace != keys[j].Namespace {
			return keys[i].Namespace < keys[j].Namespace
		}
		return keys[i].Name < keys[j].Name
	})
	for _, k := range keys {
		if r, ok := latestAt(v.st.byKey[k], opts.At); ok {
			if err := fn(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *view) History(ctx context.Context, key model.ObjectKey, from, to time.Time, fn func(model.Record) error) error {
	if err := v.check(ctx); err != nil {
		return err
	}
	for _, r := range v.st.byKey[key] {
		if !r.ObservedAt.Before(from) && !r.ObservedAt.After(to) {
			if err := fn(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *view) Changes(ctx context.Context, opts store.ChangesOptions, fn func(model.Record) error) error {
	if err := v.check(ctx); err != nil {
		return err
	}
	n := 0
	i := sort.Search(len(v.st.log), func(i int) bool { return !v.st.log[i].ObservedAt.Before(opts.From) })
	for ; i < len(v.st.log); i++ {
		r := v.st.log[i]
		if r.ObservedAt.After(opts.To) {
			break
		}
		if opts.Resource != "" && r.Key.Resource != opts.Resource {
			continue
		}
		if opts.Namespace != "" && r.Key.Namespace != opts.Namespace {
			continue
		}
		if err := fn(r); err != nil {
			return err
		}
		n++
		if opts.Limit > 0 && n >= opts.Limit {
			break
		}
	}
	return nil
}

func (v *view) Coverage(ctx context.Context, resource string) ([]model.CoverageEvent, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	return slices.Clone(v.st.coverage[resource]), nil
}

func (v *view) Resources(ctx context.Context) ([]string, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	var out []string
	for r, cs := range v.st.coverage {
		if len(cs) > 0 {
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (v *view) Horizon() time.Time { return v.st.horizon }

func (v *view) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closed = true
	return nil
}
