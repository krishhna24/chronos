// Package query answers historical questions ("what did this look like at
// T?") over a store.Store. It owns the coverage and existence semantics of
// ADR 0004: an answer is only "present" or "absent" when Chronos was
// observing the resource type at that instant; otherwise it is "unknown".
//
// Every Engine method that touches the store opens exactly one store.View,
// so all parts of an answer (coverage, records, horizon) come from the same
// consistent snapshot, and closes it before returning.
package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/utils/clock"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store"
)

// ErrInvalidArgument marks errors caused by the request (unknown resource,
// time in the future, from >= to, ...). The HTTP layer maps it to 400.
var ErrInvalidArgument = errors.New("invalid argument")

const (
	// DefaultLimit is the page size of History and Changes when none is
	// given.
	DefaultLimit = 1000
	// MaxLimit is the largest page size History and Changes accept.
	MaxLimit = 10000
	// DefaultMaxClockSkew is how far in the future a query instant may lie
	// before it is rejected. Instants within the skew are clamped to now, so
	// a client whose clock is slightly ahead still gets an answer.
	DefaultMaxClockSkew = 5 * time.Second
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// Engine evaluates historical queries. It is safe for concurrent use.
type Engine struct {
	store store.Store
	clock clock.PassiveClock
	skew  time.Duration
}

// Option configures an Engine.
type Option func(*Engine)

// WithMaxClockSkew sets how far in the future a query instant may be before
// it is rejected (default DefaultMaxClockSkew).
func WithMaxClockSkew(d time.Duration) Option {
	return func(e *Engine) { e.skew = d }
}

// New returns an Engine reading s. The clock defines "now" for defaults and
// for rejecting future instants.
func New(s store.Store, c clock.PassiveClock, opts ...Option) *Engine {
	e := &Engine{store: s, clock: c, skew: DefaultMaxClockSkew}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Now returns the engine clock's current time.
func (e *Engine) Now() time.Time { return e.clock.Now() }

// instant resolves a user-supplied instant: zero means now, an instant up to
// the allowed skew ahead of now is clamped to now, anything further ahead
// is rejected (ADR 0004 rule 3).
func (e *Engine) instant(name string, t time.Time) (time.Time, error) {
	now := e.clock.Now()
	if t.IsZero() {
		return now, nil
	}
	if t.After(now.Add(e.skew)) {
		return time.Time{}, invalidf("%s %s is in the future (server time %s)",
			name, t.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}
	if t.After(now) {
		return now, nil
	}
	return t, nil
}

func lookupResource(id string) (resources.Type, error) {
	if id == "" {
		return resources.Type{}, invalidf("resource is required")
	}
	t, ok := resources.ByID(id)
	if !ok {
		return resources.Type{}, invalidf("unknown resource %q (use the canonical ID, e.g. v1/pods)", id)
	}
	return t, nil
}

func checkNamespace(t resources.Type, ns string, required bool) error {
	if strings.ContainsAny(ns, "/\x00") {
		return invalidf("namespace %q must not contain '/' or NUL", ns)
	}
	if !t.Namespaced && ns != "" {
		return invalidf("resource %s is cluster-scoped; namespace must be empty", t.ID())
	}
	if required && t.Namespaced && ns == "" {
		return invalidf("resource %s is namespaced; namespace is required", t.ID())
	}
	return nil
}

func checkKey(key model.ObjectKey) error {
	t, err := lookupResource(key.Resource)
	if err != nil {
		return err
	}
	if key.Name == "" {
		return invalidf("name is required")
	}
	if err := checkNamespace(t, key.Namespace, true); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	return nil
}

func checkLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return DefaultLimit, nil
	case limit < 0:
		return 0, invalidf("limit must be positive, got %d", limit)
	case limit > MaxLimit:
		return 0, invalidf("limit %d exceeds the maximum of %d", limit, MaxLimit)
	}
	return limit, nil
}

// withView runs fn against one fresh view and always closes it.
func (e *Engine) withView(ctx context.Context, fn func(store.View) error) (err error) {
	v, err := e.store.View(ctx)
	if err != nil {
		return fmt.Errorf("open view: %w", err)
	}
	defer func() {
		if cerr := v.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close view: %w", cerr)
		}
	}()
	return fn(v)
}

// errStop ends a store iteration early; it never escapes this package.
var errStop = errors.New("stop iteration")

func apiKey(k model.ObjectKey) api.ObjectKey {
	return api.ObjectKey{Resource: k.Resource, Namespace: k.Namespace, Name: k.Name}
}

func version(r model.Record) *api.Version {
	return &api.Version{
		Seq:             r.Seq,
		ObservedAt:      r.ObservedAt,
		Type:            r.Type.String(),
		Origin:          r.Origin.String(),
		UID:             r.UID,
		ResourceVersion: r.ResourceVersion,
	}
}

// objectJSON copies a record's object (views do not let callers retain it)
// and verifies it is JSON, so a corrupt record surfaces as an error rather
// than as a malformed response.
func objectJSON(r model.Record) (json.RawMessage, error) {
	if len(r.Object) == 0 {
		return nil, nil
	}
	if !json.Valid(r.Object) {
		return nil, fmt.Errorf("%w: record %d of %s: object is not valid JSON", store.ErrCorrupt, r.Seq, r.Key)
	}
	return slices.Clone(r.Object), nil
}

// objectState applies the ADR 0004 existence rules to the latest record at
// or before at (rec, valid when ok).
//
//   - observed: non-delete → present with object; delete → absent with the
//     delete version but no object; no record → absent.
//   - gap: unknown; the last known record is returned as such. Its object is
//     included only if it is not a delete, so a deleted object's last body
//     is never presented as a possible current state.
//   - unobserved / expired: unknown, no version, no object.
func objectState(key model.ObjectKey, at time.Time, cov api.Coverage, rec model.Record, ok bool) (api.ObjectState, error) {
	st := api.ObjectState{Key: apiKey(key), At: at, Coverage: cov, Existence: api.ExistenceUnknown}
	switch cov.State {
	case api.CoverageObserved:
		st.Existence = api.ExistenceAbsent
		if !ok {
			return st, nil
		}
		st.Version = version(rec)
		if rec.Type == model.ChangeDeleted {
			return st, nil
		}
		st.Existence = api.ExistencePresent
	case api.CoverageGap:
		if !ok {
			return st, nil
		}
		st.Version = version(rec)
		if rec.Type == model.ChangeDeleted {
			return st, nil
		}
	default:
		return st, nil
	}
	obj, err := objectJSON(rec)
	if err != nil {
		return api.ObjectState{}, err
	}
	st.Object = obj
	return st, nil
}

// stateIn computes the state of key at at within v.
func (e *Engine) stateIn(ctx context.Context, v store.View, events []model.CoverageEvent, key model.ObjectKey, at time.Time) (api.ObjectState, error) {
	cov := classify(events, v.Horizon(), at)
	rec, ok, err := v.Get(ctx, key, at)
	if err != nil {
		return api.ObjectState{}, fmt.Errorf("get %s at %s: %w", key, at.Format(time.RFC3339Nano), err)
	}
	return objectState(key, at, cov, rec, ok)
}

// State returns the state of one object at an instant (zero: now).
func (e *Engine) State(ctx context.Context, key model.ObjectKey, at time.Time) (api.ObjectState, error) {
	if err := checkKey(key); err != nil {
		return api.ObjectState{}, err
	}
	at, err := e.instant("at", at)
	if err != nil {
		return api.ObjectState{}, err
	}
	var out api.ObjectState
	err = e.withView(ctx, func(v store.View) error {
		events, err := v.Coverage(ctx, key.Resource)
		if err != nil {
			return fmt.Errorf("coverage of %s: %w", key.Resource, err)
		}
		out, err = e.stateIn(ctx, v, events, key, at)
		return err
	})
	return out, err
}

// Resources describes every supported resource type with its retained
// coverage intervals.
func (e *Engine) Resources(ctx context.Context) (api.ResourcesResponse, error) {
	out := api.ResourcesResponse{Now: e.clock.Now(), Resources: []api.Resource{}}
	err := e.withView(ctx, func(v store.View) error {
		if h := v.Horizon(); !h.IsZero() {
			out.Horizon = timePtr(h)
		}
		for _, t := range resources.Builtin {
			ivs, err := e.Intervals(ctx, v, t.ID())
			if err != nil {
				return err
			}
			out.Resources = append(out.Resources, api.Resource{
				ID:           t.ID(),
				Group:        t.Group,
				Version:      t.Version,
				Resource:     t.Resource,
				Kind:         t.Kind,
				Namespaced:   t.Namespaced,
				MetadataOnly: t.MetadataOnly,
				ShortNames:   slices.Clone(t.ShortNames),
				Coverage:     ivs,
			})
		}
		return nil
	})
	if err != nil {
		return api.ResourcesResponse{}, err
	}
	return out, nil
}

// ObjectAtSeq returns the state described by exactly the record with the
// given Seq of key, evaluated at that record's ObservedAt (so a resync
// record, which lies at the end of a gap, reports existence unknown).
func (e *Engine) ObjectAtSeq(ctx context.Context, key model.ObjectKey, seq uint64) (api.ObjectState, error) {
	if err := checkKey(key); err != nil {
		return api.ObjectState{}, err
	}
	if seq == 0 {
		return api.ObjectState{}, invalidf("seq is required")
	}
	var out api.ObjectState
	err := e.withView(ctx, func(v store.View) error {
		var (
			found model.Record
			ok    bool
		)
		err := v.History(ctx, key, time.Time{}, maxTime, func(r model.Record) error {
			if r.Seq == seq {
				found, ok = r, true
				return errStop
			}
			if r.Seq > seq {
				return errStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return fmt.Errorf("history of %s: %w", key, err)
		}
		if !ok {
			return invalidf("no record with seq %d for %s (it may have been compacted)", seq, key)
		}
		events, err := v.Coverage(ctx, key.Resource)
		if err != nil {
			return fmt.Errorf("coverage of %s: %w", key.Resource, err)
		}
		cov := classify(events, v.Horizon(), found.ObservedAt)
		out, err = objectState(key, found.ObservedAt, cov, found, true)
		if err != nil {
			return err
		}
		// The caller asked for this record explicitly, so its version and
		// (non-delete) body are returned even when coverage at its time is
		// not one that objectState attaches records to (e.g. the retained
		// record from before the horizon). Existence stays as classified.
		out.Version = version(found)
		if found.Type != model.ChangeDeleted && out.Object == nil {
			out.Object, err = objectJSON(found)
		}
		return err
	})
	return out, err
}

// maxTime is later than any record time.
var maxTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
