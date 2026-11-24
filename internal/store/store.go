// Package store defines the storage contract between the collector (writer)
// and the query engine (reader). Implementations: memstore (reference
// implementation and test oracle) and pebblestore (production).
//
// Every implementation must pass the conformance suite in
// internal/store/storetest.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/krishhna24/chronos/internal/model"
)

var (
	// ErrInvalid: the write violates a store invariant (bad key, coverage
	// events out of order, ...). Nothing from the batch was written.
	ErrInvalid = errors.New("store: invalid write")
	// ErrClosed: the store or view has been closed.
	ErrClosed = errors.New("store: closed")
	// ErrCorrupt: persisted data failed validation.
	ErrCorrupt = errors.New("store: corrupt data")
)

// Entry is one element of a write batch. Exactly one field is set.
type Entry struct {
	Record   *model.Record
	Coverage *model.CoverageEvent
}

// Store is the historical store.
//
// Time and ordering rules (see docs/adr/0002-time-and-ordering.md):
//   - The store assigns Seq to every entry, strictly increasing in commit
//     order across all resources.
//   - For records and CoverageStart events the store clamps the caller's
//     time so that it is strictly greater than every previously assigned
//     record/Start time (+1ns when the clock does not advance). Times are
//     therefore unique and "state at T" is unambiguous.
//   - CoverageEnd keeps the caller's At (the moment continuity was lost,
//     which may be in the past) but it must be >= the latest record time and
//     the latest Start time for that resource, and the resource's latest
//     coverage event must be a Start. Otherwise the batch fails ErrInvalid.
//   - A CoverageStart requires that the resource's latest coverage event
//     is an End or that there is none. Its At is clamped like a record's
//     and, additionally, to be strictly after that End's At.
type Store interface {
	// Write applies entries atomically and in order. When Write returns nil
	// the batch is durable (survives process crash). The assigned Seq and
	// clamped times are written back into the entries.
	Write(ctx context.Context, entries []Entry) error

	// View returns a consistent read-only snapshot of the store. Writes and
	// compactions that commit after View returns are not visible to it.
	View(ctx context.Context) (View, error)

	// SetHeartbeat durably records that the collector was alive and
	// continuously observing at t. Used after a crash to bound the gap.
	SetHeartbeat(ctx context.Context, t time.Time) error
	// Heartbeat returns the last heartbeat, or the zero time if none.
	Heartbeat(ctx context.Context) (time.Time, error)

	// Compact enforces retention. After it returns, the horizon is
	// max(old horizon, horizon) and:
	//   - for every key, records with ObservedAt < horizon are removed
	//     except the latest one, which is kept only if it is not a delete
	//     (so Get at any t >= horizon is unchanged);
	//   - for every resource, coverage events with At < horizon are removed
	//     except the latest one (so coverage at any t >= horizon is
	//     unchanged).
	// Compact must be safe to run concurrently with Write and open Views.
	Compact(ctx context.Context, horizon time.Time) error

	Close() error
}

// ListOptions selects objects for View.List.
type ListOptions struct {
	Resource  string // required: ResourceType.ID()
	Namespace string // empty: all namespaces
	At        time.Time
}

// ChangesOptions selects records for View.Changes.
type ChangesOptions struct {
	From, To  time.Time // inclusive
	Resource  string    // empty: all resources
	Namespace string    // empty: all namespaces
	Limit     int       // <= 0: no limit
}

// View is a consistent read snapshot. Callbacks must not retain Record.Object
// beyond the call unless they copy it. Returning an error from a callback
// stops iteration and is returned unchanged.
type View interface {
	// Get returns the latest record for key with ObservedAt <= at. ok is
	// false when there is none. The record may be a delete.
	Get(ctx context.Context, key model.ObjectKey, at time.Time) (rec model.Record, ok bool, err error)

	// List calls fn, in key order, with the latest record (ObservedAt <= at)
	// of every key of the resource (and namespace, if set). Keys whose
	// latest record is a delete are included; callers decide what to show.
	List(ctx context.Context, opts ListOptions, fn func(model.Record) error) error

	// History calls fn, in Seq order, for every record of key with
	// ObservedAt in [from, to].
	History(ctx context.Context, key model.ObjectKey, from, to time.Time, fn func(model.Record) error) error

	// Changes calls fn, in Seq order, for records matching opts.
	Changes(ctx context.Context, opts ChangesOptions, fn func(model.Record) error) error

	// Coverage returns all retained coverage events of resource in Seq order.
	Coverage(ctx context.Context, resource string) ([]model.CoverageEvent, error)

	// Resources returns the IDs of every resource with at least one
	// coverage event, sorted.
	Resources(ctx context.Context) ([]string, error)

	// Horizon returns the retention horizon: state is reconstructable for
	// t >= Horizon. Zero when nothing has been compacted.
	Horizon() time.Time

	Close() error
}
