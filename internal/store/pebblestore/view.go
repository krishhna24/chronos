package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// pebbleReader is the read surface shared by *pebble.DB and *pebble.Snapshot.
type pebbleReader interface {
	NewIterWithContext(ctx context.Context, o *pebble.IterOptions) (*pebble.Iterator, error)
	Get(key []byte) ([]byte, io.Closer, error)
}

// view is a store.View over a Pebble snapshot. Writes and compactions
// committed after the snapshot is taken are invisible to it.
type view struct {
	s       *Store
	snap    *pebble.Snapshot
	horizon time.Time

	mu     sync.Mutex
	cond   *sync.Cond
	active int // calls in progress
	closed bool
}

var _ store.View = (*view)(nil)

// newView is called with s.mu held.
func newView(s *Store) (*view, error) {
	snap := s.db.NewSnapshot()
	v := &view{s: s, snap: snap}
	v.cond = sync.NewCond(&v.mu)
	// The horizon is read from the snapshot itself so it is always
	// consistent with the data the view sees.
	val, closer, err := snap.Get(keyHorizon)
	switch {
	case errors.Is(err, pebble.ErrNotFound):
	case err != nil:
		_ = snap.Close()
		return nil, fmt.Errorf("pebblestore: read horizon: %w", err)
	default:
		v.horizon, err = decodeTimeValue(keyHorizon, val)
		_ = closer.Close()
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
	}
	return v, nil
}

// begin marks a call in progress; end must follow. Calls made after Close
// (including nested calls from a callback racing with Close) fail with
// ErrClosed instead of blocking.
func (v *view) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return store.ErrClosed
	}
	v.active++
	return nil
}

func (v *view) end() {
	v.mu.Lock()
	v.active--
	if v.active == 0 {
		v.cond.Broadcast()
	}
	v.mu.Unlock()
}

// closeSnapshot waits for in-flight calls and releases the snapshot.
func (v *view) closeSnapshot() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	v.closed = true
	for v.active > 0 {
		v.cond.Wait()
	}
	_ = v.snap.Close()
}

func (v *view) Close() error {
	v.closeSnapshot()
	v.s.forgetView(v)
	return nil
}

func (v *view) Horizon() time.Time { return v.horizon }

func iterValue(it *pebble.Iterator) ([]byte, error) {
	val, err := it.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("pebblestore: read %s: %w", describeKey(it.Key()), err)
	}
	return val, nil
}

func (v *view) Get(ctx context.Context, key model.ObjectKey, at time.Time) (model.Record, bool, error) {
	if err := v.begin(ctx); err != nil {
		return model.Record{}, false, err
	}
	defer v.end()
	obj := objectPrefix(key)
	it, err := v.snap.NewIterWithContext(ctx, &pebble.IterOptions{LowerBound: obj, UpperBound: prefixEnd(obj)})
	if err != nil {
		return model.Record{}, false, err
	}
	if !it.SeekLT(atOrBefore(obj, at)) {
		return model.Record{}, false, closeIter(it)
	}
	val, err := iterValue(it)
	if err != nil {
		_ = it.Close()
		return model.Record{}, false, err
	}
	rec, err := decodeVersion(it.Key(), val)
	if err == nil {
		rec.Object = slices.Clone(rec.Object)
	}
	if cerr := closeIter(it); err == nil {
		err = cerr
	}
	if err != nil {
		return model.Record{}, false, err
	}
	return rec, true, nil
}

// scanLatest calls fn with the key and value of the latest version at or
// before at of every object under prefix (a resource or namespace prefix
// of the version table), in key order. It costs two seeks per object ever
// seen (within retention), however many versions each has.
func scanLatest(ctx context.Context, r pebbleReader, prefix []byte, at time.Time, fn func(key, val []byte) error) error {
	it, err := r.NewIterWithContext(ctx, &pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	n := 0
	for valid := it.SeekGE(prefix); valid; {
		if n++; n%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				_ = it.Close()
				return err
			}
		}
		k := it.Key()
		if len(k) < len(prefix)+timeLen {
			err := corruptf(k, "version key too short")
			_ = it.Close()
			return err
		}
		obj := slices.Clone(k[:len(k)-timeLen])
		// The first version of an object is its oldest; when it is already
		// newer than at the object did not exist yet and the seek is saved.
		if bytes.Compare(k[len(obj):], appendTime(nil, at)) <= 0 {
			if it.SeekLT(atOrBefore(obj, at)) {
				k = it.Key()
				if len(k) == len(obj)+timeLen && bytes.HasPrefix(k, obj) {
					val, err := iterValue(it)
					if err == nil {
						err = fn(k, val)
					}
					if err != nil {
						_ = it.Close()
						return err
					}
				}
			}
		}
		valid = it.SeekGE(prefixEnd(obj))
	}
	return closeIter(it)
}

func listPrefix(opts store.ListOptions) []byte {
	if opts.Namespace != "" {
		return versionNamespacePrefix(opts.Resource, opts.Namespace)
	}
	return versionResourcePrefix(opts.Resource)
}

func (v *view) List(ctx context.Context, opts store.ListOptions, fn func(model.Record) error) error {
	if err := v.begin(ctx); err != nil {
		return err
	}
	defer v.end()
	if done, err := v.listFromManifest(ctx, opts, fn); done {
		v.s.stats.manifest.Add(1)
		return err
	}
	v.s.stats.scan.Add(1)
	return v.listScan(ctx, opts, fn)
}

// listScan is the reference List path: a prefix scan of the version table.
func (v *view) listScan(ctx context.Context, opts store.ListOptions, fn func(model.Record) error) error {
	return scanLatest(ctx, v.snap, listPrefix(opts), opts.At, func(key, val []byte) error {
		rec, err := decodeVersion(key, val)
		if err != nil {
			return err
		}
		return fn(rec)
	})
}

func (v *view) History(ctx context.Context, key model.ObjectKey, from, to time.Time, fn func(model.Record) error) error {
	if err := v.begin(ctx); err != nil {
		return err
	}
	defer v.end()
	if to.Before(from) {
		return nil
	}
	obj := objectPrefix(key)
	it, err := v.snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: appendTime(slices.Clone(obj), from),
		UpperBound: atOrBefore(obj, to),
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
		var rec model.Record
		if err == nil {
			rec, err = decodeVersion(it.Key(), val)
		}
		if err == nil {
			err = fn(rec)
		}
		if err != nil {
			_ = it.Close()
			return err
		}
	}
	return closeIter(it)
}

// getVersion reads one version by exact key and calls fn with it.
func getVersion(r pebbleReader, key []byte, fn func(model.Record) error) (found bool, err error) {
	val, closer, err := r.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pebblestore: read %s: %w", describeKey(key), err)
	}
	defer func() { _ = closer.Close() }()
	rec, err := decodeVersion(key, val)
	if err != nil {
		return true, err
	}
	return true, fn(rec)
}

func (v *view) Changes(ctx context.Context, opts store.ChangesOptions, fn func(model.Record) error) error {
	if err := v.begin(ctx); err != nil {
		return err
	}
	defer v.end()
	if opts.To.Before(opts.From) {
		return nil
	}
	it, err := v.snap.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: indexKey(opts.From),
		UpperBound: atOrBefore([]byte{tabIndex}, opts.To),
	})
	if err != nil {
		return err
	}
	n, steps := 0, 0
	for ok := it.First(); ok; ok = it.Next() {
		if steps++; steps%ctxCheckEvery == 0 {
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
			return err
		}
		if opts.Resource != "" && ref.key.Resource != opts.Resource {
			continue
		}
		if opts.Namespace != "" && ref.key.Namespace != opts.Namespace {
			continue
		}
		vk := versionKey(ref.key, ref.time)
		found, err := getVersion(v.snap, vk, func(rec model.Record) error {
			if rec.Seq != ref.seq {
				return corruptf(vk, "seq %d does not match time index seq %d", rec.Seq, ref.seq)
			}
			return fn(rec)
		})
		if err == nil && !found {
			err = corruptf(it.Key(), "time index points at missing version %s", describeKey(vk))
		}
		if err != nil {
			_ = it.Close()
			return err
		}
		if n++; opts.Limit > 0 && n >= opts.Limit {
			break
		}
	}
	return closeIter(it)
}

func (v *view) Coverage(ctx context.Context, resource string) ([]model.CoverageEvent, error) {
	if err := v.begin(ctx); err != nil {
		return nil, err
	}
	defer v.end()
	return readCoverage(ctx, v.snap, resource)
}

func readCoverage(ctx context.Context, r pebbleReader, resource string) ([]model.CoverageEvent, error) {
	p := coveragePrefix(resource)
	it, err := r.NewIterWithContext(ctx, &pebble.IterOptions{LowerBound: p, UpperBound: prefixEnd(p)})
	if err != nil {
		return nil, err
	}
	var out []model.CoverageEvent
	for ok := it.First(); ok; ok = it.Next() {
		val, err := iterValue(it)
		var c model.CoverageEvent
		if err == nil {
			c, err = decodeCoverage(it.Key(), val)
		}
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		out = append(out, c)
	}
	return out, closeIter(it)
}

func (v *view) Resources(ctx context.Context) ([]string, error) {
	if err := v.begin(ctx); err != nil {
		return nil, err
	}
	defer v.end()
	return readResources(ctx, v.snap)
}

func readResources(ctx context.Context, r pebbleReader) ([]string, error) {
	it, err := r.NewIterWithContext(ctx, &pebble.IterOptions{LowerBound: []byte{tabCoverage}, UpperBound: []byte{tabCoverage + 1}})
	if err != nil {
		return nil, err
	}
	var out []string
	for ok := it.First(); ok; {
		res, _, err := decodeComponent(it.Key()[1:])
		if err != nil {
			err = corruptf(it.Key(), "bad coverage key: %v", err)
			_ = it.Close()
			return nil, err
		}
		out = append(out, res)
		ok = it.SeekGE(prefixEnd(coveragePrefix(res)))
	}
	// Key order of escaped components equals string order.
	return out, closeIter(it)
}

// LatestRecordTime implements store.View. It reads the resource's L key,
// which every Write that adds a record of the resource updates in the
// same batch and which Compact never deletes, so it equals the bound
// CoverageEnd validation uses, survives reopen, and is consistent with
// the rest of the view's snapshot.
func (v *view) LatestRecordTime(ctx context.Context, resource string) (time.Time, error) {
	if err := v.begin(ctx); err != nil {
		return time.Time{}, err
	}
	defer v.end()
	k := lastRecKey(resource)
	val, closer, err := v.snap.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("pebblestore: read %s: %w", describeKey(k), err)
	}
	defer func() { _ = closer.Close() }()
	return decodeTimeValue(k, val)
}
