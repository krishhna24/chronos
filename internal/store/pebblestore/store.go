package pebblestore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// FormatVersion is the on-disk format this build reads and writes. It is
// stored under the meta "format" key; Open refuses any other value.
const FormatVersion = 1

// DefaultSnapshotInterval is how often a snapshot manifest is taken when
// Options.SnapshotInterval is zero (ADR 0001).
const DefaultSnapshotInterval = time.Hour

// Compression selects Pebble block compression.
type Compression string

const (
	CompressionZstd   Compression = "zstd"   // default: zstd level 3 on every level
	CompressionSnappy Compression = "snappy" // Pebble's default; faster, larger
	CompressionNone   Compression = "none"
)

// Options configures a Store. The zero value is usable.
type Options struct {
	// Logger receives structured logs. Nil means slog.Default().
	Logger *slog.Logger
	// SnapshotInterval is the period of the background snapshot-manifest
	// job. Zero means DefaultSnapshotInterval; negative disables the job
	// (Snapshot can still be called explicitly).
	SnapshotInterval time.Duration
	// Compression for SSTable blocks. Empty means CompressionZstd. Exposed so
	// operators and benchmarks can trade CPU for disk; the format version
	// does not depend on it (Pebble records compression per block).
	Compression Compression
	// CacheSize is the Pebble block cache size in bytes. Zero means 64 MiB.
	CacheSize int64

	// fs, if set, replaces the OS filesystem (tests inject I/O failures).
	fs vfs.FS
}

// Store is a durable store.Store backed by Pebble. It is safe for
// concurrent use; writes are serialized (single writer, ADR 0001).
type Store struct {
	db  *pebble.DB
	log *slog.Logger
	dir string

	// mu guards the writer bookkeeping below and serializes Write,
	// SetHeartbeat and View creation against Close.
	mu        sync.Mutex
	closed    bool
	seq       uint64
	lastTime  time.Time
	lastRec   map[string]time.Time           // per resource: latest record time
	lastCov   map[string]model.CoverageEvent // per resource: latest coverage event
	heartbeat time.Time
	horizon   time.Time
	views     map[*view]struct{}
	// lastManifest is the time of the newest snapshot manifest (zero: none).
	lastManifest time.Time

	// maintMu serializes Compact and Snapshot, which both rewrite manifests.
	maintMu sync.Mutex

	stopBG chan struct{}
	bgDone chan struct{}

	stats listStats

	// poisoned is set after an ambiguous commit failure; every later
	// mutation returns it (it wraps store.ErrClosed).
	poisonMu sync.Mutex
	poisoned error
}

// listStats counts which path served View.List (tests assert on it).
type listStats struct {
	manifest  atomic.Int64 // served from a snapshot manifest
	scan      atomic.Int64 // served by prefix scan (no usable manifest)
	fallbacks atomic.Int64 // manifest present but unusable
}

var _ store.Store = (*Store)(nil)

// Open opens or creates a store in dir.
func Open(dir string, opts Options) (*Store, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "pebblestore", "dir", dir)

	popts, cache, err := pebbleOptions(opts, log)
	if err != nil {
		return nil, err
	}
	// pebble.Open takes its own reference on the cache.
	defer cache.Unref()
	db, err := pebble.Open(dir, popts)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: open %s: %w", dir, err)
	}
	s := &Store{
		db:      db,
		log:     log,
		dir:     dir,
		lastRec: map[string]time.Time{},
		lastCov: map[string]model.CoverageEvent{},
		views:   map[*view]struct{}{},
	}
	if err := s.load(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pebblestore: open %s: %w", dir, err)
	}
	log.Info("store opened",
		"format", FormatVersion, "seq", s.seq, "lastTime", s.lastTime,
		"horizon", s.horizon, "heartbeat", s.heartbeat,
		"resources", len(s.lastRec), "lastManifest", s.lastManifest)

	interval := opts.SnapshotInterval
	if interval == 0 {
		interval = DefaultSnapshotInterval
	}
	if interval > 0 {
		s.stopBG = make(chan struct{})
		s.bgDone = make(chan struct{})
		go s.snapshotLoop(interval)
	}
	return s, nil
}

func pebbleOptions(opts Options, log *slog.Logger) (*pebble.Options, *pebble.Cache, error) {
	var profile *sstable.CompressionProfile
	switch opts.Compression {
	case "", CompressionZstd:
		profile = sstable.ZstdCompression
	case CompressionSnappy:
		profile = sstable.SnappyCompression
	case CompressionNone:
		profile = sstable.NoCompression
	default:
		return nil, nil, fmt.Errorf("pebblestore: unknown compression %q", opts.Compression)
	}
	cacheSize := opts.CacheSize
	if cacheSize <= 0 {
		cacheSize = 64 << 20
	}
	cache := pebble.NewCache(cacheSize)
	popts := &pebble.Options{
		// Pin the format so upgrading the library never silently ratchets
		// the on-disk format; bump deliberately with FormatVersion.
		FormatMajorVersion: pebble.FormatValueSeparation,
		Cache:              cache,
		Logger:             slogAdapter{log},
	}
	if opts.fs != nil {
		popts.FS = opts.fs
	}
	for i := range popts.Levels {
		popts.Levels[i].Compression = func() *sstable.CompressionProfile { return profile }
		// Whole-key bloom filters speed up exact-key Gets (Changes and the
		// manifest List path look versions up by exact key).
		popts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	popts.EnsureDefaults()
	return popts, cache, nil
}

// slogAdapter routes Pebble's printf-style logs to slog.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Infof(format string, args ...any) {
	a.l.Info(fmt.Sprintf(format, args...), "source", "pebble")
}

func (a slogAdapter) Errorf(format string, args ...any) {
	a.l.Error(fmt.Sprintf(format, args...), "source", "pebble")
}

func (a slogAdapter) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	a.l.Error(msg, "source", "pebble", "fatal", true)
	panic(pebbleFatal{msg: msg})
}

// pebbleFatal is the panic value of slogAdapter.Fatalf. Pebble calls
// Fatalf when its commit pipeline fails (for example a WAL fsync error),
// after which it is unknown whether the batch was applied. commit recovers
// it on the committing goroutine and poisons the store.
type pebbleFatal struct{ msg string }

// commit commits b with pebble.Sync. Any failure is treated as ambiguous:
// Pebble may have applied the batch to its memtable and written part or
// all of it to the WAL, so the store poisons itself and the returned error
// wraps store.ErrClosed. A caller that retries therefore never applies a
// batch twice (store.go Write contract). Must be called with s.mu held
// or, for maintenance batches, after checking s.poisoned under s.mu.
func (s *Store) commit(b *pebble.Batch, what string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(pebbleFatal)
			if !ok {
				panic(r)
			}
			err = errors.New(f.msg)
		}
		if err != nil {
			err = s.poison(what, err)
		}
	}()
	return b.Commit(pebble.Sync)
}

// poison marks the store unusable for writes after an ambiguous failure.
// It must not take s.mu when the caller holds it, so it uses its own lock.
func (s *Store) poison(what string, cause error) error {
	err := fmt.Errorf("%w: pebblestore: %s failed after the commit point, store is poisoned and must be reopened: %w", store.ErrClosed, what, cause)
	s.poisonMu.Lock()
	if s.poisoned == nil {
		s.poisoned = err
		s.log.Error("store poisoned: refusing further writes", "op", what, "err", cause)
	}
	s.poisonMu.Unlock()
	return err
}

// poisonErr returns the poisoning error, or nil.
func (s *Store) poisonErr() error {
	s.poisonMu.Lock()
	defer s.poisonMu.Unlock()
	return s.poisoned
}

func (s *Store) getValue(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", describeKey(key), err)
	}
	out := slices.Clone(v)
	_ = closer.Close()
	return out, true, nil
}

// load initializes a new store or restores bookkeeping from an existing one.
func (s *Store) load() error {
	val, ok, err := s.getValue(keyFormat)
	if err != nil {
		return err
	}
	if !ok {
		empty, err := s.isEmpty()
		if err != nil {
			return err
		}
		if !empty {
			return errors.New("database has data but no format version key; refusing to open")
		}
		payload := binary.AppendUvarint(nil, FormatVersion)
		if err := s.db.Set(keyFormat, seal(keyFormat, payload), pebble.Sync); err != nil {
			return fmt.Errorf("write format version: %w", err)
		}
		return nil
	}
	v, err := decodeFormat(keyFormat, val)
	if err != nil {
		return err
	}
	if v != FormatVersion {
		return fmt.Errorf("unsupported store format version %d (this build supports %d)", v, FormatVersion)
	}

	if val, ok, err := s.getValue(keyState); err != nil {
		return err
	} else if ok {
		if s.seq, s.lastTime, err = decodeState(keyState, val); err != nil {
			return err
		}
	}
	for _, m := range []struct {
		key []byte
		dst *time.Time
	}{{keyHorizon, &s.horizon}, {keyHeartbeat, &s.heartbeat}} {
		val, ok, err := s.getValue(m.key)
		if err != nil {
			return err
		}
		if ok {
			if *m.dst, err = decodeTimeValue(m.key, val); err != nil {
				return err
			}
		}
	}

	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{tabLastRec}, UpperBound: []byte{tabLastRec + 1}})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		res, _, err := decodeComponent(it.Key()[1:])
		if err != nil {
			_ = it.Close()
			return corruptf(it.Key(), "bad lastrec key: %v", err)
		}
		t, err := decodeTimeValue(it.Key(), it.Value())
		if err != nil {
			_ = it.Close()
			return err
		}
		s.lastRec[res] = t
	}
	if err := closeIter(it); err != nil {
		return err
	}

	// Coverage tables are small (a few events per resource per day), so a
	// full scan on open is cheap; the latest event per resource is the last
	// one under its prefix.
	it, err = s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{tabCoverage}, UpperBound: []byte{tabCoverage + 1}})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		c, err := decodeCoverage(it.Key(), it.Value())
		if err != nil {
			_ = it.Close()
			return err
		}
		s.lastCov[c.Resource] = c
	}
	if err := closeIter(it); err != nil {
		return err
	}

	it, err = s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{tabManifest}, UpperBound: []byte{tabManifest + 1}})
	if err != nil {
		return err
	}
	if it.Last() {
		if t, err := decodeTime(it.Key()[1:]); err == nil {
			s.lastManifest = t
		} else {
			s.log.Warn("ignoring malformed manifest key", "key", describeKey(it.Key()), "err", err)
		}
	}
	return closeIter(it)
}

func (s *Store) isEmpty() (bool, error) {
	it, err := s.db.NewIter(nil)
	if err != nil {
		return false, err
	}
	empty := !it.First()
	return empty, closeIter(it)
}

func closeIter(it *pebble.Iterator) error {
	err := it.Error()
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	return err
}

// Write implements store.Store.
func (s *Store) Write(ctx context.Context, entries []store.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}
	if err := s.poisonErr(); err != nil {
		return err
	}

	// Validate and assign against scratch bookkeeping; nothing is mutated
	// and nothing is committed unless the whole batch is valid.
	seq, lastTime := s.seq, s.lastTime
	lastRec := map[string]time.Time{}
	lastCov := map[string]model.CoverageEvent{}
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
		c, ok := s.lastCov[r]
		return c, ok
	}
	clamp := func(t time.Time) time.Time {
		t = normTime(t)
		if !t.After(lastTime) {
			t = lastTime.Add(time.Nanosecond)
		}
		lastTime = t
		return t
	}
	recs := make([]model.Record, len(entries))
	covs := make([]model.CoverageEvent, len(entries))
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
			lastRec[r.Key.Resource] = r.ObservedAt
			recs[i] = r
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
				c.At = normTime(c.At)
				if hasPrev && !c.At.After(prev.At) {
					// The preceding End keeps a caller time that may be
					// ahead of the clamped clock (wall clock moved back).
					c.At = prev.At.Add(time.Nanosecond)
				}
				c.At = clamp(c.At)
			case model.CoverageEnd:
				if !hasPrev || prev.Type != model.CoverageStart {
					return fmt.Errorf("%w: entry %d: End without open Start for %s", store.ErrInvalid, i, c.Resource)
				}
				c.At = normTime(c.At)
				if c.At.Before(prev.At) || c.At.Before(getLastRec(c.Resource)) {
					return fmt.Errorf("%w: entry %d: End at %s precedes earlier data for %s", store.ErrInvalid, i, c.At, c.Resource)
				}
			default:
				return fmt.Errorf("%w: entry %d: bad coverage type %d", store.ErrInvalid, i, c.Type)
			}
			seq++
			c.Seq = seq
			lastCov[c.Resource] = c
			covs[i] = c
		default:
			return fmt.Errorf("%w: entry %d: exactly one of Record and Coverage must be set", store.ErrInvalid, i)
		}
	}
	if len(entries) == 0 {
		return nil
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	for i, e := range entries {
		if e.Record != nil {
			r := &recs[i]
			vk := versionKey(r.Key, r.ObservedAt)
			ik := indexKey(r.ObservedAt)
			if err := errors.Join(
				b.Set(vk, seal(vk, encodeRecord(r)), nil),
				b.Set(ik, seal(ik, encodeIndexRef(r)), nil),
			); err != nil {
				return fmt.Errorf("pebblestore: build batch: %w", err)
			}
		} else {
			c := &covs[i]
			ck := coverageKey(c.Resource, c.Seq)
			if err := b.Set(ck, seal(ck, encodeCoverage(c)), nil); err != nil {
				return fmt.Errorf("pebblestore: build batch: %w", err)
			}
		}
	}
	for res, t := range lastRec {
		lk := lastRecKey(res)
		if err := b.Set(lk, seal(lk, encodeTimeValue(t)), nil); err != nil {
			return fmt.Errorf("pebblestore: build batch: %w", err)
		}
	}
	if err := b.Set(keyState, seal(keyState, encodeState(seq, lastTime)), nil); err != nil {
		return fmt.Errorf("pebblestore: build batch: %w", err)
	}
	if err := s.commit(b, fmt.Sprintf("commit of a %d-entry batch", len(entries))); err != nil {
		return err
	}

	// Committed: publish bookkeeping and write back assigned values.
	for i, e := range entries {
		if e.Record != nil {
			e.Record.Seq, e.Record.ObservedAt = recs[i].Seq, recs[i].ObservedAt
		} else {
			e.Coverage.Seq, e.Coverage.At = covs[i].Seq, covs[i].At
		}
	}
	maps.Copy(s.lastRec, lastRec)
	maps.Copy(s.lastCov, lastCov)
	s.seq, s.lastTime = seq, lastTime
	return nil
}

// SetHeartbeat implements store.Store. Like memstore, the heartbeat only
// moves forward.
func (s *Store) SetHeartbeat(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}
	if err := s.poisonErr(); err != nil {
		return err
	}
	t = normTime(t)
	if !t.After(s.heartbeat) {
		return nil
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(keyHeartbeat, seal(keyHeartbeat, encodeTimeValue(t)), nil); err != nil {
		return fmt.Errorf("pebblestore: write heartbeat: %w", err)
	}
	if err := s.commit(b, "heartbeat"); err != nil {
		return err
	}
	s.heartbeat = t
	return nil
}

// Heartbeat implements store.Store.
func (s *Store) Heartbeat(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return time.Time{}, store.ErrClosed
	}
	return s.heartbeat, nil
}

// View implements store.Store.
func (s *Store) View(ctx context.Context) (store.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, store.ErrClosed
	}
	v, err := newView(s)
	if err != nil {
		return nil, err
	}
	s.views[v] = struct{}{}
	return v, nil
}

func (s *Store) forgetView(v *view) {
	s.mu.Lock()
	delete(s.views, v)
	s.mu.Unlock()
}

// Close implements store.Store. It waits for a running Compact or Snapshot
// and for in-flight View calls, closes all open Views, then closes Pebble.
// Close is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	if s.stopBG != nil {
		close(s.stopBG)
		<-s.bgDone
	}
	s.maintMu.Lock()
	defer s.maintMu.Unlock()

	s.mu.Lock()
	views := slices.Collect(maps.Keys(s.views))
	s.views = map[*view]struct{}{}
	s.mu.Unlock()
	for _, v := range views {
		v.closeSnapshot()
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("pebblestore: close: %w", err)
	}
	s.log.Info("store closed", "seq", s.seq)
	return nil
}

// Dir returns the directory the store was opened in.
func (s *Store) Dir() string { return s.dir }

// Metrics exposes Pebble's metrics (for benchmarks and diagnostics).
func (s *Store) Metrics() *pebble.Metrics { return s.db.Metrics() }

// Flush flushes memtables to SSTables (for benchmarks measuring disk use).
func (s *Store) Flush() error { return s.db.Flush() }

// CompactLSM runs a full manual Pebble compaction so that deleted data is
// physically reclaimed (for benchmarks measuring disk use).
func (s *Store) CompactLSM(ctx context.Context) error {
	return s.db.Compact(ctx, []byte{0}, []byte{0xFF}, true)
}

// ctxCheckEvery bounds how many iterator steps run between context checks.
const ctxCheckEvery = 256
