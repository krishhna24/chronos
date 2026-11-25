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

// DefaultCloseTimeout is the default Options.CloseTimeout.
const DefaultCloseTimeout = 10 * time.Second

// DefaultBlockSize is the default Options.BlockSize; see the package
// documentation for the measurement behind it.
const DefaultBlockSize = 64 << 10

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
	// BlockSize is the target uncompressed SSTable data block size. Zero
	// means DefaultBlockSize. Normalized objects are 2-8 KB, so with
	// Pebble's 4 KiB default each block holds about one version and the
	// compressor never sees two versions of an object together.
	BlockSize int
	// CloseTimeout bounds how long Close waits for commits already inside
	// Pebble (an fsync on a hung disk cannot be interrupted). Zero means
	// DefaultCloseTimeout.
	CloseTimeout time.Duration

	// fs, if set, replaces the OS filesystem (tests inject I/O failures).
	fs vfs.FS
	// Compaction chunk bounds and a hook called after each intermediate
	// chunk commit (tests use it to simulate a crash between chunks).
	compactChunkDeletes int
	compactChunkBytes   int
	compactChunkHook    func(chunk int) error
}

// Store is a durable store.Store backed by Pebble. It is safe for
// concurrent use; writes are serialized (single writer, ADR 0001).
type Store struct {
	db  *pebble.DB
	log *slog.Logger
	dir string

	// writeSem serializes Write and SetHeartbeat (single writer). It is a
	// channel rather than a mutex so that a writer waiting behind a stuck
	// commit can give up when the store closes or its context ends.
	writeSem chan struct{}
	// closing is canceled when Close starts; waiting writers and running
	// maintenance observe it.
	closing      context.Context
	cancelClose  context.CancelFunc
	closeTimeout time.Duration
	// ops counts mutating calls and commit goroutines in flight; Close
	// waits for it (bounded by closeTimeout) before closing Pebble.
	ops sync.WaitGroup

	// mu guards closed, the views set, and publication of the writer
	// bookkeeping below. The bookkeeping is mutated only by the writeSem
	// holder (under mu); other readers take mu.
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
	// pending is the horizon of an unfinished compaction (zero: none).
	pending time.Time

	compactChunkDeletes int
	compactChunkBytes   int
	compactChunkHook    func(chunk int) error

	// maintMu serializes Compact and Snapshot, which both rewrite manifests.
	maintMu sync.Mutex

	stopBG chan struct{}
	bgDone chan struct{}

	stats listStats

	// poisoned is set after an ambiguous commit failure; every later
	// mutation returns it (it wraps store.ErrClosed).
	poisonMu sync.Mutex
	poisoned error
	// closeErr is the error of a Close that left Pebble open; repeated
	// Close calls return it (guarded by mu).
	closeErr error
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

		writeSem:     make(chan struct{}, 1),
		closeTimeout: opts.CloseTimeout,

		compactChunkDeletes: opts.compactChunkDeletes,
		compactChunkBytes:   opts.compactChunkBytes,
		compactChunkHook:    opts.compactChunkHook,
	}
	if s.compactChunkDeletes <= 0 {
		s.compactChunkDeletes = defaultCompactChunkDeletes
	}
	if s.compactChunkBytes <= 0 {
		s.compactChunkBytes = defaultCompactChunkBytes
	}
	if s.closeTimeout <= 0 {
		s.closeTimeout = DefaultCloseTimeout
	}
	s.closing, s.cancelClose = context.WithCancel(context.Background())
	if err := s.load(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pebblestore: open %s: %w", dir, err)
	}
	log.Info("store opened",
		"format", FormatVersion, "seq", s.seq, "lastTime", s.lastTime,
		"horizon", s.horizon, "heartbeat", s.heartbeat,
		"resources", len(s.lastRec), "lastManifest", s.lastManifest)
	if !s.pending.IsZero() {
		log.Warn("a compaction was interrupted; the next Compact finishes it", "horizon", s.pending)
	}

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
	// Pebble's default DataCorruption handler calls Logger.Fatalf on the
	// reading goroutine. Corruption of one table must surface as an error
	// from the read (Pebble returns it to the caller after notifying the
	// listener; mapPebbleErr turns it into store.ErrCorrupt), not crash
	// the process, so log it instead. Background errors are logged as by
	// default; Pebble itself decides which of them are fatal.
	popts.EventListener = &pebble.EventListener{
		DataCorruption: func(info pebble.DataCorruptionInfo) {
			log.Error("pebble detected on-disk corruption", "path", info.Path, "details", info.Details)
		},
		BackgroundError: func(err error) {
			log.Error("pebble background error", "err", err)
		},
	}
	blockSize := opts.BlockSize
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	for i := range popts.Levels {
		popts.Levels[i].BlockSize = blockSize
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

// beginOp registers a mutating call; the caller must call s.ops.Done.
// It fails once Close has started or the store is poisoned.
func (s *Store) beginOp() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.ErrClosed
	}
	if err := s.poisonErr(); err != nil {
		return err
	}
	s.ops.Add(1)
	return nil
}

// acquireWriter takes the single-writer slot, giving up when ctx ends or
// the store closes. The caller releases it with <-s.writeSem.
func (s *Store) acquireWriter(ctx context.Context) error {
	select {
	case s.writeSem <- struct{}{}:
	case <-s.closing.Done():
		return store.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.closing.Err() != nil {
		<-s.writeSem
		return store.ErrClosed
	}
	return nil
}

// opContext returns ctx that is also canceled when Close starts.
func (s *Store) opContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.closing, cancel)
	return ctx, func() { stop(); cancel() }
}

// commitWait commits b (taking ownership of it) on a separate goroutine
// and waits for the result, for ctx to end, or for Close. Pebble commits
// cannot be interrupted, so when ctx or Close wins the commit's outcome is
// unknown and the error wraps store.ErrClosed, which the Write contract
// reserves for that case. Abandoning a commit because ctx ended also
// poisons the store: the batch may still land, so no later mutation may
// be accepted (a retry would apply it twice). The goroutine is counted in
// s.ops so Close does not close Pebble under it.
func (s *Store) commitWait(ctx context.Context, b *pebble.Batch, what string) error {
	done := make(chan error, 1)
	s.ops.Add(1)
	go func() {
		defer s.ops.Done()
		defer func() { _ = b.Close() }()
		done <- s.commit(b, what)
	}()
	select {
	case err := <-done:
		return err
	case <-s.closing.Done():
		select {
		case err := <-done:
			return err
		default:
		}
		s.log.Warn("store closed while a commit was in flight", "op", what)
		return fmt.Errorf("%w: pebblestore: store closed while the %s was in flight; whether it committed is unknown", store.ErrClosed, what)
	case <-ctx.Done():
		select {
		case err := <-done:
			return err
		default:
		}
		if s.closing.Err() != nil {
			return fmt.Errorf("%w: pebblestore: store closed while the %s was in flight; whether it committed is unknown", store.ErrClosed, what)
		}
		return s.poison(what, fmt.Errorf("abandoned while still inside Pebble (stuck I/O?), outcome unknown: %w", ctx.Err()))
	}
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
	}{{keyHorizon, &s.horizon}, {keyHeartbeat, &s.heartbeat}, {keyCompacting, &s.pending}} {
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

	it, err := newIter(context.Background(), s.db, &pebble.IterOptions{LowerBound: []byte{tabLastRec}, UpperBound: []byte{tabLastRec + 1}})
	if err != nil {
		return err
	}
	defer func(it *iter) { _ = it.Close() }(it)
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
	it, err = newIter(context.Background(), s.db, &pebble.IterOptions{LowerBound: []byte{tabCoverage}, UpperBound: []byte{tabCoverage + 1}})
	if err != nil {
		return err
	}
	defer func(it *iter) { _ = it.Close() }(it)
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

	it, err = newIter(context.Background(), s.db, &pebble.IterOptions{LowerBound: []byte{tabManifest}, UpperBound: []byte{tabManifest + 1}})
	if err != nil {
		return err
	}
	defer func(it *iter) { _ = it.Close() }(it)
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
	it, err := newIter(context.Background(), s.db, nil)
	if err != nil {
		return false, err
	}
	defer func(it *iter) { _ = it.Close() }(it)
	empty := !it.First()
	return empty, closeIter(it)
}

// iter wraps a Pebble iterator so Close is idempotent: every iterator is
// closed by a deferred Close (so a panic cannot leak it and make a later
// Store.Close fail), and code paths may still close it early.
type iter struct {
	*pebble.Iterator
	closed bool
}

func newIter(ctx context.Context, r pebbleReader, o *pebble.IterOptions) (*iter, error) {
	it, err := r.NewIterWithContext(ctx, o)
	if err != nil {
		return nil, mapPebbleErr(err)
	}
	return &iter{Iterator: it}, nil
}

func (i *iter) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	return i.Iterator.Close()
}

// closeIter closes it and returns its first error.
func closeIter(it *iter) error {
	if it.closed {
		return nil
	}
	err := it.Error()
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	return mapPebbleErr(err)
}

// mapPebbleErr turns Pebble's on-disk corruption errors (SSTable block
// checksum mismatches and the like) into errors wrapping store.ErrCorrupt.
// Pebble's message names the table ("file 000012: block ..."); where the
// read was for a known key the store's own context names that too.
func mapPebbleErr(err error) error {
	if err == nil || errors.Is(err, store.ErrCorrupt) || !pebble.IsCorruptionError(err) {
		return err
	}
	return fmt.Errorf("%w: %w", store.ErrCorrupt, err)
}

// Write implements store.Store.
//
// Writes are serialized. A Write that is waiting for the writer slot, or
// whose commit is still inside Pebble, returns as soon as Close starts:
// in the first case nothing was applied and the error is store.ErrClosed;
// in the second the outcome is unknown and the error also wraps
// store.ErrClosed (see commitWait).
func (s *Store) Write(ctx context.Context, entries []store.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.ops.Done()
	if err := s.acquireWriter(ctx); err != nil {
		return err
	}
	defer func() { <-s.writeSem }()

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
	owned := true // until handed to commitWait
	defer func() {
		if owned {
			_ = b.Close()
		}
	}()
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
	owned = false
	if err := s.commitWait(ctx, b, fmt.Sprintf("commit of a %d-entry batch", len(entries))); err != nil {
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
	s.mu.Lock()
	maps.Copy(s.lastRec, lastRec)
	maps.Copy(s.lastCov, lastCov)
	s.seq, s.lastTime = seq, lastTime
	s.mu.Unlock()
	return nil
}

// SetHeartbeat implements store.Store. Like memstore, the heartbeat only
// moves forward.
func (s *Store) SetHeartbeat(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.ops.Done()
	if err := s.acquireWriter(ctx); err != nil {
		return err
	}
	defer func() { <-s.writeSem }()
	t = normTime(t)
	if !t.After(s.heartbeat) {
		return nil
	}
	b := s.db.NewBatch()
	if err := b.Set(keyHeartbeat, seal(keyHeartbeat, encodeTimeValue(t)), nil); err != nil {
		_ = b.Close()
		return fmt.Errorf("pebblestore: write heartbeat: %w", err)
	}
	if err := s.commitWait(ctx, b, "heartbeat commit"); err != nil {
		return err
	}
	s.mu.Lock()
	s.heartbeat = t
	s.mu.Unlock()
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

// Close implements store.Store. Close is idempotent and never blocks
// indefinitely on a writer:
//
//  1. New calls fail with store.ErrClosed. Writes waiting for the writer
//     slot return store.ErrClosed (nothing applied). A Write whose commit
//     is inside Pebble returns at once with an error wrapping
//     store.ErrClosed (outcome unknown). Compact and Snapshot see their
//     context canceled.
//  2. Close waits up to Options.CloseTimeout for commits already inside
//     Pebble and for running Compact/Snapshot calls to finish. If they do
//     not (an fsync on a hung disk cannot be interrupted), Close returns an
//     error and leaves Pebble open; the process is expected to exit.
//  3. Otherwise it closes all open Views (waiting for their in-flight
//     calls) and then Pebble.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	s.mu.Unlock()
	s.cancelClose()

	if s.stopBG != nil {
		close(s.stopBG)
	}
	idle := make(chan struct{})
	go func() {
		s.ops.Wait()
		if s.bgDone != nil {
			<-s.bgDone
		}
		close(idle)
	}()
	select {
	case <-idle:
	case <-time.After(s.closeTimeout):
		s.log.Error("close timed out waiting for an in-flight commit; leaving Pebble open", "timeout", s.closeTimeout)
		err := fmt.Errorf("pebblestore: close: an in-flight commit did not finish within %s (stuck I/O?); Pebble was left open", s.closeTimeout)
		s.mu.Lock()
		s.closeErr = err // later Close calls report it too
		s.mu.Unlock()
		return err
	}

	s.mu.Lock()
	views := slices.Collect(maps.Keys(s.views))
	s.views = map[*view]struct{}{}
	s.mu.Unlock()
	for _, v := range views {
		v.closeSnapshot()
	}
	if err := s.db.Close(); err != nil {
		err = fmt.Errorf("pebblestore: close: %w", err)
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
		return err
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
