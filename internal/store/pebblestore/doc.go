// Package pebblestore is the production store.Store: a durable change
// journal on Pebble (github.com/cockroachdb/pebble/v2), per ADR 0001.
//
// # Dependency
//
// Pebble v2.1.7, the latest v2 release at the time of writing (tagged
// 2026-08-24). v2 is Pebble's current major line, it is pure Go
// (zstd uses a cgo binding when cgo is enabled), and it has the APIs this package relies on: atomic batches committed with
// pebble.Sync, consistent snapshots, and per-level compression profiles.
// The on-disk Pebble format is pinned to FormatValueSeparation, the
// newest non-experimental format of v2.1.7, so a library upgrade never
// ratchets the format without a deliberate change here.
//
// # Key layout
//
// Every key begins with a one-byte table tag. String components are
// encoded with an order-preserving, self-delimiting escape: 0x00 becomes
// 0x00 0xFF and each component ends with 0x00 0x01. Encoded components sort
// exactly like the raw strings and no encoded component is a prefix of
// another, so the prefix for namespace "prod" never matches "prod-2" and
// object "a" never matches "ab". Times are 12 bytes: big-endian seconds
// since the Unix epoch with the sign bit flipped, then big-endian
// nanoseconds; this sorts chronologically and covers every time.Time
// second, including the zero value.
//
//	M format                          format version (uvarint); Open rejects unknown versions
//	M state                           last Seq (u64) | last assigned time
//	M horizon                         retention horizon
//	M heartbeat                       last heartbeat
//	L <resource>                      latest record time of the resource (CoverageEnd validation)
//	V <resource> <ns> <name> <time>   one object version (record)
//	T <time>                          time index: resource, ns, name, type, seq of the record at <time>
//	C <resource> <seq u64>            coverage event
//	N <S>                             snapshot manifest header at time S
//	S <S> <resource> <ns>             snapshot manifest chunk: names in ns with their latest record at S
//
// Record times are unique across the store (the store clamps them to be
// strictly increasing), so <time> alone identifies a record and orders it
// exactly like Seq. Get(key, at) is a single SeekLT to
// V<key><time(at)>0xFF. History is a bounded forward scan of V<key>.
// Changes is a forward scan of T over [from, to] with a point lookup of
// each matching version. List without a usable manifest scans
// V<resource>[<ns>] with two seeks per object (to its latest version at
// the requested time, then past it).
//
// # Values
//
// Every value is framed as [codec version byte][CRC32C][payload]. The
// checksum covers the key, the codec byte and the payload, so damaged,
// truncated or misplaced values are detected. A mismatch is reported as an
// error wrapping store.ErrCorrupt that names the key; data that fails
// validation is never returned.
//
// # Compression and block size
//
// SSTable blocks use zstd (level 3, Pebble's ZstdCompression profile) on
// every level, with 64 KiB data blocks (Options.BlockSize). Objects are
// stored whole (ADR 0001); successive versions of one object are adjacent
// in the V table, and a block must hold several of them for the
// compressor to exploit that. Normalized Pods and Deployments are 2-8 KB,
// so Pebble's 4 KiB default holds about one version per block. Measured
// by BenchmarkDiskPerChange (2,000 Pods and 500 Deployments, 20 status
// updates each, 52,500 changes, avg 5,376 B raw JSON, 857 B per change as
// initial object plus JSON merge patches):
//
//	zstd   4 KiB 1,892 B/change   32 KiB 419   64 KiB 279  (0.33x merge patch)
//	snappy 4 KiB 2,609 B/change   32 KiB 775   64 KiB 605
//	none          ~5,500 B/change
//
// 64 KiB blocks were also faster than 32 KiB for Get and List on the
// churn dataset (fewer index entries and block loads), so they are the
// default. Options.Compression and Options.BlockSize exist so operators
// can trade CPU and read amplification for disk.
//
// # Snapshot manifests
//
// A manifest at time S lists, for every resource and namespace, each key
// with a retained record at or before S, the time of its latest such
// record and whether that record is a delete. List(at=T) uses the newest
// manifest with S <= T: it merges the manifest entries with the keys
// changed in (S, T] (from the T index) and reads each key's version by
// exact key. Manifests are written by a background job every
// Options.SnapshotInterval and are always derivable from the journal.
//
// Paged Lists stay O(page). Namespace chunks are read only when the merge
// reaches them, and chunks before ListOptions.After are never read. The
// time-index window (S, T] is ordered by time rather than key, so it must
// be read in full before the first record; the manifest path is therefore
// used only when the window is small (at most 1,024 records, or a quarter
// of the manifest's entries for the resource), which the store knows
// without reading it because every change since S consumed one Seq.
// Otherwise List takes the prefix scan, which is lazy by construction.
// A corrupt or missing manifest header is detected before anything is
// returned and List uses the scan; a chunk found unusable part-way
// through hands over to the scan after the last key already emitted, so
// results are identical and nothing is repeated.
//
// ADR 0001 describes manifests as listing only keys whose latest record is
// not a delete. View.List, however, must report keys whose latest record
// is a delete, so a live-only manifest cannot produce the same answer as
// the scan; this package therefore includes deleted keys. That limits what
// a manifest can save: List must still read the record of every key
// deleted within retention. On the 7-day churn dataset (10k live objects,
// 40,620 pod keys within retention) the manifest path was about as fast as
// the scan for namespace Lists and for cluster-wide Lists at the latest
// time, and about 1.4x faster for cluster-wide Lists three days back; see
// bench_test.go. Manifests are kept because they are cheap, always
// correct, and their benefit grows with the ratio of dead to live keys.
//
// Compact deletes manifests older than the new horizon (they depend on time
// index entries Compact removes). A newer manifest can still name a key
// whose latest record at S was a delete older than the horizon, which
// Compact removed with the key; the manifest header records the horizon it
// was built under, and when the horizon has moved since (or a compaction
// is still pending in the view) such an entry is skipped if its record is
// gone, exactly as the scan path would no longer find it. No manifest is
// written while a compaction is pending.
//
// # Concurrency, durability and shutdown
//
// Writes are validated in full against scratch bookkeeping, then committed
// as one Pebble batch with pebble.Sync, so a failing batch leaves no trace
// and a returned Write survives a crash. A View is a Pebble snapshot;
// it reads the horizon from that snapshot too. Compact publishes the new
// horizon first, then deletes records in bounded, synced chunks computed
// from a snapshot, without blocking writers; see Store.Compact for why
// every answer at t >= horizon is final at each step and how a crash
// between chunks is resumed.
//
// Any commit failure is treated as ambiguous (Pebble may already have
// applied the batch; a failed WAL fsync often leaves it durable), so the
// store poisons itself: that call and every later mutation return an
// error wrapping store.ErrClosed until the store is reopened, and a
// retried batch can never be applied twice. Reads keep working. The same
// applies when a commit is abandoned because the caller's context ended
// while it was still inside Pebble.
//
// On-disk corruption found while reading (an SSTable block checksum
// mismatch) is logged through a Pebble event listener and returned to the
// reader as an error wrapping store.ErrCorrupt; it never terminates the
// process.
//
// Not every Pebble failure is recoverable. Pebble calls Logger.Fatalf on
// its own background goroutines for state it cannot continue from, for
// example a failed MANIFEST write or sync during a flush or compaction
// (version_set.go) or a failed version edit. The adapter turns Fatalf
// into a panic, and on those goroutines nothing recovers it, so the
// process exits; that is deliberate, because the in-memory LSM state no
// longer matches disk, and a restart (which replays the WAL) is the right
// recovery. Errors Pebble reports as recoverable reach the
// EventListener.BackgroundError hook and are only logged. Fatalf on the
// goroutine of a Store or View call (the commit pipeline, or a read) is
// recovered: a commit poisons the store, a read returns ErrCorrupt.
//
// Close never blocks indefinitely on a writer. Writes queued for the
// single writer slot return store.ErrClosed with nothing applied; a Write
// whose commit is inside Pebble returns at once with an error wrapping
// store.ErrClosed (outcome unknown, the commit cannot be interrupted).
// Close then waits up to Options.CloseTimeout for such commits before
// closing Pebble, and returns an error, leaving Pebble open, if the disk
// never answers.
package pebblestore
