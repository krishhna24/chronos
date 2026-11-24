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
// # Compression
//
// SSTable blocks use zstd (level 3, Pebble's ZstdCompression profile) on
// every level. Objects are stored whole (ADR 0001); successive versions of
// one object are adjacent in the V table and share data blocks, and zstd's
// larger window and entropy coder exploit that redundancy far better than
// Snappy, Pebble's default. The write rate of a cluster journal (hundreds
// of changes per second at most) leaves ample CPU for it. Options.
// Compression can select Snappy or none; the benchmark in this package
// reports bytes per change for each.
//
// # Snapshot manifests
//
// A manifest at time S lists, for every resource and namespace, each key
// with a retained record at or before S, the time of its latest such
// record and whether that record is a delete. List(at=T) uses the newest
// manifest with S <= T: it merges the manifest entries with the keys
// changed in (S, T] (from the T index) and reads each key's version by
// exact key. Manifests are written by a background job every
// Options.SnapshotInterval and are always derivable from the journal: a
// missing, truncated or corrupt manifest (header or chunk) is detected
// before anything is returned and List falls back to the prefix scan.
//
// ADR 0001 describes manifests as listing only keys whose latest record is
// not a delete. View.List, however, must report keys whose latest record
// is a delete, so a live-only manifest cannot produce the same answer as
// the scan; this package therefore includes deleted keys. See the
// benchmark results in bench_test.go for what that means for the speed-up.
//
// Compact deletes manifests older than the new horizon (they depend on time
// index entries Compact removes). A newer manifest can still name a key
// whose latest record at S was a delete older than the horizon, which
// Compact removed with the key; the manifest header records the horizon it
// was built under, and such entries are skipped when the horizon has moved
// since.
//
// # Concurrency and durability
//
// Writes are validated in full against scratch bookkeeping, then committed
// as one Pebble batch with pebble.Sync, so a failing batch leaves no trace
// and a returned Write survives a crash. A View is a Pebble snapshot;
// it reads the horizon from that snapshot too. Compact computes its
// deletions from a snapshot and commits them in one synced batch without
// blocking writers.
package pebblestore
