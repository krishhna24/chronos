# ADR 0001: Storage engine and record encoding

Status: accepted (2026-11-24). Benchmarks to confirm: see "Validation".

## Context
Chronos must persist a change journal for ~10k+ objects with 7-day
retention, survive crashes, serve point-in-time reads in <100 ms, and stay
consistent while retention runs. Options considered:

| Option | Crash safety | Effort | Notes |
| --- | --- | --- | --- |
| Custom append-only segment files | must build WAL, fsync discipline, index, compaction | high | highest correctness risk |
| **Pebble** (CockroachDB LSM) | WAL + atomic batches, consistent snapshots | low | pure Go, no cgo, used in production at scale |
| bbolt | B+tree, single writer, copy-on-write | low | write amplification on large values; no compression |
| PostgreSQL | strong | medium | external dependency to operate; kept as a future backend behind `store.Store` |

Encodings considered: full object per change; RFC 6902 JSON Patch;
RFC 7386 merge patch; periodic snapshot + deltas.

## Decision
- **Pebble**, single process, `pebble.Sync` on every batch (group commit
  happens naturally because the collector batches).
- **Full normalized object per change.** Values compressed by Pebble's
  block compression (zstd). Versions of one object sort adjacently, so they
  share blocks and compress against each other; this recovers most of the
  space a delta encoding would save without a patch chain. Reconstruction
  is one reverse seek — no replay, no patch-application bugs, and corruption
  of one record cannot poison later versions.
- **Snapshots are list accelerators, not reconstruction bases.** A snapshot
  manifest at time S lists, per resource, the keys whose latest record at S
  is not a delete. `List(at=T)` uses the newest snapshot ≤ T plus the time
  index over (S, T], instead of scanning every key ever seen (7 days of Pod
  churn leaves many dead keys). Snapshots are taken periodically (default
  1h) and are always derivable from the journal, so a missing or corrupt
  snapshot degrades to a prefix scan, never to wrong answers.
- **Retention** = `Store.Compact(horizon)` with the semantics in
  `internal/store/store.go`, run periodically; it uses Pebble deletes in a
  batch, so open iterators (Views) are unaffected.
- **Format versioning**: a `meta/format` key holds an integer version;
  opening a store with an unknown version fails; migrations are explicit
  code paths.
- **Validation**: every value carries a CRC32C; mismatches return
  `store.ErrCorrupt` with the key, never silently-wrong data.

## Validation (to be measured, not assumed)
Benchmarks in `internal/store/pebblestore` must report: write throughput,
bytes on disk per change for a representative Deployment/Pod update mix
(compared against raw JSON and against JSON merge-patch size), Get and List
latency at 10k live objects with 7 days of churn, compaction time.
If full-object storage exceeds 3× the merge-patch size on disk, revisit.
