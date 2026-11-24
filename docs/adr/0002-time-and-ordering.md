# ADR 0002: Timestamps and ordering

Status: accepted (2026-11-24)

## Context
Kubernetes offers no global change timestamp. `resourceVersion` is opaque:
the API conventions forbid clients from comparing it numerically, and it is
not comparable across resource types or meaningful as time. Object
timestamps (`creationTimestamp`, managedFields `time`) are set by different
components, have second granularity, and do not exist for every change.

## Decision
- Each record's time is **Chronos's wall clock when the change was
  received** (`ObservedAt`). The store clamps it to be strictly greater than
  every earlier record time, so time and Seq order agree and "state at T"
  has one answer.
- `Seq` (store-assigned, strictly increasing in commit order) is the only
  total order. Cross-resource ordering is "the order Chronos saw it".
- `resourceVersion` is stored for reference and equality checks only.
- Consequences documented to users: answers are accurate to watch delivery
  lag (normally sub-second); after a Chronos restart or relist, inferred
  changes carry the relist time with `origin=resync`; a wall-clock jump
  backwards is absorbed by clamping (times stall, never reorder).
- Query timestamps are parsed in the client's local zone unless given as
  RFC 3339 with an offset; relative forms (`-20m`) are relative to the
  client's clock.
