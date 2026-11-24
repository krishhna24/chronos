# ADR 0003: Object identity, deletion and recreation

Status: accepted (2026-11-24)

- Storage and queries are keyed by `ObjectKey{resource ID, namespace, name}`
  because that is what users ask about.
- Each record carries the object `UID`. A delete followed by a create with
  the same name produces a `Deleted` record (UID A) then an `Added` record
  (UID B). History shows both; diff reports `recreated=true`.
- A relist that finds an object with the same key but a different UID than
  last known emits a resync `Deleted` (old UID) followed by a resync `Added`
  (new UID), both at the relist time.
- Delete records store the last state Chronos knows. If the watch delivered
  a final state, that state is used.
