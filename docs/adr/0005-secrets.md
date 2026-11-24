# ADR 0005: Secret handling

Status: accepted (2026-11-24)

- Secrets are watched through the **metadata API** (`PartialObjectMetadata`)
  so `data` and `stringData` are never deserialized into Chronos memory,
  logs, or storage.
- The metadata still contains annotations, and
  `kubectl.kubernetes.io/last-applied-configuration` on a Secret contains
  the full Secret including data. The normalizer drops that annotation for
  Secrets (and any annotation listed in `--redact-annotations`).
- Change detection uses the normalized metadata; a data-only change is
  visible only as a `resourceVersion` change and is recorded as a
  Modified record with no content difference.
- RBAC: list/watch on secrets is still required (the metadata API uses the
  same verbs). This is a privilege of the Chronos ServiceAccount; the
  process never requests full Secret objects. A test asserts the collector
  uses the metadata client for metadata-only types.
- The normalizer applies to every type: `managedFields` are dropped
  (configurable) because they are large and change on every write.
