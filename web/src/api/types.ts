// Wire types of the Chronos HTTP API (/api/v1).
//
// This file mirrors internal/api/types.go exactly; that file is normative.
// Times are RFC 3339 strings with up to nanosecond precision. They are kept
// as strings (never round-tripped through Date) so a time the server
// returned can be sent back without losing sub-millisecond precision.

/** RFC 3339 timestamp, e.g. "2026-11-24T14:00:00.123456789Z". */
export type Time = string;

/** How much Chronos knows about a resource type at a point in time. */
export type CoverageState =
  /** Continuously watching; answers are authoritative. */
  | 'observed'
  /** Continuity was lost; last known state is for reference only. */
  | 'gap'
  /** Before Chronos first observed this resource type. */
  | 'unobserved'
  /** Older than the retention horizon. */
  | 'expired';

/** Did the object exist at that time? */
export type Existence = 'present' | 'absent' | 'unknown';

/** Coverage of one resource type at one instant. */
export interface Coverage {
  state: CoverageState;
  /** Since/Until bound the coverage interval containing the instant. */
  since?: Time;
  /** Absent while the interval is still open. */
  until?: Time;
  /** Why the preceding interval ended, for gaps. */
  reason?: string;
}

/** One interval of continuous observation. */
export interface Interval {
  start: Time;
  /** Absent: still observing. */
  end?: Time;
  endReason?: string;
}

/** A recorded resource type. */
export interface Resource {
  /** Canonical ID, e.g. "apps/v1/deployments". */
  id: string;
  group: string;
  version: string;
  resource: string;
  kind: string;
  namespaced: boolean;
  metadataOnly: boolean;
  shortNames?: string[];
  coverage: Interval[];
}

/** GET /api/v1/resources */
export interface ResourcesResponse {
  now: Time;
  horizon?: Time;
  resources: Resource[];
}

export interface ObjectKey {
  resource: string;
  namespace?: string;
  name: string;
}

export type ChangeType = 'Added' | 'Modified' | 'Deleted';
export type Origin = 'watch' | 'baseline' | 'resync';

/** Summary of one journal record. */
export interface Version {
  seq: number;
  observedAt: Time;
  type: ChangeType;
  origin: Origin;
  uid: string;
  resourceVersion: string;
}

/** A Kubernetes object as JSON (normalized by Chronos). */
export type KubeObject = Record<string, unknown>;

/** State of one object at one instant. */
export interface ObjectState {
  key: ObjectKey;
  at: Time;
  existence: Existence;
  coverage: Coverage;
  /** The record the answer is based on (latest record at or before `at`). */
  version?: Version;
  /** Set when present, or when unknown and a last known state exists. */
  object?: KubeObject;
}

/** GET /api/v1/list */
export interface ListResponse {
  at: Time;
  coverage: Coverage;
  items: ObjectState[];
}

/** GET /api/v1/diff */
export interface DiffResponse {
  from: ObjectState;
  to: ObjectState;
  /** Both sides exist but have different UIDs. */
  recreated: boolean;
  /** Unified diff of the YAML renderings (empty if equal). */
  diff: string;
  /** False when any part of [from, to] was not observed. */
  complete: boolean;
}

/** GET /api/v1/history */
export interface HistoryResponse {
  key: ObjectKey;
  versions: Version[];
  coverage: Interval[];
  truncated: boolean;
}

/** A Version with its object key (Go embeds Version, so fields are flat). */
export interface Change extends Version {
  key: ObjectKey;
}

/** GET /api/v1/changes */
export interface ChangesResponse {
  changes: Change[];
  truncated: boolean;
}

/** Body of every non-2xx response. */
export interface ErrorBody {
  code: 'bad_request' | 'not_found' | 'internal' | 'unavailable' | (string & {});
  message: string;
}
