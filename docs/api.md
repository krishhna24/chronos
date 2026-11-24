# Chronos HTTP API (v1)

Read-only JSON API. Wire types: `internal/api/types.go` (normative).
All times are RFC 3339 with nanoseconds (`2026-11-24T14:00:00.123456789Z`).
Query parameter `at`, `from`, `to` accept RFC 3339; the server never parses
relative times (clients resolve `-20m` against their own clock).

Errors: non-2xx with body `api.Error{code, message}`.
`400 bad_request` (missing/invalid parameter, unknown resource, time in the
future), `404 not_found` (unknown route only — a missing *object* is a 200
with `existence: "absent"` or `"unknown"`), `500 internal`,
`503 unavailable` (store not open yet).

`resource` is always the canonical ID (`v1/pods`, `apps/v1/deployments`).
Clients resolve aliases using `GET /api/v1/resources`.

| Endpoint | Parameters | Response |
| --- | --- | --- |
| `GET /api/v1/resources` | – | `ResourcesResponse`: supported types with coverage intervals, retention horizon, server time |
| `GET /api/v1/state` | `resource`, `namespace` (namespaced types), `name`, `at` (default now) | `ObjectState` |
| `GET /api/v1/list` | `resource`, `namespace` (empty = all), `at` (default now), `labelSelector` (Kubernetes syntax), `includeDeleted` (bool, default false) | `ListResponse`. Items are objects with existence `present` (observed) or `unknown` (gap, last known). With `includeDeleted`, keys whose latest record is a delete are included with `absent`. Sorted by namespace, name. |
| `GET /api/v1/diff` | `resource`, `namespace`, `name`, `from`, `to` (from < to) | `DiffResponse` |
| `GET /api/v1/history` | `resource`, `namespace`, `name`, `from`, `to` (default: horizon..now), `limit` (default 1000) | `HistoryResponse` |
| `GET /api/v1/changes` | `from`, `to` (required), `resource`, `namespace`, `limit` (default 1000, max 10000) | `ChangesResponse`, Seq order |
| `GET /api/v1/object` | `resource`, `namespace`, `name`, `seq` | `ObjectState` for exactly that record (used by the dashboard to open a version from history) |
| `GET /healthz`, `GET /readyz` | – | `200 ok` / `503` |

Semantics of `coverage` and `existence`: see ADR 0004.

Diff rendering: both sides are rendered as YAML with keys sorted; the
`diff` field is a unified diff with headers `--- <from RFC3339>` and
`+++ <to RFC3339>`. A side whose existence is not `present` renders as empty.
