# Chronos architecture

Chronos records every change to selected Kubernetes resources and answers
"what did this look like at time T?" without fabricating continuity it did
not observe.

## Components and data flow

```text
 kube-apiserver
      │  LIST + WATCH (one reflector per resource type; metadata-only for Secrets)
      ▼
 internal/collector ── normalize ── change detection (hash of normalized object)
      │  []store.Entry: Records + CoverageEvents, one atomic batch per decision
      ▼
 internal/store (contract)  ◄── memstore (reference/oracle)
      │                     ◄── pebblestore (production, on a PVC)
      ▼
 internal/query  (coverage classification, existence, diff)
      │
      ▼
 internal/server  HTTP /api/v1 + embedded dashboard (web/)
      ▲                               ▲
      │ via kube-apiserver service proxy (user's kubeconfig + RBAC)
 cmd/kubectl-at                    browser (kubectl port-forward / proxy)
```

One process (`cmd/chronos`) runs collector, store, retention, and HTTP
server. It is a single-replica StatefulSet: Pebble is a single-writer
embedded store, and one writer gives a single total order (Seq).

## Package ownership

| Path | Owner | Contract it implements / consumes |
| --- | --- | --- |
| `internal/model`, `internal/store/store.go`, `internal/store/memstore`, `internal/store/storetest`, `internal/api`, `internal/resources` | Orchestrator | **Shared contracts. Changes need orchestrator review.** |
| `internal/store/pebblestore` | Storage | implements `store.Store`; must pass `storetest.Run` |
| `internal/collector`, `internal/normalize` | Collector | writes `store.Store` |
| `internal/query`, `internal/server` | Query/API | reads `store.View`; serves `internal/api` types; `docs/api.md` |
| `cmd/kubectl-at`, `internal/cli` | CLI | consumes `/api/v1` |
| `web/` | Frontend | consumes `/api/v1`; `web/src/api/types.ts` mirrors `internal/api` |
| `cmd/chronos` | Orchestrator (integration) | wires everything |
| `Dockerfile`, `deploy/`, `.github/`, `hack/` | DevOps | |

## Core semantics (summary — the ADRs are normative)

- **Identity** (ADR 0003): `ObjectKey{resource, namespace, name}` is what a
  user asks about; `UID` distinguishes instances. A delete followed by a
  create of the same name is two records with different UIDs; diff reports
  `recreated`.
- **Time and order** (ADR 0002): Chronos's clock at observation time, made
  strictly increasing by the store. `Seq` is the total order.
  `resourceVersion` is stored but never compared.
- **Coverage** (ADR 0004): per resource type, alternating Start/End events.
  A query instant is `observed`, `gap`, `unobserved`, or `expired`.
  Existence is only `present`/`absent` when `observed`; otherwise `unknown`.
- **Origins**: `baseline` (first LIST: existed then, nothing known before),
  `watch` (live event), `resync` (inferred by diffing a relist after a gap:
  happened at an unknown time inside the gap).
- **Storage** (ADR 0001): full normalized object per change in Pebble,
  keyed by object then Seq; time index for changes; periodic snapshot
  manifests to bound list cost; retention by compaction to a horizon.
- **Secrets** (ADR 0005): collected through the metadata API only; the
  `last-applied-configuration` annotation is stripped.
- **Access** (ADR 0006): the API listens on a ClusterIP Service; the CLI goes
  through the kube-apiserver service proxy, so Kubernetes authn/authz gate
  access. Anyone who can reach the API can read all recorded history.

## Collector state machine (per resource type)

```text
           LIST ok (Replace)                       watch lost, relist needed
 [start] ───────────────────► observing ─────────────────────────────────┐
   │ batch: [End(lastAlive)]?   ▲   │ watch event → Record(origin=watch)  │
   │        baseline/resync      │   │ heartbeat every 10s                 │
   │        records, Start       │   ▼                                     │
   │                             └── relist (Replace): batch [End(lostAt), resync records, Start]
   ▼
 crash: on restart, last coverage event is Start → write End(at = max(heartbeat, last record))
```
