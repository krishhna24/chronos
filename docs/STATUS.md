# Chronos engineering status

Updated: 2026-11-24

## Phase
- [x] Phase 1 — Reconnaissance: local dir and GitHub repo
  (krishhna24/chronos, private) were empty; no prior history. Starting from scratch.
- [x] Phase 2 — Architecture and contracts (this commit): ADRs 0001–0006,
  `internal/model`, `internal/store` contract, memstore reference store,
  storetest conformance suite, `internal/api` wire types, `docs/api.md`.
- [ ] Phase 3 — Core implementation (parallel workstreams below)
- [ ] Phase 4 — Integration: `cmd/chronos` wired (bd68e72); smoke-tested on kind with memory storage (deployment image diff correct, dashboard served, clean SIGTERM). Pebble wiring pending.
- [ ] Phase 5 — Adversarial verification
- [ ] Phase 6 — Rebuild and hardening
- [ ] Phase 7 — Release readiness (image/chart/CI/install docs exist; blocked on pebble + review fixes)

## Environment
Go 1.26.11, Node 26, pnpm 12, Docker 29, kind 0.33, kubectl 1.36,
golangci-lint 2.15. Not installed: helm (chart lint uses a container).

## Workstreams
| Workstream | Owner | Branch | Status |
| --- | --- | --- | --- |
| Pebble store | Storage agent | `ws/storage` | in progress (resumed): benchmarks + Write poisoning |
| Collector + normalization | Collector agent | `ws/collector` | merged; unit + kind integration tests re-run by orchestrator: pass |
| Query engine + HTTP server | Query agent | `ws/query` | merged (c7443c2); verified by orchestrator |
| Dashboard | Frontend agent | `ws/web` | merged (a6c21ed); verified by orchestrator; not yet viewed in a real browser |
| CLI (`kubectl at`) | CLI agent, finished by orchestrator | `ws/cli` | merged (58403cd); 38 golden cases + proxy/RBAC-error tests pass; live-tested on kind |
| Packaging/CI | DevOps agent | `ws/devops` | merged (71b211b); image, chart (helm lint --strict), RBAC (can-i verified), kind demo, service proxy verified on kind; workflows actionlint-only (never run on GitHub) |

## Test results
| Date | Command | Result |
| --- | --- | --- |
| 2026-11-24 | `go test -race ./...` | memstore conformance: pass |
| 2026-11-24 | web: `pnpm typecheck`, `pnpm test` (102 passed), `pnpm build` (orchestrator re-run) | pass |
| 2026-11-24 | `go test -race -count=1 ./...` after query merge (orchestrator re-run) | query, server, resources, memstore: pass; golangci-lint 0 issues |

## Constraints from the owner
- No open-source license (no LICENSE file, no license fields).
- Commits authored as the repo owner, no AI attribution trailers.
- Nothing pushed to the remote without explicit approval.

## Independent review (2026-11-25) — remediation
| # | Severity | Finding | Owner | Status |
| --- | --- | --- | --- | --- |
| 1 | critical | Shutdown during failed relist claims continuity to shutdown time | collector | fixing |
| 2 | high | Events dropped at shutdown under backpressure, no gap | collector | fixing |
| 3 | high | Crash recovery after compaction crash-loops (End < latest record) | contract + collector + storage | contract done (0e31ed1: View.LatestRecordTime); impl in progress |
| 4 | medium | Clean watch close advances continuity past undelivered events | collector | fixing |
| 5 | medium | /list unbounded (84 MB body at 10k pods) | contract + query + cli + web | contract done; CLI paging done (dbc4fed); server + web in progress |
| 6 | medium | ANSI escape injection in CLI tables | cli | fixed (37c505b) |
| 7 | medium | Shutdown hangs if a store write blocks | collector | fixing |
| 8 | medium | ObservedAt late by store stalls while coverage says observed | collector | fixing |
| 9 | low | Live End at last record time makes that record classify as gap | collector | fixing |
| – | low | Secret annotations other than last-applied survive | collector (normalize) + ADR 0005 | ADR amended; impl in progress |
| – | low | Watch Delete with different UID loses old-UID delete | collector | fixing |
| – | low | Overflowing relative time reported as future | cli | fixed (37c505b) |
| – | low | /readyz ignores collector sync and store health | orchestrator | open: decide |

## Known defects / follow-ups
- memstore View copies the key map (~1 ms per View at 10k keys): fine for tests, not for production (pebblestore is the production path).
- Label-selector List decodes every object (44 ms at 10k on memstore); revisit after pebblestore benchmarks.
- `/object?seq=` scans the key history; consider `View.GetSeq`.
- API backlog from the dashboard: namespaces endpoint, `ownerUID`/paging on `/list`, newest-first `/history`.
- Dashboard under the service proxy needs a trailing slash; server should redirect.
