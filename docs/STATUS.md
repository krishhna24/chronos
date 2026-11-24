# Chronos engineering status

Updated: 2026-11-24

## Phase
- [x] Phase 1 — Reconnaissance: local dir and GitHub repo
  (krishhna24/chronos, private) were empty; no prior history. Starting from scratch.
- [x] Phase 2 — Architecture and contracts (this commit): ADRs 0001–0006,
  `internal/model`, `internal/store` contract, memstore reference store,
  storetest conformance suite, `internal/api` wire types, `docs/api.md`.
- [ ] Phase 3 — Core implementation (parallel workstreams below)
- [ ] Phase 4 — Integration (`cmd/chronos`)
- [ ] Phase 5 — Adversarial verification
- [ ] Phase 6 — Rebuild and hardening
- [ ] Phase 7 — Release readiness

## Environment
Go 1.26.11, Node 26, pnpm 12, Docker 29, kind 0.33, kubectl 1.36,
golangci-lint 2.15. Not installed: helm (chart lint uses a container).

## Workstreams
| Workstream | Owner | Branch | Status |
| --- | --- | --- | --- |
| Pebble store | Storage agent | `ws/storage` | in progress |
| Collector + normalization | Collector agent | `ws/collector` | in progress |
| Query engine + HTTP server | Query agent | `ws/query` | merged (c7443c2); verified by orchestrator |
| Dashboard | Frontend agent | `ws/web` | merged (a6c21ed); verified by orchestrator; not yet viewed in a real browser |
| CLI (`kubectl at`) | CLI agent | `ws/cli` | in progress |
| Packaging/CI | DevOps agent | after integration | blocked on cmd/chronos flags |

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

## Known defects / follow-ups
- memstore View copies the key map (~1 ms per View at 10k keys): fine for tests, not for production (pebblestore is the production path).
- Label-selector List decodes every object (44 ms at 10k on memstore); revisit after pebblestore benchmarks.
- `/object?seq=` scans the key history; consider `View.GetSeq`.
- API backlog from the dashboard: namespaces endpoint, `ownerUID`/paging on `/list`, newest-first `/history`.
- Dashboard under the service proxy needs a trailing slash; server should redirect.
