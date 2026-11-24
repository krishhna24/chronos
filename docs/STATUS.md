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
| Pebble store | Storage agent | `ws/storage` | not started |
| Collector + normalization | Collector agent | `ws/collector` | not started |
| Query engine + HTTP server | Query agent | `ws/query` | not started |
| Dashboard | Frontend agent | `ws/web` | not started |
| CLI | CLI agent | after query | blocked on query API |
| Packaging/CI | DevOps agent | after integration | blocked on cmd/chronos flags |

## Test results
| Date | Command | Result |
| --- | --- | --- |
| 2026-11-24 | `go test -race ./...` | memstore conformance: pass |

## Known defects
None recorded yet.
