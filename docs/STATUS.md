# Chronos engineering status

Updated: 2026-11-25

## Phase
- [x] Phase 1 — Reconnaissance: local dir and GitHub repo
  (krishhna24/chronos, private) were empty; no prior history. Starting from scratch.
- [x] Phase 2 — Architecture and contracts (this commit): ADRs 0001–0006,
  `internal/model`, `internal/store` contract, memstore reference store,
  storetest conformance suite, `internal/api` wire types, `docs/api.md`.
- [ ] Phase 3 — Core implementation (parallel workstreams below)
- [x] Phase 4 — Integration: all components wired; pebble default.
- [~] Phase 5 — Adversarial verification: one independent review round
  (13 findings, all fixed with regression tests); restart/crash verified;
  chaos and scale testing still to do.
- [ ] Phase 5 — Adversarial verification
- [ ] Phase 6 — Rebuild and hardening
- [ ] Phase 7 — Release readiness (image/chart/CI/install docs exist; blocked on pebble + review fixes)

## Environment
Go 1.26.11, Node 26, pnpm 12, Docker 29, kind 0.33, kubectl 1.36,
golangci-lint 2.15. Not installed: helm (chart lint uses a container).

## Workstreams
| Workstream | Owner | Branch | Status |
| --- | --- | --- | --- |
| Pebble store | Storage agent | `ws/storage` | merged (1f1fa70), default backend (cmd/chronos); conformance + differential + SIGKILL crash + poison + corruption tests re-run by orchestrator: pass |
| Collector + normalization | Collector agent | `ws/collector` | merged incl. review fixes (a926075); reviewer's original repros re-run by orchestrator: 5/6 pass, 1 obsolete (asserted End <= last record, contradicting the +1ns fix; no false continuity) |
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

## MVP definition of done (verified 2026-11-25 on kind chronos-dev)
| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | Install into a cluster | `hack/kind-demo.sh` (Helm chart, pebble on PVC), rc=0 twice |
| 2 | Collect supported resources | all 7 types `observed` in /resources |
| 3 | Make changes | demo scenario: rollout, scale, configmap delete+recreate |
| 4 | Query historical state | `kubectl at <t> get` via service proxy |
| 5 | Compare two states | `kubectl at diff` incl. recreated warning |
| 6 | `kubectl at` plugin | 38 golden cases + live use through the service proxy |
| 7 | Dashboard | all four views screenshotted in headless Chromium against the in-cluster server; "now" bug found and fixed (94e38da) |
| 8 | Restart without losing history | pod deleted: prior records intact, 3 s gap shown, change made while down recorded as `resync` |
| 9 | Incomplete history is visible | GAP rows (CLI), gap shading/banners (dashboard), `unknown` existence, crash-recovery End |

## Independent review (2026-11-25) — remediation
| # | Severity | Finding | Owner | Status |
| --- | --- | --- | --- | --- |
| 1 | critical | Shutdown during failed relist claims continuity to shutdown time | collector | fixed (a926075), regression test ported |
| 2 | high | Events dropped at shutdown under backpressure, no gap | collector | fixed (a926075), regression test ported |
| 3 | high | Crash recovery after compaction crash-loops (End < latest record) | contract + collector + storage | fixed (a926075 collector, 1f1fa70 pebble) |
| 4 | medium | Clean watch close advances continuity past undelivered events | collector | fixed (a926075), regression test ported |
| 5 | medium | /list unbounded (84 MB body at 10k pods) | contract + query + cli + web | fixed: server paging + 503 load bound (69d5ade, default page 4.2 MB), CLI (dbc4fed), dashboard (279dec5), O(page) resume via ListOptions.After (369393c, pebble seek in 1f1fa70) |
| 6 | medium | ANSI escape injection in CLI tables | cli | fixed (37c505b) |
| 7 | medium | Shutdown hangs if a store write blocks | collector | fixed (a926075), regression test ported |
| 8 | medium | ObservedAt late by store stalls while coverage says observed | collector | fixed (a926075), regression test ported |
| 9 | low | Live End at last record time makes that record classify as gap | collector | fixed (a926075), regression test ported |
| – | low | Secret annotations other than last-applied survive | collector (normalize) + ADR 0005 | fixed (a926075) + flag `--secret-annotation-allowlist` (d63b6c3) |
| – | low | Watch Delete with different UID loses old-UID delete | collector | fixed (a926075) |
| – | low | Overflowing relative time reported as future | cli | fixed (37c505b) |
| – | low | /readyz ignores collector sync and store health | orchestrator | decided (d63b6c3): ready = store readable; sync deliberately excluded |

## Independent review round 2 (2026-11-25) — remediation
| # | Severity | Finding | Owner | Status |
| --- | --- | --- | --- | --- |
| 1 | high | SST corruption panics readers (Fatalf) and leaks iterators; Close then panics | storage | fixed: ErrCorrupt naming the table, deferred closes, recover at View boundary |
| 2 | medium | Compact ignores ctx on hung commit; main waited for retention before Store.Close | storage + orchestrator | fixed: shutdown order (55765f8); Compact/Write honour ctx and poison on ambiguous outcome (collector writes use a non-signal context, so SIGTERM cannot poison) |
| 3 | medium | Compact builds one unbounded batch (OOM risk after long outage) | storage | fixed: chunked (≤50k keys/64 MB), horizon written first + pending marker, crash-between-chunks test; peak heap 108→34 MB on 300k records |
| 4 | medium | Actions/base images pinned by tag; curl\|sh lint install; no SBOM/provenance | devops | fixed (44a65f5): SHA-pinned actions, digest-pinned bases, checksum-verified golangci-lint and pnpm, SBOM+provenance; workflows still never run on GitHub |
| 5 | low-med | RBAC grants secrets list/watch even when secrets not recorded | devops | fixed (44a65f5) + drift test chart vs Builtin (98103cc) |
| 6 | low | Pebble background Fatalf crashes process; doc overclaims | storage | doc corrected (restart + WAL replay is the recovery) |
| 7 | low | Chart args unquoted; large ints render as floats | devops | fixed (44a65f5) |
| 8 | low | Close-after-timeout returns nil; qps/burst/interval unvalidated | storage + orchestrator | fixed |

## Known defects / follow-ups
- Cluster-wide List at 10k live + 30k dead keys takes ~580 ms (target 100 ms);
  paging mitigates. Option: ListOptions.IncludeDeleted so the store can skip
  deleted keys (measured ~2.1x faster).
- `kubectl at ... -o jsonpath` / `custom-columns` not supported.
- Not yet tested: end-to-end scale (10k objects through the collector),
  API-server interruption chaos, disk-full, GitHub Actions workflows.
- memstore View copies the key map (~1 ms per View at 10k keys): fine for tests, not for production (pebblestore is the production path).
- Label-selector List decodes every object (44 ms at 10k on memstore); revisit after pebblestore benchmarks.
- `/object?seq=` scans the key history; consider `View.GetSeq`.
- API backlog from the dashboard: namespaces endpoint, `ownerUID`/paging on `/list`, newest-first `/history`.
- Dashboard under the service proxy needs a trailing slash; server should redirect.
