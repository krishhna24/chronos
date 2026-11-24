# Chronos

A time machine for Kubernetes. Chronos continuously records changes to
Pods, Deployments, ReplicaSets, Services, ConfigMaps, Events and Secret
*metadata*, and lets you ask what they looked like at any time in the
retention window:

```sh
kubectl at -20m get pods -n prod
kubectl at 2026-11-24T14:00:00 get deployment api -n prod
kubectl at diff 14:00 14:05 deployment/api -n prod
```

Chronos says when it does not know. Every answer carries its coverage:
`observed`, `gap`, `unobserved`, or `expired`.

Status: MVP feature-complete, pre-release. See [docs/STATUS.md](docs/STATUS.md).

## Quick start (kind)

```sh
hack/kind-demo.sh          # builds the image, installs the chart into kind
                           # cluster "chronos-dev", runs a short scenario
make build-plugin          # builds bin/kubectl-at
export PATH=$PWD/bin:$PATH
kubectl at -5m get pods -n demo --context kind-chronos-dev
```

Install into a real cluster: [docs/install.md](docs/install.md).

## Repository layout

| Path | What |
| --- | --- |
| `cmd/chronos` | the recorder: collector + store + retention + HTTP API + dashboard |
| `cmd/kubectl-at` | the `kubectl at` plugin |
| `internal/collector`, `internal/normalize` | list/watch, change detection, coverage |
| `internal/store` | storage contract; `memstore` (reference), `pebblestore` (production) |
| `internal/query`, `internal/server` | historical queries and the `/api/v1` HTTP API |
| `internal/cli` | plugin implementation |
| `web/` | React + TypeScript dashboard (embedded in the binary) |
| `deploy/helm/chronos` | Helm chart |

## Development

```sh
make lint test-race        # Go: gofmt, vet, golangci-lint, tests with -race
make web                   # dashboard build (pnpm)
cd web && pnpm dev:mock    # dashboard against in-browser fixtures
go test -tags integration ./internal/collector/   # needs CHRONOS_KUBECONFIG
```

- Architecture: [docs/architecture.md](docs/architecture.md)
- Decisions: [docs/adr/](docs/adr/)
- HTTP API: [docs/api.md](docs/api.md)
