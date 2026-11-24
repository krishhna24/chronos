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

Status: under construction. See [docs/STATUS.md](docs/STATUS.md).

- Architecture: [docs/architecture.md](docs/architecture.md)
- Decisions: [docs/adr/](docs/adr/)
- HTTP API: [docs/api.md](docs/api.md)
