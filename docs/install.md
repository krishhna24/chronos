# Installing Chronos

Chronos runs as one pod (a single-replica StatefulSet) with its history
on a PersistentVolumeClaim. Users reach it through the Kubernetes API
server's service proxy, so the cluster's own authentication and RBAC gate
access (ADR 0006). There is no Ingress and no LoadBalancer.

## Prerequisites

- Kubernetes 1.27 or newer.
- Helm 3 (the chart is tested with 3.19).
- A default StorageClass, or set `persistence.storageClass`. 10Gi is the
  default request.
- Permission to create cluster-scoped RBAC (a ClusterRole and a
  ClusterRoleBinding) at install time.
- For users: `kubectl` and the `kubectl-at` plugin.

## Install with Helm

The chart is in `deploy/helm/chronos` and is attached to every GitHub
release as `chronos-<version>.tgz`.

```sh
helm install chronos deploy/helm/chronos \
  --namespace chronos --create-namespace
kubectl -n chronos rollout status statefulset/chronos
kubectl get --raw /api/v1/namespaces/chronos/services/chronos:http/proxy/readyz
```

Use the release name `chronos` in namespace `chronos`: that produces a
Service named `chronos`, which is what `kubectl at` looks for by default.
Other names work, but every user then has to set `CHRONOS_NAMESPACE` and
`CHRONOS_SERVICE` (see below), or you set `fullnameOverride=chronos`.

The default image is `ghcr.io/krishhna24/chronos:<chart appVersion>`.
To run a local build instead:

```sh
make image IMAGE=chronos:dev
# load it into your cluster (kind: kind load docker-image chronos:dev), then
helm install chronos deploy/helm/chronos -n chronos --create-namespace \
  --set image.repository=chronos --set image.tag=dev --set image.pullPolicy=Never
```

`hack/kind-demo.sh` (or `make kind-demo`) does all of this against a kind
cluster and replays a short demo scenario.

### What the chart creates

| Object | Name (default release) | Purpose |
| --- | --- | --- |
| StatefulSet, 1 replica | `chronos` | the server; never scale it (single writer) |
| PVC (from the volumeClaimTemplate) | `data-chronos-0` | history; kept on uninstall |
| Service, ClusterIP, port `http` | `chronos` | API and dashboard |
| Service, headless | `chronos-headless` | StatefulSet governing service only |
| ServiceAccount | `chronos` | identity of the collector |
| ClusterRole + ClusterRoleBinding | `chronos` | `list`, `watch` on exactly the types in `config.resources` (default: pods, services, configmaps, events, secrets, deployments, replicasets) |
| Role | `chronos-reader` | for users: `get` on `services/proxy` for the Chronos Service only |
| NetworkPolicy (optional) | `chronos` | ingress to the API port only |

The collector only lists and watches; it never `get`s single objects.
The ClusterRole is generated from `config.resources`, so a type you do not
record is not granted.

**Secrets:** Chronos reads Secrets through the metadata API and never
requests their `data` (ADR 0005), but Kubernetes RBAC cannot narrow a
grant to metadata. Recording Secret metadata needs `list`/`watch` on
`secrets` cluster-wide, and that same grant lets anyone holding the
Chronos ServiceAccount token read every Secret's data through the normal
API. If you do not need Secret history, leave `secrets` out of
`config.resources` and the chart drops the grant, for example
`--set 'config.resources={pods,deploy,rs,svc,cm,ev}'`.
Cluster-scoped objects are named `<namespace>-<fullname>` when the release
is not the default, so two installs cannot collide.

## Granting users access

Users need `get` on `services/proxy` in the Chronos namespace. The chart's
`chronos-reader` Role grants exactly that, for the Chronos Service only.
Bind it to the people who should use `kubectl at` and the dashboard:

```sh
kubectl -n chronos create rolebinding chronos-readers \
  --role chronos-reader --group sre
# or --user jane@example.com
```

Check a user's access:

```sh
kubectl auth can-i get services/chronos:http --subresource=proxy -n chronos --as jane@example.com
```

The name the authorizer checks includes the port name (`chronos:http`),
which is why the Role lists both `chronos` and `chronos:http` in
`resourceNames`. If you write your own Role, include `chronos:http`.

**Anyone with this access can read the recorded history of every
namespace, including deleted objects and Secret metadata.** Per-user
filtering is future work (ADR 0006). Grant it as you would read access to
the whole cluster.

## Installing the kubectl-at plugin

Each GitHub release has `kubectl-at_<version>_<os>_<arch>.tar.gz`
(`.zip` on Windows) for linux, darwin and windows on amd64 and arm64, and
`kubectl-at_<version>_checksums.txt`.

```sh
V=v0.1.0 OS=linux ARCH=amd64
curl -LO https://github.com/krishhna24/chronos/releases/download/$V/kubectl-at_${V}_${OS}_${ARCH}.tar.gz
curl -LO https://github.com/krishhna24/chronos/releases/download/$V/kubectl-at_${V}_checksums.txt
sha256sum --ignore-missing -c kubectl-at_${V}_checksums.txt
tar -xzf kubectl-at_${V}_${OS}_${ARCH}.tar.gz
install kubectl-at_${V}_${OS}_${ARCH}/kubectl-at ~/.local/bin/   # any directory on PATH
kubectl plugin list | grep kubectl-at
```

From a checkout: `make build-plugin` writes `bin/kubectl-at`.

```sh
kubectl at -20m get pods -n prod
kubectl at diff -1h now deployment/web -n prod
kubectl at history configmap/app-config -n prod
```

kubectl does not accept its own flags before a plugin name, so put
`--context`, `--kubeconfig` and `-n` after the plugin arguments:
`kubectl at -5m get pods -n demo --context kind-chronos-dev`.

For a non-default install, set `CHRONOS_NAMESPACE` and `CHRONOS_SERVICE`
(or `--chronos-namespace`, `--chronos-service`). To bypass the service
proxy, port-forward and use `--server http://localhost:8080` (or
`CHRONOS_SERVER`).

## Accessing the dashboard

Port-forward (needs `create` on `pods/portforward` in the namespace, which
the `chronos-reader` Role does not grant):

```sh
kubectl -n chronos port-forward svc/chronos 8080:http
# http://localhost:8080/
```

Or through `kubectl proxy`, which needs only the `chronos-reader` access:

```sh
kubectl proxy
# http://localhost:8001/api/v1/namespaces/chronos/services/chronos:http/proxy/
```

Under `kubectl proxy` the URL must end with a trailing slash: the
dashboard loads its assets relative to the page URL. The slash-less URL
is currently answered with a redirect, but bookmark the form with the slash.

## Configuration reference

Every server flag and the chart value that sets it:

| Flag | Chart value | Default | Notes |
| --- | --- | --- | --- |
| `--storage` | `config.storage` | `pebble` | `memory` is for development: history is lost on every restart |
| `--data-dir` | fixed `/var/lib/chronos` | | the PVC (or emptyDir) mount; see `persistence.*` |
| `--retention` | `config.retention` | `168h` | at least `1h` |
| `--retention-interval` | `config.retentionInterval` | `10m` | |
| `--resources` | `config.resources` (list) | `[]` = all | e.g. `[pods, deploy, cm]`; names resolve like the server (plural, singular, kind, short name, `<plural>.<group>`, canonical ID) and an unknown name fails `helm install`; also drives the ClusterRole |
| `--redact-annotations` | `config.redactAnnotations` (list) | `[]` | removed from every object |
| `--secret-annotation-allowlist` | `config.secretAnnotationAllowlist` (list) | `[]` | the only annotations kept on Secrets; all others are dropped (ADR 0005) |
| `--stall-timeout` | `config.stallTimeout` | `5s` | a change waiting longer than this for the store ends coverage of its type and forces a relist |
| `--keep-managed-fields` | `config.keepManagedFields` | `false` | |
| `--log-level` | `config.logLevel` | `info` | `debug`, `info`, `warn`, `error` |
| `--log-format` | `config.logFormat` | `json` | `json` or `text` |
| `--kube-api-qps` | `config.kubeApiQPS` | `20` | |
| `--kube-api-burst` | `config.kubeApiBurst` | `40` | |
| `--listen` | `containerPort` | `8080` | the Service port is `service.port` (default `8080`), named `http` |
| `--kubeconfig` | not exposed | in-cluster | the pod always uses its ServiceAccount |
| `--version` | n/a | | |
| anything else | `extraArgs` (list) | `[]` | appended after the generated flags, each quoted; `--resources` is rejected here (use `config.resources`, which RBAC follows) |

Other values:

| Value | Default | Notes |
| --- | --- | --- |
| `image.repository`, `image.tag`, `image.pullPolicy` | `ghcr.io/krishhna24/chronos`, appVersion, `IfNotPresent` | |
| `persistence.enabled` | `true` | `false` uses an emptyDir and NOTES print a warning: history is lost when the pod moves |
| `persistence.size`, `persistence.storageClass`, `persistence.accessModes` | `10Gi`, cluster default, `[ReadWriteOnce]` | `storageClass: "-"` requests no class |
| `persistence.retentionPolicy` | `Retain` / `Retain` | StatefulSet PVC retention on delete / scale-down |
| `resources` | requests 100m / 256Mi, limits 1 CPU / 1Gi | memory grows with the number of objects recorded |
| `terminationGracePeriodSeconds` | `45` | must be at least 40 (rendering fails otherwise): on SIGTERM the server flushes and writes coverage End markers |
| `podSecurityContext`, `securityContext` | non-root 65532, fsGroup 65532, read-only root FS, no capabilities, seccomp RuntimeDefault | |
| `livenessProbe` (`/healthz`), `readinessProbe` (`/readyz`), `startupProbe` (`/healthz`, up to 5 min) | | |
| `serviceAccount.create`, `serviceAccount.name`, `rbac.create`, `readerRole.create` | `true` | |
| `networkPolicy.enabled`, `networkPolicy.ingressFrom`, `networkPolicy.egress` | `false`, `[]`, `[]` | if you restrict egress, allow DNS and the kube-apiserver |
| `fullnameOverride`, `nameOverride` | `""` | |
| `nodeSelector`, `tolerations`, `affinity`, `priorityClassName`, `podAnnotations`, `podLabels`, `extraEnv`, `imagePullSecrets` | empty | |

There is intentionally no `replicaCount`.

## Upgrade

```sh
helm upgrade chronos deploy/helm/chronos -n chronos --reuse-values
```

The pod is replaced, so Chronos stops observing for the few seconds the
new pod takes to start. That interval is recorded as a gap: queries
inside it report coverage `gap` and existence `unknown`, and changes made
during it appear afterwards with origin `resync`. This is expected.

`volumeClaimTemplates` cannot be changed on an existing StatefulSet, so
`persistence.size` and `persistence.storageClass` only apply to new
installs. To grow the volume, if the StorageClass allows expansion:

```sh
kubectl -n chronos patch pvc data-chronos-0 -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'
kubectl -n chronos delete statefulset chronos --cascade=orphan
helm upgrade chronos deploy/helm/chronos -n chronos --reuse-values --set persistence.size=20Gi
```

## Uninstall

```sh
helm uninstall chronos -n chronos
```

The PVC `data-chronos-0` is **kept**, so reinstalling with the same
release name picks up the existing history (pebble storage). To delete
the history:

```sh
kubectl -n chronos delete pvc data-chronos-0
kubectl delete namespace chronos
```

## Troubleshooting

**Pod not ready.**
`kubectl -n chronos describe pod chronos-0` and
`kubectl -n chronos logs chronos-0`.

- PVC `Pending`: no default StorageClass; set `persistence.storageClass`.
- `pebble storage not available in this build yet`: the image predates
  the pebble backend. Use a newer image, or `--set config.storage=memory`
  for evaluation only.
- `forbidden` in the logs: `rbac.create=false` without equivalent RBAC,
  or a different ServiceAccount. The collector needs `list` and `watch` on
  every type it records (`config.resources`).
- `CrashLoopBackOff` with a flag error: check `extraArgs` and `config.*`
  (for example `--retention` below `1h`).
- Startup can take a while on a large store after an unclean shutdown; the
  startup probe allows 5 minutes.

**403 on services/proxy.**
`services "chronos:http" is forbidden: User "..." cannot get resource
"services/proxy"`: the user has no RoleBinding to `chronos-reader` in the
Chronos namespace, or a custom Role lists only `chronos` and not
`chronos:http`. Check with
`kubectl auth can-i get services/chronos:http --subresource=proxy -n chronos`.

**503 `no endpoints available for service "chronos"`.**
The pod is not ready (above), or the request used `services/chronos/proxy`
without the port name. Use `chronos:http`.

**Gaps after restarts.**
Every restart, upgrade, node drain or lost watch leaves an interval
Chronos did not observe. Chronos never guesses across it: answers for
times inside it say `gap` (or `unobserved` before the first observation,
`expired` beyond retention), and `kubectl at` prints a warning. With
`config.storage=memory` all history before the restart is gone.
