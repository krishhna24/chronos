# Chronos performance (end to end)

Measured 2026-11-25 on kind `chronos-dev`. Unlike the Pebble benchmarks
in ADR 0001, every change here goes through the whole path: client →
kube-apiserver → watch → collector → Pebble → HTTP API. All numbers
below are copied from real runs. The raw output is reproducible with
the commands in [Reproducing](#reproducing).

## Environment

| Item | Value |
| --- | --- |
| Host | AMD Ryzen 7 6800HS (8C/16T, 12 visible to WSL2), 7.9 GB RAM, WSL2 kernel 6.18.58, ext4 on vhdx |
| Cluster | kind v0.33.0, one node, Kubernetes v1.37.0, default etcd and apiserver settings |
| Chronos under test | `bin/chronos` built from `ws/perf` (product code unchanged from `b46c1c5`), run **locally** against the kind kubeconfig: `--data-dir <scratch> --listen 127.0.0.1:18080 --log-format json --kube-api-qps 50 --kube-api-burst 100 --retention-interval 1m`, all other flags default (pebble, 7d retention, 5s stall timeout) |
| Memory/CPU source | `/proc/<pid>/status` (VmRSS, VmHWM) and `/proc/<pid>/stat` (utime+stime), sampled every 1 s; Go heap from `GODEBUG=gctrace=1` (no pprof was added to the product) |
| Also on the machine | the in-cluster Chronos (chart, 1 CPU / 1 Gi limit) recording the same cluster, a second kind cluster (`velero-csi`), the load generator |

The local process was used for memory and CPU numbers because the image
is distroless and `kubectl top` has no metrics-server on kind. The
in-cluster pod was sampled from the node too (see [In-cluster pod](#in-cluster-pod)).

## Dataset

Created by `chronosperf populate` in namespaces `perf-0` .. `perf-9`
(9,500 creates in 45.5 s; the client was limited to 200 QPS):

| Type | Count at initial sync | Notes |
| --- | --- | --- |
| ConfigMaps | 7,029 | 7,000 perf ones of ~3 KB (`config.ini` with random hex values, so compression is not flattered) |
| Pods | 2,513 | 2,000 bare + 500 from Deployments; `nodeSelector` no node has, so all stay Pending and use no CPU |
| Deployments | 504 | 1 replica each |
| ReplicaSets | 506 | |
| Events | 3,364 (6,160 later) | mostly FailedScheduling for the Pending Pods |
| Secrets, Services | 6, 4 | |
| **Total** | **13,926** objects | |

The kind apiserver and etcd took this without trouble; the target scale
was reached, so no reduced-scale fallback was needed.

Update storm (`chronosperf storm`): merge-patches of
`metadata.annotations` with the client send time and a sequence number,
80% random perf ConfigMaps, 20% random Deployments (metadata only, no
rollout). Every patch is a real change, so every one must be recorded.
Each write is logged with its client send time `t0`, response time `t1`
and the returned resourceVersion; `chronosperf lag` then joins the log
with `/api/v1/history` by resourceVersion.

## Results

| Metric | Value | Target / reference | Met? |
| --- | --- | --- | --- |
| Objects recorded | 13,926 at initial sync, ~16.7k by the end of the run | ADR 0001: 10k+ | yes |
| Initial sync, empty store, 13,926 objects | **2.29 s** from process start to the last of 7 `Start` events | – | – |
| Warm restart (graceful stop, same store, ~16.7k objects) | **3.33 s** to all `Start` events | – | – |
| Restart after SIGKILL (same store, during a 100/s storm) | **8.32 s** (1.4 s Pebble open + WAL replay, 3.3 s crash recovery, then relist) | – | – |
| Writes lost (100/s for 3 min + 200/s for 3 min) | **0 of 53,640**; 0 ordering violations | 0 | yes |
| Storm rate achieved | 100.0/s (17,998 writes); 199.7/s (35,642 writes; 303 ticks skipped because all 32 writers were busy) | 50–200/s | yes |
| Collection lag, ObservedAt − client send, 100/s | p50 6.43 ms, p95 14.81 ms, p99 37.82 ms, max 1.33 s | ADR 0002: "normally sub-second" | p99 yes; max no (see below) |
| Collection lag, ObservedAt − client send, 200/s | p50 6.89 ms, p95 19.02 ms, p99 39.92 ms, max 1.83 s | | p99 yes; max no |
| ObservedAt − apiserver response, 100/s / 200/s | p50 0.32 / 0.34 ms, p99 1.14 / 2.95 ms, max 1.14 s / 99.8 ms | | |
| RSS after sync, idle | 160.5 MiB (HWM during the initial LIST 199.6 MiB) | – | – |
| RSS during 100/s storm | avg 73.4 MiB, peak 162.3 MiB | | |
| RSS during 200/s storm | avg 127.5 MiB, peak 145.2 MiB | | |
| RSS peak, whole run | 374.2 MiB (HWM 388.1 MiB), during the API benchmark with 8 concurrent full listings | in-cluster limit 1 Gi | yes |
| RSS peak, warm restart | 320.5 MiB (rebuilding change-detection state decodes every stored object) | | |
| Go heap (gctrace), whole run | max 172 MB at GC start, max 88 MB live after GC | | |
| CPU (one core = 100%) | idle 1.9%, 100/s storm 10.3%, 200/s storm 22.1%, warm restart 57.5% over 13 s | | |
| Store bytes per record | **246 B** (18,558,029 B for 75,492 records, all resources, baseline full objects included) | ADR 0001: 279 B measured in isolation | consistent |
| Store growth during the 100/s storm | 5,139,683 B for 17,998 writes = 285 B/write (WAL and memtable included) | | |
| `state` (point-in-time reconstruction), now / 2 min ago | p50 0.40 / 0.38 ms, p95 0.55 / 0.51 ms, max 1.59 ms | ADR 0001: < 100 ms | **yes** |
| `state` with 8 concurrent clients | p50 0.72 ms, p95 1.70 ms, max 7.18 ms | < 100 ms | yes |
| `history` (one object) | p50 0.33 ms, p95 0.46 ms | | |
| `diff` (2 min apart) | p50 0.91 ms, p95 1.56 ms; 8 concurrent p95 3.38 ms | | |
| `list` one page of 500, all namespaces (ConfigMaps) | p50 41.6 ms, p95 62.4 ms (1.95 MB body); 8 concurrent p95 79.5 ms | < 100 ms | yes |
| `list` one page of 500, one namespace (703 ConfigMaps) | p50 62.8 ms, **p95 137.8 ms**, max 270 ms (1.99 MB body) | < 100 ms | **no** |
| `list` one page of 500 Pods, all namespaces | p50 24.3 ms, p95 29.9 ms | | yes |
| `list` complete, 7,029 ConfigMaps in 15 pages | p50 565 ms, max 689 ms (27.9 MB); 8 concurrent p50 940 ms | 100 ms if read as "a listing" | **no** (known, STATUS.md; paging bounds each request) |
| `changes`, 1 minute window, limit 1000 | p50 13.5 ms, p95 27.1 ms | | |
| Retention loop (horizon 1h back: nothing to remove) | runs every interval; "compacted" in 1.8–3.1 ms at 75k records | | loop confirmed |
| `Compact` removing real history (copy of this store, horizon at the midpoint) | **73 ms** to remove 24,289 of 75,492 records, with 4 concurrent readers | – | – |
| Reads during compaction | Get p99 0.56 ms during vs 0.34 ms before; List(namespace) p99 13.2 ms during vs 15.9 ms before; 0 errors | no blocking | yes |
| Answers at t ≥ horizon after compaction | 2,000 probes, **0 changed** | Compact contract | yes |

Latencies are HTTP round trips on loopback with the body fully read and
JSON-decoded by the client. "8 concurrent" means 8 clients at once. The
server's heavy-request bound is also 8, and no request got a 503.

### Notes on the numbers

- **Lag tail.** Lag measured from the apiserver response is sub-ms at
  p95, so the median of about 6–7 ms from client send is mostly the
  apiserver's own write latency. The rare 1.1–1.8 s outliers came in
  clusters about 33 s apart at 100/s, and there was one at 200/s where
  five consecutive resourceVersions all arrived late. In that 200/s case
  the patch call itself took 1.83 s, so the delay was in the
  apiserver/etcd. In the 100/s cases, `ObservedAt` is stamped when the
  event is received (`internal/collector/source.go:168`), before any
  store write. A store stall therefore cannot cause that lag unless the
  1,024-item queue is full, and at 100/s it was not. That leaves watch
  delivery (the apiserver or the WSL2 host) as the likely place. I did
  not prove the exact cause.
- **Memory.** RSS is well above the live Go heap. It also includes
  Pebble's 64 MiB block cache and memtables, which use manual allocation
  (cgo is enabled in this build), and heap the Go runtime has not yet
  returned to the OS. The peak of 374 MiB came from 8 concurrent full
  listings, each building about 28 MB of JSON. Warm restart is the
  second largest consumer (320 MiB), because `loadKnown`
  (`internal/collector/writer.go`) re-normalizes every stored object.
- **Namespace-scoped list page looked slower than a cluster-wide page**
  (p95 138 ms vs 62 ms). *Follow-up (2026-11-25): not a namespace cost.*
  The two scenarios were not comparable: `list-ns-page500-cm` picks a
  random namespace per request while `list-all-page500-cm` repeats the
  same first page, so only the former missed Pebble's block cache. In
  process, with equal access patterns, a namespace page is as fast as or
  faster than a cluster-wide one (`BenchmarkPebbleListPage`,
  internal/query). The profiling did find a real cost: the store's
  snapshot-manifest List path scans every change since the last
  manifest on every page; that is being fixed in pebblestore.
- **Store size.** The 200/s storm shrank the directory (−1.0 MB)
  because Pebble's LSM compaction ran during it, so the per-write
  delta is only meaningful over the whole store. That is why the
  table's figure is 246 B per record across the whole store.

### In-cluster pod

The chart-installed pod (1 CPU / 1 Gi limit) recorded the same dataset
as it was created (watch, not one big LIST) and the 100/s storm. It was
sampled from the kind node through `/proc` and its pod cgroup every 5 s
from 06:07:28 to 06:16:16 UTC. At 06:16:16 another workstream rolled the
StatefulSet to a new image, which ended the sample:

| Metric | Value |
| --- | --- |
| RSS | avg 44.8 MiB, peak 55.5 MiB |
| cgroup `memory.current` peak (includes page cache) | 99.8 MiB |
| CPU | 16.8% of one core on average over the window |

Its RSS stayed far lower than the local process, which had to absorb
one 13.9k-object LIST at startup.

## Comparison with ADR targets

- **10k+ objects: met.** 13.9k objects at sync, about 16.7k by the end,
  and 53.6k updates recorded with no loss.
- **Sub-100 ms reconstruction: met for object reconstruction.** `state`
  p95 was 0.55 ms (1.7 ms with 8 clients), `diff` p95 1.6 ms. One
  500-item list page measured p95 62 ms (repeated first page) and
  138 ms (random namespace, mostly cache misses; see the follow-up above),
  so the 100 ms target holds only for cached pages until the manifest
  path fix lands and the benchmark is rerun with comparable scenarios. A complete 7k-item list is 565 ms and
  depends on paging, as STATUS.md already records.

## Reproducing

```sh
WORK=/tmp/chronos-scale KEEP=1 test/e2e/scale.sh | tee scale.out
# knobs: CONFIGMAPS PODS DEPLOYMENTS NAMESPACES RATES STORM_DURATION
WORK=/tmp/chronos-chaos test/e2e/chaos.sh compaction   # needs the scale store copy
go build -tags e2e -o bin/chronosperf ./test/e2e/chronosperf   # tool help: bin/chronosperf
```

`scale.sh` deletes the `perf-*` namespaces on exit unless `KEEP=1`.
