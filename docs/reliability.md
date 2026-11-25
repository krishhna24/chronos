# Chronos reliability (chaos scenarios)

Run 2026-11-25 on kind `chronos-dev` (environment as in
[performance.md](performance.md)). Except for the two in-cluster
scenarios, Chronos ran locally on an empty store with the 10k-object
perf dataset present. Each scenario writes to `perf-chaos-0`
(300 ConfigMaps) with `chronosperf storm` and ends with two checks:

- `chronosperf lag` joins every write with `/api/v1/history`. A write
  with no record is **LOST** if the coverage intervals claim Chronos
  observed it, which would be fabricated continuity. It is **gap-miss**
  (legitimate, ADR 0004) if it fell outside coverage.
- `chronosperf verify` checks that Chronos's answer "now" equals a live
  LIST: same keys, UIDs and resourceVersions.

To rerun one: `WORK=<scratch> test/e2e/chaos.sh <scenario>`.

## Summary

| # | Scenario | Expected (ADR 0004) | Observed | Result |
| --- | --- | --- | --- | --- |
| 1 | Host/WSL restart (incidental, in-cluster pod) | crash-recovery End at the last heartbeat, relist with resync records | End `crash-recovery` at the heartbeat 20:24:26.9; Start 06:02:45 after the apiserver returned; resync pods +2 ~11 −2, events +64 | PASS |
| 2 | Control plane `docker pause` 90 s during a 20/s storm | either continuous coverage (watch resumed) or End/Start around a relist; no fabricated continuity; changes after unpause recorded | HTTP/2 health check dropped all watches 46 s into the pause; relist on unpause; End `relist` for all 7 types, new Start within 0.1–3.3 s of unpause; marker created right after unpause recorded (`resync`); LOST 0 | PASS |
| 3 | Chronos process frozen (SIGSTOP 120 s) while changes continue | coverage must not claim observation of a period whose changes were not seen in time | coverage stayed **open and continuous**; about 2,400 changes made during the freeze stamped at resume (lag up to 2 m 3.9 s); point-in-time answers inside the window are wrong but labelled `observed`; diff across it reports `complete:true`, empty | **FAIL** (D1) |
| 4 | Graceful stop, changes while down, restart | End `shutdown`, gap, resync Added/Modified/Deleted at the relist | End `shutdown` 06:20:02.78, Start 06:20:39.06; deleted cm → resync Deleted, new cm → resync Added, 272 resync Modified; LOST 0; verify equal | PASS |
| 5 | 100 back-to-back updates of one object | every distinct state recorded (watch does not coalesce) | 100 resourceVersions in 579 ms, **100/100** recorded, all `watch` | PASS |
| 6 | Delete + recreate the same name 20× | alternating Deleted/Added with a new UID each; diff says recreated | 39 versions (Added 20, Deleted 19), no two of the same type adjacent, all 20 UIDs present; diff `recreated=true complete=true` | PASS |
| 7 | SIGKILL local Chronos during a 100/s storm; then a second SIGKILL idle | reopen OK, End `crash-recovery` = max(heartbeat, last record), no corruption | End at 06:22:46.0337 (last record; kill at 06:22:46.074; heartbeat 06:22:42.69); Start 06:23:14.39; 294 resync; LOST 0; verify equal; store reopened cleanly twice (seq 21020, 29816) | PASS |
| 8 | In cluster: `kubectl delete pod --grace-period=0 --force` during a 50/s storm | new pod opens the store, gap, no loss | kubelet still sent SIGTERM: End `shutdown` at 08:55:00.566 (delete 08:55:00.439); new pod's Start 08:55:05.86; 86 resync, 15 gap-miss, LOST 0; verify equal | PASS |
| 9 | In cluster: `kill -9` of the container process from the node | End `crash-recovery` at the last heartbeat | exit 137; End `crash-recovery` 08:57:05.860 = heartbeat (kill at 08:57:14.31) | PASS |
| 10a | Disk full, 20/s for 60 s (96 MiB tmpfs filled with `dd`) | writes fail, coverage ends; or no claim beyond what is durable | Pebble kept accepting synced writes into its recycled WAL; only memtable flushes failed (241 `no space left on device`); all 1,199 writes recorded; LOST 0 | PASS |
| 10b | Disk full, 100/s for 120 s, then free the space | as above; recovery after space is freed | memtables filled about 88 s in, Pebble stalled writes, and the collector's stall rule fired 15 s later, but the End could not be persisted. **While full, the API answered `observed` with 3,621 changes missing.** After the space was freed: End `stall` at 09:03:06.157, Start 09:03:36.68, 300 resync, LOST 0, verify equal | **FAIL during the outage** (D2); recovery PASS. Re-test after ADR 0007: fail-stop (process exits, no stale answers); see D2 |
| 11 | Retention loop | runs every `--retention-interval` | `compacted` logged every 1 m (1.8–3.1 ms; nothing older than the 1 h minimum horizon); real removal tested on a store copy (12) and by `TestConcurrentWriteViewCompact -race` (pass) | PASS (loop confirmed only) |
| 12 | Compaction during reads (copy of the 75k-record scale store) | reads unaffected; answers at t ≥ horizon unchanged | Compact 73 ms, 24,289 records removed; 4 concurrent readers, 0 errors, p99 Get 0.56 ms during; 2,000/2,000 probes unchanged | PASS |

## Evidence

### 1. Host restart (in-cluster pod, incidental)

The host (WSL) restarted while chronos-0 was running. Pod log after the
restart:

```text
06:02:13.248Z INFO "store opened" ... seq=736 lastTime=2026-11-24T20:24:18.498Z heartbeat=2026-11-24T20:24:26.911Z
06:02:13.256Z WARN "coverage left open by an unclean stop; closing it" resource=v1/pods at=2026-11-24T20:24:26.911Z
06:02:45.085Z INFO "list applied" resource=v1/pods origin=resync objects=13 added=2 modified=11 deleted=2
```

The 32 s between the collector starting and the first `list applied`
was the apiserver coming back. Earlier that evening events had already
ended `relist` at 20:15:45 and restarted at 20:24:18, an 8.5-minute
gap. Its container log is missing, probably because the host was
suspended.

### 2. Control plane pause

```text
paused 2026-11-25T08:46:43.946Z .. 08:48:16.255Z; apiserver ready 0 s after unpause
14:17:29.950 "Warning: watch ended with error" ... "http2: client connection lost"   (all 7 reflectors, +46 s)
v1/configmaps  08:46:12.338 .. 08:46:44.083 relist
v1/configmaps  08:48:19.592 .. open
v1/pods        08:46:10.739 .. 08:46:10.739 relist      <- see D3
marker after-unpause created 08:48:16.634 -> {"observedAt":"08:48:19.592","type":"Added","origin":"resync"}
lag: recorded by watch=2377 resync=68 | not recorded: gap-miss=7 edge=0 LOST=0
verify: live=302 chronos=302 coverage=observed missing=0 extra=0 stale=0
```

The in-cluster Chronos was frozen together with the control plane. Its
coverage stayed continuous through the pause (06:18:09 .. 08:51:01),
which is correct: with etcd frozen, no change could happen.

### 3. Process freeze

```text
SIGSTOP 08:42:30.98Z, SIGCONT 08:44:34.87Z
v1/configmaps 08:41:59.300 .. open            (no End, no relist; all 7 types)
lag: recorded by watch=4195 resync=0 | LOST=0
lag: ObservedAt - client send  p50=16.6s p90=1m43s p99=2m1.8s max=2m3.87s
```

The wrong answer, from a copy of that store:

```text
truth (storm log): last write to cm-00031 completed before 08:43:30Z -> rv=145366
GET /api/v1/state?...name=cm-00031&at=2026-11-25T08:43:30Z
{"existence":"present","coverage":{"state":"observed",...},
 "version":{"rv":"144219","observedAt":"2026-11-25T08:42:28.401Z","origin":"watch"}}
GET /api/v1/diff?...from=08:42:35Z&to=08:44:30Z  -> {"complete":true,"diffEmpty":true}
```

No change is lost (LOST 0, verify equal), but every change in the
window is placed up to 2 minutes late, and the record presents that as
an observation.

### 7. SIGKILL during a storm

```text
SIGKILL 06:22:46.074Z
WARN "coverage left open by an unclean stop; closing it" resource=v1/configmaps at=2026-11-25T06:22:46.033705863Z
v1/configmaps 06:22:03.326 .. 06:22:46.033 crash-recovery
v1/configmaps 06:23:14.386 .. open
lag: recorded by watch=12435 resync=294 | not recorded: gap-miss=997 edge=0 LOST=0
```

The apiserver slowed down while Chronos relisted about 16.7k objects:
during this run the storm's patch latency reached p99 1.23 s and max
4.49 s.

### 10b. Disk full

```text
tmpfs 98304 98304 0 100% /data                      (09:01:28)
first unrecorded write sent 09:02:55.87Z            (Pebble write stall)
09:03:11.157 WARN "writer queue full for too long; ending coverage and relisting" resource=v1/configmaps
while full (09:03:33): v1/configmaps 09:01:27.317 .. open
   lag: recorded by watch=8378 | LOST=3621
   1869 ERROR background error: write /data/store/NNNNNN.sst: no space left on device
space freed 09:03:33.82Z
   v1/configmaps 09:01:27.317 .. 09:03:06.157 stall
   v1/configmaps 09:03:36.677 .. open
   lag: recorded by watch=9407 resync=300 | gap-miss=2292 LOST=0
   verify: live=301 chronos=301 missing=0 extra=0 stale=0
```

The process neither crashed nor panicked, and recovered by itself once
space was available. Changes received before the stall End were written
from the queue once space was freed. Changes after it were reconciled
by the relist.

## Defects

| ID | Severity | Component | Summary |
| --- | --- | --- | --- |
| D1 | high | collector (`internal/collector/source.go:165` `enqueue`, stamp at `:168`; `collector.go:309` `heartbeats`) | Late delivery is recorded as continuous observation |
| D2 | medium | query + collector + server (`internal/query/classify.go:17`, `internal/collector/writer.go` `declareLost`/`markLost`, `cmd/chronos/main.go:234`) | While the store cannot persist, the API answers `observed` with stale data |
| D3 | low (conservative, not wrong) | collector (`source.go:124` `advance`) | Idle resources lose claimed coverage back to their last event or bookmark at a relist |
| D4 | low | collector (`internal/collector/writer.go:551`) | `list applied` logs `coverageEnded=true` when the batch held no End |

### D1: late delivery recorded as continuous observation (high)

- Repro: `test/e2e/chaos.sh freeze` (SIGSTOP the process for 120 s with
  a 20/s storm running). Evidence is in section 3.
- Cause: `ObservedAt` is the receipt time (ADR 0002), and continuity is
  only lost on a relist, a stall of the writer queue or shutdown. When
  the process (or the watch path) stops but the stream survives, the
  apiserver delivers the backlog on resume. Every change is then
  stamped at resume time, and the interval is never ended. The
  heartbeat ticker resumes too, so nothing durable shows the hole.
  Kubernetes-side delivery stalls (slow apiserver watch cache, a
  throttled cgroup with a 1-CPU limit, VM pause or live migration)
  follow the same path.
- Impact: history is wrong permanently, not only during the event.
  `state` inside the window returns an older version as `observed`, and
  `diff` across it reports `complete:true` with no changes.
- Suggested fix (not applied: it changes collector semantics and needs
  an ADR amendment):
  1. A watchdog compares the monotonic time between heartbeat ticks
     and source progress. If a tick comes more than the stall timeout
     late, end coverage at the last on-time tick and force a relist,
     exactly as the stall rule does.
  2. Optionally, compare each received object's newest `managedFields`
     time (1 s resolution, available before normalization) with the
     receipt time, and treat a difference larger than the stall
     timeout the same way.

### D2: stale `observed` answers while the store cannot persist (medium)

- Repro: `DISK_RATE=100 DISK_SECONDS=120 test/e2e/chaos.sh disk-full`.
  Evidence is in section 10b.
- Cause: the collector decides "coverage lost" in memory (`markLost`),
  but the End it must write cannot be written while Pebble is stalled.
  The query engine classifies only from stored coverage
  (`classify.go:17`), and readiness only checks that the store is
  readable (`main.go:234`, by design per the 2026-11-25 review).
  Answers for recent times therefore keep saying `observed`. Here that
  lasted about 40 s; with a disk that stays full it would last
  indefinitely. History is correct once the End can be written.
- Also seen: Pebble logs `background error ... no space left on device`
  about 12 times per second (1,869 lines in about 2.5 min), and nothing
  besides logs signals the condition.
- Suggested fix: the server overlays the collector's live status
  (`Collector.Status`: `lost`, last durable heartbeat) on the stored
  coverage and reports `gap` after the last durable write or heartbeat
  plus a bound. Also expose store write failures as a metric or health
  detail.

**Re-test 2026-11-25 (after ADR 0007 and the round-2 store fixes).**
`DISK_RATE=100 DISK_SECONDS=120 test/e2e/chaos.sh disk-full`. The
behaviour changed: when the tmpfs filled, Pebble failed a commit after
its commit point (`write .../000005.log: no space left on device`). That
outcome is ambiguous, so the store poisoned itself, the collector
stopped with `store closed`, and the process exited 1 about 3 s into the
storm. No query was answered while the store could not write, so no
stale `observed` answer was served: **no longer FAIL on correctness.**
In Kubernetes the pod restarts; once space is available the restart
writes a crash-recovery End and relists (covered by the crash scenarios).

The cost is availability: while the disk stays full the API is down,
although history before the loss is still valid. A degraded read-only
mode (keep serving reads from the poisoned store, with the ADR 0007
overlay marking everything after the loss as `gap`) would keep it
readable; it is on the backlog, not implemented. The live coverage
overlay still covers non-ambiguous write failures, where the collector
retries instead of the store poisoning itself.

### D3: idle resources' End placed at their last event (low)

During the pause, `v1/pods` (no changes since its LIST) ended at
08:46:10.739, the same instant as its Start, although the watch was
healthy until about 08:47:30. `contUntil` only advances on events,
bookmarks (about one a minute) and LISTs. This errs in the safe
direction (it shows a longer gap than necessary), so it is acceptable
under ADR 0004. It is noted so a zero-length interval in `/resources`
is not mistaken for a bug.

### D4: misleading `coverageEnded` log field (low)

`writer.go:551` logs `coverageEnded = entries[0].Coverage != nil`.
Every batch ends with the `Start`, so a relist with no diff records and
no End logs `true`. For example, after a crash recovery the End was
written by `recover`, not by this batch:

```text
level=INFO msg="list applied" resource=v1/services origin=resync objects=4 added=0 modified=0 deleted=0 coverageEnded=true
```

The fix is a one-liner (`entries[0].Coverage.Type == model.CoverageEnd`).
I left it alone because the brief requires a regression test for any
product change, and the right place for that test is the collector
workstream's log assertions.

## Harness issues found and fixed (test code only)

- `kubectl wait` raced the StatefulSet recreating chronos-0. The script
  now waits for the new UID first.
- Backgrounding a shell function (`perf storm &`, `k port-forward &`)
  started a subshell, so the trap killed the subshell and left the child
  running. One orphaned storm corrupted a CSV. Both now background the
  binary itself.
- The disk-full log summary died on SIGPIPE under `pipefail`, and the
  trap then removed the container. Fixed.

## Not tested, and why

- **A network partition only between Chronos and the apiserver.** It
  needs root (iptables/tc), and there is no sudo here. The SIGSTOP
  freeze and the control-plane pause cover the two ends of that
  behaviour.
- **A 410 Gone (expired resourceVersion) on watch resume.** The pause
  produced a relist through HTTP/2 connection loss, and the
  stop/restart scenarios exercise the resync diff. Forcing the
  apiserver's watch cache to expire a resourceVersion was not attempted.
- **Retention removing real history in the live process.** The minimum
  `--retention` is 1 h and the data was less than 1 h old. Removal was
  tested on a copy of the real store (scenario 12) and by the store's
  own tests.
- **`/readyz` during disk full.** Not asserted. The code says it stays
  200 while the store is readable.
- **Long soak (hours or days), memory-limit/OOM behaviour, etcd
  compaction effects, and multiple nodes.** Out of the time budget, and
  kind has a single node.
- **The in-cluster pod during the pause.** It was frozen with the
  control plane, so only its coverage was checked.
