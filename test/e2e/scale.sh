#!/usr/bin/env bash
# End-to-end scale run: populate ~10k objects, start a local Chronos on an
# empty store, measure initial sync, memory and CPU, run update storms,
# join them with Chronos history (collection lag, losses), measure API
# latency and store size, and time a warm restart.
#
#   WORK=/path/to/scratch test/e2e/scale.sh
#
# Knobs: CONFIGMAPS PODS DEPLOYMENTS NAMESPACES RATES ("100 200")
#        STORM_DURATION (3m) SKIP_POPULATE=1 KEEP=1 (leave perf-* namespaces)
# Output goes to stdout; tee it. Pending Pods use a nodeSelector no node
# matches, so the dataset costs apiserver/etcd memory but no CPU.
# shellcheck source=SCRIPTDIR/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

CONFIGMAPS=${CONFIGMAPS:-7000}
PODS=${PODS:-2000}
DEPLOYMENTS=${DEPLOYMENTS:-500}
NAMESPACES=${NAMESPACES:-10}
RATES=${RATES:-"100 200"}
STORM_DURATION=${STORM_DURATION:-3m}
dims=(-namespaces "$NAMESPACES" -configmaps "$CONFIGMAPS" -deployments "$DEPLOYMENTS")

cleanup() {
  stop_sampler
  stop_chronos
  if [[ -z "${KEEP:-}" ]]; then perf cleanup || true; fi
}
trap cleanup EXIT

build
check_kubeconfig_context

if [[ -z "${SKIP_POPULATE:-}" ]]; then
  log "populate: $CONFIGMAPS ConfigMaps, $PODS Pods, $DEPLOYMENTS Deployments in $NAMESPACES namespaces"
  perf populate "${dims[@]}" -pods "$PODS"
  log "waiting for the Deployment controller (ReplicaSets + Pods)"
  for _ in $(seq 120); do
    n=$(k get pods -A -l app=perf-deploy --no-headers 2>/dev/null | wc -l)
    ((n >= DEPLOYMENTS)) && break
    sleep 2
  done
fi
log "dataset in the cluster"
for r in configmaps pods deployments replicasets events secrets services; do
  printf '%-12s %s\n' "$r" "$(k get "$r" -A --no-headers 2>/dev/null | wc -l)"
done

rm -rf "$WORK/scale.data" "$WORK/scale.log"
log "initial sync on an empty store"
start_chronos scale --retention-interval 1m
sample_proc "$CHRONOS_PID" "$WORK/scale.proc.csv" 1
SYNC_MARK=0
wait_lists 7 "$WORK/scale.log" 600
grep '"list applied"' "$WORK/scale.log" | jq -c '{resource, objects, origin, time}'
sleep 20
log "after sync + 20s idle"
summarize_proc "$WORK/scale.proc.csv" "$(date -d '-15 sec' +%s)"
heap_from_gctrace "$WORK/scale.log"
echo "store bytes: $(store_bytes "$WORK/scale.data")"

for rate in $RATES; do
  log "storm at $rate/s for $STORM_DURATION"
  before=$(store_bytes "$WORK/scale.data")
  t0=$(date +%s)
  perf storm "${dims[@]}" -rate "$rate" -duration "$STORM_DURATION" -out "$WORK/storm-$rate.csv"
  t1=$(date +%s)
  sleep 5
  after=$(store_bytes "$WORK/scale.data")
  ok=$(awk -F, 'NR>1 && $8=="" {n++} END {print n+0}' "$WORK/storm-$rate.csv")
  echo "store bytes before=$before after=$after delta=$((after - before)) ok-writes=$ok (delta/write $(( (after - before) / (ok > 0 ? ok : 1) )) B, includes WAL and unflushed memtable)"
  log "process during the $rate/s storm"
  summarize_proc "$WORK/scale.proc.csv" "$t0" "$t1"
  log "collection lag and loss ($rate/s)"
  perf_nokube lag -server "$SERVER" -in "$WORK/storm-$rate.csv" || echo "LAG CHECK FAILED"
done

log "API latency at scale (sequential)"
perf_nokube bench -server "$SERVER" -namespaces "$NAMESPACES" -configmaps "$CONFIGMAPS" -iterations 200 -full-iterations 5
log "API latency at scale (8 concurrent clients)"
perf_nokube bench -server "$SERVER" -namespaces "$NAMESPACES" -configmaps "$CONFIGMAPS" -iterations 400 -full-iterations 8 -concurrency 8 \
  -only state-now,history,diff,list-all-page500-cm,list-full-cm-paged500

log "retention loop (horizon is 1h back; nothing old enough to remove)"
grep -E 'retention compaction|"msg":"compacted"' "$WORK/scale.log" | tail -3 | jq -c '{time, msg, took, duration, recordsRemoved}'

log "process totals for the run"
summarize_proc "$WORK/scale.proc.csv"
heap_from_gctrace "$WORK/scale.log"

log "graceful stop, then warm restart on the populated store"
stop_chronos
stop_sampler
echo "store bytes after stop: $(store_bytes "$WORK/scale.data")"
mark_log "$WORK/scale.log"
start_chronos scale --retention-interval 1m
sample_proc "$CHRONOS_PID" "$WORK/scale.restart.proc.csv" 1
wait_lists 7 "$WORK/scale.log" 600
tail -n +"$((SYNC_MARK + 1))" "$WORK/scale.log" | grep '"list applied"' | jq -c '{resource, objects, added, modified, deleted}'
sleep 10
summarize_proc "$WORK/scale.restart.proc.csv"
coverage v1/configmaps
stop_chronos
stop_sampler
cp -a "$WORK/scale.data" "$WORK/scale.data.copy"
echo "store copy for the compaction test: $WORK/scale.data.copy"
