#!/usr/bin/env bash
# Chaos / reliability scenarios against the kind cluster chronos-dev.
#
#   WORK=/path/to/scratch test/e2e/chaos.sh <scenario>
#
# Scenarios (each prints its evidence; the verdicts are in
# docs/reliability.md):
#   apiserver-pause   docker pause the control plane PAUSE (90) seconds
#                     during a storm, then unpause
#   freeze            SIGSTOP the local chronos FREEZE (120) s while
#                     changes continue, then SIGCONT
#   restart-gap       graceful stop, change objects, restart
#   crash             SIGKILL the local chronos during a 100/s storm
#   incluster-kill    kubectl delete pod --force on chronos-0 during a storm
#   burst             100 back-to-back updates of one object; 20 delete +
#                     recreate cycles of one name
#   disk-full         chronos in a container with a size-limited tmpfs;
#                     fill it during a storm, then free it
#   compaction        Compact on a copy of the scale store with concurrent
#                     readers (needs $WORK/../scale/scale.data.copy), and
#                     the store's own concurrency tests
#
# Every scenario uses the perf-chaos-0 namespace (300 ConfigMaps) for its
# writes and deletes it at the end. The control plane is always unpaused
# by a trap.
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

SCENARIO=${1:?scenario}
NCM=${NCM:-300}
chaos_dims=(-prefix perf-chaos -namespaces 1 -configmaps "$NCM" -deployments 0)
STORM_PID=
PF_PID=

cleanup() {
  docker unpause "$NODE" >/dev/null 2>&1 || true
  [[ -n "$STORM_PID" ]] && kill "$STORM_PID" 2>/dev/null || true
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
  if [[ -n "${CHRONOS_PID:-}" ]]; then
    kill -CONT "$CHRONOS_PID" 2>/dev/null || true
    stop_chronos
  fi
  docker rm -f chronos-diskfull >/dev/null 2>&1 || true
  [[ -n "${KEEP:-}" ]] || k delete ns perf-chaos-0 perf-burst --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

setup_ns() {
  perf populate "${chaos_dims[@]}" -pods 0 >/dev/null
}

# storm_bg RATE SECONDS CSV: storm on the chaos namespace in the background.
storm_bg() {
  perf storm "${chaos_dims[@]}" -rate "$1" -duration "$2s" -out "$3" >"$3.out" 2>&1 &
  STORM_PID=$!
}
storm_wait() { wait "$STORM_PID" || true; STORM_PID=; tail -2 "$1.out"; }

apiserver_ready() {
  local t0
  t0=$(date +%s)
  until k get --raw /readyz >/dev/null 2>&1; do
    (($(date +%s) - t0 > 180)) && { echo "apiserver not ready after 180s" >&2; return 1; }
    sleep 0.5
  done
  echo "apiserver ready $(($(date +%s) - t0)) s after unpause"
}

warnings() { # warnings LOG: collector warnings/errors and list applications
  grep -E '"level":"(WARN|ERROR)"|"list applied"|forcing relist' "$1" | grep -v gctrace |
    jq -c '{time, level, msg, resource, err, at, lostAt, objects, added, modified, deleted}' 2>/dev/null | tail -n "${2:-40}"
}

fresh_local() { # fresh_local NAME [flags]: empty store, started and synced
  rm -rf "${WORK:?}/$1.data" "$WORK/$1.log"
  start_chronos "$@"
  SYNC_MARK=0
  wait_lists 7 "$WORK/$1.log" 300
}

scenario_apiserver_pause() {
  local pause=${PAUSE:-90} csv=$WORK/pause.csv
  setup_ns
  fresh_local pause
  storm_bg 20 $((pause + 120)) "$csv"
  sleep 30
  log "docker pause $NODE for ${pause}s"
  local tp tu
  tp=$(date -u +%FT%T.%NZ)
  docker pause "$NODE" >/dev/null
  sleep "$pause"
  docker unpause "$NODE" >/dev/null
  tu=$(date -u +%FT%T.%NZ)
  log "unpaused (paused $tp .. $tu)"
  apiserver_ready
  local tm
  tm=$(date -u +%FT%T.%NZ)
  k -n perf-chaos-0 create configmap after-unpause --from-literal=k=v >/dev/null
  echo "marker ConfigMap after-unpause created at $tm"
  storm_wait "$csv"
  sleep 5
  log "coverage (local chronos)"
  coverage
  log "marker history"
  curl -fsS "$SERVER/api/v1/history?resource=v1/configmaps&namespace=perf-chaos-0&name=after-unpause" | jq -c '.versions[] | {observedAt, type, origin}'
  log "lag / loss"
  perf_nokube lag -server "$SERVER" -in "$csv" || echo "LAG CHECK FAILED"
  log "verify now-state"
  perf verify -server "$SERVER" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  log "collector log"
  warnings "$WORK/pause.log"
}

scenario_freeze() {
  local freeze=${FREEZE:-120} csv=$WORK/freeze.csv
  setup_ns
  fresh_local freeze
  storm_bg 20 $((freeze + 90)) "$csv"
  sleep 30
  log "SIGSTOP chronos for ${freeze}s while the storm continues"
  date -u +%FT%T.%NZ
  kill -STOP "$CHRONOS_PID"
  sleep "$freeze"
  kill -CONT "$CHRONOS_PID"
  date -u +%FT%T.%NZ
  storm_wait "$csv"
  sleep 5
  coverage
  perf_nokube lag -server "$SERVER" -in "$csv" || echo "LAG CHECK FAILED"
  perf verify -server "$SERVER" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  warnings "$WORK/freeze.log"
}

scenario_restart_gap() {
  local csv=$WORK/restart.csv
  setup_ns
  fresh_local restart
  storm_bg 20 120 "$csv"
  sleep 30
  log "graceful stop (SIGTERM)"
  stop_chronos
  date -u +%FT%T.%NZ
  log "while stopped: delete cm-00001, create gap-new, the storm keeps patching"
  k -n perf-chaos-0 delete configmap cm-00001 >/dev/null
  k -n perf-chaos-0 create configmap gap-new --from-literal=k=v >/dev/null
  sleep 30
  log "restart"
  mark_log "$WORK/restart.log"
  start_chronos restart
  wait_lists 7 "$WORK/restart.log" 300
  storm_wait "$csv"
  sleep 5
  coverage v1/configmaps
  for n in cm-00001 gap-new; do
    echo "history $n:"
    curl -fsS "$SERVER/api/v1/history?resource=v1/configmaps&namespace=perf-chaos-0&name=$n" | jq -c '.versions[] | {observedAt, type, origin}' | tail -3
  done
  perf_nokube lag -server "$SERVER" -in "$csv" || echo "LAG CHECK FAILED"
  perf verify -server "$SERVER" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  warnings "$WORK/restart.log"
}

scenario_crash() {
  local csv=$WORK/crash.csv
  setup_ns
  fresh_local crash
  storm_bg 100 150 "$csv"
  sleep 40
  log "SIGKILL chronos during the storm"
  date -u +%FT%T.%NZ
  kill9_chronos
  sleep 20
  log "restart on the same store"
  mark_log "$WORK/crash.log"
  start_chronos crash
  wait_lists 7 "$WORK/crash.log" 300
  storm_wait "$csv"
  sleep 5
  coverage v1/configmaps
  perf_nokube lag -server "$SERVER" -in "$csv" || echo "LAG CHECK FAILED"
  perf verify -server "$SERVER" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  warnings "$WORK/crash.log" 20
  log "second SIGKILL with no load, reopen (store must open cleanly twice)"
  kill9_chronos
  mark_log "$WORK/crash.log"
  start_chronos crash
  wait_lists 7 "$WORK/crash.log" 300
  grep -E 'store opened|corrupt' "$WORK/crash.log" | tail -2 | jq -c '{time, msg, seq, heartbeat, err}'
}

pf_start() { # port-forward the in-cluster service to $PORT
  k -n chronos port-forward svc/chronos "$PORT:http" >"$WORK/pf.log" 2>&1 &
  PF_PID=$!
  for _ in $(seq 50); do curl -fsS "$SERVER/readyz" >/dev/null 2>&1 && return 0; sleep 0.2; done
  echo "port-forward not ready" >&2; return 1
}
pf_stop() { [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true; wait "$PF_PID" 2>/dev/null || true; PF_PID=; }

scenario_incluster_kill() {
  local csv=$WORK/incluster.csv
  setup_ns
  storm_bg 50 150 "$csv"
  sleep 40
  log "kubectl delete pod chronos-0 --grace-period=0 --force"
  date -u +%FT%T.%NZ
  k -n chronos delete pod chronos-0 --grace-period=0 --force 2>&1 | tail -1
  k -n chronos wait --for=condition=Ready pod/chronos-0 --timeout=180s
  date -u +%FT%T.%NZ
  storm_wait "$csv"
  sleep 5
  pf_start
  coverage v1/configmaps | tail -3
  perf_nokube lag -server "$SERVER" -in "$csv" || echo "LAG CHECK FAILED"
  perf verify -server "$SERVER" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  pf_stop
  log "pod log after restart"
  k -n chronos logs chronos-0 | grep -vE 'http request' | grep -E 'WARN|ERROR|list applied|store opened' | tail -12
}

scenario_burst() {
  fresh_local burst
  log "100 back-to-back updates of one ConfigMap"
  perf burst -server "$SERVER" -n 100 || echo "BURST FAILED"
  log "20 delete+recreate cycles"
  perf recreate -server "$SERVER" -n 20 || echo "RECREATE FAILED"
  curl -fsS "$SERVER/api/v1/history?resource=v1/configmaps&namespace=perf-burst&name=phoenix" |
    jq -r '.versions[-6:][] | "\(.observedAt) \(.type) \(.origin) uid=\(.uid[0:8])"'
}

scenario_disk_full() {
  local port=$((PORT + 1)) csv=$WORK/diskfull.csv size=${TMPFS_SIZE:-96m}
  local srv=http://127.0.0.1:$port
  setup_ns
  (cd "$E2E_ROOT" && CGO_ENABLED=0 go build -o bin/chronos-static ./cmd/chronos)
  log "chronos in a container, data on a ${size} tmpfs"
  docker rm -f chronos-diskfull >/dev/null 2>&1 || true
  docker run -d --name chronos-diskfull --network host --user "$(id -u):$(id -g)" \
    --tmpfs "/data:rw,size=$size,mode=1777" \
    -v "$E2E_ROOT/bin/chronos-static:/chronos:ro" -v "$KUBECONFIG_FILE:/kubeconfig:ro" \
    golang:1.26.11-alpine /chronos --kubeconfig /kubeconfig --data-dir /data/store \
    --listen "127.0.0.1:$port" --log-format json >/dev/null
  for _ in $(seq 300); do
    (($(docker logs chronos-diskfull 2>&1 | grep -c '"list applied"') >= 7)) && break
    sleep 0.5
  done
  docker exec chronos-diskfull df -k /data | tail -1
  log "fill the tmpfs"
  docker exec chronos-diskfull sh -c 'dd if=/dev/zero of=/data/filler bs=64k 2>&1 | tail -1; df -k /data | tail -1'
  date -u +%FT%T.%NZ
  storm_bg 20 60 "$csv"
  storm_wait "$csv"
  log "while full: logs, coverage, and what queries say"
  docker logs chronos-diskfull 2>&1 | grep -E '"level":"(WARN|ERROR)"|panic|fatal' | cut -c1-400 | sort | uniq -c | sort -rn | head -12
  docker ps -a --filter name=chronos-diskfull --format '{{.Status}}'
  if curl -fsS "$srv/healthz" >/dev/null 2>&1; then
    SERVER=$srv coverage v1/configmaps
    perf_nokube lag -server "$srv" -in "$csv" | grep -E 'recorded|LOST' || true
  else
    echo "server not answering"
  fi
  log "free the space"
  docker exec chronos-diskfull rm -f /data/filler 2>/dev/null || echo "container gone; restarting it on the same tmpfs is impossible (tmpfs is lost)"
  date -u +%FT%T.%NZ
  sleep 45
  docker ps -a --filter name=chronos-diskfull --format '{{.Status}}'
  if curl -fsS "$srv/healthz" >/dev/null 2>&1; then
    SERVER=$srv coverage v1/configmaps
    perf_nokube lag -server "$srv" -in "$csv" || echo "LAG CHECK FAILED"
    perf verify -server "$srv" -namespace perf-chaos-0 || echo "VERIFY FAILED"
  fi
  docker logs chronos-diskfull >"$WORK/diskfull.log" 2>&1 || true
  warnings "$WORK/diskfull.log" 25
}

scenario_compaction() {
  local copy=${STORE_COPY:-$WORK/../scale/scale.data.copy}
  [[ -d "$copy" ]] || { echo "no store copy at $copy (run scale.sh first)"; return 1; }
  rm -rf "$WORK/compact.data"
  cp -a "$copy" "$WORK/compact.data"
  log "Compact(midpoint) on a copy of the scale store, 4 concurrent readers"
  perf_nokube compact -data-dir "$WORK/compact.data" -readers 4
  log "store concurrency/compaction tests"
  (cd "$E2E_ROOT" && go test -count=1 -race -run 'Concurrent|Compact' ./internal/store/... 2>&1 | tail -5)
}

build
check_kubeconfig_context
log "scenario $SCENARIO"
"scenario_${SCENARIO//-/_}"
