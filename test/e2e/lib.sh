# shellcheck shell=bash
# Shared helpers for the Chronos e2e scale and chaos scripts. Source it.
#
# Every kubectl call passes the kubeconfig and context explicitly; the
# current context and other clusters are never touched.
#
# Environment:
#   KUBECONFIG_FILE  (~/.kube/chronos-dev.kubeconfig)
#   CONTEXT          (kind-chronos-dev)
#   NODE             kind control-plane container (chronos-dev-control-plane)
#   WORK             scratch dir for data dirs, logs, CSVs (required)
#   PORT             local chronos HTTP port (18080)

set -euo pipefail

E2E_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
KUBECONFIG_FILE=${KUBECONFIG_FILE:-$HOME/.kube/chronos-dev.kubeconfig}
CONTEXT=${CONTEXT:-kind-chronos-dev}
NODE=${NODE:-chronos-dev-control-plane}
PORT=${PORT:-18080}
: "${WORK:?set WORK to a scratch directory}"
mkdir -p "$WORK"

CHRONOS_BIN=$E2E_ROOT/bin/chronos
PERF_BIN=$E2E_ROOT/bin/chronosperf
SERVER=http://127.0.0.1:$PORT

log() { printf '\n==> [%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
k() { kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" "$@"; }
perf() { "$PERF_BIN" "$1" -kubeconfig "$KUBECONFIG_FILE" -context "$CONTEXT" "${@:2}"; }
perf_nokube() { "$PERF_BIN" "$@"; }

build() {
  (cd "$E2E_ROOT" && go build -o bin/chronos ./cmd/chronos &&
    go build -tags e2e -o bin/chronosperf ./test/e2e/chronosperf)
}

# start_chronos NAME [extra flags...]: runs chronos in the background
# with data dir $WORK/NAME.data, log $WORK/NAME.log, pid in CHRONOS_PID.
# GODEBUG=gctrace=1 makes the runtime print heap sizes after every GC
# (to the log), which gives heap numbers without adding pprof.
start_chronos() {
  local name=$1; shift
  mkdir -p "$WORK/$name.data"
  GODEBUG=gctrace=1 "$CHRONOS_BIN" --kubeconfig "$KUBECONFIG_FILE" \
    --data-dir "$WORK/$name.data" --listen "127.0.0.1:$PORT" \
    --log-format json --kube-api-qps 50 --kube-api-burst 100 "$@" \
    >>"$WORK/$name.log" 2>&1 &
  CHRONOS_PID=$!
  CHRONOS_START_NS=$(date +%s%N)
  echo "chronos pid $CHRONOS_PID (log $WORK/$name.log)"
}

# The kubeconfig's current-context is kind-chronos-dev; chronos has no
# --context flag, so refuse to run if that ever changes.
check_kubeconfig_context() {
  local cur
  cur=$(kubectl --kubeconfig "$KUBECONFIG_FILE" config current-context)
  [[ "$cur" == "$CONTEXT" ]] || { echo "kubeconfig current-context is $cur, not $CONTEXT" >&2; exit 1; }
}

stop_chronos() { # graceful
  [[ -n "${CHRONOS_PID:-}" ]] || return 0
  kill -TERM "$CHRONOS_PID" 2>/dev/null || true
  wait "$CHRONOS_PID" 2>/dev/null || true
  CHRONOS_PID=
}

kill9_chronos() {
  [[ -n "${CHRONOS_PID:-}" ]] || return 0
  kill -KILL "$CHRONOS_PID" 2>/dev/null || true
  wait "$CHRONOS_PID" 2>/dev/null || true
  CHRONOS_PID=
}

# wait_synced N LOGFILE [timeout_s]: wait until N "list applied" lines
# appeared since the last start (counts lines after the log's size at
# start, kept in SYNC_MARK). Prints the elapsed time since start_chronos.
mark_log() { SYNC_MARK=$(wc -l <"$1"); }
wait_lists() {
  local want=$1 file=$2 timeout=${3:-300} t0 n
  t0=$(date +%s)
  while :; do
    n=$(tail -n +"$((SYNC_MARK + 1))" "$file" | grep -c '"list applied"' || true)
    ((n >= want)) && break
    if (($(date +%s) - t0 > timeout)); then echo "timeout waiting for $want lists (have $n)" >&2; return 1; fi
    sleep 0.2
  done
  echo "synced: $n lists applied, $(( ($(date +%s%N) - CHRONOS_START_NS) / 1000000 )) ms after start"
}

# sample_proc PID OUT INTERVAL: background sampler of RSS/HWM (kB) and
# cumulative CPU (ticks) from /proc. Kill with stop_sampler.
sample_proc() {
  local pid=$1 out=$2 iv=${3:-1}
  echo "ts,rss_kb,hwm_kb,utime,stime" >"$out"
  (
    while [[ -d /proc/$pid ]]; do
      local rss hwm ut st
      rss=$(awk '/^VmRSS/{print $2}' "/proc/$pid/status" 2>/dev/null) || break
      hwm=$(awk '/^VmHWM/{print $2}' "/proc/$pid/status" 2>/dev/null) || break
      read -r ut st < <(awk '{print $14, $15}' "/proc/$pid/stat" 2>/dev/null) || break
      echo "$(date +%s.%N),$rss,$hwm,$ut,$st" >>"$out"
      sleep "$iv"
    done
  ) &
  SAMPLER_PID=$!
}
stop_sampler() { [[ -n "${SAMPLER_PID:-}" ]] && kill "$SAMPLER_PID" 2>/dev/null || true; SAMPLER_PID=; }

# summarize_proc OUT [from_ts] [to_ts]: peak/avg RSS and CPU% over a window.
summarize_proc() {
  local out=$1 from=${2:-0} to=${3:-9999999999}
  awk -F, -v from="$from" -v to="$to" -v hz="$(getconf CLK_TCK)" '
    NR>1 && $1>=from && $1<=to {
      if (!n) { t0=$1; c0=$4+$5 } n++; t1=$1; c1=$4+$5
      s+=$2; if ($2>m) m=$2; h=$3
    }
    END { if (n<2) { print "no samples"; exit }
      printf "samples=%d window=%.0fs rss_avg=%.1fMiB rss_peak=%.1fMiB hwm=%.1fMiB cpu_avg=%.1f%% (of one core)\n",
        n, t1-t0, s/n/1024, m/1024, h/1024, 100*(c1-c0)/hz/(t1-t0) }' "$out"
}

# heap_from_gctrace LOG: last and peak live heap from gctrace lines
# ("gc N @... X->Y->Z MB, W MB goal").
heap_from_gctrace() {
  grep -oE '[0-9]+->[0-9]+->[0-9]+ MB' "$1" | awk -F'[- >]+' '
    { if ($1>peakh) peakh=$1; live=$3; if ($3>peakl) peakl=$3; n++ }
    END { printf "gc cycles=%d heap_peak_at_gc_start=%dMB live_heap_peak=%dMB live_heap_last=%dMB\n", n, peakh, peakl, live }'
}

coverage() { # coverage [resource-id]: print intervals from the local server
  curl -fsS "$SERVER/api/v1/resources" | jq -r --arg id "${1:-}" '
    .resources[] | select($id == "" or .id == $id) | .id as $i | .coverage[] |
    "\($i)\t\(.start) .. \(.end // "open") \(.endReason // "")"'
}

store_bytes() { du -sb "$1" | cut -f1; }
