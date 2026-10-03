#!/usr/bin/env bash
# Shared settings for the validation harness scripts. Source it; do not run it.
set -euo pipefail

LT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LT_DIR/.." && pwd)"
RUN_DIR="${RUN_DIR:-$LT_DIR/run}"
mkdir -p "$RUN_DIR"

# Real service ports (containers run with host networking) and the Toxiproxy front ports the
# Trama processes use, so faults can be injected between Trama and its dependencies.
PG_PORT=55433;    PG_PROXY_PORT=55432
REDIS_PORT=56380; REDIS_PROXY_PORT=56379
TOXI_API=http://127.0.0.1:8474
MOCK_PORT=7070
API_PORT=9100     # process 0: receives runs and callbacks; never killed by scenarios

cp_file="$ROOT/build/loadtest.classpath"
harness() { java -cp "$(cat "$cp_file")" run.trama.loadtest.MainKt "$@"; }
# For background use: `(harness_exec cmd ...) &` makes $! the JVM's own PID, so it can be killed.
harness_exec() { exec java -cp "$(cat "$cp_file")" run.trama.loadtest.MainKt "$@"; }

ts() { date +%H:%M:%S; }
say() { echo "[$(ts)] $*"; }

# ── Scenario helpers ──────────────────────────────────────────────────────────
# scenario_begin <name> <redis persistence> <processes> [workerCount]
# Fresh stack + mock + processes + background collector; results go to results/<name>/.
scenario_begin() {
  SCENARIO="$1"
  # RESULTS_SET groups a full run (one per Trama version); results/v2.0.1 holds the first one.
  RESULTS="$LT_DIR/results/${RESULTS_SET:-v2.1.0}/$SCENARIO"
  export RUN_DIR="$LT_DIR/run/$SCENARIO"
  rm -rf "$RESULTS" "$RUN_DIR"; mkdir -p "$RESULTS" "$RUN_DIR"
  "$LT_DIR/trama.sh" stop >/dev/null 2>&1 || true
  "$LT_DIR/trama.sh" mock-stop >/dev/null 2>&1 || true
  "$LT_DIR/stack.sh" down >/dev/null
  "$LT_DIR/stack.sh" up "$2"
  "$LT_DIR/trama.sh" mock-start
  "$LT_DIR/trama.sh" start "$3" "${4:-4}"
  (harness_exec collect --pids="$RUN_DIR/pids" --out="$RESULTS/metrics.csv" > "$RUN_DIR/collector.log" 2>&1) &
  COLLECTOR_PID=$!
  T0=$(date +%s)
  say "scenario $SCENARIO started"
}

elapsed() { echo $(( $(date +%s) - T0 )); }
at() { local wait=$(( $1 - $(elapsed) )); [ "$wait" -gt 0 ] && sleep "$wait"; return 0; }
drive() { harness drive --out="$RUN_DIR/runs.csv" "$@" 2>&1 | { grep -v '] submitted [0-9]' || true; } | tee -a "$RESULTS/driver.log"; }
mock_stats() { curl -s "http://127.0.0.1:$MOCK_PORT/stats"; }

# scenario_end [checker waitSec]: check outcomes, keep summaries, tear everything down.
scenario_end() {
  harness check --runs="$RUN_DIR/runs.csv" --waitSec="${1:-180}" --out="$RESULTS/summary.json" 2>&1 \
    | { grep -v '] waiting: ' || true; } | tee "$RESULTS/check.log"
  mock_stats > "$RESULTS/mock-stats.json"
  harness pgtop > "$RESULTS/pgtop.tsv" 2>&1 || true
  podman exec lt-redis redis-cli -p $REDIS_PORT INFO commandstats > "$RESULTS/redis-commandstats.txt" 2>/dev/null || true
  cp "$RUN_DIR/runs.csv" "$RESULTS/" 2>/dev/null || true
  for log in "$RUN_DIR"/trama-*.log; do
    grep -hE '"level":"(WARN|ERROR)"' "$log" | head -200 > "$RESULTS/$(basename "$log" .log)-warnings.log" || true
  done
  kill "$COLLECTOR_PID" 2>/dev/null || true
  "$LT_DIR/trama.sh" stop >/dev/null
  "$LT_DIR/trama.sh" mock-stop >/dev/null
  "$LT_DIR/stack.sh" down >/dev/null
  say "scenario $SCENARIO done → $RESULTS"
}
