#!/usr/bin/env bash
# 1c. Redis loses everything (FLUSHALL) while executions are parked in each state.
# Produces the reconstructibility matrix (statusByDef in summary.json).
# Enough workers that every execution actually reaches its parked state before the flush
# (otherwise claimed-but-unstarted work sits in the processes' in-memory buffers instead).
source "$(dirname "$0")/../lib.sh"
HOLD="${HOLD:-60}"
scenario_begin s1c-redis-data-loss rdb 2 64
harness park --perState=20 --holdSec="$HOLD" --out="$RUN_DIR/runs.csv" | tee -a "$RESULTS/driver.log"
at 12
harness check --runs="$RUN_DIR/runs.csv" --waitSec=0 --out="$RESULTS/before-flush.json" >/dev/null 2>&1 || true
mock_stats > "$RESULTS/mock-before-flush.json"
"$LT_DIR/stack.sh" redis-flush; echo "flush_at_s=$(elapsed) hold_s=$HOLD" >> "$RESULTS/timeline.txt"
# Give every parked state time to resume normally (hold + callback timeout + scanners).
scenario_end $(( HOLD * 3 + 120 ))
