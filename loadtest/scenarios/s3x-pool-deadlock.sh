#!/usr/bin/env bash
# 3x. Redis under concurrency with default settings: 64 concurrent submitters on one process.
# v2.0.1 deadlocked here (pool of 16 borrowed from coroutine threads): every request timed out and
# /healthz stopped answering. Expected now: all 200 accepted, /healthz 200.
source "$(dirname "$0")/../lib.sh"
scenario_begin s3x-pool-deadlock rdb 1 4
drive --def=chain --count=200 --concurrency=64
code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/healthz" || true)
echo "healthz_after_load=$code" | tee -a "$RESULTS/timeline.txt"
jstack "$(awk '$1==0 {print $2}' "$RUN_DIR/pids")" > "$RESULTS/jstack.txt" 2>&1 || true
scenario_end 10
