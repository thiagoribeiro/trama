#!/usr/bin/env bash
# 3x. Reproduces the Redis pool deadlock: default pool (16), 64 concurrent submitters.
# Expected (bug): requests time out and the process stops answering /healthz.
source "$(dirname "$0")/../lib.sh"
export REDIS_POOL=16
scenario_begin s3x-pool-deadlock rdb 1 4
drive --def=chain --count=200 --concurrency=64
code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/healthz" || true)
echo "healthz_after_load=$code" | tee -a "$RESULTS/timeline.txt"
jstack "$(awk '$1==0 {print $2}' "$RUN_DIR/pids")" > "$RESULTS/jstack.txt" 2>&1 || true
scenario_end 10
