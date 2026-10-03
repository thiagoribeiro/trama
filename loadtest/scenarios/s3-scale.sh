#!/usr/bin/env bash
# 3. Throughput/latency vs number of worker processes. Usage: s3-scale.sh "1 2 4 8" [workerCount]
# Closed loop: 3000 chain workflows (20ms nodes) as fast as 128 concurrent submitters allow.
source "$(dirname "$0")/../lib.sh"
for P in ${1:-1 2 4 8}; do
  scenario_begin "s3-scale-p$P-w${2:-8}" rdb "$P" "${2:-8}"
  harness pgtop --reset=true >/dev/null 2>&1 || true
  drive --def=chain --count=3000 --concurrency=128
  scenario_end 300
done
