#!/usr/bin/env bash
# 2a. SIGKILL a worker while its HTTP calls are in flight; another worker must take over after
# the claim lease. 3 processes; 60 chain workflows whose nodes take 6s.
source "$(dirname "$0")/../lib.sh"
scenario_begin s2a-kill-inflight rdb 3 8
drive --def=chain --count=60 --latencyMs=6000 --concurrency=60
at 3
mock_stats > "$RESULTS/mock-before-kill.json"
"$LT_DIR/trama.sh" kill 1; echo "kill_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
scenario_end 240
