#!/usr/bin/env bash
# 2b. "Zombie" worker: SIGSTOP longer than the claim lease (20s), then SIGCONT. Its work is
# re-delivered to another worker meanwhile; checks whether both then advance the same executions.
source "$(dirname "$0")/../lib.sh"
scenario_begin s2b-zombie rdb 3 8
drive --def=chain --count=60 --latencyMs=6000 --concurrency=60
at 3
mock_stats > "$RESULTS/mock-before-pause.json"
"$LT_DIR/trama.sh" pause 1; echo "pause_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 45
"$LT_DIR/trama.sh" resume 1; echo "resume_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
scenario_end 240
