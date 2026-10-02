#!/usr/bin/env bash
# 1a. Redis restarts under load. Usage: s1a-redis-restart.sh none|rdb|aof
# 3 processes; 1500 mixed workflows at 50/s; Redis restarted at t=15s.
source "$(dirname "$0")/../lib.sh"
P="${1:-rdb}"
scenario_begin "s1a-redis-restart-$P" "$P" 3 4
drive --def=mixed --count=1500 --rate=50 --failPct=5 --sleepMs=3000 --callbackDelayMs=2000 &
DRIVER=$!
at 15; "$LT_DIR/stack.sh" redis-restart; echo "restart_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
wait $DRIVER
scenario_end 300
