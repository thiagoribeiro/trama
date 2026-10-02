#!/usr/bin/env bash
# 1a'. Same as s1a, but the Trama processes are restarted right after Redis comes back, which
# reloads the Lua scripts (a plain Redis restart leaves every process failing with NOSCRIPT, see
# s1a). Isolates what each persistence profile actually preserves. Usage: ... none|rdb|aof
source "$(dirname "$0")/../lib.sh"
P="${1:-rdb}"
scenario_begin "s1a2-redis-restart-persistence-$P" "$P" 3 4
drive --def=mixed --count=1500 --rate=50 --failPct=5 --sleepMs=3000 --callbackDelayMs=2000 &
DRIVER=$!
at 15; "$LT_DIR/stack.sh" redis-restart; echo "restart_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
until podman exec lt-redis redis-cli -p $REDIS_PORT PING 2>/dev/null | grep -q PONG; do sleep 0.5; done
"$LT_DIR/trama.sh" stop >/dev/null
for i in 0 1 2; do "$LT_DIR/trama.sh" add $i 4 >/dev/null; done
echo "trama_restarted_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
wait $DRIVER
scenario_end 300
