#!/usr/bin/env bash
# 4. Combined load with faults: 5000 mixed workflows at 30/s on 4 processes, with
# t=60 kill worker 2, t=120 Redis cut 10s, t=180 Postgres cut 10s, t=200 worker 2 back.
source "$(dirname "$0")/../lib.sh"
scenario_begin s4-combined rdb 4 6
drive --def=mixed --count=5000 --rate=30 --failPct=5 --sleepMs=5000 --callbackDelayMs=3000 --concurrency=128 &
DRIVER=$!
at 60;  "$LT_DIR/trama.sh" kill 2;     echo "kill trama-2 at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 120; "$LT_DIR/stack.sh" cut redis;  echo "redis cut at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 130; "$LT_DIR/stack.sh" restore redis; echo "redis restored at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 180; "$LT_DIR/stack.sh" cut pg;     echo "pg cut at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 190; "$LT_DIR/stack.sh" restore pg; echo "pg restored at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 200; "$LT_DIR/trama.sh" add 2 6;    echo "trama-2 back at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
wait $DRIVER
scenario_end 600
