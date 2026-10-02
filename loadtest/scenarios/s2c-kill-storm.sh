#!/usr/bin/env bash
# 2c. Kill storm: while mixed workflows (split/join, callbacks, sleeps, 30% failing with slow
# compensation) run, a non-API worker is SIGKILLed and restarted every 10s, so deaths land during
# compensation, split fan-out and callback processing.
source "$(dirname "$0")/../lib.sh"
scenario_begin s2c-kill-storm rdb 4 4
drive --def=mixed --count=600 --rate=10 --failPct=30 --undoLatencyMs=2000 --sleepMs=3000 --callbackDelayMs=2000 &
DRIVER=$!
for t in 10 20 30 40 50; do
  at $t
  i=$(( (t / 10) % 3 + 1 ))
  "$LT_DIR/trama.sh" kill $i; echo "kill trama-$i at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
  "$LT_DIR/trama.sh" add $i 4 >/dev/null
done
wait $DRIVER
scenario_end 300
