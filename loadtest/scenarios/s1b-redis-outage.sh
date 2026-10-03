#!/usr/bin/env bash
# 1b. Redis unreachable for 30s (Toxiproxy drops connections), then back. 3 processes,
# chain workflows at 20/s for 75s. Records API answers during the outage and recovery.
source "$(dirname "$0")/../lib.sh"
scenario_begin s1b-redis-outage rdb 3 4
drive --def=chain --count=1500 --rate=20 --concurrency=32 &
DRIVER=$!
at 20; "$LT_DIR/stack.sh" cut redis; echo "cut_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
at 50; "$LT_DIR/stack.sh" restore redis; echo "restore_at_s=$(elapsed)" >> "$RESULTS/timeline.txt"
for i in 0 1 2; do
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$((API_PORT + i))/healthz" || true)
  echo "healthz_after_restore trama-$i=$code" >> "$RESULTS/timeline.txt"
done
wait $DRIVER
scenario_end 300
