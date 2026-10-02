#!/usr/bin/env bash
# 3y. Impact of the default per-definition failure breaker (rateLimit: 5 failures / 60s blocks the
# whole workflow type for 60s). 600 mixed workflows at 10/s, 10% business failures (FAILED after
# compensation, the expected outcome), with the breaker on (default) and off.
source "$(dirname "$0")/../lib.sh"
for RL in true false; do
  export RATE_LIMIT=$RL
  scenario_begin "s3y-rate-limit-$RL" rdb 2 4
  drive --def=mixed --count=600 --rate=10 --failPct=10 --sleepMs=2000 --callbackDelayMs=1000
  scenario_end 400
done
