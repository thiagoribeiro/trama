#!/usr/bin/env bash
# Harness self-test: 100 chain + 50 mixed workflows, one process, no faults. Expect 0 stuck/lost/duplicates.
source "$(dirname "$0")/../lib.sh"
scenario_begin smoke rdb 1 4
drive --def=chain --count=100 --concurrency=16
drive --def=mixed --count=50 --failPct=20 --sleepMs=2000 --callbackDelayMs=1000 --concurrency=16
scenario_end 120
