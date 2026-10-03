#!/usr/bin/env bash
# Runs every validation scenario in sequence (~2h on an 8-core machine).
# Results: results/$RESULTS_SET/<scenario>/ (RESULTS_SET defaults to the version under test, see lib.sh).
self="$(readlink -f "$0")"
cd "$(dirname "$self")"
# Re-exec under a sleep inhibitor: a suspend freezes every process at once and corrupts results.
[ -z "${LT_INHIBITED:-}" ] && exec env LT_INHIBITED=1 systemd-inhibit --what=sleep:idle:handle-lid-switch --why="trama validation run" bash "$self" "$@"
for s in "smoke" \
         "s1a-redis-restart none" "s1a-redis-restart rdb" "s1a-redis-restart aof" \
         "s1a2-redis-restart-persistence none" "s1a2-redis-restart-persistence rdb" "s1a2-redis-restart-persistence aof" \
         "s1b-redis-outage" "s1c-redis-data-loss" \
         "s2a-kill-inflight" "s2b-zombie" "s2c-kill-storm" \
         "s3x-pool-deadlock" "s3y-rate-limit" "s3-scale" "s4-combined"; do
  set -- $s
  echo "=== $(date +%T) $*"
  bash "scenarios/$1.sh" "${@:2}" || echo "!!! $1 exited with $?"
done
echo "=== $(date +%T) all done"
