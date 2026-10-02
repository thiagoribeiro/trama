#!/usr/bin/env bash
# Trama processes for the validation harness (build/install/trama, via Toxiproxy).
#   trama.sh build                  installDist + harness classpath
#   trama.sh start N [workerCount]  start processes 0..N-1 on ports 9100+i (process 0 = API + callbacks)
#   trama.sh add I [workerCount]    start one more process with index I
#   trama.sh kill I                 SIGKILL process I
#   trama.sh pause I | resume I     SIGSTOP / SIGCONT process I
#   trama.sh stop                   stop all processes
#   trama.sh mock-start | mock-stop
# Extra JVM options for every process can be passed in TRAMA_EXTRA_OPTS; REDIS_POOL sets the Redis pool size;
# RATE_LIMIT=true re-enables the per-definition failure breaker.
source "$(dirname "$0")/lib.sh"
PIDS="$RUN_DIR/pids"

start_one() {
  local i=$1 workers=${2:-4} port=$((API_PORT + $1))
  # Anything still bound to the port (e.g. a process from an earlier scenario) would answer the
  # readiness probe below in place of the new process.
  fuser -k -9 "$port/tcp" >/dev/null 2>&1 || true
  while ss -ltn "sport = :$port" | grep -q LISTEN; do sleep 0.2; done
  # REDIS_POOL: the default (16) deadlocks the process under concurrency, see
  # scenarios/s3x-pool-deadlock.sh; the other scenarios raise it so they can measure anything else.
  # RATE_LIMIT: the per-definition failure breaker pauses a whole workflow type after 5 failures
  # (scenarios/s3y-rate-limit.sh); it is off elsewhere so business failures don't skew measurements.
  JAVA_OPTS="-Xmx512m -Dconfig.override.runtime.workerCount=$workers -Dconfig.override.redis.pool.maxTotal=${REDIS_POOL:-256} -Dconfig.override.redis.pool.maxIdle=${REDIS_POOL:-256} -Dconfig.override.rateLimit.enabled=${RATE_LIMIT:-false} ${TRAMA_EXTRA_OPTS:-}" \
  PORT=$port HOSTNAME="lt-worker-$i" \
  DATABASE_HOST=127.0.0.1 DATABASE_PORT=$PG_PROXY_PORT DATABASE_DATABASE=saga DATABASE_USER=saga DATABASE_PASSWORD=saga \
  REDIS_URL="redis://127.0.0.1:$REDIS_PROXY_PORT" \
  RUNTIME_ENABLED=true METRICS_ENABLED=true TELEMETRY_ENABLED=false \
  RUNTIME_CALLBACK_BASEURL="http://127.0.0.1:$API_PORT" RUNTIME_CALLBACK_HMACSECRET=loadtest-secret \
    nohup "$ROOT/build/install/trama/bin/trama" >> "$RUN_DIR/trama-$i.log" 2>&1 &
  echo "$i $! $port" >> "$PIDS"
  local pid=$!
  until curl -sf "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; do
    kill -0 $pid 2>/dev/null || { echo "process $i died, see $RUN_DIR/trama-$i.log" >&2; exit 1; }
    sleep 0.5
  done
  # The listener must be the process just started.
  ss -ltnp "sport = :$port" | grep -q "pid=$pid," || { echo "port $port is not served by pid $pid" >&2; exit 1; }
  say "trama-$i ready on :$port (pid $!, workers $workers)"
}

pid_of() { awk -v i="$1" '$1==i {print $2}' "$PIDS" | tail -1; }

case "${1:-}" in
  build) (cd "$ROOT" && ./gradlew -q loadtestClasspath) && say "built" ;;
  start) : > "$PIDS"; for ((i = 0; i < $2; i++)); do start_one $i "${3:-4}"; done ;;
  add) start_one "$2" "${3:-4}" ;;
  kill) kill -9 "$(pid_of "$2")"; say "trama-$2 SIGKILL" ;;
  pause) kill -STOP "$(pid_of "$2")"; say "trama-$2 SIGSTOP" ;;
  resume) kill -CONT "$(pid_of "$2")"; say "trama-$2 SIGCONT" ;;
  stop)
    [ -f "$PIDS" ] && awk '{print $2}' "$PIDS" | xargs -r kill -9 2>/dev/null || true
    for ((p = API_PORT; p < API_PORT + 16; p++)); do fuser -k -9 "$p/tcp" >/dev/null 2>&1 || true; done
    : > "$PIDS"; say "trama stopped" ;;
  mock-start)
    # A mock left over from an earlier run would keep answering on the port (and keep its counts).
    pgrep -f "MainKt mock --port=$MOCK_PORT" | xargs -r kill 2>/dev/null || true
    while curl -sf "http://127.0.0.1:$MOCK_PORT/stats" >/dev/null 2>&1; do sleep 0.2; done
    (harness_exec mock --port=$MOCK_PORT > "$RUN_DIR/mock.log" 2>&1) &
    echo $! > "$RUN_DIR/mock.pid"
    until curl -sf "http://127.0.0.1:$MOCK_PORT/stats" >/dev/null; do sleep 0.3; done
    say "mock ready on :$MOCK_PORT" ;;
  mock-stop) [ -f "$RUN_DIR/mock.pid" ] && kill "$(cat "$RUN_DIR/mock.pid")" 2>/dev/null || true ;;
  *) sed -n '2,11p' "$0"; exit 1 ;;
esac
