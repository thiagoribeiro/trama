#!/usr/bin/env bash
# Infrastructure for the validation harness (podman, host networking).
#   stack.sh up [none|rdb|aof]   Postgres (pg_stat_statements) + Redis (persistence profile) + Toxiproxy
#   stack.sh down
#   stack.sh redis-restart | redis-flush
#   stack.sh cut redis|pg        disable the Toxiproxy proxy (drops connections)
#   stack.sh restore redis|pg
#   stack.sh latency redis|pg MS add latency to every response; latency ... 0 removes it
source "$(dirname "$0")/lib.sh"

toxi() { curl -sf -X "$1" "$TOXI_API$2" ${3:+-H 'Content-Type: application/json' -d "$3"} >/dev/null; }

up() {
  local persistence="${1:-rdb}" redis_args
  case "$persistence" in
    none) redis_args=(--save "" --appendonly no) ;;
    rdb)  redis_args=() ;;                                   # Redis default snapshots
    aof)  redis_args=(--appendonly yes --appendfsync everysec) ;;
    *) echo "unknown persistence: $persistence" >&2; exit 1 ;;
  esac
  podman run -d --rm --name lt-pg --network host \
    -e POSTGRES_USER=saga -e POSTGRES_PASSWORD=saga -e POSTGRES_DB=saga \
    docker.io/library/postgres:15-alpine \
    -c port=$PG_PORT -c max_connections=400 -c shared_preload_libraries=pg_stat_statements >/dev/null 2>&1
  podman run -d --name lt-redis --network host docker.io/library/redis:7-alpine \
    redis-server --port $REDIS_PORT "${redis_args[@]}" >/dev/null 2>&1
  podman run -d --rm --name lt-toxi --network host ghcr.io/shopify/toxiproxy:2.9.0 >/dev/null 2>&1
  until podman exec lt-pg pg_isready -q -p $PG_PORT >/dev/null 2>&1; do sleep 0.5; done
  until curl -sf "$TOXI_API/version" >/dev/null; do sleep 0.3; done
  podman exec lt-pg psql -q -U saga -p $PG_PORT -d saga -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' >/dev/null 2>&1
  toxi POST /proxies "{\"name\":\"redis\",\"listen\":\"127.0.0.1:$REDIS_PROXY_PORT\",\"upstream\":\"127.0.0.1:$REDIS_PORT\"}"
  toxi POST /proxies "{\"name\":\"pg\",\"listen\":\"127.0.0.1:$PG_PROXY_PORT\",\"upstream\":\"127.0.0.1:$PG_PORT\"}"
  say "stack up (redis persistence: $persistence)"
}

down() {
  podman rm -f lt-pg lt-redis lt-toxi >/dev/null 2>&1 || true
  say "stack down"
}

case "${1:-}" in
  up) up "${2:-rdb}" ;;
  down) down ;;
  redis-restart) podman restart lt-redis >/dev/null 2>&1; say "redis restarted" ;;
  redis-flush) podman exec lt-redis redis-cli -p $REDIS_PORT FLUSHALL >/dev/null 2>&1; say "redis FLUSHALL" ;;
  cut) toxi POST "/proxies/$2" '{"enabled":false}'; say "$2 cut" ;;
  restore) toxi POST "/proxies/$2" '{"enabled":true}'; say "$2 restored" ;;
  latency)
    toxi DELETE "/proxies/$2/toxics/lat" 2>/dev/null || true
    if [ "${3:-0}" != 0 ]; then
      toxi POST "/proxies/$2/toxics" "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":$3}}"
    fi
    say "$2 latency ${3:-0}ms" ;;
  *) sed -n '2,9p' "$0"; exit 1 ;;
esac
