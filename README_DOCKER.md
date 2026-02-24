# Docker Setup

## Build and Run
```bash
docker compose up --build
```

## Services
- App (Trama): http://localhost:8080
- Postgres: localhost:5432
- Redis: localhost:6379

## Environment Overrides
You can override these in `docker-compose.yml`:
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- `REDIS_URL`
- `RUNTIME_ENABLED`, `METRICS_ENABLED`, `TELEMETRY_ENABLED`

## Testcontainers
To enable container reuse, the project includes:
- `.testcontainers.properties`
- `src/test/resources/testcontainers.properties`

If you prefer to disable reuse, remove those files or set:
```
testcontainers.reuse.enable=false
```
