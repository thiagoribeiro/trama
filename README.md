# Trama.io Saga Orchestrator

Trama is a lightweight saga orchestration service for coordinating distributed workflows using the **saga pattern**. It exposes a simple HTTP API to register saga definitions and execute them, while handling retries, compensations, persistence, and metrics.

## Highlights

- HTTP API to create, list, and run saga definitions.
- Redis-backed execution queue with in-flight tracking and retries.
- Postgres persistence for saga state + step results (used for template rendering and diagnostics).
- Compensation (down) steps on failure.
- Optional rate-limiting per saga name.
- OpenTelemetry tracing + Prometheus metrics.
- Partition maintenance for execution tables.

## Architecture (High-Level)

- **API (Ktor)** accepts saga definitions and run requests.
- **Redis** is used as the execution queue.
- **Workers** pull from Redis, execute HTTP steps, and persist results.
- **Postgres** stores saga definitions, executions, and step results.
- **Template renderer** builds HTTP calls using payload and prior step results.

## Requirements

- JDK 21
- Redis
- Postgres

## Quick Start (Docker)

```bash
docker compose up --build
```

The API will be available at `http://localhost:8080`.

## Local Dev (no Docker)

1. Start Postgres + Redis locally.
2. Configure `src/main/resources/application.yaml` or environment variables.
3. Run the app:

```bash
./gradlew run
```

## Configuration

Configuration defaults live in:

- `src/main/resources/application.yaml`

Environment overrides are supported via `RUNTIME_ENABLED`, `METRICS_ENABLED`, and `TELEMETRY_ENABLED` (see `io.trama.config.ConfigLoader`).

Key sections:

- `redis`: connection + queue settings
- `database`: Postgres connection and pool
- `runtime`: worker settings
  - `runtime.store`: choose `REDIS` (default) or `POSTGRES` for execution storage
- `rateLimit`: saga rate limiting
- `telemetry`: OpenTelemetry exporter settings
- `metrics`: Prometheus metrics toggle

## API Overview

### Health
- `GET /healthz`
- `GET /readyz`

### Saga Definitions
- `POST /sagas/definitions` – create a new immutable definition
- `GET /sagas/definitions` – list definitions
- `GET /sagas/definitions/{id}` – fetch definition
- `PUT /sagas/definitions/{id}` – create definition with explicit id (fails if exists)
- `DELETE /sagas/definitions/{id}` – delete definition

### Run Sagas
- `POST /sagas/definitions/{name}/{version}/run` – run a stored definition
- `POST /sagas/run` – run a definition inline

### Status & Retry
- `GET /sagas/{id}` – fetch execution status
- `POST /sagas/{id}/retry` – retry a failed execution

### Example: Create + Run

```bash
curl -X POST http://localhost:8080/sagas/definitions \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "order-saga",
    "version": "v1",
    "failureHandling": {"type": "retry", "maxAttempts": 1, "delayMillis": 200},
    "steps": [
      {"name": "reserve", "up": {"url": {"value": "http://service/reserve"}, "verb": "POST"},
                           "down": {"url": {"value": "http://service/release"}, "verb": "POST"}}
    ]
  }'
```

```bash
curl -X POST http://localhost:8080/sagas/definitions/{name}/{version}/run \
  -H 'Content-Type: application/json' \
  -d '{"payload": {"orderId": "123"}}'
```

## Demo Scripts

A small demo harness is in `scripts/saga_demo/`:

- `flask_server.py`: local HTTP service used by saga steps
- `run_saga.py`: creates a definition, runs it, and validates final status

See `scripts/saga_demo/README.md` for details.

## Template Rendering

Saga steps use **template strings** to compose URLs, headers, and bodies at runtime. Templates can reference:

- The saga payload: `{{payload.orderId}}`
- Prior step results: `{{step.0.up.body.someField}}`
- Convenience lists: `{{steps}}` for iterating in templates (see `TemplateContextBuilder`)

This allows you to chain calls where a later step depends on the response of an earlier one.

Example (simplified):

```json
{
  "name": "charge",
  "up": {
    "url": { "value": "http://service/charge?reservation={{step.0.up.body.id}}" },
    "verb": "POST",
    "body": { "value": "{\"amount\": \"{{payload.amount}}\"}" }
  }
}
```

Notes:
- Step results are loaded from Postgres, so template chaining requires DB persistence to be enabled.
- See `scripts/saga_demo/run_saga.py` for a working example of response-based composition.

## Testing

```bash
./gradlew test
```

Integration tests that use Testcontainers are skipped if Docker is unavailable.

## Project Layout

- `src/main/kotlin/io/trama/app` – API / Ktor entry
- `src/main/kotlin/io/trama/runtime` – runtime bootstrap + workers
- `src/main/kotlin/io/trama/saga` – core saga model + execution
- `src/main/kotlin/io/trama/saga/redis` – Redis queue + rate limit
- `src/main/kotlin/io/trama/saga/store` – Postgres persistence
- `src/main/kotlin/io/trama/config` – configuration
- `src/main/kotlin/io/trama/telemetry` – tracing + metrics

## License

Add a LICENSE file (e.g., Apache-2.0 or MIT) before publishing.
