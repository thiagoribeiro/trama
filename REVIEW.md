# Review Notes (SOLID/KISS/Observability)

This document highlights key improvements to increase maintainability, configurability, resilience, and observability for the current solution.

## Architecture & SOLID
- Separate responsibilities:
  - `Application.module()` currently wires runtime, starts workers, and defines HTTP routes. Move startup wiring to a dedicated bootstrap class/module (e.g., `RuntimeBootstrap`) and keep routing focused on HTTP concerns.
  - `DefaultSagaExecutor` handles HTTP calls, retry/backoff, re-enqueue, database persistence, and template rendering. Split into smaller collaborators:
    - `SagaExecutionStore` (persist execution + step results)
    - `SagaHttpInvoker` (HTTP calls + response parsing)
    - `RetryPolicy`/`BackoffStrategy`
    - `TemplateRenderer`
    - `ExecutionEnqueuer` (Redis enqueue abstraction)
- Introduce interfaces for infrastructure boundaries:
  - `RedisQueue` and `RedisConsumer` should be interfaces with implementations so tests can use in-memory fakes.
  - `Database` should expose a `DataSource`/`DSLContext` provider instead of raw `withConnection`.
- Reduce direct static access:
  - Replace `object` singletons (`Database`, `RedisClientProvider`, `SagaRepository`, `SagaHttpClient`) with injectable components to reduce hidden global state.

## KISS & Coupling
- Avoid heavy logic in `DefaultSagaExecutor`:
  - The executor should orchestrate steps, not manage low-level storage concerns or templating.
- Avoid repeated `String`-based templating context generation on every call:
  - Cache or precompute static context values.
  - Consider a structured context object instead of raw `Map<String, Any?>`.

## Configuration & Parameters
- Move hardcoded settings to config/env:
  - Redis keys, batch size, poll intervals, buffer sizes, processing timeout, worker count.
  - HTTP client connection pool parameters and timeouts.
  - DB pool sizes.
  - Retry/backoff defaults.
- Support `application.conf` (HOCON) or a config class + env overrides.
- Expose defaults in a single config file for clarity.

## Error Handling & Resilience
- Avoid silent failures:
  - In `SagaExecutionProcessor`, exceptions in `runWorker` are swallowed; log and increment a failure metric.
  - In `DefaultSagaExecutor.executeHttpCall`, errors are swallowed; capture reason and store it in DB.
- Ensure in-flight entries are always acked or re-queued deterministically:
  - Explicitly record processing errors and decide whether to retry or fail.
- Consider transactional boundaries:
  - Step result persistence should be reliable. At least capture errors and avoid breaking execution flow.

## Observability (Logs + Tracing)
- Add OpenTelemetry:
  - Trace incoming HTTP requests.
  - Trace saga execution lifecycle and each step execution.
  - Propagate trace context into outgoing HTTP headers.
- Correlate logs with trace/span IDs:
  - Add MDC fields (`traceId`, `spanId`, `sagaId`) for all logs.
- Structured logs:
  - Prefer JSON logs for easier ingestion.
  - Log key events: enqueue, dequeue, step success/failure, retry scheduling, compensation start/end.

## Database & Persistence
- Liquibase is included but not wired. Decide whether migrations are run at startup or via CI/CD.
- Partitioning:
  - Ensure partitions are created ahead of time with a scheduled job and old partitions are dropped.
  - Consider a job to enforce the 15-day TTL.
- Query filters:
  - Ensure *all* queries include `started_at >= now - 15 days` to avoid partition scans.
- Schema:
  - Consider adding a unique constraint on `(saga_id, step_index, phase, created_at)` if needed.

## Redis & Queueing
- Redis sorted-set usage:
  - Ensure payload size and encoding are stable; consider compression if payloads are large.
  - Provide a max attempt or DLQ strategy for messages that repeatedly fail.
- Add explicit backoff for empty polls to avoid hot loops.

## HTTP Client
- `SagaHttpClient` currently uses default CIO settings; explicit pooling was removed due to API mismatch.
  - Re-check Ktor CIO engine tuning in current Ktor version and reapply if supported.
- Add retries for transient errors or integrate with saga retry policy explicitly.

## Security & Validation
- Validate URLs and HTTP verbs more strictly.
- Consider allowing/denying certain headers to avoid unsafe or leaking data.

## Testing
- Add integration tests:
  - Redis (ZSET claim/requeue logic)
  - Postgres persistence and partition behavior
- Add unit tests for retry/backoff logic and template rendering.

## Suggested Next Steps (Priority)
1. Introduce config module and remove hardcoded settings.
2. Split `DefaultSagaExecutor` into smaller components (SOLID).
3. Add OpenTelemetry + structured logging with trace correlation.
4. Add Redis/DB integration tests and basic health checks.
