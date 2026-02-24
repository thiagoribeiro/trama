# Architecture Review Notes

This file lists suggested improvements, grouped by priority. Each item includes the rationale and options so you can choose and apply them one by one.

## High Priority
1. **Route behavior when runtime is disabled**
   - Issue: `GET /sagas/{id}` returns `404` when `runtime.enabled=false`, hiding misconfiguration.
   - Suggestion:
     - Option A (recommended): Return `503 Service Unavailable` with a clear message when runtime is disabled.
     - Option B: Do not register the `/sagas/{id}` route if runtime is disabled.

  - Decision: When saga is not found, we return 204 instead of 404

2. **Ktor request tracing span coverage**
   - Issue: Request spans may not capture final response status on exceptions.
   - Suggestion:
     - Option A (recommended): Read response status in `finally` block and record it regardless of success/failure.
     - Option B: Add a dedicated Ktor plugin for tracing (if you decide to include official instrumentation).

3. **Partition retention safety**
   - Issue: Month-based partition deletion can drop recent data if `retentionDays < 31`.
   - Suggestion:
     - Option A (recommended): Only drop partitions strictly older than the month containing `(now - retentionDays)`.
     - Option B: Disable dropping when `retentionDays < 31` and just keep extra partitions.

  - Decision: We dont need to solve it now

## Medium Priority
4. **Retry/backoff and DLQ**
   - Issue: Failed executions are requeued but may loop forever.
   - Suggestion:
     - Option A (recommended): Track failure count and send to a dead-letter queue after N attempts.
     - Option B: Add exponential backoff on repeated failures at the processor level.

  - Decision: Lets create a rate limit system on redis where each saga type (name) has it own control to re-enqueue with new delay all sagas that are crashing more than specified befor processing (our kind of circuit breaker)

5. **Callback failure handling**
   - Issue: `onSuccessCallback` failures are ignored.
   - Suggestion:
     - Option A (recommended): Record a warning in DB/log and include in status response.
     - Option B: Treat callback failure as overall saga failure.

  - Decision: A

6. **Metrics visibility**
   - Issue: No metrics for queue depth, throughput, or error rate.
   - Suggestion:
     - Option A (recommended): Add Micrometer counters/gauges for enqueue/dequeue, retries, failures.
     - Option B: Log periodic statistics if metrics aren’t required.

  - Decision: A

7. **Template context performance**
   - Issue: Step results are loaded and reprocessed on every call.
   - Suggestion:
     - Option A (recommended): Cache step context per saga execution.
     - Option B: Load only the needed step(s) if the template references specific indexes.

  - Decision: lets keep to next version

## Low Priority / Cleanup
8. **Structured logging consistency**
   - Issue: Logs are JSON but messages aren’t structured as fields.
   - Suggestion:
     - Option A: Standardize logs with key/value fields.

9. **Documentation**
   - Issue: Core classes lack KDoc.
   - Suggestion:
     - Option A: Add KDoc to key components (consumer, executor, maintenance).

10. **Batching/IO efficiency**
    - Issue: Redis/DB writes are per-item.
    - Suggestion:
      - Option A: Batch Redis `ZADD` and DB inserts for step results.

    - Decision: Need to do with calm to avoid out of sync (the store on db happens after reenqueue and the info is not there yet)
