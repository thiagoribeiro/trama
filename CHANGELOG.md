# Changelog

All notable changes to Trama are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [2.2.0] - 2026-10-02

Performance release. Same guarantees as 2.1.0. Measured on the validation host (8 threads,
shared with Postgres, Redis and the downstream mock), one process, 3-node workflows with 20 ms calls:
- **At equal concurrency (64 workers):** 182 → 229 workflows/s, with Trama CPU per workflow down
  from 19.1 to 12.0 ms (−37%).
- **With defaults:** 4 → 32 workers takes a process from about 55 to 231 workflows/s.

### Changed
- **`runtime.workerCount` defaults to 32 (was 4).** Workers are coroutines that mostly wait on
  HTTP calls, so 4 left a process idle: with 20 ms downstream calls it capped at about 55
  workflows/s. 32 is where this host's CPU saturates (231/s). This also raises the concurrent
  calls a process makes to downstream services; set `RUNTIME_WORKERCOUNT=4` to keep the previous
  behavior.
- **Workflow HTTP calls reuse connections.** The client now runs on OkHttp with a keep-alive pool.
  Ktor CIO opened a new TCP connection per node call, leaving one `TIME_WAIT` socket behind each.
  OkHttp sends `Accept-Encoding: gzip` and decompresses responses transparently.
- **One INFO log line per execution** (`saga finished`, with status and duration). The per-node
  lines (`executing node`, `node completed` on success) and the per-slice ones (`processing saga`,
  `saga execution started`) moved to DEBUG. Failed nodes still log `node completed` at INFO.
- **Hot-path queries use plain JDBC.** That covers step-result loading and step-call inserts. Step
  results are not loaded before the first node completes.

## [2.1.0] - 2026-10-02

Fixes every gap found by the durability, recovery and load validation
(`docs/validation-report.md`, harness in `loadtest/`). No API or definition-format changes.

### Fixed
- **Consumers stop for good after any Redis error** (outage, restart, timeout) while `/healthz` and
  `/readyz` keep answering 200. Claim loops now log, back off (100 ms to 5 s) and keep going;
  `/readyz` reports `consumer_stalled` and `/healthz` fails after `runtime.livenessStallMillis`
  without progress.
- **Every Lua script failed with `NOSCRIPT` after a Redis restart**, stopping claims, claim renewal
  and join arrivals until Trama itself restarted. Scripts now fall back to `EVAL` once, which also
  reloads them.
- **Process-wide deadlock above 16 concurrent Redis operations.** The connection pool was borrowed
  with a blocking call from coroutine threads. Trama now uses one multiplexed Lettuce connection,
  with a command timeout (`redis.commandTimeoutMillis`, default 5 s) and commands rejected right
  away while disconnected.
- **A worker paused past its claim lease could overwrite a `SUCCEEDED` execution with `FAILED` and
  compensate it.** Executions are now fenced (see *Changed*); a stale worker stops before its next call.
- **Executions lost or stuck forever after Redis data loss.** Running executions and retry backoffs
  vanished, and sleeping, callback-waiting and join-waiting ones never resumed. Postgres now holds
  every execution's resume point, and a reconciler re-sends executions whose queue item disappeared.
- **Executions stuck after worker crashes or Redis errors in narrow windows:**
  - a split registered its branches but the worker died before enqueuing them;
  - a parent was taken off its join barrier but the worker died before resuming it;
  - a branch finished but its arrival on the join barrier was never recorded. The join backstop
    scanner now also resumes parents whose branches all finished, and runs every 30 s (was 5 min).

### Changed
- **Postgres is the source of truth for execution state.** Each node boundary writes a checkpoint
  (state, sequence number, step result) in one statement, as a compare-and-set on the sequence. A
  copy of an execution that fell behind (a redelivered or paused worker) can no longer write.
  Redis keeps the queue, claims and callback nonces; the execution meta, step history and join
  counters it used to hold now go straight to Postgres. Liquibase changeset `009` (additive).
- **Claimers only visit shards with due work**, tracked in a due index in Redis, plus a full sweep
  every `runtime.fullSweepIntervalMillis` (5 s). Idle polling used to scan every owned shard every 50 ms.
- **A process claims at most `workerCount + prefetch` executions** (`runtime.prefetch`, default
  `workerCount`). It used to claim up to 200 beyond what its workers could run.
- **The failure rate limiter only counts worker failures** (exceptions while processing). Executions
  that end `FAILED` by their own logic no longer pause every execution of their workflow.
- **`runtime.enabled=false` (API-only) accepts runs and callbacks** and queues them for worker
  processes. It used to answer `503`.
- **Redis outages no longer reject work that Postgres can hold:**
  - runs, accepted callbacks and `/wake` calls succeed once Postgres has recorded them; the
    reconciler queues them later if Redis cannot take them right away;
  - callbacks skip the Redis nonce check, since Postgres already lets a callback resume an
    execution only once;
  - the failure rate limiter fails open.
- **Join arrivals are counted in Postgres only.** The Redis fast path duplicated a Postgres write
  that happened anyway, and could fail on its own.
- `saga_inmemory_queue_size` now reports claimed executions waiting for a worker.

### Added
- `ExecutionReconciler` (`reconciler.*`: `enabled`, `intervalMillis` 30 s, `staleAfterMillis` 2 min,
  `batchSize`). It re-sends running or sleeping executions that stopped advancing.
- Metrics: `saga_redis_errors_total{operation}`, `saga_fenced_total{reason}`,
  `saga_reconciled_total{status}`.
- Environment overrides:
  - `REDIS_COMMANDTIMEOUTMILLIS`;
  - `RUNTIME_WORKERCOUNT`, `RUNTIME_PREFETCH`;
  - `RECONCILER_ENABLED`, `RECONCILER_INTERVALMILLIS`, `RECONCILER_STALEAFTERMILLIS`;
  - `RATELIMIT_ENABLED`.
- `loadtest/`: the validation harness (fault injection with Toxiproxy, effect-counting mock,
  outcome checker) and its scenarios.

### Deprecated
- `redis.pool.*` and `runtime.bufferSize` are accepted but ignored.

### Upgrade notes
- Rolling upgrade from 2.0.x is supported. An execution started by 2.0.x has its step history
  imported from Redis into Postgres the first time 2.1.0 runs it.
- Redis persistence is no longer needed to avoid losing executions. Without it, a Redis loss delays
  affected executions until the reconciler re-sends them (about `staleAfterMillis`). AOF `everysec`
  still keeps that delay short.

## [2.0.1] - 2026-10-01

### Fixed
- Switch conditions written as `{"var": "input.x"}` always fell through to the `default` branch in
  production, although `trama validate` (dry-run) accepted them. Switch, async callback and template
  contexts now all accept both `payload` and `input` (aliases), and the dry-run uses the exact switch
  context of the runtime, `prev` and `step` included.
- `REDIS_TOPOLOGY` and `REDIS_CLUSTER_NODES` environment overrides were documented but ignored, so
  Redis Cluster could not be enabled through the environment. `REDIS_CLUSTER_NODES` is comma-separated.
- The Grafana dashboard queried two series the runtime never emitted:
  - `saga_node_duration_seconds` (labels `node_kind`, `mode`) is now recorded;
  - the switch panel now uses the real series name, `saga_switch_evaluated_total`.
- `openapi.json` now documents every route: `GET /workflows`, `GET /workflows/{id}/steps`,
  `GET /workflows/{id}/steps/calls`, `GET /workflows/definitions/{name}/{version}` and the async callback
  `POST /workflows/{executionId}/node/{nodeId}/callback`.

## [2.0.0] - 2026-10-01

### ⚠️ Breaking changes
- **HTTP API moved from `/sagas/*` to `/workflows/*`.** This covers list, status, steps, step calls,
  retry and run. The async callback URL injected into requests (`{{runtime.callback.url}}`) is now
  `/workflows/{executionId}/node/{nodeId}/callback`, so callbacks still pointing at the old path are rejected.
- **`POST /workflows/{id}/retry` only accepts `FAILED` executions** and answers `409` for any other
  status. Previously it re-ran sagas in any state, including `SUCCEEDED` ones.
- **Template escaping depends on where the value lands.** `{{ }}` no longer HTML-escapes everywhere:
  - JSON bodies get JSON string escaping;
  - XML/HTML bodies keep entity escaping;
  - `application/x-www-form-urlencoded` bodies are form-encoded;
  - URLs, headers (line breaks removed) and other bodies get the literal value.

  `{{{ }}}` is still raw everywhere. Values without special characters render exactly as before.
- **Shard → pod mapping changed** (rendezvous scores now use a mixing finalizer). During a rolling
  deploy with mixed versions, a shard can briefly have no owner. Prefer a quick rollout or a recreate
  deploy for this release.

### Added
- **`split` / `join` nodes** for parallel branches in v2 workflows. A split fans out into independent
  child executions, and the join resumes the parent once every branch has finished. Branches support
  async tasks, sleeps and nested splits. They are also supported in the visual editor and the execution inspector.
- **Claim heartbeat.** Workers renew the claims they are still processing, so a slow but healthy
  execution is never re-delivered to a second worker.
- **`saga_execution.payload`.** The run payload is persisted, so retried executions keep it
  (Liquibase changeset `008`, additive).
- **`database.pool.definitionCacheTtlMillis`** (`DATABASE_POOL_DEFINITIONCACHETTLMILLIS`, default
  `5000`). This is the maximum time a pod may keep serving a definition deleted through another pod.
- **`RUNTIME_EMPTYPOLLDELAYMILLIS` and `REDIS_SHARDING_VIRTUALSHARDCOUNT`** environment overrides.
- **OpenAPI documentation for the v2 node-graph format.**

### Changed
- **Work claimed by a pod that died is recovered within the claim lease.** This used to take up to
  ~21 minutes with default settings: expired claims are now returned to the queue by the regular
  claim pass instead of a slow per-shard poller.
- **`redis.consumer.processingTimeoutMillis` now defaults to `20000`** (was `60000`). With claim
  renewal in place, it only bounds how long a dead pod's work waits.
- **`saga_execution.definition` stores the real v2 graph** for v2 executions, instead of a name/version stub.

### Deprecated
- `redis.consumer.requeueIntervalMillis` is ignored. The requeue poller no longer exists.

### Fixed
- **Sleep / wake**
  - `POST /workflows/{id}/wake` always answered `404` under the default REDIS store, and the status
    API never showed `SLEEPING`.
  - Waking a sleep that is the last node crashed the execution.
  - A delayed queue copy arriving after a wake could run the next node a second time. Wake-ups are now claimed exactly once.
  - With `runtime.store=POSTGRES`, sleeps longer than 12h never woke and `/wake` did nothing.
- **Async nodes**
  - Any v2 async node with `callback.successWhen` / `failureWhen` made `POST /workflows/run` fail with `500`.
  - When the last node of a split branch was async, the join never fired and the parent stayed `WAITING_JOIN` forever.
  - When the last node of a saga was async, `onSuccessCallback` never fired.
- **Retry**
  - `/retry` lost the original payload.
  - `/retry` returned `500` for v2 executions instead of the documented `422`.
- **State and history**
  - A saga parked for longer than 10 minutes (sleep, callback, join, backoff) lost its step history
    in Redis: `{{nodes.*}}` templates rendered empty after resuming and `/steps` was incomplete.
  - `GET /workflows/{id}/steps` returned steps in arbitrary order, and `latencyMs` included the rest of the saga.
- **Definitions**
  - Two definitions with the same name and version but different graphs could execute each other's
    graph (the normalized-definition cache never checked the content).
  - A definition deleted through one pod kept being served by other pods indefinitely.
- **Partitions**
  - Partition maintenance never created or pruned `saga_step_call` partitions, so that table grew without bound.
  - On a JVM not running in UTC, partition maintenance failed on every run.
- **Other**
  - The `saga_enqueue_total` metric (used by the Grafana dashboard) was never emitted.
  - Pods with sequential names (`trama-0`, `trama-1`, …) received unbalanced shares of the shards.
  - Split/join hardened against at-least-once queue redelivery: no duplicate branches, no double-counted
    arrivals, and join resume no longer stalls.
  - Templates resolved a repeated step name to an older entry, and a node could not see the previous
    node's response (`{{nodes.<id>.response.body}}`) when both ran in the same execution slice.

### Upgrade notes
1. Liquibase applies changeset `008` (nullable `payload` column) automatically on startup.
2. Update callers and integrations to the `/workflows/*` paths. Async services must call back on the
   new callback URL; they receive it via `{{runtime.callback.url}}`, so templated callers need no change.
3. Remove `redis.consumer.requeueIntervalMillis` from your configuration (optional, it is ignored).
   Review any custom `processingTimeoutMillis`.
4. Deploy all pods quickly (or with a recreate strategy) because the shard mapping changed.

## [1.0.0] - 2026-04-11

First public release: v2 workflow node graph with async calls and callbacks, the visual definition
editor, and the sleep node. See the [release notes](https://github.com/thiagoribeiro/trama/releases/tag/v1.0.0).

[2.2.0]: https://github.com/thiagoribeiro/trama/releases/tag/v2.2.0
[2.1.0]: https://github.com/thiagoribeiro/trama/releases/tag/v2.1.0
[2.0.1]: https://github.com/thiagoribeiro/trama/releases/tag/v2.0.1
[2.0.0]: https://github.com/thiagoribeiro/trama/releases/tag/v2.0.0
[1.0.0]: https://github.com/thiagoribeiro/trama/releases/tag/v1.0.0
