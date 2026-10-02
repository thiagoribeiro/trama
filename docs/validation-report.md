# Trama validation report — durability, recovery and load (v2.0.1)

**Date:** 2026-10-02 · **Version under test:** `main` @ v2.0.1 · **Harness:** [`loadtest/`](../loadtest/README.md)

## Summary

| Checklist item | Verdict |
|---|---|
| 1. Durability (Redis restart, outage, data loss) | ❌ **Not met.** A Redis restart or a 30s outage stops all processing permanently and silently. A Redis data loss destroys every execution not yet parked in Postgres and strands parked ones. |
| 2. Recovery under concurrency | ⚠️ **Partially met.** A crashed worker is recovered correctly (at-least-once). A *paused* worker that comes back produces **wrong final outcomes**, so fencing is needed. A few executions stay stuck after repeated worker kills. |
| 3. Load and scalability | ⚠️ **Bounded by design choices.** A process deadlocks above 16 concurrent Redis operations (default pool). Throughput peaks around 131 workflows/s on this host and is dominated by queue polling, not by useful work. Postgres is far from saturated. |
| 4. Load combined with faults | ❌ **Not met.** Dominated by the item 1 defects: after a 10s Redis cut under load, 865 of 4,700 accepted workflows were lost and 425 stuck. |

The good news first. Every scenario *without* infrastructure faults finished with **0 lost executions, 0 wrong outcomes and 0 duplicate effects**. A killed worker's work is redelivered and completed. Postgres handles the load with sub-millisecond statements. Most defects below are in **failure handling of the Redis client and queue consumer**, are concentrated in a few files, and are fixable.

### Gaps, prioritized
| # | Severity | Gap | Reproduce |
|---|---|---|---|
| G1 | Critical | Any Redis error kills the claim loop. The process stops consuming forever, while `/healthz` and `/readyz` keep answering 200 and the API keeps accepting work. Nothing is logged. | `s1b`, `s4` |
| G2 | Critical | After a Redis restart every `EVALSHA` fails with `NOSCRIPT` forever (scripts are loaded once at startup and never reloaded), so claim, heartbeat and join stop working. | `s1a` |
| G3 | Critical | Redis pool deadlock: `borrowObject()` blocks coroutine and Netty threads while connection holders are suspended. With the default pool (16), 64 concurrent requests freeze the whole process, `/healthz` included. | `s3x` |
| G4 | Critical | No fencing: a worker that pauses past its lease and resumes keeps acting on executions another worker took over. It overwrites `SUCCEEDED` with `FAILED` and compensates sagas that succeeded. | `s2b` |
| G5 | Critical | Redis is the only copy of in-flight state. On data loss, `IN_PROGRESS` and retry-backoff executions vanish, and `SLEEPING` / `WAITING_CALLBACK` / `WAITING_JOIN` executions are stuck forever. | `s1c`, `s1a2` |
| G6 | High | The default failure "rate limit" is a breaker per *workflow name*: 5 business failures (normal `FAILED` outcomes) in 60s pause **every** execution of that workflow for 60s. | `s3y` |
| G7 | High | Throughput is bounded by queue polling: ~66 claim scans (`EVALSHA`) per workflow, which saturates CPU before Postgres or Redis are busy with real work. | `s3` |
| G8 | Medium | A small number of executions stay stuck after worker kills or a Redis restart, even with persistence: 4/600 in `s2c`, ~60/1,500 in `s1a2`. Root cause not isolated yet. | `s2c`, `s1a2` |
| G9 | Medium | Over-claiming: each process claims up to `bufferSize` (200) executions beyond what its workers can run. This causes head-of-line blocking, and on a crash up to 200 executions wait for the lease. | `s1c` (first design) |
| G10 | Medium | The documented "API-only" profile (`runtime.enabled=false`) answers `503` to every run request. | manual |

Delivery is **at-least-once**: a crashed worker's in-flight HTTP call is repeated by the worker that takes over (8 of 60 executions in `s2a`, the ones in flight at the kill). Downstream services must be idempotent. A per-node idempotency key such as `{{sagaId}}:<nodeId>` in a header is the recommended pattern.

---

## Method
- **Environment:** one machine, 8 cores, 14 GB RAM. Podman containers with host networking: Postgres 15 (`pg_stat_statements`), Redis 7, Toxiproxy in front of both. N Trama processes from `installDist` (`-Xmx512m`). Numbers are **relative**: they show bottlenecks and trends, not production capacity.
- **Workloads:**
  - **chain:** 3 sync tasks.
  - **mixed:** task → split[sync | async with callback | sleep → task] → join → task.
  - In both, a configurable share is forced to fail and compensate. Node retries are disabled, so every extra call the downstream mock records is an effect duplicated by a fault.
- **Checks:** per submitted run, Postgres status (terminal, stuck, or missing = lost), the mock's count of downstream effects per (run, node, phase), duplicate `saga_step_result` rows, and end-to-end latency. A collector samples CPU, RSS, queue depth, Redis ops and Postgres commits every 5s.
- **Harness defaults that differ from Trama's:**
  - Redis pool 256, because the default deadlocks (G3).
  - Rate limiter off, because it distorts every measurement with failures (G6).
  - Each defect is measured with the defaults in its own scenario.
- **Validity:** scenarios that ran across an unintended machine suspend were discarded and re-run under `systemd-inhibit`. Scenarios 2a and 2b were run twice with the same outcome.

---

## 1. Durability

### 1a. Redis restarts under load (`s1a`, 1,500 mixed workflows at 50/s, restart at t=15s)
| Redis persistence | Lost | Stuck | Completed |
|---|---|---|---|
| none | 989 | 506 | 0 |
| RDB (default) | 1,001 | 496 | 0 |
| AOF everysec | 987 | 503 | 0 |

Persistence makes **no difference**, because nothing is processed after the restart (G2). Every process logs `NOSCRIPT No matching script` on each claim, heartbeat and join operation, plus `LOADING` while Redis reloads.

### 1a'. Redis restart with Trama restarted afterwards (`s1a2`, isolates persistence)
Restarting the Trama processes reloads the Lua scripts:

| Redis persistence | Lost | Stuck | Duplicate effects |
|---|---|---|---|
| none | 201 | 511 | 0 |
| RDB | 0 | 68 | 51 |
| AOF everysec | 0 | 58 | 58 |

- RDB looks as good as AOF here only because `podman restart` is a graceful shutdown, and Redis writes a snapshot on shutdown. After a crash, RDB loses everything since the last snapshot (minutes); AOF everysec loses ≤1s.
- **Recommendation:** AOF `everysec` is the minimum for Redis in production. The remaining ~60 stuck executions are G8.

### 1b. Redis unreachable for 30s (`s1b`, chain at 20/s)
- **During the outage:** every submission fails fast with `500` (600/600). That is acceptable: nothing is accepted and then lost.
- **After it comes back:** `/healthz` returns 200 on all processes, but **no process consumes again**. The dequeue counters freeze at the moment of the cut, the ready queue grows to 536, and those accepted executions never run (G1).

### 1c. Redis data loss (`s1c`, `FLUSHALL` with 20 executions parked in each state)
Reconstructibility matrix: what Postgres alone allows to recover.

| State at the moment of loss | Outcome | Why |
|---|---|---|
| Running (HTTP call in flight) | **lost**, no row in Postgres | Under the REDIS store an execution's row is written only when it parks or finishes. |
| Retry backoff (delayed redelivery) | **lost** | Same; the delayed queue item was the only copy. |
| `SLEEPING` | **stuck forever** | The row exists, but the sentinel and queue item were Redis-only. |
| `WAITING_CALLBACK` | **stuck** | The callback arrives, but `consumeWaiting` is Redis-only, so it gets `410`. Only the callback timeout scanner (Postgres) can end it, as a failure. |
| `WAITING_JOIN` | **stuck forever** | The barrier is in Postgres, but branches that were running or sleeping are lost. |

Identical across two runs. **Guarantee today:** only terminal outcomes and parked-state rows are durable. Anything in motion exists only in Redis.

---

## 2. Recovery under concurrency

### 2a. SIGKILL with HTTP calls in flight (`s2a`, 60 chain workflows with 6s nodes, 2 runs)
| Run | Succeeded | Lost | Stuck | Duplicate downstream calls | Duplicate step rows |
|---|---|---|---|---|---|
| 1 | 60/60 | 0 | 0 | 8 | 0 |
| 2 | 60/60 | 0 | 0 | 8 | 0 |

Recovery works: work claimed by the dead worker returns after the 20s lease and completes. The duplicated calls are exactly the ones in flight at the kill (at-least-once).

### 2b. Zombie worker: SIGSTOP past the lease, then SIGCONT (`s2b`, 2 runs)
| Run | Wrong final outcome | Duplicate step rows | Extra downstream calls |
|---|---|---|---|
| 1 | 8/60 (`FAILED` instead of `SUCCEEDED`) | 18 | 34 |
| 2 | 7/60 | 20 | 44 |

**What happens:**
1. Another worker takes over and completes the executions.
2. The resumed worker's own HTTP calls hit the 30s client timeout, because wall-clock time passed while it was paused.
3. It treats them as node failures, **runs compensations for sagas that already succeeded**, and finalizes them as `FAILED`, overwriting the correct outcome. The logs show `missing redis meta on finalization`.

**Conclusion: fencing is required** (G4). Every state transition, at least terminal ones, must check it still holds the claim: a fencing token or generation per claim, plus compare-and-set on terminal status. Long GC pauses, VM migrations and node suspension all produce this in production.

### 2c. Kill storm (`s2c`, mixed with 30% failing and 2s compensations, a worker SIGKILLed and restarted every 10s)
600 workflows: 408 succeeded, 188 failed (as expected), **0 lost, 4 stuck** (1 `IN_PROGRESS`, 3 `WAITING_JOIN` with a branch that never finished), and 7 duplicate downstream calls. Recovery mostly works. The 4 stuck executions are G8.

---

## 3. Load and scalability

### Throughput vs processes (`s3`, 3,000 chain workflows submitted at once, 8 workers per process)
| Processes | Workflows/s | Trama CPU (sum of cores×100) | Redis ops/s (peak) | Postgres commits/s (peak) | Postgres connections |
|---|---|---|---|---|---|
| 1 | 95 | 154% | 51k | 357 | 12 |
| 2 | **131** | 369% | 55k | 614 | 18 |
| 4 | 103 | 525% | 32k | 548 | 34 |
| 8 | 74 | 463% | 54k | 281 | 67 |

- **Bottleneck: polling (G7).** For 3,000 workflows (9,000 node executions), Redis served ~200k `EVALSHA` claim scans and ~381k `ZRANGEBYSCORE`. Claimers keep scanning all owned shards (1,024 virtual shards) every 50ms whether or not there is work. That is ~16ms of Trama CPU per 3-node workflow. Past 2 processes the host's 8 cores are saturated by Trama, Redis and Postgres together, and throughput drops.
- **Postgres is not a bottleneck:** the top statements are the finalization inserts at 0.1–0.3ms each, and commits/s stay low.
- **Memory:** ~430–490 MB RSS per process, stable.
- **Latency:** with steady load below capacity (`s3y`, 10 mixed workflows/s, each with a 2s sleep and a 1s callback), end-to-end time is P50 2.2s / P95 2.6s / P99 4.3s. Orchestration overhead is a few hundred ms. In the closed-loop runs above, latency is queueing time for the 3,000-workflow backlog.

### Configuration hazards
- **G3, pool deadlock (`s3x`):** with the default `redis.pool.maxTotal=16`, 64 concurrent submitters → 200/200 requests time out, `/healthz` stops answering, and 64 threads are parked in `GenericObjectPool.borrowObject`. With a pool of 256 the same load passes in ~2s. Capacity is tied to pool size and fails as a hard freeze instead of degrading.
- **G6, rate limiter (`s3y`, 10% business failures):**

| Rate limiter | Workflows/s | P50 | P95 | Unfinished after 400s |
|---|---|---|---|---|
| on (default) | 1.3 | 86s | 404s | 27 |
| off | 9.7 | 2.2s | 2.6s | 0 |

---

## 4. Load combined with faults (`s4`)
5,000 mixed workflows at 30/s on 4 processes. Timeline: worker killed at t=60s; Redis cut 10s at t=120s; Postgres cut 10s at t=180s; worker back at t=204s.

| Submitted | Rejected at submit | Succeeded / failed (expected) | Lost | Stuck |
|---|---|---|---|---|
| 5,000 | 300 | 3,238 / 172 | 865 | 425 |

- The kill at t=60s is absorbed.
- At the Redis cut (t=120s) every surviving process stops consuming for good (G1): dequeue counters freeze, and only the worker restarted at t=204s consumes again, and only its own shards.
- The Postgres cut's own effect cannot be separated until G1 is fixed. **This scenario should be re-run after G1/G2 are fixed** to measure real recovery times.

---

## Recommended next steps (fix plan input)
1. **G1 + G2:**
   - Claim, heartbeat and scanner loops survive Redis errors with backoff.
   - `NOSCRIPT` triggers a script reload.
   - Readiness reflects consumer liveness.
   - Re-run `s1a`, `s1b` and `s4`.
2. **G3:** stop pooling around suspending code. Lettuce connections are thread-safe and multiplexed for non-transactional commands; alternatively bound the wait and move blocking off coroutine threads. Re-run `s3x`.
3. **G4:** fencing token per claim, and compare-and-set terminal transitions. Re-run `s2b`.
4. **G5:** decide the durability contract. Options:
   - persist execution state to Postgres at each step boundary (Redis as cache and queue only);
   - reconcile Redis from Postgres on loss.
   - At minimum, require AOF and document the guarantee.
5. **G6:** count only infrastructure failures, or scope the breaker per downstream; never block on expected business failures. Make it opt-in.
6. **G7:** replace idle polling with notification (e.g. blocking pops or streams) or adaptive per-shard backoff. Re-run `s3`.
7. **G8:** isolate the residual stuck executions (kill and restart traces).
8. **G9 / G10:** bound claims to free worker capacity. Fix or remove the API-only profile.

Results for every scenario are in `loadtest/results/<scenario>/` (`summary.json`, `metrics.csv`, `timeline.txt`, `warnings-summary.tsv`, `pgtop.tsv`, `redis-commandstats.txt`).
