# Trama validation harness

Reproducible durability, recovery and load scenarios for Trama, run locally with podman.
Results feed [`docs/validation-report.md`](../docs/validation-report.md).

## Requirements
- JDK 21, podman, `curl`
- Images: `postgres:15-alpine`, `redis:7-alpine`, `ghcr.io/shopify/toxiproxy:2.9.0`
- Free host ports: 55432–55433 (Postgres), 56379–56380 (Redis), 8474 (Toxiproxy), 7070 (mock), 9100+ (Trama)

## Pieces
| Piece | What it does |
|---|---|
| `stack.sh` | Postgres (with `pg_stat_statements`), Redis (`none`/`rdb`/`aof` persistence) and Toxiproxy in front of both. Fault commands: `redis-restart`, `redis-flush`, `cut`/`restore redis\|pg`, `latency redis\|pg MS`. |
| `trama.sh` | Builds (`installDist`) and runs N Trama processes on ports 9100+i, all reaching Redis/Postgres **through Toxiproxy**. Process 0 receives runs and callbacks and is never killed by scenarios. `kill`, `pause`/`resume` (SIGSTOP/SIGCONT), `add`. |
| `app/loadtest/kotlin` | The JVM tools, run through `lib.sh`'s `harness` function: `mock` (downstream service that counts every effect per run/node/phase), `drive` (submits workflows), `collect` (process/Redis/Postgres/queue metrics every 5s), `check` (outcomes and invariants), `park` (executions parked in each state), `pgtop`. |
| `scenarios/*.sh` | One script per validation scenario (see header comments). Each one starts a fresh stack and writes `results/$RESULTS_SET/<scenario>/` (`RESULTS_SET` defaults to `v2.1.0`; `results/v2.0.1/` holds the first run). |

## Run
```bash
loadtest/trama.sh build              # once, and after code changes
loadtest/scenarios/smoke.sh          # harness self-test: expect 0 stuck, lost and duplicate calls
loadtest/scenarios/s2a-kill-inflight.sh
loadtest/run-all.sh                  # everything, ~2h (runs under systemd-inhibit)
```

## Settings
Every Trama setting is its default except `runtime.workerCount`, which each scenario sets. `RATE_LIMIT=true|false` overrides the failure breaker (used by `s3y-rate-limit.sh`).
The v2.0.1 run raised the Redis pool to 256 and turned the rate limiter off, because those defaults were themselves findings (G3, G6); both are fixed in v2.1.0.

## Workflows used
- **chain**: `t1 → t2 → t3`. `t3` fails when `payload.fail`, which compensates `t1` and `t2`.
- **mixed**: `t0 → split[b-sync | b-async (callback) | b-sleep → b-sleep-task] → join → final`. `final` fails when `payload.fail`, which compensates `t0`.
- `failureHandling` allows no retries, so every node should hit the mock exactly once per phase. Any extra call the mock records is an effect duplicated by a fault.

## Reading results
`results/<set>/<scenario>/summary.json` (from `check`):

| Field | Meaning |
|---|---|
| `status`, `statusByDef` | Final status of each submitted execution (`MISSING` = no row in Postgres, so the execution is lost). |
| `stuck` | Non-terminal executions after the wait. |
| `lost` | Executions with no row in Postgres. |
| `wrongOutcome` | Terminal executions with an unexpected status. |
| `extraExternalCalls` / `runsWithDuplicateCalls` | Downstream effects that happened more than once. |
| `missingEffectsOnCorrectOutcome` | Executions with the right status but a downstream call that never happened. |
| `duplicateStepRows` | The same step recorded twice in `saga_step_result`. |
| `latencyMs`, `throughputPerSec` | End to end, from `started_at` to `completed_at`. |

Other files:
- `metrics.csv`: long format (`epochMs,source,metric,value`) for CPU, RSS, queue counters, Redis ops and queue depth, and Postgres commits.
- `pgtop.tsv`, `redis-commandstats.txt`: where database and Redis time went.
- `timeline.txt`: when each fault was injected.
- `trama-*-warnings.log`: WARN and ERROR lines from each process.
