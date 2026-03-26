# Trama

> Stop building orchestration logic inside your services.

Trama is a lightweight way to orchestrate distributed workflows using HTTP — without heavy infrastructure, complex runtimes, or vendor lock-in.

---

## The problem

Most systems don’t lack orchestration — they implement it implicitly.

Teams start with event-driven choreography:

- Service A emits an event  
- Service B reacts and emits another event  
- Service C continues the flow  

At first, it works.

As complexity grows, teams start adding:

- retries inside each service  
- ad-hoc logic to handle async flows  
- cron jobs to recover inconsistencies  

Over time, this becomes an **implicit workflow engine**:

- no single place to understand the flow  
- no clear execution state  
- hard to debug and reason about  
- difficult to evolve safely  

---

## The solution

Make orchestration explicit.

Define your workflow as JSON. Trama handles execution, retries, async callbacks, and state.

---

## Example

A payment flow with async authorization and sync capture:

```json
{
  "name": "payment-flow",
  "version": "v1",
  "entrypoint": "authorize",
  "nodes": [
    {
      "kind": "task",
      "id": "authorize",
      "action": {
        "mode": "async",
        "request": {
          "url": "http://payments/authorize",
          "verb": "POST",
          "body": {
            "orderId": "{{payload.orderId}}",
            "callbackUrl": "{{runtime.callback.url}}",
            "callbackToken": "{{runtime.callback.token}}"
          }
        },
        "acceptedStatusCodes": [202],
        "callback": {
          "timeoutMillis": 30000
        }
      },
      "next": "capture"
    },
    {
      "kind": "task",
      "id": "capture",
      "action": {
        "mode": "sync",
        "request": {
          "url": "http://payments/capture",
          "verb": "POST"
        }
      }
    }
  ]
}
```

👉 No polling. No cron. No hidden state machines.

---

## Why not just use events and queues?

Event-driven systems are great — but choreography has limits.

As flows become complex, you end up with:

- implicit execution order  
- duplicated retry logic  
- inconsistent recovery strategies  
- no global visibility of the workflow  

You already built an orchestrator — just not an explicit one.

---

## Why not Temporal?

Temporal is powerful — but often too heavy for most teams.

It introduces:

- new programming model  
- dedicated infrastructure  
- operational complexity  

Trama focuses on a different tradeoff:

- minimal setup  
- HTTP-first integration  
- simple mental model  
- fast adoption  

---

## Quick start

```bash
docker compose up --build
```

API: http://localhost:8080

---

## When NOT to use Trama

Trama is not for every case.

Avoid it if:

- you need long-running workflows (days or weeks)  
- you need full event sourcing  
- you already run Temporal/Cadence successfully  

---

## Core capabilities

- JSON-defined workflows (v1 linear, v2 node graph)
- branching with JSON Logic (`switch` nodes)
- async HTTP tasks with callback resumption
- retries and compensation strategies
- Redis-backed execution queue
- Postgres persistence
- OpenTelemetry tracing
- Prometheus metrics

---

## Architecture (simplified)

```mermaid
flowchart LR
  C[Client] --> A[API]
  A --> Q[Queue]
  Q --> P[Processor]
  P --> E[Executor]
  E --> S[Services]
  E --> ST[State Store]
```

---

## Usage

### Run a workflow

```bash
curl -X POST http://localhost:8080/sagas/run \
  -H 'Content-Type: application/json' \
  -d '{
    "definition": { ... },
    "payload": { ... }
  }'
```

### Check status

```bash
curl http://localhost:8080/sagas/<execution-id>
```

---

## Definition formats

Trama supports two formats:

### v1 — Linear steps
Simple sequential workflows with compensation.

### v2 — Node graph
Supports:

- branching (`switch`)
- async tasks
- DAG-style execution

---

## Async callbacks

Async tasks pause execution and resume via callback.

Trama injects:

- `callbackUrl`
- `callbackToken` (HMAC signed)

The external service must call back using:

```
X-Callback-Token: <token>
```

---

## Observability

- Prometheus metrics at `/metrics`
- OpenTelemetry tracing
- Execution-level visibility

---

## Development

```bash
./gradlew run
```

---

## License

Apache License 2.0
