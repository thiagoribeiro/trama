# Saga Demo Scripts

These scripts spin up a local Flask service that the saga executor calls, then create/run saga definitions against the Trama API and validate the outcome.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/saga_demo/requirements.txt
```

## 1) Start the local service

```bash
python scripts/saga_demo/flask_server.py
```

Optional failure controls (env overrides, or use scenario payload headers automatically):

```bash
SAGA_DEMO_FAIL_STEP=charge SAGA_DEMO_FAIL_PHASE=up SAGA_DEMO_FAIL_MODE=once \
  python scripts/saga_demo/flask_server.py
```

## 2) Run saga scenarios

```bash
python scripts/saga_demo/run_saga.py --scenario success
python scripts/saga_demo/run_saga.py --scenario fail_up
python scripts/saga_demo/run_saga.py --scenario fail_down
```

Configurable endpoints (defaults shown):

```bash
SAGA_API=http://127.0.0.1:8080 \
SAGA_SERVICE=http://127.0.0.1:5001 \
python scripts/saga_demo/run_saga.py --scenario success
```

## Expected results

- `success` prints a final status of `SUCCEEDED`.
- `fail_up` or `fail_down` should end in `FAILED` or `CORRUPTED` (depending on compensation flow).

Note:
- The `charge` step now references the previous step response via templates (e.g. `{{step.0.up.body.step}}`). This requires the Trama runtime to be running with DB persistence enabled so step results are available for rendering.
- Your Trama runtime must be running and configured to connect to Redis/Postgres as usual.
