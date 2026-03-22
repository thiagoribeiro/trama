#!/usr/bin/env python3
"""
Async callback demo for Trama v2 workflow definitions.

Runs a v2 saga where the `authorize` node uses mode=async:
  1. The orchestrator sends a POST to the mock service and pauses.
  2. The mock service (flask_server.py) returns 202 immediately.
  3. After a short delay, the service fires a callback to the orchestrator.
  4. The orchestrator resumes and completes the saga.

The definition:

    authorize (async) → capture (sync) → notify (sync)

The orchestrator injects {{runtime.callback.url}} and
{{runtime.callback.token}} into the authorize request body. The mock
service reads those and calls back via POST /sagas/{id}/node/{node}/callback
with the X-Callback-Token header.

Requirements:
    pip install -r requirements.txt
    # flask_server.py must be running on SAGA_SERVICE (default 127.0.0.1:5002)
    # Trama must have runtime.callback.baseUrl pointing to this machine
    # Trama must be running on SAGA_API (default 127.0.0.1:8080)

Usage:
    # Terminal 1 — start mock service
    SAGA_DEMO_ASYNC_DELAY=2 python flask_server.py

    # Terminal 2 — run demo
    python run_async_demo.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import requests


# ─── API client ──────────────────────────────────────────────────────────────

class SagaApi:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def post(self, path: str, payload: dict) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", json=payload, timeout=10)

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", timeout=10)


# ─── Definition builder ───────────────────────────────────────────────────────

def make_definition(service_url: str) -> dict:
    svc = service_url.rstrip("/")

    def sync_task(node_id: str, next_node: str | None = None) -> dict:
        node: dict = {
            "kind": "task",
            "id": node_id,
            "action": {
                "mode": "sync",
                "request": {
                    "url": f"{svc}/step/{node_id}?phase=up",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": {
                        "orderId": "{{payload.orderId}}",
                        "node": node_id,
                    },
                },
            },
            "compensation": {
                "url": f"{svc}/step/{node_id}?phase=down",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {"node": node_id, "action": "compensate"},
            },
        }
        if next_node is not None:
            node["next"] = next_node
        return node

    # The authorize node is ASYNC. The orchestrator will inject callback URL
    # and token into the request body via template substitution. The mock
    # service (flask_server.py /async-step/<name>) reads those and fires the
    # callback after SAGA_DEMO_ASYNC_DELAY seconds.
    authorize_node: dict = {
        "kind": "task",
        "id": "authorize",
        "action": {
            "mode": "async",
            "request": {
                "url": f"{svc}/async-step/authorize",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {
                    "orderId": "{{payload.orderId}}",
                    "amount": "{{payload.amount}}",
                    # These template variables are injected by the orchestrator
                    # when mode=async. The mock service reads them to call back.
                    "callbackUrl": "{{runtime.callback.url}}",
                    "callbackToken": "{{runtime.callback.token}}",
                },
            },
            # acceptedStatusCodes: 202 is what the mock service returns
            "acceptedStatusCodes": [202],
            "callback": {
                "timeoutMillis": 30000,
            },
        },
        "compensation": {
            "url": f"{svc}/step/authorize?phase=down",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {"node": "authorize", "action": "compensate"},
        },
        "next": "capture",
    }

    return {
        "name": "payment-async-v2",
        "version": "v1",
        "failureHandling": {"type": "retry", "maxAttempts": 2, "delayMillis": 200},
        "entrypoint": "authorize",
        "nodes": [
            authorize_node,
            sync_task("capture", next_node="notify"),
            sync_task("notify"),
        ],
        "onSuccessCallback": {
            "url": f"{svc}/callback/success",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"status\":\"ok\",\"orderId\":\"{{payload.orderId}}\"}",
        },
        "onFailureCallback": {
            "url": f"{svc}/callback/failure",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"status\":\"failed\",\"orderId\":\"{{payload.orderId}}\"}",
        },
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CORRUPTED"}


def wait_for_terminal(api: SagaApi, saga_id: str, timeout_s: int = 60) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    prev_status = None
    while time.time() < deadline:
        resp = api.get(f"/sagas/{saga_id}")
        if resp.status_code == 204:
            time.sleep(0.5)
            continue
        resp.raise_for_status()
        last = resp.json()
        status = last.get("status")
        if status != prev_status:
            print(f"  status → {status}")
            prev_status = status
        if status in TERMINAL_STATUSES:
            return last
        time.sleep(0.5)
    raise TimeoutError(f"saga {saga_id} did not reach terminal status within {timeout_s}s; last={last}")


def check_service(service_url: str) -> bool:
    try:
        resp = requests.get(f"{service_url.rstrip('/')}/healthz", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--saga-api",    default=os.getenv("SAGA_API",     "http://127.0.0.1:8080"))
    parser.add_argument("--service-url", default=os.getenv("SAGA_SERVICE", "http://127.0.0.1:5002"))
    parser.add_argument("--order-id",   default="ord-async-001")
    parser.add_argument("--amount",     default="250.00")
    args = parser.parse_args()

    api = SagaApi(args.saga_api)
    definition = make_definition(args.service_url)

    # Verify mock service is reachable before starting
    if not check_service(args.service_url):
        print(f"ERROR: mock service not reachable at {args.service_url}")
        print("       Start it with: python flask_server.py")
        return 1

    print("=" * 60)
    print("Async callback workflow demo")
    print("=" * 60)
    print(f"  Orchestrator : {args.saga_api}")
    print(f"  Mock service : {args.service_url}")
    print(f"  Order ID     : {args.order_id}")
    print()
    print("Flow: authorize (async) → capture (sync) → notify (sync)")
    print()
    print("The authorize step returns 202 immediately.")
    print("The mock service will fire the callback after a short delay.")
    print("The orchestrator then resumes with capture and notify.")
    print()

    print("Starting saga …")
    resp = api.post("/sagas/run", {
        "definition": definition,
        "payload": {
            "orderId": args.order_id,
            "amount": args.amount,
        },
    })

    if resp.status_code not in (200, 201, 202):
        print(f"FAIL — could not start saga: {resp.status_code} {resp.text}")
        return 1

    saga_id = resp.json()["id"]
    print(f"Saga ID: {saga_id}")
    print()
    print("Waiting for saga to complete (callback fires after mock delay) …")

    try:
        final = wait_for_terminal(api, saga_id)
    except TimeoutError as exc:
        print(f"\nFAIL — {exc}")
        return 1

    print()
    print("─" * 60)
    print(json.dumps(final, indent=2))
    print()

    if final.get("status") == "SUCCEEDED":
        print("PASS — saga completed successfully via async callback")
        return 0
    else:
        print(f"FAIL — unexpected final status: {final.get('status')}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
