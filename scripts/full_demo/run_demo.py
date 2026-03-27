#!/usr/bin/env python3
"""
Full v2 demo — runs the checkout-full-demo saga and watches it complete.

Demonstrates all three v2 node types in a single saga:
  - sync    : validate, pix-payment or notify
  - switch  : choose-payment (routes on paymentMethod from payload)
  - async   : card-payment (server fires callback after 5 s)

The payment method is chosen randomly each run, so you can see different
execution paths across runs:

    Run 1 → validate → choose-payment ─[pix]──► pix-payment  → notify → SUCCEEDED
    Run 2 → validate → choose-payment ─[card]─► card-payment → notify → SUCCEEDED
                                                  (waits 5 s for callback)

Usage:
    # Terminal 1 — start mock service
    python server.py

    # Terminal 2 — run the demo
    python run_demo.py

    # Force a specific payment method
    python run_demo.py --payment-method pix
    python run_demo.py --payment-method card

Requirements:
    pip install -r requirements.txt
    Trama orchestrator running on SAGA_API (default http://127.0.0.1:8080)
    server.py running on SAGA_SERVICE   (default http://127.0.0.1:5003)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid

import requests

# Import the static definition — it is defined once and shared between scripts.
from save_definition import DEFINITION, SERVICE_URL

# ─── API client ───────────────────────────────────────────────────────────────

class SagaApi:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def post(self, path: str, body: dict) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", json=body, timeout=10)

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", timeout=10)


# ─── Polling ──────────────────────────────────────────────────────────────────

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CORRUPTED"}


def wait_for_terminal(api: SagaApi, saga_id: str, timeout_s: int = 90) -> dict:
    """Poll GET /sagas/{id} and print each status transition until terminal."""
    deadline   = time.time() + timeout_s
    prev_status = None

    while time.time() < deadline:
        resp = api.get(f"/sagas/{saga_id}")
        if resp.status_code == 204:
            time.sleep(0.5)
            continue
        resp.raise_for_status()

        state  = resp.json()
        status = state.get("status")

        if status != prev_status:
            _print_status(status)
            prev_status = status

        if status in TERMINAL_STATUSES:
            return state

        time.sleep(0.5)

    raise TimeoutError(f"saga {saga_id} did not reach terminal status within {timeout_s}s")


def _print_status(status: str) -> None:
    annotations = {
        "IN_PROGRESS":       "executing nodes …",
        "RETRYING":          "a node failed, retrying …",
        "WAITING_CALLBACK":  "async node paused — waiting for callback …",
        "SUCCEEDED":         "all nodes completed ✓",
        "FAILED":            "saga failed, compensations ran",
        "CORRUPTED":         "internal error",
    }
    note = annotations.get(status, "")
    print(f"    status → {status:<20}  {note}")


# ─── Health check ─────────────────────────────────────────────────────────────

def check_service(url: str) -> bool:
    try:
        resp = requests.get(f"{url.rstrip('/')}/healthz", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--saga-api",       default=os.getenv("SAGA_API",     "http://127.0.0.1:8080"))
    parser.add_argument("--service-url",    default=os.getenv("SAGA_SERVICE", SERVICE_URL))
    parser.add_argument("--order-id",       default=f"ord-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--payment-method", choices=["pix", "card"],
                        help="Force a payment method. Omit to choose randomly.")
    args = parser.parse_args()

    payment_method = args.payment_method or random.choice(["pix", "card"])
    api = SagaApi(args.saga_api)

    # ── Preflight ─────────────────────────────────────────────────────────────
    if not check_service(args.service_url):
        print(f"ERROR: mock service not reachable at {args.service_url}")
        print("       Start it first:  python server.py")
        return 1

    # ── Banner ────────────────────────────────────────────────────────────────
    print()
    print("━" * 60)
    print("  Trama full-demo — all v2 node types in one saga")
    print("━" * 60)
    print(f"  Orchestrator  : {args.saga_api}")
    print(f"  Mock service  : {args.service_url}")
    print(f"  Order ID      : {args.order_id}")
    print(f"  Payment       : {payment_method.upper()}")
    print()

    if payment_method == "pix":
        print("  Flow  →  validate  ──[pix]──►  pix-payment  →  notify")
    else:
        print("  Flow  →  validate  ──[card]─►  card-payment  →  notify")
        print(f"           (card-payment fires callback after {os.getenv('SAGA_DEMO_ASYNC_DELAY', '5')} s)")
    print()

    # ── Start saga ────────────────────────────────────────────────────────────
    print("Starting saga …")
    resp = api.post("/sagas/run", {
        "definition": DEFINITION,
        "payload": {
            "orderId":       args.order_id,
            "amount":        "149.90",
            "paymentMethod": payment_method,   # the switch reads this
        },
    })

    if resp.status_code not in (200, 201, 202):
        print(f"FAIL — orchestrator returned {resp.status_code}: {resp.text}")
        return 1

    saga_id = resp.json()["id"]
    print(f"  Saga ID : {saga_id}")
    print()

    # ── Watch ─────────────────────────────────────────────────────────────────
    print("Watching execution …")
    try:
        final = wait_for_terminal(api, saga_id)
    except TimeoutError as exc:
        print(f"\nFAIL — {exc}")
        return 1

    # ── Result ────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print(json.dumps(final, indent=2))
    print()

    status = final.get("status")
    if status == "SUCCEEDED":
        print(f"PASS — saga SUCCEEDED via {payment_method.upper()} path")
        return 0
    else:
        desc = final.get("failureDescription") or ""
        print(f"FAIL — saga ended with status={status}  {desc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
