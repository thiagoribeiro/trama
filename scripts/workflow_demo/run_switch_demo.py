#!/usr/bin/env python3
"""
Switch node (branching) demo for Trama v2 workflow definitions.

Submits a v2 saga definition with a `switch` node that routes to a
different payment task depending on `payload.paymentMethod`.

  pix   → pix-payment  → notify
  card  → card-payment → notify
  <any> → fallback     → notify

Each scenario is run and the terminal status plus visited step names
are printed so you can confirm the correct branch was taken.

Requirements:
    pip install -r requirements.txt
    # flask_server.py must be running on SAGA_SERVICE (default 127.0.0.1:5002)
    # Trama orchestrator must be running on SAGA_API (default 127.0.0.1:8080)

Usage:
    python run_switch_demo.py [--scenario pix|card|boleto|all]
"""
from __future__ import annotations

import argparse
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

    def task(node_id: str, path: str, next_node: str | None = None) -> dict:
        node: dict = {
            "kind": "task",
            "id": node_id,
            "action": {
                "mode": "sync",
                "request": {
                    "url": f"{svc}/step/{path}?phase=up",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": {
                        "orderId": "{{payload.orderId}}",
                        "method": "{{payload.paymentMethod}}",
                        "node": node_id,
                    },
                },
            },
            "compensation": {
                "url": f"{svc}/step/{path}?phase=down",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {"node": node_id, "action": "compensate"},
            },
        }
        if next_node is not None:
            node["next"] = next_node
        return node

    return {
        "name": "checkout-v2",
        "version": "v1",
        "failureHandling": {"type": "retry", "maxAttempts": 2, "delayMillis": 100},
        "entrypoint": "choose-payment",
        "nodes": [
            # Switch node: inspect payload.paymentMethod, route to the right task
            {
                "kind": "switch",
                "id": "choose-payment",
                "cases": [
                    {
                        "name": "pix",
                        "when": {"==": [{"var": "input.paymentMethod"}, "pix"]},
                        "target": "pix-payment",
                    },
                    {
                        "name": "card",
                        "when": {"==": [{"var": "input.paymentMethod"}, "card"]},
                        "target": "card-payment",
                    },
                ],
                "default": "fallback-payment",
            },
            task("pix-payment",      "pix-payment",      next_node="notify"),
            task("card-payment",     "card-payment",     next_node="notify"),
            task("fallback-payment", "fallback-payment", next_node="notify"),
            # Terminal notification step (shared by all branches)
            task("notify", "notify"),
        ],
        "onSuccessCallback": {
            "url": f"{svc}/callback/success",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"status\":\"ok\",\"orderId\":\"{{payload.orderId}}\"}",
        },
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CORRUPTED"}


def wait_for_terminal(api: SagaApi, saga_id: str, timeout_s: int = 30) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        resp = api.get(f"/sagas/{saga_id}")
        if resp.status_code == 204:
            time.sleep(0.2)
            continue
        resp.raise_for_status()
        last = resp.json()
        if last.get("status") in TERMINAL_STATUSES:
            return last
        time.sleep(0.2)
    raise TimeoutError(f"saga {saga_id} did not reach terminal status; last={last}")


def register_definition(api: SagaApi, definition: dict) -> None:
    resp = api.post("/sagas/definitions", definition)
    if resp.status_code not in (200, 201, 409):
        raise RuntimeError(f"failed to register definition: {resp.status_code} {resp.text}")


# ─── Scenarios ────────────────────────────────────────────────────────────────

SCENARIOS = {
    "pix": {
        "paymentMethod": "pix",
        "orderId": "ord-001",
        "expected_branch": "pix-payment",
    },
    "card": {
        "paymentMethod": "card",
        "orderId": "ord-002",
        "expected_branch": "card-payment",
    },
    "boleto": {
        "paymentMethod": "boleto",      # no matching case → default
        "orderId": "ord-003",
        "expected_branch": "fallback-payment",
    },
}


def run_scenario(api: SagaApi, definition: dict, name: str, scenario: dict) -> bool:
    print(f"\n{'='*60}")
    print(f"Scenario: {name!r}  (paymentMethod={scenario['paymentMethod']!r})")
    print(f"Expected branch: {scenario['expected_branch']}")
    print("─" * 60)

    resp = api.post("/sagas/run", {
        "definition": definition,
        "payload": {
            "orderId": scenario["orderId"],
            "paymentMethod": scenario["paymentMethod"],
        },
    })
    if resp.status_code not in (200, 201, 202):
        print(f"FAIL — could not start saga: {resp.status_code} {resp.text}")
        return False

    saga_id = resp.json()["id"]
    print(f"Saga started: {saga_id}")

    final = wait_for_terminal(api, saga_id)
    status = final.get("status")
    print(f"Final status: {status}")

    ok = status == "SUCCEEDED"
    print("PASS" if ok else "FAIL")
    return ok


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS.keys(), "all"],
        default="all",
        help="Which scenario to run (default: all)",
    )
    parser.add_argument("--saga-api",     default=os.getenv("SAGA_API",     "http://127.0.0.1:8080"))
    parser.add_argument("--service-url",  default=os.getenv("SAGA_SERVICE", "http://127.0.0.1:5002"))
    args = parser.parse_args()

    api = SagaApi(args.saga_api)
    definition = make_definition(args.service_url)

    print("Registering v2 definition …")
    register_definition(api, {k: v for k, v in definition.items() if k != "onSuccessCallback"})

    to_run = SCENARIOS if args.scenario == "all" else {args.scenario: SCENARIOS[args.scenario]}
    results = []
    for name, scenario in to_run.items():
        passed = run_scenario(api, definition, name, scenario)
        results.append((name, passed))

    print(f"\n{'='*60}")
    print("Summary")
    print("─" * 60)
    all_passed = True
    for name, passed in results:
        label = "PASS" if passed else "FAIL"
        print(f"  {label}  {name}")
        all_passed = all_passed and passed

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
