#!/usr/bin/env python3
"""
Save the full-demo saga definition to the orchestrator.

The definition covers all three v2 node kinds in a single flow:

    validate (sync)
        └─► choose-payment (switch, routes on payload.paymentMethod)
                ├─ "pix"  ──► pix-payment  (sync, instant)    ──► notify (terminal)
                └─ "card" ──► card-payment (async, 5 s delay)  ──► notify (terminal)

Usage:
    python save_definition.py [--saga-api http://127.0.0.1:8080]

After saving, the definition can be inspected at:
    GET /sagas/definitions/<returned-id>
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

# ─── Service base URL ─────────────────────────────────────────────────────────

# The mock server (server.py) runs here by default.
SERVICE_URL = os.getenv("SAGA_SERVICE", "http://127.0.0.1:5003")

# ─── Static saga definition ───────────────────────────────────────────────────
#
# Written out in full — no helper functions, no loops.
# Every field is visible exactly as the orchestrator receives it.

DEFINITION = {
    "name": "checkout-full-demo",
    "version": "v1",

    # Retry once before giving up and running compensations.
    "failureHandling": {
        "type": "retry",
        "maxAttempts": 2,
        "delayMillis": 500,
    },

    "entrypoint": "validate",

    "nodes": [

        # ── 1. validate (sync) ────────────────────────────────────────────────
        # A straightforward sync HTTP call. Validates the order exists and is
        # ready to be charged. Has a compensation step in case a later node fails.
        {
            "kind": "task",
            "id": "validate",
            "action": {
                "mode": "sync",
                "request": {
                    "url": f"{SERVICE_URL}/step/validate",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    # {{payload.*}} injects values from the caller's payload.
                    "body": {
                        "orderId": "{{payload.orderId}}",
                        "amount":  "{{payload.amount}}",
                    },
                },
            },
            "compensation": {
                "url": f"{SERVICE_URL}/step/validate?phase=down",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {"orderId": "{{payload.orderId}}"},
            },
            "next": "choose-payment",
        },

        # ── 2. choose-payment (switch) ────────────────────────────────────────
        # Routes execution based on the caller's payload.paymentMethod.
        # In the switch context, payload fields are accessed via `input.*`.
        # `default` is required and acts as a fallback if no case matches.
        {
            "kind": "switch",
            "id": "choose-payment",
            "cases": [
                {
                    "name": "pix",
                    # json-logic: input.paymentMethod == "pix"
                    "when": {"==": [{"var": "input.paymentMethod"}, "pix"]},
                    "target": "pix-payment",
                },
                {
                    "name": "card",
                    # json-logic: input.paymentMethod == "card"
                    "when": {"==": [{"var": "input.paymentMethod"}, "card"]},
                    "target": "card-payment",
                },
            ],
            # Fallback: if neither case matches, go to pix-payment.
            "default": "pix-payment",
        },

        # ── 3a. pix-payment (sync) ────────────────────────────────────────────
        # Taken when paymentMethod == "pix". Instant synchronous HTTP call.
        {
            "kind": "task",
            "id": "pix-payment",
            "action": {
                "mode": "sync",
                "request": {
                    "url": f"{SERVICE_URL}/step/pix-payment",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": {
                        "orderId": "{{payload.orderId}}",
                        "method":  "pix",
                    },
                },
            },
            "compensation": {
                "url": f"{SERVICE_URL}/step/pix-payment?phase=down",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {"orderId": "{{payload.orderId}}"},
            },
            "next": "notify",
        },

        # ── 3b. card-payment (async) ──────────────────────────────────────────
        # Taken when paymentMethod == "card".
        #
        # The orchestrator injects two template variables into the request body:
        #   {{runtime.callback.url}}   — the URL the service must POST to when done
        #   {{runtime.callback.token}} — a signed token to include as X-Callback-Token
        #
        # The service returns 202 immediately, processes in the background,
        # and fires the callback after it is done (here: 5 seconds later).
        {
            "kind": "task",
            "id": "card-payment",
            "action": {
                "mode": "async",
                "request": {
                    "url": f"{SERVICE_URL}/async-step/card-payment",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": {
                        "orderId":       "{{payload.orderId}}",
                        "method":        "card",
                        # These two are injected by the orchestrator at runtime.
                        "callbackUrl":   "{{runtime.callback.url}}",
                        "callbackToken": "{{runtime.callback.token}}",
                    },
                },
                # The orchestrator expects this status code to confirm acceptance.
                "acceptedStatusCodes": [202],
                "callback": {
                    # How long to wait for the callback before failing this node.
                    "timeoutMillis": 60000,
                },
            },
            "compensation": {
                "url": f"{SERVICE_URL}/step/card-payment?phase=down",
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": {"orderId": "{{payload.orderId}}"},
            },
            "next": "notify",
        },

        # ── 4. notify (sync, terminal) ────────────────────────────────────────
        # Shared final step for both payment branches.
        # No `next` field → this is the terminal node; saga ends here.
        {
            "kind": "task",
            "id": "notify",
            "action": {
                "mode": "sync",
                "request": {
                    "url": f"{SERVICE_URL}/step/notify",
                    "verb": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": {"orderId": "{{payload.orderId}}"},
                },
            },
        },
    ],

    # ── Completion webhooks ───────────────────────────────────────────────────
    # The orchestrator calls these after the saga reaches a terminal state.
    "onSuccessCallback": {
        "url": f"{SERVICE_URL}/callback/success",
        "verb": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": {
            "orderId": "{{payload.orderId}}",
            "result":  "succeeded",
        },
    },
    "onFailureCallback": {
        "url": f"{SERVICE_URL}/callback/failure",
        "verb": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": {
            "orderId": "{{payload.orderId}}",
            "result":  "failed",
        },
    },
}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--saga-api", default=os.getenv("SAGA_API", "http://127.0.0.1:8080"))
    args = parser.parse_args()

    print("Saving definition to orchestrator …")
    print(f"  API        : {args.saga_api}")
    print(f"  Service    : {SERVICE_URL}")
    print(f"  Name       : {DEFINITION['name']}")
    print()

    resp = requests.post(
        f"{args.saga_api}/sagas/definitions",
        json=DEFINITION,
        timeout=10,
    )

    if resp.status_code not in (200, 201):
        print(f"FAIL — {resp.status_code}: {resp.text}")
        return 1

    data = resp.json()
    print("Definition saved.")
    print(f"  ID      : {data.get('id')}")
    print(f"  Name    : {data.get('name')} / {data.get('version')}")
    print()
    print("To inspect it:")
    print(f"  curl {args.saga_api}/sagas/definitions/{data.get('id')}")
    print()
    print("To run the demo:")
    print(f"  python run_demo.py --saga-api {args.saga_api}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
