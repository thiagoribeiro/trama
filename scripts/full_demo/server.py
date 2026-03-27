#!/usr/bin/env python3
"""
Mock service for the full-demo saga.

Handles all step types used by the checkout-full-demo saga definition:
  - Synchronous steps  (validate, pix-payment, notify, compensations)
  - Async step         (card-payment: returns 202, fires callback after 5 s)
  - Completion hooks   (onSuccessCallback, onFailureCallback)

Every request is logged with its step name, phase, and key body fields
so you can follow the saga's progress in the terminal.

Usage:
    pip install -r requirements.txt
    python server.py

Environment variables:
    SAGA_DEMO_HOST         bind host          (default: 127.0.0.1)
    SAGA_DEMO_PORT         bind port          (default: 5003)
    SAGA_DEMO_ASYNC_DELAY  seconds before callback fires  (default: 5)
    SAGA_DEMO_DEBUG        "true" for Flask debug mode
"""
from __future__ import annotations

import os
import random
import threading
import time
from typing import Any

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

ASYNC_DELAY = float(os.getenv("SAGA_DEMO_ASYNC_DELAY", "5"))

# ─── Logging helpers ──────────────────────────────────────────────────────────

def _log(tag: str, msg: str) -> None:
    print(f"  [{tag:<16}] {msg}", flush=True)


def _response(step: str, status: int, extra: dict | None = None) -> tuple[Any, int]:
    body: dict[str, Any] = {"step": step, "status": status}
    if extra:
        body.update(extra)
    return jsonify(body), status


# ─── validate (sync) ──────────────────────────────────────────────────────────

@app.route("/step/validate", methods=["POST"])
def step_validate():
    """
    Validates the order. Always succeeds.
    The payment method comes from the caller's payload, not from here.
    """
    phase = request.args.get("phase", "up")
    body  = request.get_json(silent=True) or {}
    order_id = body.get("orderId", "?")

    if phase == "down":
        _log("validate ↩", f"compensating order={order_id}")
        return _response("validate", 200, {"action": "compensated"})

    _log("validate ✓", f"order={order_id} amount={body.get('amount', '?')} → OK")
    return _response("validate", 200, {"valid": True})


# ─── pix-payment (sync) ───────────────────────────────────────────────────────

@app.route("/step/pix-payment", methods=["POST"])
def step_pix_payment():
    """Instant synchronous PIX payment step."""
    phase    = request.args.get("phase", "up")
    body     = request.get_json(silent=True) or {}
    order_id = body.get("orderId", "?")

    if phase == "down":
        _log("pix-payment ↩", f"compensating order={order_id}")
        return _response("pix-payment", 200, {"action": "compensated"})

    _log("pix-payment ✓", f"order={order_id} → charged via PIX")
    return _response("pix-payment", 200, {"charged": True, "method": "pix"})


# ─── card-payment (async) ─────────────────────────────────────────────────────

@app.route("/async-step/card-payment", methods=["POST"])
def async_step_card_payment():
    """
    Async card-payment step.

    Returns 202 immediately to acknowledge the request.
    The orchestrator pauses execution (WAITING_CALLBACK).

    A background thread fires the callback after SAGA_DEMO_ASYNC_DELAY seconds,
    simulating a real payment processor that takes time to authorize.

    The callback URL and token are injected by the orchestrator via:
        {{runtime.callback.url}}   → callbackUrl in request body
        {{runtime.callback.token}} → callbackToken in request body
    The token must be sent back as the X-Callback-Token header.
    """
    body           = request.get_json(silent=True) or {}
    order_id       = body.get("orderId", "?")
    callback_url   = body.get("callbackUrl", "")
    callback_token = body.get("callbackToken", "")

    _log("card-payment →", f"order={order_id} accepted, callback fires in {ASYNC_DELAY:.0f} s …")

    if not callback_url or not callback_token:
        _log("card-payment !", "missing callbackUrl/callbackToken — cannot resume saga")
        return _response("card-payment", 202)

    def fire_callback() -> None:
        time.sleep(ASYNC_DELAY)
        try:
            resp = requests.post(
                callback_url,
                json={"status": "approved", "orderId": order_id, "method": "card"},
                headers={
                    "Content-Type":    "application/json",
                    "X-Callback-Token": callback_token,
                },
                timeout=10,
            )
            _log("card-payment ✓", f"callback fired → {resp.status_code}")
        except Exception as exc:
            _log("card-payment ✗", f"callback failed: {exc}")

    threading.Thread(target=fire_callback, daemon=True).start()
    return _response("card-payment", 202, {"queued": True})


@app.route("/step/card-payment", methods=["POST"])
def step_card_payment_compensation():
    """Compensation for card-payment (reversal)."""
    body     = request.get_json(silent=True) or {}
    order_id = body.get("orderId", "?")
    _log("card-payment ↩", f"compensating (reversal) order={order_id}")
    return _response("card-payment", 200, {"action": "reversed"})


# ─── notify (sync, terminal) ──────────────────────────────────────────────────

@app.route("/step/notify", methods=["POST"])
def step_notify():
    """Final notification step. Shared by both payment branches."""
    body     = request.get_json(silent=True) or {}
    order_id = body.get("orderId", "?")
    _log("notify ✓", f"order={order_id} → customer notified")
    return _response("notify", 200, {"notified": True})


# ─── Completion callbacks ─────────────────────────────────────────────────────

@app.route("/callback/success", methods=["POST"])
def callback_success():
    """
    Called by the orchestrator when the saga reaches SUCCEEDED.
    This is onSuccessCallback from the definition.
    """
    body = request.get_json(silent=True) or {}
    _log("✅ SUCCESS", f"order={body.get('orderId', '?')} saga completed")
    return jsonify({"received": True}), 200


@app.route("/callback/failure", methods=["POST"])
def callback_failure():
    """
    Called by the orchestrator when the saga reaches FAILED.
    This is onFailureCallback from the definition.
    """
    body = request.get_json(silent=True) or {}
    _log("❌ FAILURE", f"order={body.get('orderId', '?')} saga failed")
    return jsonify({"received": True}), 200


# ─── Health ───────────────────────────────────────────────────────────────────

@app.route("/healthz")
def healthz():
    return "ok", 200


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    host  = os.getenv("SAGA_DEMO_HOST",  "127.0.0.1")
    port  = int(os.getenv("SAGA_DEMO_PORT", "5003"))
    debug = os.getenv("SAGA_DEMO_DEBUG", "false").lower() == "true"

    print()
    print("Full-demo mock service")
    print(f"  Listening on  : http://{host}:{port}")
    print(f"  Async delay   : {ASYNC_DELAY:.0f} s")
    print()
    print("Waiting for requests …")
    print()

    app.run(host=host, port=port, debug=debug)
