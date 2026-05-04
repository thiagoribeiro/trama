#!/usr/bin/env python3
"""
Mock service for the workflow v2 demo.

Handles both synchronous task steps and asynchronous task steps.
For async steps the service returns 202 immediately and fires the saga
callback in a background thread after an optional delay, simulating a
real external system that processes work and then calls back.

Usage:
    cd scripts/workflow_demo
    pip install -r requirements.txt
    python flask_server.py

Environment variables:
    SAGA_DEMO_HOST        bind host (default: 127.0.0.1)
    SAGA_DEMO_PORT        bind port (default: 5002)
    SAGA_DEMO_ASYNC_DELAY delay in seconds before firing callbacks (default: 1)
    SAGA_DEMO_DEBUG       set to "true" for Flask debug mode
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

SERVICE_NAME = os.getenv("SAGA_DEMO_SERVICE", "workflow-demo-service")
ASYNC_DELAY = float(os.getenv("SAGA_DEMO_ASYNC_DELAY", "1"))


def _response(step: str, phase: str, status: int, extra: dict | None = None) -> tuple[Any, int]:
    body: dict[str, Any] = {
        "service": SERVICE_NAME,
        "step": step,
        "phase": phase,
        "status": status,
        "timestamp": time.time(),
    }
    if extra:
        body.update(extra)
    return jsonify(body), status


# ─── Synchronous step ─────────────────────────────────────────────────────────

@app.route("/step/<step>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def handle_step(step: str):
    """Generic synchronous step endpoint used by TaskNodes with mode=sync."""
    phase = request.args.get("phase", "up")
    body = request.get_json(silent=True) or {}
    print(f"[sync] {step} phase={phase} body={body}")
    return _response(step, phase, 200, {"result": f"{step}-{phase}-ok"})


# ─── Asynchronous step ────────────────────────────────────────────────────────

@app.route("/async-step/<step>", methods=["POST"])
def handle_async_step(step: str):
    """
    Async step endpoint used by TaskNodes with mode=async.

    The saga orchestrator injects runtime.callback.url and
    runtime.callback.token into the request body via template substitution.
    This handler immediately returns 202, then fires the callback from a
    background thread after SAGA_DEMO_ASYNC_DELAY seconds.
    """
    body = request.get_json(silent=True) or {}
    callback_url = body.get("callbackUrl", "")
    callback_token = body.get("callbackToken", "")
    print(f"[async] {step} received — callback_url={callback_url!r}")

    if not callback_url or not callback_token:
        print(f"[async] {step} missing callbackUrl or callbackToken — cannot fire callback")
        return _response(step, "up", 202)

    def fire_callback() -> None:
        time.sleep(ASYNC_DELAY)
        try:
            resp = requests.post(
                callback_url,
                json={"status": "success", "step": step, "processedBy": SERVICE_NAME},
                headers={
                    "Content-Type": "application/json",
                    "X-Callback-Token": callback_token,
                },
                timeout=10,
            )
            print(f"[async] callback fired → {resp.status_code}")
        except Exception as exc:
            print(f"[async] callback failed: {exc}")

    threading.Thread(target=fire_callback, daemon=True).start()
    return _response(step, "up", 202)


# ─── On-completion callbacks ──────────────────────────────────────────────────

@app.route("/callback/<name>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def handle_completion_callback(name: str):
    """Receives onSuccessCallback / onFailureCallback from the orchestrator."""
    body = request.get_json(silent=True) or {}
    print(f"[completion-callback] name={name} body={body}")
    return _response(f"callback-{name}", "n/a", 200)


# ─── Health ───────────────────────────────────────────────────────────────────

@app.route("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    host = os.getenv("SAGA_DEMO_HOST", "127.0.0.1")
    port = int(os.getenv("SAGA_DEMO_PORT", "5002"))
    debug = os.getenv("SAGA_DEMO_DEBUG", "false").lower() == "true"
    print(f"Workflow demo service listening on {host}:{port}")
    app.run(host=host, port=port, debug=debug)
