from __future__ import annotations

import json
import os
import time
from typing import Any
from flask import Flask, request, jsonify

app = Flask(__name__)

SERVICE_NAME = os.getenv("SAGA_DEMO_SERVICE", "local-saga-service")
FAIL_STEP = os.getenv("SAGA_DEMO_FAIL_STEP")  # e.g. "charge" or "reserve"
FAIL_PHASE = os.getenv("SAGA_DEMO_FAIL_PHASE", "up")  # up|down
FAIL_MODE = os.getenv("SAGA_DEMO_FAIL_MODE", "always")  # always|once

_fail_once_state: dict[str, int] = {}


def _should_fail(step: str, phase: str, fail_step: str | None, fail_phase: str | None, fail_mode: str | None) -> bool:
    if not fail_step:
        return False
    if step != fail_step or phase != (fail_phase or "up"):
        return False
    mode = fail_mode or "always"
    if mode == "always":
        return True
    if mode == "once":
        count = _fail_once_state.get(step, 0)
        if count == 0:
            _fail_once_state[step] = 1
            return True
    return False


def _response(step: str, phase: str, status: int) -> tuple[Any, int]:
    payload = {
        "service": SERVICE_NAME,
        "step": step,
        "phase": phase,
        "status": status,
        "method": request.method,
        "path": request.path,
        "headers": {k: v for k, v in request.headers.items()},
        "query": request.args.to_dict(flat=True),
        "body": request.get_json(silent=True),
        "timestamp": time.time(),
    }
    return jsonify(payload), status


@app.route("/step/<step>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def handle_step(step: str):
    print(step)
    phase = request.args.get("phase", "up")
    header_fail_step = request.headers.get("X-Fail-Step") or None
    header_fail_phase = request.headers.get("X-Fail-Phase") or None
    header_fail_mode = request.headers.get("X-Fail-Mode") or None
    fail_step = header_fail_step or FAIL_STEP
    fail_phase = header_fail_phase or FAIL_PHASE
    fail_mode = header_fail_mode or FAIL_MODE
    if _should_fail(step, phase, fail_step, fail_phase, fail_mode):
        print(" FAILING {step} {phase}")
        return _response(step, phase, 500)
    print(" SUCCESS {step} {phase}")
    return _response(step, phase, 200)


@app.route("/callback/<name>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def handle_callback(name: str):
    phase = request.args.get("phase", "up")
    return _response(f"callback-{name}", phase, 200)


@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200


if __name__ == "__main__":
    host = os.getenv("SAGA_DEMO_HOST", "127.0.0.1")
    port = int(os.getenv("SAGA_DEMO_PORT", "5001"))
    debug = os.getenv("SAGA_DEMO_DEBUG", "false").lower() == "true"
    app.run(host=host, port=port, debug=debug)
