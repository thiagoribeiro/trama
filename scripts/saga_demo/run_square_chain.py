from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/square", methods=["POST"])
def square() -> tuple[Any, int]:
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({"error": "request body must be valid json"}), 400

    # Accept raw numeric body (preferred) and object body for compatibility.
    candidate = body.get("value") if isinstance(body, dict) else body
    try:
        value = float(candidate)
    except (TypeError, ValueError):
        return jsonify({"error": "request body must be numeric"}), 400

    result = value * value
    if result.is_integer():
        result = int(result)
    if value.is_integer():
        value = int(value)

    print(f"input {value}, result {result}")
    return jsonify({"input": value, "result": result}), 200

@app.route("/done_square", methods=["POST"])
def done_square() -> tuple[Any, int]:
    body = request.get_json(silent=True) or {}
    print(body)
    return jsonify({"status": "done", "body": body}), 200

@app.route("/compensate", methods=["POST"])
def compensate() -> tuple[Any, int]:
    body = request.get_json(silent=True) or {}
    return jsonify({"status": "compensated", "body": body}), 200


@app.route("/healthz", methods=["GET"])
def healthz() -> tuple[str, int]:
    return "ok", 200


@dataclass
class SagaApi:
    base_url: str

    def post(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", json=payload, timeout=10)

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", timeout=10)


def make_definition(service_url: str, name: str, version: str, step_count: int) -> Dict[str, Any]:
    steps = []
    final_expr = f"{{{{step.{step_count - 1}.up.body.result}}}}" if step_count > 0 else "null"

    for index in range(step_count):
        if index == 0:
            value_expr = "{{payload.initial_value}}"
        else:
            value_expr = f"{{{{step.{index - 1}.up.body.result}}}}"

        steps.append(
            {
                "name": f"square-{index + 1}",
                "up": {
                    "url": f"{service_url}/square",
                    "verb": "POST",
                    "headers": {
                        "Content-Type": "application/json",
                    },
                    "body": value_expr,
                },
                "down": {
                    "url": f"{service_url}/compensate",
                    "verb": "POST",
                    "headers": {
                        "Content-Type": "application/json",
                    },
                    "body": {
                        "step": f"square-{index + 1}",
                        "lastKnown": value_expr,
                    },
                },
            }
        )

    return {
        "name": name,
        "version": version,
        "failureHandling": {
            "type": "retry",
            "maxAttempts": 1,
            "delayMillis": 200,
        },
        "steps": steps,
        "onSuccessCallback": {
            "url": f"{service_url}/done_square",
            "verb": "POST",
            "headers": {
                "Content-Type": "application/json",
            },
            "body": {
                "status": "done",
                "sagaId": "{{sagaId}}",
                "final": final_expr,
            },
        },
    }


def wait_for_status(api: SagaApi, saga_id: str, timeout_s: int = 30) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    last: Dict[str, Any] = {}

    while time.time() < deadline:
        resp = api.get(f"/sagas/{saga_id}")
        if resp.status_code == 204:
            time.sleep(0.25)
            continue

        resp.raise_for_status()
        last = resp.json()
        status = last.get("status")
        if status and status not in ("IN_PROGRESS", "RETRYING"):
            return last
        time.sleep(0.25)

    raise TimeoutError(f"timed out waiting for saga status; last={last}")


def run_saga(args: argparse.Namespace) -> int:
    api = SagaApi(args.saga_api)

    definition = make_definition(
        service_url=args.service_url,
        name=args.name,
        version=args.version,
        step_count=args.steps,
    )

    print(json.dumps(definition))

    create_resp = api.post("/sagas/definitions", definition)
    if create_resp.status_code not in (200, 201, 202):
        print(f"failed to create definition: {create_resp.status_code} {create_resp.text}")
        return 1

    run_payload = {
        "payload": {
            "initial_value": args.initial_value,
        }
    }
    run_resp = api.post(f"/sagas/definitions/{args.name}/{args.version}/run", run_payload)
    if run_resp.status_code not in (200, 201, 202):
        print(f"failed to run definition: {run_resp.status_code} {run_resp.text}")
        return 1
    saga_id = run_resp.json()["id"]

    final_status = wait_for_status(api, saga_id, timeout_s=args.timeout)
    print(json.dumps(final_status, indent=2))

    status = final_status.get("status")
    return 0 if status == "SUCCEEDED" else 2


def run_server(args: argparse.Namespace) -> int:
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Square-chain saga demo")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Start local Flask square service")
    serve.add_argument("--host", default=os.getenv("SAGA_DEMO_HOST", "127.0.0.1"))
    serve.add_argument("--port", type=int, default=int(os.getenv("SAGA_DEMO_PORT", "5002")))
    serve.add_argument("--debug", action="store_true")

    run = sub.add_parser("run", help="Create and run a 4-step square saga")
    run.add_argument("--saga-api", default=os.getenv("SAGA_API", "http://127.0.0.1:8080"))
    run.add_argument("--service-url", default=os.getenv("SAGA_SERVICE", "http://127.0.0.1:5002"))
    run.add_argument("--name", default="square-chain-demo")
    run.add_argument("--version", default=f"v{int(time.time())}")
    run.add_argument("--initial-value", type=float, default=2)
    run.add_argument("--steps", type=int, default=4)
    run.add_argument("--timeout", type=int, default=30)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        return run_server(args)
    return run_saga(args)


if __name__ == "__main__":
    raise SystemExit(main())
