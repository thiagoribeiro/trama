from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class SagaApi:
    base_url: str

    def post(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", json=payload, timeout=10)

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", timeout=10)


@dataclass
class Scenario:
    name: str
    version: str
    fail_step: Optional[str]
    fail_phase: Optional[str]


def make_definition(service_url: str, scenario: Scenario) -> Dict[str, Any]:
    def step(name: str, with_prev: bool = False) -> Dict[str, Any]:
        if with_prev:
            up_url = f"{service_url}/step/{name}?phase=up&from={{{{step.0.up.body.step}}}}"
            down_url = f"{service_url}/step/{name}?phase=down&from={{{{step.0.up.body.step}}}}"
            up_body = {"prevStep": "{{step.0.up.body.step}}", "prevStatus": "{{step.0.up.body.status}}"}
            down_body = {"prevStep": "{{step.0.up.body.step}}", "compensate": name}
        else:
            up_url = f"{service_url}/step/{name}?phase=up"
            down_url = f"{service_url}/step/{name}?phase=down"
            up_body = {"step": name}
            down_body = {"compensate": name}

        return {
            "name": name,
            "up": {
                "url": up_url,
                "verb": "POST",
                "headers": {
                    "Content-Type": "application/json",
                    "X-Fail-Step": "{{payload.fail_step}}",
                    "X-Fail-Phase": "{{payload.fail_phase}}",
                    "X-Fail-Mode": "once",
                },
                "body": up_body,
            },
            "down": {
                "url": down_url,
                "verb": "POST",
                "headers": {
                    "Content-Type": "application/json",
                    "X-Fail-Step": "{{payload.fail_step}}",
                    "X-Fail-Phase": "{{payload.fail_phase}}",
                    "X-Fail-Mode": "once",
                },
                "body": down_body,
            },
        }

    return {
        "name": scenario.name,
        "version": scenario.version,
        "failureHandling": {
            "type": "retry",
            "maxAttempts": 1,
            "delayMillis": 200,
        },
        "steps": [step("reserve"), step("charge", with_prev=True)],
        "onSuccessCallback": {
            "url": f"{service_url}/callback/success",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {"status": "ok"},
        },
        "onFailureCallback": {
            "url": f"{service_url}/callback/failure",
            "verb": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": {"status": "failed"},
        },
    }


SCENARIOS = {
    "success": Scenario(name="demo-saga2", version="v1", fail_step=None, fail_phase=None),
    "fail_up": Scenario(name="demo-saga2", version="v2", fail_step="charge", fail_phase="up"),
    "fail_down": Scenario(name="demo-saga2", version="v3", fail_step="reserve", fail_phase="down"),
}


def wait_for_status(api: SagaApi, saga_id: str, timeout_s: int = 30) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    last = {}
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=SCENARIOS.keys(), default="success")
    parser.add_argument("--saga-api", default=os.getenv("SAGA_API", "http://127.0.0.1:8080"))
    parser.add_argument("--service-url", default=os.getenv("SAGA_SERVICE", "http://127.0.0.1:5001"))
    args = parser.parse_args()

    scenario = SCENARIOS[args.scenario]
    api = SagaApi(args.saga_api)

    definition = make_definition(args.service_url, scenario)

    create_payload = {
        "name": definition["name"],
        "version": definition["version"],
        "failureHandling": definition["failureHandling"],
        "steps": definition["steps"],
        "onSuccessCallback": definition["onSuccessCallback"],
        "onFailureCallback": definition["onFailureCallback"],
    }

    """
    resp = api.post("/sagas/definitions", create_payload)
    if resp.status_code not in (200, 201, 202):
        print(f"failed to create definition: {resp.status_code} {resp.text}")
        return 1
    definition_id = resp.json()["id"]
    #definition_id = "c059cb6f-19bf-4d94-8000-1b7f9975f085"
    """
    run_payload = {
        "payload": {
            "fail_step": scenario.fail_step or "",
            "fail_phase": scenario.fail_phase or "",
        }
    }
    resp = api.post(f"/sagas/definitions/{definition['name']}/{definition['version']}/run", run_payload)
    resp.raise_for_status()
    saga_id = resp.json()["id"]

    final_status = wait_for_status(api, saga_id)
    print(json.dumps(final_status, indent=2))

    status = final_status.get("status")
    if args.scenario == "success" and status == "SUCCEEDED":
        return 0
    if args.scenario != "success" and status in ("FAILED", "CORRUPTED"):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
