from __future__ import annotations

import os
import random

from locust import HttpUser, between, task

SAGA_NAME = os.getenv("SAGA_NAME", "square-chain-demo")
SAGA_VERSION = os.getenv("SAGA_VERSION", "v1772632940")


class SquareChainRunUser(HttpUser):
    wait_time = between(0.1, 0.3)

    @task
    def run_saved_square_chain(self) -> None:
        initial_value = random.randint(2, 8)
        payload = {
            "payload": {
                "initial_value": initial_value,
            }
        }
        path = f"/sagas/definitions/{SAGA_NAME}/{SAGA_VERSION}/run"
        with self.client.post(
            path,
            json=payload,
            name="/sagas/definitions/{name}/{version}/run",
            catch_response=True,
        ) as response:
            if response.status_code not in (200, 201, 202):
                response.failure(f"status={response.status_code} body={response.text}")
                return
            data = response.json()
            if "id" not in data:
                response.failure(f"missing id in response body={response.text}")

