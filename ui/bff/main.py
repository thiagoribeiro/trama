import asyncio
import logging
import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:9080").rstrip("/")
STATIC_DIR = Path(__file__).parent / "static"

# Headers that must not be forwarded from the upstream response.
# httpx already decodes content-encoding and reassembles transfer-encoding,
# so forwarding those headers causes browsers to try to decode already-decoded bodies.
_HOP_BY_HOP = {
    "content-encoding",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "upgrade",
}

app = FastAPI(title="Trama BFF", docs_url="/api/docs")

logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def startup():
    global _client
    _client = httpx.AsyncClient(base_url=ORCHESTRATOR_URL, timeout=30.0)


@app.on_event("shutdown")
async def shutdown():
    if _client:
        await _client.aclose()


def client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("HTTP client not initialised")
    return _client


async def _proxy(request: Request, upstream_path: str) -> Response:
    url = f"{ORCHESTRATOR_URL}{upstream_path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"
    body = await request.body()
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    try:
        resp = await client().request(
            method=request.method,
            url=url,
            content=body,
            headers=headers,
        )
    except httpx.ConnectError:
        return JSONResponse({"errors": ["orchestrator unavailable"]}, status_code=503)
    except httpx.TimeoutException:
        return JSONResponse({"errors": ["orchestrator request timed out"]}, status_code=504)
    except Exception as e:
        logger.exception("Unexpected error while proxying request", exc_info=e)
        return JSONResponse({"errors": ["internal server error"]}, status_code=500)

    forwarded_headers = {
        k: v for k, v in resp.headers.items() if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=forwarded_headers,
        media_type=resp.headers.get("content-type"),
    )


# ── Definitions ───────────────────────────────────────────────────────────────

@app.api_route("/api/definitions", methods=["GET", "POST"])
async def definitions(request: Request):
    return await _proxy(request, "/sagas/definitions")


@app.api_route("/api/definitions/{rest:path}", methods=["GET", "PUT", "DELETE"])
async def definitions_by_id(request: Request, rest: str):
    return await _proxy(request, f"/sagas/definitions/{rest}")


# ── Executions ────────────────────────────────────────────────────────────────

@app.api_route("/api/executions", methods=["GET"])
async def executions(request: Request):
    return await _proxy(request, "/sagas")


@app.api_route("/api/executions/{execution_id}/steps/calls", methods=["GET"])
async def execution_step_calls(request: Request, execution_id: str):
    return await _proxy(request, f"/sagas/{execution_id}/steps/calls")


@app.api_route("/api/executions/{execution_id}/steps", methods=["GET"])
async def execution_steps(request: Request, execution_id: str):
    return await _proxy(request, f"/sagas/{execution_id}/steps")


@app.api_route("/api/executions/{execution_id}/retry", methods=["POST"])
async def execution_retry(request: Request, execution_id: str):
    return await _proxy(request, f"/sagas/{execution_id}/retry")


@app.get("/api/executions/{execution_id}/detail")
async def execution_detail(execution_id: str):
    """Aggregate: execution status + step results + step calls + definition in a single response."""
    try:
        status_resp = await client().get(f"/sagas/{execution_id}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="orchestrator unavailable")

    if status_resp.status_code == 204:
        raise HTTPException(status_code=404, detail="execution not found")

    execution = status_resp.json() if status_resp.is_success else None
    name = execution.get("name") if execution else None
    version = execution.get("version") if execution else None

    try:
        gather_calls = [
            client().get(f"/sagas/{execution_id}/steps"),
            client().get(f"/sagas/{execution_id}/steps/calls"),
        ]
        if name and version:
            gather_calls.append(client().get(f"/sagas/definitions/{name}/{version}"))
        results = await asyncio.gather(*gather_calls)
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="orchestrator unavailable")

    steps_resp = results[0]
    calls_resp = results[1]
    def_resp = results[2] if len(results) > 2 else None

    return {
        "execution": execution,
        "steps": steps_resp.json() if steps_resp.is_success else [],
        "calls": calls_resp.json() if calls_resp.is_success else [],
        "definition": def_resp.json() if (def_resp and def_resp.is_success) else None,
    }


@app.api_route("/api/executions/{execution_id}", methods=["GET"])
async def execution_by_id(request: Request, execution_id: str):
    return await _proxy(request, f"/sagas/{execution_id}")


# ── Definitions run ───────────────────────────────────────────────────────────

@app.api_route("/api/definitions/{name}/{version}/run", methods=["POST"])
async def run_definition(request: Request, name: str, version: str):
    return await _proxy(request, f"/sagas/definitions/{name}/{version}/run")


# ── Static files (Vue SPA + definition-editor) ────────────────────────────────
# Serves any existing file under STATIC_DIR; unknown paths fall back to the SPA.

@app.get("/{full_path:path}", include_in_schema=False)
async def static_files(full_path: str):
    if not STATIC_DIR.exists():
        raise HTTPException(status_code=503, detail="UI not built")

    static_root = STATIC_DIR.resolve()
    candidate = (static_root / full_path).resolve()

    try:
        candidate.relative_to(static_root)
    except ValueError:
        return FileResponse(static_root / "index.html")

    if candidate.is_file():
        return FileResponse(candidate)
    return FileResponse(static_root / "index.html")
