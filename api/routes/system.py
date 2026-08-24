import asyncio
from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from fastapi import Response as FastAPIResponse
from fastapi.responses import Response
from sqlalchemy import text

from api.dependencies import get_app_settings, get_database, get_queue
from api.schemas.common import HealthResponse
from core.config import Settings
from core.database import Database
from core.metrics import render_metrics
from core.redis import RedisQueue

router = APIRouter(tags=["system"])


@router.get("/livez", response_model=HealthResponse)
async def livez() -> HealthResponse:
    return HealthResponse(status="ok", checks=None)


@router.get("/readyz", response_model=HealthResponse)
async def readyz(
    response: FastAPIResponse,
    database: Annotated[Database, Depends(get_database)],
    queue: Annotated[RedisQueue, Depends(get_queue)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> HealthResponse:
    checks: dict[str, Literal["ok", "error"]] = {"postgresql": "error"}
    try:
        async with asyncio.timeout(settings.health_check_timeout):
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
        checks["postgresql"] = "ok"
    except Exception:
        checks["postgresql"] = "error"

    # API-key authentication uses Redis-backed fail-closed rate limiting by
    # default. In that mode the API process can remain live and self-recover,
    # but it must not receive load-balanced traffic while Redis is unavailable.
    if not settings.rate_limit_fail_open:
        checks["redis"] = "error"
        try:
            async with asyncio.timeout(settings.health_check_timeout):
                if await queue.rate_limit_backend_ready():
                    checks["redis"] = "ok"
        except Exception:
            checks["redis"] = "error"

    ready = all(value == "ok" for value in checks.values())
    if not ready:
        response.status_code = 503
    return HealthResponse(
        status="ok" if ready else "degraded",
        checks=checks,
    )


@router.get("/health", response_model=HealthResponse)
async def health(
    response: FastAPIResponse,
    database: Annotated[Database, Depends(get_database)],
    queue: Annotated[RedisQueue, Depends(get_queue)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> HealthResponse:
    checks: dict[str, Literal["ok", "error"]] = {
        "postgresql": "error",
        "redis": "error",
    }
    try:
        async with asyncio.timeout(settings.health_check_timeout):
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
        checks["postgresql"] = "ok"
    except Exception:
        checks["postgresql"] = "error"
    try:
        async with asyncio.timeout(settings.health_check_timeout):
            if await queue.rate_limit_backend_ready():
                checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "error"
    overall = "ok" if all(value == "ok" for value in checks.values()) else "degraded"
    # PostgreSQL is the write-path source of truth. Redis is an accelerator;
    # its failure is visible as degraded health but does not make the API dead.
    if checks["postgresql"] == "error":
        response.status_code = 503
    return HealthResponse(status=overall, checks=checks)


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=render_metrics(), media_type="text/plain; version=0.0.4")
