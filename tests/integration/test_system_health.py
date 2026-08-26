from typing import NoReturn

import pytest
from httpx import AsyncClient
from redis.exceptions import RedisError
from starlette.responses import Response

from api.routes.system import readyz
from core.config import Settings
from core.database import Database
from core.redis import RedisQueue

pytestmark = pytest.mark.integration


async def test_fail_closed_redis_failure_rejects_readiness_but_not_dependency_health(
    api_client: AsyncClient,
    redis_queue: RedisQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unavailable_rate_limit_backend() -> bool:
        raise RedisError("redis unavailable")

    monkeypatch.setattr(
        redis_queue,
        "rate_limit_backend_ready",
        unavailable_rate_limit_backend,
    )

    health = await api_client.get("/health")
    assert health.status_code == 200
    assert health.json() == {
        "status": "degraded",
        "checks": {"postgresql": "ok", "redis": "error"},
    }

    readiness = await api_client.get("/readyz")
    assert readiness.status_code == 503
    assert readiness.json() == {
        "status": "degraded",
        "checks": {"postgresql": "ok", "redis": "error"},
    }


async def test_fail_open_readiness_does_not_require_redis(
    database: Database,
    redis_queue: RedisQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_rate_limit_backend() -> bool:
        raise AssertionError("fail-open readiness must not require Redis")

    monkeypatch.setattr(
        redis_queue,
        "rate_limit_backend_ready",
        unexpected_rate_limit_backend,
    )
    response = Response()
    result = await readyz(
        response,
        database,
        redis_queue,
        Settings(_env_file=None, rate_limit_fail_open=True),
    )

    assert response.status_code == 200
    assert result.model_dump() == {"status": "ok", "checks": {"postgresql": "ok"}}


async def test_postgres_failure_rejects_readiness_and_dependency_health(
    api_client: AsyncClient,
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable_session() -> NoReturn:
        raise ConnectionError("postgres unavailable")

    monkeypatch.setattr(database, "session", unavailable_session)

    readiness = await api_client.get("/readyz")
    assert readiness.status_code == 503
    assert readiness.json() == {
        "status": "degraded",
        "checks": {"postgresql": "error", "redis": "ok"},
    }

    health = await api_client.get("/health")
    assert health.status_code == 503
    assert health.json() == {
        "status": "degraded",
        "checks": {"postgresql": "error", "redis": "ok"},
    }

    liveness = await api_client.get("/livez")
    assert liveness.status_code == 200
    assert liveness.json() == {"status": "ok", "checks": None}
