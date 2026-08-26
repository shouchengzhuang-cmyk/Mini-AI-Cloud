from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.main import create_app
from api.services.fake_replica_runtime import FakeReplicaRuntimeController
from api.services.gateway import GatewayService
from core.config import Settings
from core.database import Database
from core.redis import RedisQueue
from repositories.services import ServiceRepository

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


@pytest_asyncio.fixture
async def tenant_gateway_client(
    database: Database,
    redis_queue: RedisQueue,
    tmp_path: Any,
) -> AsyncIterator[tuple[AsyncClient, FastAPI]]:
    settings = Settings(
        app_env="test",
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        legacy_anonymous_enabled=False,
        artifact_local_root=str(tmp_path / "tenant-gateway-artifacts"),
        service_proxy_connect_timeout=2,
        service_proxy_first_token_timeout=2,
        service_proxy_timeout=10,
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, app
    finally:
        runtime = app.state.fake_replica_runtime
        gateway = app.state.gateway_service
        assert isinstance(runtime, FakeReplicaRuntimeController)
        assert isinstance(gateway, GatewayService)
        try:
            await runtime.close()
        finally:
            await gateway.http_client.aclose()


def _bootstrap_payload() -> dict[str, object]:
    return {
        "user": {
            "username": "tenant-owner",
            "email": "tenant-owner@example.com",
            "password": "correct horse battery staple",
        },
        "project": {"name": "Tenant A", "slug": "tenant-a"},
        "api_key_name": "tenant-a-bootstrap",
    }


async def test_gateway_api_key_cannot_route_to_another_projects_private_service(
    tenant_gateway_client: tuple[AsyncClient, FastAPI],
    database: Database,
) -> None:
    client, app = tenant_gateway_client
    bootstrapped = await client.post("/api/v1/bootstrap", json=_bootstrap_payload())
    assert bootstrapped.status_code == 201
    tenant_a = bootstrapped.json()
    project_a_id = uuid.UUID(tenant_a["project"]["id"])
    project_a_auth = {"Authorization": f"Bearer {tenant_a['api_key']['api_key']}"}

    created_model = await client.post(
        f"/api/v1/projects/{project_a_id}/models",
        headers=project_a_auth,
        json={
            "name": "tenant-a-private-model",
            "provider": "local",
            "source": "fake/tenant-a-private-model",
            "runtime": "fake",
            "default_gpu_count": 0,
        },
    )
    assert created_model.status_code == 201
    registered_model = created_model.json()
    registered_model_id = uuid.UUID(registered_model["id"])
    assert uuid.UUID(registered_model["project_id"]) == project_a_id
    assert registered_model["source"] == "fake/tenant-a-private-model"

    created_b = await client.post(
        "/api/v1/projects",
        json={
            "name": "Tenant B",
            "slug": "tenant-b",
            "api_key_name": "tenant-b-initial",
        },
        headers=project_a_auth,
    )
    assert created_b.status_code == 201
    tenant_b = created_b.json()
    project_b_id = uuid.UUID(tenant_b["id"])
    project_b_auth = {"Authorization": f"Bearer {tenant_b['api_key']['api_key']}"}
    assert project_a_id != project_b_id

    whoami_b = await client.get("/api/v1/auth/whoami", headers=project_b_auth)
    assert whoami_b.status_code == 200
    assert uuid.UUID(whoami_b.json()["project_id"]) == project_b_id

    created_service = await client.post(
        "/api/v1/services",
        headers=project_a_auth,
        json={
            "name": "tenant-private-chat",
            "registered_model_id": str(registered_model_id),
            "replicas": 2,
        },
    )
    assert created_service.status_code == 201
    service = created_service.json()
    service_id = uuid.UUID(service["id"])
    assert uuid.UUID(service["project_id"]) == project_a_id
    assert uuid.UUID(service["registered_model_id"]) == registered_model_id
    assert service["model"] == "fake/tenant-a-private-model"
    assert service["runtime"] == "fake"
    assert service["runtime_type"] == "fake"

    runtime = app.state.fake_replica_runtime
    assert isinstance(runtime, FakeReplicaRuntimeController)
    started = await runtime.run_once()
    assert started.claimed == 2
    assert started.started == 2

    visible_to_a = await client.get("/v1/models", headers=project_a_auth)
    hidden_from_b = await client.get("/v1/models", headers=project_b_auth)
    assert visible_to_a.status_code == 200
    assert [item["id"] for item in visible_to_a.json()["data"]] == ["tenant-private-chat"]
    assert hidden_from_b.status_code == 200
    assert hidden_from_b.json()["data"] == []

    allowed = await client.post(
        "/v1/chat/completions",
        headers=project_a_auth,
        json={
            "model": "tenant-private-chat",
            "messages": [{"role": "user", "content": "tenant A control request"}],
        },
    )
    assert allowed.status_code == 200
    assert allowed.json()["model"] == "fake/tenant-a-private-model"
    assert allowed.json()["choices"][0]["message"]["content"] == (
        "fake response: tenant A control request"
    )

    async with database.session() as session:
        persisted = await ServiceRepository.get(
            session,
            service_id,
            project_id=project_a_id,
        )
    assert persisted is not None
    cursor_before_cross_project_request = persisted.round_robin_cursor

    rejected = await client.post(
        "/v1/chat/completions",
        headers=project_b_auth,
        json={
            "model": "tenant-private-chat",
            "messages": [{"role": "user", "content": "cross-project request"}],
        },
    )
    assert rejected.status_code == 404
    assert rejected.json()["error"]["code"] == "MODEL_NOT_FOUND"

    async with database.session() as session:
        persisted = await ServiceRepository.get(
            session,
            service_id,
            project_id=project_a_id,
        )
    assert persisted is not None
    assert persisted.round_robin_cursor == cursor_before_cross_project_request
