import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from core.database import Database
from models.outbox import OutboxEvent
from models.task import Task

pytestmark = pytest.mark.integration


def _payload(command: list[str] | None = None) -> dict[str, object]:
    return {
        "image": "python:3.12-slim",
        "command": command or ["python", "-c", "print('hello')"],
        "environment": {"MODE": "test"},
        "timeout_seconds": 30,
        "max_retries": 1,
        "cpu_limit": 1.0,
        "memory_limit_mb": 128,
        "labels": {"runtime": "docker"},
        "network_enabled": False,
        "gpu_count": 0,
    }


async def test_api_create_query_and_cancel(api_client: AsyncClient) -> None:
    created = await api_client.post("/api/v1/tasks", json=_payload())
    assert created.status_code == 201
    created_body = created.json()
    task_id = uuid.UUID(created_body["id"])
    assert created_body["status"] == "queued"

    queried = await api_client.get(f"/api/v1/tasks/{task_id}")
    assert queried.status_code == 200
    assert queried.json()["command"] == ["python", "-c", "print('hello')"]
    assert queried.json()["network_enabled"] is False

    cancelled = await api_client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    assert cancelled.json()["cancel_requested"] is True

    persisted = await api_client.get(f"/api/v1/tasks/{task_id}")
    assert persisted.status_code == 200
    assert persisted.json()["status"] == "cancelled"


async def test_api_idempotency_reuses_task_and_rejects_changed_payload(
    api_client: AsyncClient,
    database: Database,
) -> None:
    headers = {"Idempotency-Key": "integration-create-1"}
    first = await api_client.post("/api/v1/tasks", json=_payload(), headers=headers)
    repeated = await api_client.post("/api/v1/tasks", json=_payload(), headers=headers)

    assert first.status_code == 201
    assert repeated.status_code == 201
    assert repeated.json() == first.json()

    changed = await api_client.post(
        "/api/v1/tasks",
        json=_payload(["python", "-c", "print('different')"]),
        headers=headers,
    )
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"

    task_id = uuid.UUID(first.json()["id"])
    async with database.session() as session:
        task_count = await session.scalar(select(func.count(Task.id)))
        ready_events = await session.scalar(
            select(func.count(OutboxEvent.id)).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.ready",
            )
        )

    assert task_count == 1
    assert ready_events == 1
