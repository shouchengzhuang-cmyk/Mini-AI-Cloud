import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from core.database import Database
from core.enums import TaskStatus
from models.outbox import OutboxEvent
from models.scheduling import PlacementAttempt
from models.task import Task
from repositories.tasks import TaskRepository

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

    timeline = await api_client.get(f"/api/v1/tasks/{task_id}/timeline")
    assert timeline.status_code == 200
    assert [event["status"] for event in timeline.json()["events"]] == [
        "pending",
        "queued",
        "cancelled",
    ]


async def test_api_persists_and_returns_structured_retry_policy(
    api_client: AsyncClient,
) -> None:
    payload = _payload()
    payload["max_retries"] = 0
    payload["retry_policy"] = {
        "max_attempts": 4,
        "backoff": "linear",
        "base_seconds": 2.0,
        "max_seconds": 30.0,
        "retry_on_exit_codes": [1, 137],
    }

    created = await api_client.post("/api/v1/tasks", json=payload)
    assert created.status_code == 201
    queried = await api_client.get(f"/api/v1/tasks/{created.json()['id']}")

    assert queried.status_code == 200
    assert queried.json()["max_retries"] == 3
    assert queried.json()["retry_policy"] == payload["retry_policy"]


async def test_api_rejects_schema_ready_ascend_before_persistence_support(
    api_client: AsyncClient,
) -> None:
    payload = _payload()
    payload.pop("gpu_count")
    payload["accelerator"] = {
        "count": 1,
        "memory_mb_per_device": 32_000,
        "allowed_vendors": ["huawei-ascend"],
        "allowed_kinds": ["npu"],
        "selection_policy": "ascend-only",
    }

    response = await api_client.post("/api/v1/tasks", json=payload)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "ACCELERATOR_EXECUTION_NOT_READY"


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


async def test_task_dependencies_enter_ready_queue_only_after_success(
    api_client: AsyncClient,
    database: Database,
) -> None:
    prerequisite = await api_client.post("/api/v1/tasks", json=_payload())
    assert prerequisite.status_code == 201
    prerequisite_id = uuid.UUID(prerequisite.json()["id"])

    dependent_payload = _payload(["python", "-c", "print('dependent')"])
    dependent_payload["depends_on"] = [str(prerequisite_id)]
    dependent = await api_client.post("/api/v1/tasks", json=dependent_payload)
    assert dependent.status_code == 201
    dependent_id = uuid.UUID(dependent.json()["id"])
    assert dependent.json()["status"] == "pending"

    async with database.session() as session:
        ready_events = int(
            await session.scalar(
                select(func.count(OutboxEvent.id)).where(
                    OutboxEvent.aggregate_id == dependent_id,
                    OutboxEvent.event_type == "task.ready",
                )
            )
            or 0
        )
        assert not await TaskRepository.dependencies_ready(session, dependent_id)
    assert ready_events == 0

    async with database.session() as session, session.begin():
        task = await session.get(Task, prerequisite_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.SUCCEEDED
        changed = await TaskRepository.resolve_dependency_readiness(session, limit=100)
    assert changed == [dependent_id]

    queried = await api_client.get(f"/api/v1/tasks/{dependent_id}")
    assert queried.status_code == 200
    assert queried.json()["status"] == "queued"
    async with database.session() as session:
        assert await TaskRepository.dependencies_ready(session, dependent_id)
        ready_events = int(
            await session.scalar(
                select(func.count(OutboxEvent.id)).where(
                    OutboxEvent.aggregate_id == dependent_id,
                    OutboxEvent.event_type == "task.ready",
                )
            )
            or 0
        )
    assert ready_events == 1


async def test_task_dependency_must_exist_in_same_project(api_client: AsyncClient) -> None:
    payload = _payload()
    payload["depends_on"] = [str(uuid.uuid4())]
    response = await api_client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "INVALID_TASK_DEPENDENCY"


async def test_scheduler_explain_aggregates_decisions_without_worker_details(
    api_client: AsyncClient,
    database: Database,
) -> None:
    created = await api_client.post("/api/v1/tasks", json=_payload())
    assert created.status_code == 201
    task_id = uuid.UUID(created.json()["id"])

    async with database.session() as session, session.begin():
        task = await session.get(Task, task_id, with_for_update=True)
        assert task is not None
        task.unschedulable_reason = "insufficient_gpu_memory"
        session.add_all(
            [
                PlacementAttempt(
                    task_id=task_id,
                    scheduler_id="scheduler-a",
                    worker_id="sensitive-worker-a",
                    policy="binpack",
                    outcome="rejected",
                    reason="insufficient_gpu_memory",
                    effective_priority=50,
                ),
                PlacementAttempt(
                    task_id=task_id,
                    scheduler_id="scheduler-a",
                    worker_id="sensitive-worker-b",
                    policy="binpack",
                    outcome="rejected",
                    reason="label_mismatch",
                    effective_priority=50,
                ),
                PlacementAttempt(
                    task_id=task_id,
                    scheduler_id="scheduler-b",
                    worker_id="sensitive-worker-a",
                    policy="binpack",
                    outcome="rejected",
                    reason="insufficient_gpu_memory",
                    effective_priority=51,
                ),
            ]
        )

    response = await api_client.get(f"/api/v1/tasks/{task_id}/scheduling")
    assert response.status_code == 200
    body = response.json()
    assert body == {
        "task_id": str(task_id),
        "state": "unschedulable",
        "reason": "insufficient_gpu_memory",
        "considered_workers": 2,
        "attempts_total": 3,
        "rejections": {"insufficient_gpu_memory": 2, "label_mismatch": 1},
        "outcomes": {"rejected": 3},
        "latest_attempt_at": body["latest_attempt_at"],
    }
    assert body["latest_attempt_at"] is not None
    assert "sensitive-worker" not in response.text
