import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_principal
from api.errors import register_exception_handlers
from api.routes import usage
from core.database import Database
from core.enums import ProjectRole, RuntimeType, TaskStatus
from core.rbac import Principal, PrincipalKind, ProjectStatus
from models.identity import Project
from models.task import Task
from models.usage import TaskExecution, UsageLedger
from repositories.cleanup import CleanupRepository
from repositories.quotas import QuotaRepository
from repositories.usage import UsageRepository

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("20000000-0000-0000-0000-000000000001")
OTHER_PROJECT_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")


def _principal(project_id: uuid.UUID, role: ProjectRole) -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=project_id,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=role,
        key_prefix="mkc_0123456789abcdef",
    )


@pytest_asyncio.fixture
async def usage_client(
    database: Database,
) -> AsyncIterator[tuple[AsyncClient, dict[str, Principal]]]:
    async with database.session() as session, session.begin():
        for project_id, slug in (
            (PROJECT_ID, "usage-api"),
            (OTHER_PROJECT_ID, "usage-api-other"),
        ):
            session.add(
                Project(
                    id=project_id,
                    name=slug,
                    slug=slug,
                    status=ProjectStatus.ACTIVE,
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)

    holder = {"principal": _principal(PROJECT_ID, ProjectRole.OWNER)}
    app = FastAPI()
    app.state.database = database
    register_exception_handlers(app)
    app.include_router(usage.router)
    app.dependency_overrides[get_principal] = lambda: holder["principal"]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client, holder


async def test_quota_usage_and_cost_endpoints_are_scoped_and_exact(
    usage_client: tuple[AsyncClient, dict[str, Principal]],
    database: Database,
) -> None:
    client, holder = usage_client
    quota = await client.put(
        f"/api/v1/projects/{PROJECT_ID}/quota",
        json={"max_queued_tasks": 2, "max_running_tasks": 1, "daily_cost_limit": "5.0"},
    )
    assert quota.status_code == 200
    assert quota.json()["limits"]["max_running_tasks"] == 1

    base = datetime(2026, 1, 1, tzinfo=UTC)
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Task(
                id=task_id,
                project_id=PROJECT_ID,
                image="python:3.12",
                command=["python", "-V"],
                status=TaskStatus.SUCCEEDED,
            )
        )
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=task_id,
                project_id=PROJECT_ID,
                worker_id=None,
                worker_session_id=None,
                attempt=1,
                status="succeeded",
                cpu_millicores=1_000,
                memory_mb=1_024,
                gpu_count=0,
                gpu_model=None,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1"),
                assigned_at=base,
                started_at=base,
                finished_at=base + timedelta(minutes=1),
                runtime_type=RuntimeType.DOCKER.value,
            )
        )
        await session.flush()
        await UsageRepository.record_execution(
            session,
            execution_id=execution_id,
            project_id=PROJECT_ID,
            task_id=task_id,
            started_at=base,
            finished_at=base + timedelta(minutes=1),
            cpu_seconds=Decimal("60"),
            memory_gb_seconds=Decimal("60"),
            gpu_seconds=Decimal("0"),
            cost=Decimal("0.01"),
        )

    params = {
        "from": base.isoformat(),
        "to": (base + timedelta(hours=1)).isoformat(),
    }
    usage_response = await client.get(f"/api/v1/projects/{PROJECT_ID}/usage", params=params)
    cost_response = await client.get(f"/api/v1/projects/{PROJECT_ID}/cost", params=params)
    assert usage_response.status_code == 200
    assert usage_response.json()["execution_count"] == 1
    assert Decimal(usage_response.json()["cpu_seconds"]) == Decimal("60")
    assert usage_response.json()["serving"] == {
        "request_count": 0,
        "requests_with_reported_token_usage": 0,
        "reported_input_tokens": 0,
        "reported_output_tokens": 0,
        "reported_total_tokens": 0,
        "allocated_gpu_seconds": "0.000000",
        "replica_gpu_seconds": "0.000000",
    }
    assert cost_response.status_code == 200
    assert Decimal(cost_response.json()["costs"][0]["cost"]) == Decimal("0.01")

    cross_project = await client.get(f"/api/v1/projects/{OTHER_PROJECT_ID}/usage", params=params)
    assert cross_project.status_code == 404

    holder["principal"] = _principal(PROJECT_ID, ProjectRole.VIEWER)
    assert (
        await client.get(f"/api/v1/projects/{PROJECT_ID}/usage", params=params)
    ).status_code == 200
    assert (await client.get(f"/api/v1/projects/{PROJECT_ID}/quota")).status_code == 403


async def test_usage_window_validation_returns_stable_422(
    usage_client: tuple[AsyncClient, dict[str, Principal]],
) -> None:
    client, _holder = usage_client
    response = await client.get(
        f"/api/v1/projects/{PROJECT_ID}/usage",
        params={
            "from": "2026-01-02T00:00:00+00:00",
            "to": "2026-01-01T00:00:00+00:00",
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_USAGE_WINDOW"


async def test_task_retention_preserves_immutable_usage_and_cost_aggregates(
    usage_client: tuple[AsyncClient, dict[str, Principal]],
    database: Database,
) -> None:
    client, _holder = usage_client
    finished_at = datetime.now(UTC) - timedelta(days=5)
    started_at = finished_at - timedelta(minutes=2)
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Task(
                id=task_id,
                project_id=PROJECT_ID,
                image="python:3.12",
                command=["python", "-V"],
                status=TaskStatus.SUCCEEDED,
                created_at=started_at,
                started_at=started_at,
                finished_at=finished_at,
            )
        )
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=task_id,
                project_id=PROJECT_ID,
                worker_id=None,
                worker_session_id=None,
                attempt=1,
                status="succeeded",
                cpu_millicores=1_000,
                memory_mb=1_024,
                gpu_count=0,
                gpu_model=None,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1"),
                assigned_at=started_at,
                started_at=started_at,
                finished_at=finished_at,
                runtime_type=RuntimeType.DOCKER.value,
                runtime_object_id="expired-runtime-object",
            )
        )
        await session.flush()
        ledger, created = await UsageRepository.record_execution(
            session,
            execution_id=execution_id,
            project_id=PROJECT_ID,
            task_id=task_id,
            started_at=started_at,
            finished_at=finished_at,
            cpu_seconds=Decimal("120"),
            memory_gb_seconds=Decimal("120"),
            gpu_seconds=Decimal("0"),
            cost=Decimal("0.02"),
        )
        assert created is True
        ledger_id = ledger.id

    params = {
        "from": (started_at - timedelta(minutes=1)).isoformat(),
        "to": (finished_at + timedelta(minutes=1)).isoformat(),
    }
    usage_before = await client.get(f"/api/v1/projects/{PROJECT_ID}/usage", params=params)
    cost_before = await client.get(f"/api/v1/projects/{PROJECT_ID}/cost", params=params)
    assert usage_before.status_code == 200
    assert cost_before.status_code == 200

    async with database.session() as session, session.begin():
        cleanup = await CleanupRepository.run_database_cleanup(
            session,
            task_retention_days=1,
            log_retention_days=1,
            audit_retention_days=365,
            limit=100,
        )
    assert cleanup.tasks_deleted == 1
    assert cleanup.execution_runtime_handles_cleared == 1

    async with database.session() as session:
        retained_ledger = await session.get(UsageLedger, ledger_id)
        assert await session.get(Task, task_id) is None
        assert await session.get(TaskExecution, execution_id) is None
        assert retained_ledger is not None
        assert retained_ledger.task_id == task_id
        assert retained_ledger.execution_id == execution_id

    usage_after = await client.get(f"/api/v1/projects/{PROJECT_ID}/usage", params=params)
    cost_after = await client.get(f"/api/v1/projects/{PROJECT_ID}/cost", params=params)
    assert usage_after.status_code == 200
    assert cost_after.status_code == 200
    assert usage_after.json() == usage_before.json()
    assert cost_after.json() == cost_before.json()
