import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_principal
from api.errors import register_exception_handlers
from api.routes import admin
from core.config import Settings
from core.database import Database
from core.enums import ProjectRole, TaskStatus
from core.rbac import Principal, PrincipalKind
from models.task import Task
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


def _principal(role: ProjectRole) -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=PROJECT_ID,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=role,
        key_prefix="mkc_0123456789abcdef",
    )


class _ControlPlane:
    def snapshot(self) -> dict[str, dict[str, object]]:
        now = datetime.now(UTC)
        return {
            "scheduler": {
                "runs": 4,
                "failures": 1,
                "last_started_at": now,
                "last_succeeded_at": now,
                "last_error": "sensitive backend detail",
            }
        }


@pytest_asyncio.fixture
async def admin_client(
    database: Database,
) -> AsyncIterator[tuple[AsyncClient, dict[str, Principal], Database]]:
    state = {"principal": _principal(ProjectRole.ADMIN)}
    app = FastAPI()
    app.state.database = database
    app.state.settings = Settings(
        database_url=str(database.engine.url),
        control_plane_enabled=True,
        scheduler_mode="global",
    )
    app.state.control_plane = _ControlPlane()
    register_exception_handlers(app)
    app.include_router(admin.router)
    app.dependency_overrides[get_principal] = lambda: state["principal"]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client, state, database


async def test_admin_diagnostics_require_admin_and_do_not_expose_raw_controller_errors(
    admin_client: tuple[AsyncClient, dict[str, Principal], Database],
) -> None:
    client, state, database = admin_client
    response = await client.get("/api/v1/admin/diagnostics")

    assert response.status_code == 200
    body = response.json()
    assert body["project_id"] == str(PROJECT_ID)
    assert body["scheduler"]["status"] == "degraded"
    assert body["scheduler"]["last_error_present"] is True
    assert body["repair"]["supported"] is True
    assert body["consistency"]["status"] == "incomplete"
    checks = {check["name"]: check for check in body["consistency"]["checks"]}
    assert checks["orphan_container"]["status"] == "not_observable"
    assert checks["orphan_pod"]["status"] == "not_observable"
    assert "sensitive backend detail" not in response.text

    terminal_task_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Task(
                id=terminal_task_id,
                project_id=PROJECT_ID,
                image="python:3.12",
                command=["python", "-V"],
                status=TaskStatus.SUCCEEDED,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
                finished_at=datetime.now(UTC),
            )
        )

    repair = await client.post("/api/v1/admin/diagnostics/repair")
    assert repair.status_code == 200
    assert repair.json()["repaired_total"] == 1
    assert repair.json()["actions"] == [
        {
            "check": "terminal_task_with_lease",
            "resource_type": "task",
            "resource_id": str(terminal_task_id),
            "action": "clear_lease",
            "outcome": "repaired",
            "reason": "terminal task lease cleared without contacting the runtime",
        }
    ]

    state["principal"] = _principal(ProjectRole.MEMBER)
    forbidden = await client.get("/api/v1/admin/diagnostics")
    assert forbidden.status_code == 403
    forbidden_repair = await client.post("/api/v1/admin/diagnostics/repair")
    assert forbidden_repair.status_code == 403


async def test_admin_can_drain_worker_without_interrupting_existing_capacity(
    admin_client: tuple[AsyncClient, dict[str, Principal], Database],
) -> None:
    client, state, database = admin_client
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id="worker-maintenance",
            hostname="worker-maintenance",
            concurrency=4,
            cpu_count=8,
            memory_total_mb=16_384,
            docker_version="test",
            labels={"region": "local"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )
        worker.running_tasks = 2
        worker.reserved_cpu = 2.0
        worker.reserved_memory_mb = 1024

    drained = await client.post(
        "/api/v1/admin/workers/worker-maintenance/drain",
        json={"reason": "kernel maintenance"},
    )
    assert drained.status_code == 200
    assert drained.json()["status"] == "draining"
    assert drained.json()["drain_reason"] == "kernel maintenance"
    assert drained.json()["running_tasks"] == 2
    assert drained.json()["reserved_cpu"] == 2.0
    assert drained.json()["reserved_memory_mb"] == 1024

    missing = await client.post(
        "/api/v1/admin/workers/missing/drain",
        json={"reason": "maintenance"},
    )
    assert missing.status_code == 404

    state["principal"] = _principal(ProjectRole.MEMBER)
    forbidden = await client.post(
        "/api/v1/admin/workers/worker-maintenance/drain",
        json={"reason": "not allowed"},
    )
    assert forbidden.status_code == 403
