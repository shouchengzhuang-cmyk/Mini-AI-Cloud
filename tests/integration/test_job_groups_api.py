import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event

from api.dependencies import get_principal
from api.errors import register_exception_handlers
from api.routes import job_groups
from core.database import Database
from core.enums import ProjectRole, TaskStatus
from core.rbac import Principal, PrincipalKind, ProjectStatus
from models.identity import Project
from models.task import Task

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("30000000-0000-0000-0000-000000000001")
OTHER_PROJECT_ID = uuid.UUID("30000000-0000-0000-0000-000000000002")


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
async def job_group_client(
    database: Database,
) -> AsyncIterator[tuple[AsyncClient, dict[str, object]]]:
    task_ids = {
        "dependent": uuid.uuid4(),
        "prerequisite": uuid.uuid4(),
        "blocked_dependent": uuid.uuid4(),
        "blocked_prerequisite": uuid.uuid4(),
    }
    async with database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=PROJECT_ID,
                    name="DAG API",
                    slug="dag-api",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=OTHER_PROJECT_ID,
                    name="Other DAG API",
                    slug="dag-api-other",
                    status=ProjectStatus.ACTIVE,
                ),
            ]
        )
        for task_id in task_ids.values():
            session.add(
                Task(
                    id=task_id,
                    project_id=PROJECT_ID,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                )
            )

    state: dict[str, object] = {
        "principal": _principal(PROJECT_ID, ProjectRole.OWNER),
        **task_ids,
    }
    app = FastAPI()
    app.state.database = database
    register_exception_handlers(app)
    app.include_router(job_groups.router)
    app.dependency_overrides[get_principal] = lambda: state["principal"]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client, state


async def test_job_group_api_readiness_cycle_rbac_and_project_isolation(
    job_group_client: tuple[AsyncClient, dict[str, object]],
    database: Database,
) -> None:
    client, state = job_group_client
    dependent = state["dependent"]
    prerequisite = state["prerequisite"]
    created = await client.post(
        f"/api/v1/projects/{PROJECT_ID}/job-groups",
        json={
            "name": "training-pipeline",
            "dependencies": [
                {
                    "task_id": str(dependent),
                    "depends_on_task_id": str(prerequisite),
                    "failure_policy": "cancel",
                }
            ],
        },
    )
    assert created.status_code == 201
    group_id = created.json()["id"]
    assert created.json()["task_count"] == 2
    assert created.json()["waiting_tasks"] == 1

    waiting = await client.get(
        f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}/tasks/{dependent}/dependency-state"
    )
    assert waiting.status_code == 200
    assert waiting.json()["dependency_state"] == "waiting"

    roots = await client.get(f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}/ready-tasks")
    assert roots.status_code == 200
    assert [item["task_id"] for item in roots.json()["items"]] == [str(prerequisite)]

    async with database.session() as session, session.begin():
        task = await session.get(Task, prerequisite, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.SUCCEEDED
        task.finished_at = datetime.now(UTC)

    ready = await client.get(
        f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}/tasks/{dependent}/dependency-state"
    )
    assert ready.status_code == 200
    assert ready.json()["dependency_state"] == "ready"

    cycle = await client.post(
        f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}/dependencies",
        json={
            "task_id": str(prerequisite),
            "depends_on_task_id": str(dependent),
            "failure_policy": "cancel",
        },
    )
    assert cycle.status_code == 409
    assert cycle.json()["error"]["code"] == "DAG_CYCLE"

    cross_project = await client.get(f"/api/v1/projects/{OTHER_PROJECT_ID}/job-groups/{group_id}")
    assert cross_project.status_code == 404

    state["principal"] = _principal(PROJECT_ID, ProjectRole.VIEWER)
    assert (
        await client.get(f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}")
    ).status_code == 200
    forbidden = await client.post(
        f"/api/v1/projects/{PROJECT_ID}/job-groups",
        json={"name": "viewer-cannot-create"},
    )
    assert forbidden.status_code == 403


async def test_block_policy_is_explicit_without_inventing_a_task_status(
    job_group_client: tuple[AsyncClient, dict[str, object]],
    database: Database,
) -> None:
    client, state = job_group_client
    dependent = state["blocked_dependent"]
    prerequisite = state["blocked_prerequisite"]
    created = await client.post(
        f"/api/v1/projects/{PROJECT_ID}/job-groups",
        json={
            "name": "blocked-pipeline",
            "dependencies": [
                {
                    "task_id": str(dependent),
                    "depends_on_task_id": str(prerequisite),
                    "failure_policy": "block",
                }
            ],
        },
    )
    assert created.status_code == 201
    group_id = created.json()["id"]

    async with database.session() as session, session.begin():
        task = await session.get(Task, prerequisite, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.FAILED
        task.finished_at = datetime.now(UTC)

    response = await client.get(
        f"/api/v1/projects/{PROJECT_ID}/job-groups/{group_id}/tasks/{dependent}/dependency-state"
    )
    assert response.status_code == 200
    assert response.json()["dependency_state"] == "blocked"
    async with database.session() as session:
        unchanged = await session.get(Task, dependent)
    assert unchanged is not None and unchanged.status == TaskStatus.QUEUED


async def test_job_group_list_batches_summary_queries(
    job_group_client: tuple[AsyncClient, dict[str, object]],
    database: Database,
) -> None:
    client, _state = job_group_client
    dependency_pairs = [(uuid.uuid4(), uuid.uuid4()) for _index in range(3)]
    async with database.session() as session, session.begin():
        for task_id in {item for pair in dependency_pairs for item in pair}:
            session.add(
                Task(
                    id=task_id,
                    project_id=PROJECT_ID,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                )
            )
    for index, (task_id, depends_on_task_id) in enumerate(dependency_pairs):
        created = await client.post(
            f"/api/v1/projects/{PROJECT_ID}/job-groups",
            json={
                "name": f"batched-group-{index}",
                "dependencies": [
                    {
                        "task_id": str(task_id),
                        "depends_on_task_id": str(depends_on_task_id),
                    }
                ],
            },
        )
        assert created.status_code == 201

    selects: list[str] = []

    def count_selects(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            selects.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", count_selects)
    try:
        response = await client.get(f"/api/v1/projects/{PROJECT_ID}/job-groups")
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", count_selects)

    assert response.status_code == 200
    assert len(response.json()["items"]) == len(dependency_pairs)
    assert len(selects) == 4
