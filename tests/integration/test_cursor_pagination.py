from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pytest
import pytest_asyncio
from fastapi import Header
from httpx import ASGITransport, AsyncClient

from api.dependencies import get_principal
from api.main import create_app
from api.pagination import encode_cursor
from core.artifacts import ArtifactState
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus
from core.rbac import Principal, PrincipalKind
from core.redis import RedisQueue
from models.artifact import Artifact
from models.identity import Project
from models.service import ModelService, ServiceStatus, ServingRuntime
from models.task import Task
from models.worker import Worker

pytestmark = pytest.mark.integration

PROJECT_A = uuid.UUID("00000000-0000-0000-0000-000000000001")
PROJECT_B = uuid.UUID("20000000-0000-0000-0000-000000000002")
BASE_TIME = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
QueryValue = str | int | float | bool | None


@pytest_asyncio.fixture
async def cursor_client(
    database: Database,
    redis_queue: RedisQueue,
    tmp_path: Path,
) -> AsyncIterator[AsyncClient]:
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_B, name="Pagination B", slug="pagination-b"))
        await session.flush()
        for project_id, project_number in ((PROJECT_A, 1), (PROJECT_B, 2)):
            row_count = 5 if project_id == PROJECT_A else 2
            for index in range(row_count):
                # Deliberately share timestamps so UUID is exercised as the tie-breaker.
                created_at = BASE_TIME - timedelta(minutes=index // 2)
                item_number = project_number * 100 + index
                session.add_all(
                    [
                        Task(
                            id=uuid.UUID(int=1_000 + item_number),
                            project_id=project_id,
                            image="python:3.12-slim",
                            command=["true"],
                            status=(TaskStatus.QUEUED if index % 2 == 0 else TaskStatus.RUNNING),
                            created_at=created_at,
                        ),
                        ModelService(
                            id=uuid.UUID(int=2_000 + item_number),
                            project_id=project_id,
                            name=f"service-{project_number}-{index}",
                            model="example/model",
                            runtime=ServingRuntime.FAKE,
                            status=(
                                ServiceStatus.RUNNING if index % 2 == 0 else ServiceStatus.PENDING
                            ),
                            desired_replicas=0,
                            created_at=created_at,
                            updated_at=created_at,
                        ),
                        Artifact(
                            id=uuid.UUID(int=3_000 + item_number),
                            project_id=project_id,
                            name=f"artifact-{project_number}-{index}.bin",
                            state=(
                                ArtifactState.READY.value
                                if index % 2 == 0
                                else ArtifactState.PENDING.value
                            ),
                            backend="local",
                            object_key=f"pagination/{project_number}/{index}",
                            content_type="application/octet-stream",
                            size_bytes=0,
                            sha256="0" * 64,
                            created_at=created_at,
                        ),
                    ]
                )
        for index in range(5):
            started_at = BASE_TIME - timedelta(minutes=index // 2)
            session.add(
                Worker(
                    id=f"cursor-worker-{index}",
                    hostname=f"cursor-worker-{index}",
                    cpu_count=4,
                    memory_total_mb=8192,
                    started_at=started_at,
                    last_heartbeat_at=started_at,
                )
            )

    settings = Settings(
        _env_file=None,
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        artifact_local_root=str(tmp_path),
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )

    async def project_principal(
        x_test_project: Annotated[str, Header(alias="X-Test-Project")] = str(PROJECT_A),
    ) -> Principal:
        return Principal(kind=PrincipalKind.SYSTEM, project_id=uuid.UUID(x_test_project))

    app.dependency_overrides[get_principal] = project_principal
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.mark.parametrize(
    ("path", "filter_params", "filtered_state", "cross_filter_params", "cross_state"),
    [
        (
            "/api/v1/tasks",
            {"status": "queued"},
            "queued",
            {"status": "running"},
            "running",
        ),
        (
            "/api/v1/services",
            {"status": "running"},
            "running",
            {"status": "pending"},
            "pending",
        ),
        (
            "/api/v1/artifacts",
            {"state": "ready"},
            "ready",
            {"state": "pending"},
            "pending",
        ),
    ],
)
async def test_cursor_pages_are_complete_filtered_and_project_scoped(
    cursor_client: AsyncClient,
    path: str,
    filter_params: dict[str, str],
    filtered_state: str,
    cross_filter_params: dict[str, str],
    cross_state: str,
) -> None:
    all_ids = await _collect_ids(cursor_client, path, params={"limit": 2})

    assert len(all_ids) == 5
    assert len(set(all_ids)) == 5

    offset_page = await cursor_client.get(path, params={"limit": 2, "offset": 1})
    assert offset_page.status_code == 200
    assert [item["id"] for item in offset_page.json()["items"]] == all_ids[1:3]
    assert offset_page.json()["pagination"]["offset"] == 1

    filtered_ids = await _collect_ids(
        cursor_client,
        path,
        params={"limit": 1, **filter_params},
        expected_state=filtered_state,
    )
    assert len(filtered_ids) == 3
    assert set(filtered_ids).issubset(set(all_ids))

    source_page = await cursor_client.get(path, params={"limit": 1, **filter_params})
    filter_cursor = source_page.json()["pagination"]["next_cursor"]
    assert filter_cursor is not None
    cross_filter = await cursor_client.get(
        path,
        params={"limit": 10, "cursor": filter_cursor, **cross_filter_params},
    )
    assert cross_filter.status_code == 200
    state_field = "state" if path.endswith("artifacts") else "status"
    cross_items = cross_filter.json()["items"]
    assert cross_items
    assert all(item[state_field] == cross_state for item in cross_items)

    project_b_first = await cursor_client.get(
        path,
        params={"limit": 1},
        headers={"X-Test-Project": str(PROJECT_B)},
    )
    assert project_b_first.status_code == 200
    foreign_ids = {item["id"] for item in project_b_first.json()["items"]}
    foreign_cursor = project_b_first.json()["pagination"]["next_cursor"]
    assert foreign_cursor is not None

    cross_project = await cursor_client.get(
        path,
        params={"limit": 10, "cursor": foreign_cursor},
    )
    assert cross_project.status_code == 200
    assert foreign_ids.isdisjoint(item["id"] for item in cross_project.json()["items"])


@pytest.mark.parametrize(
    "path",
    ["/api/v1/tasks", "/api/v1/services", "/api/v1/artifacts", "/api/v1/workers"],
)
async def test_cursor_lists_reject_invalid_or_mixed_pagination(
    cursor_client: AsyncClient,
    path: str,
) -> None:
    invalid = await cursor_client.get(path, params={"cursor": "not-a-cursor"})
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "INVALID_CURSOR"

    cursor = encode_cursor(BASE_TIME, uuid.UUID(int=1))
    mixed = await cursor_client.get(path, params={"cursor": cursor, "offset": 1})
    assert mixed.status_code == 400
    assert mixed.json()["error"]["code"] == "INVALID_PAGINATION"


async def test_worker_cursor_pages_are_complete_and_offset_compatible(
    cursor_client: AsyncClient,
) -> None:
    all_ids = await _collect_ids(cursor_client, "/api/v1/workers", params={"limit": 2})

    assert len(all_ids) == 5
    assert len(set(all_ids)) == 5
    offset_page = await cursor_client.get(
        "/api/v1/workers",
        params={"limit": 2, "offset": 1},
    )
    assert offset_page.status_code == 200
    assert [item["id"] for item in offset_page.json()["items"]] == all_ids[1:3]


async def _collect_ids(
    client: AsyncClient,
    path: str,
    *,
    params: dict[str, QueryValue],
    expected_state: str | None = None,
) -> list[str]:
    page_params = dict(params)
    collected: list[str] = []
    expected_total: int | None = None
    for _page_number in range(10):
        response = await client.get(path, params=page_params)
        assert response.status_code == 200, response.text
        body = response.json()
        if expected_total is None:
            expected_total = body["pagination"]["total"]
        assert body["pagination"]["total"] == expected_total
        assert body["pagination"]["offset"] == 0
        for item in body["items"]:
            if expected_state is not None:
                state_field = "state" if path.endswith("artifacts") else "status"
                assert item[state_field] == expected_state
            collected.append(item["id"])
        cursor = body["pagination"]["next_cursor"]
        if cursor is None:
            break
        page_params = {**params, "cursor": cursor}
    else:
        pytest.fail("cursor pagination did not terminate")
    assert len(collected) == expected_total
    return collected
