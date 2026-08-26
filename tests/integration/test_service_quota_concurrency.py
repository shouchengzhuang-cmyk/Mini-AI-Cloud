import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import delete, text

from core.database import Database
from core.rbac import ProjectStatus
from models.identity import Project
from repositories.quotas import QuotaExceededError, QuotaRepository

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"


@pytest_asyncio.fixture
async def live_database() -> AsyncIterator[Database]:
    database = Database(os.getenv("LIVE_DATABASE_URL", DEFAULT_LIVE_DATABASE_URL))
    try:
        async with asyncio.timeout(2):
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
    except Exception as exc:
        await database.dispose()
        pytest.skip(f"live PostgreSQL is unavailable ({type(exc).__name__})")
    try:
        yield database
    finally:
        await database.dispose()


async def test_concurrent_service_admission_has_one_quota_winner(
    live_database: Database,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4().hex
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Service quota race {run_id}",
                    slug=f"service-quota-race-{run_id}",
                    status=ProjectStatus.ACTIVE,
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)
            await QuotaRepository.replace(
                session,
                project_id=project_id,
                max_queued_tasks=None,
                max_running_tasks=1,
                max_cpu_millicores=100,
                max_memory_mb=128,
                max_gpus=0,
                max_services=1,
                max_service_replicas=1,
                max_artifact_bytes=None,
                daily_cost_limit=None,
            )

        start = asyncio.Event()

        async def contend() -> bool:
            await start.wait()
            try:
                async with live_database.session() as session, session.begin():
                    await QuotaRepository.replace_service_commitment(
                        session,
                        project_id=project_id,
                        current_replicas=0,
                        desired_replicas=1,
                        cpu_millicores=100,
                        memory_mb=128,
                        gpu_count=0,
                    )
                return True
            except QuotaExceededError:
                return False

        contenders = [asyncio.create_task(contend()) for _ in range(2)]
        start.set()
        results = await asyncio.gather(*contenders)

        assert sorted(results) == [False, True]
        async with live_database.session() as session, session.begin():
            snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        assert snapshot.state.service_count == 1
        assert snapshot.state.service_replicas == 1
        assert snapshot.state.service_reserved_cpu_millicores == 100
        assert snapshot.state.service_reserved_memory_mb == 128
    finally:
        async with live_database.session() as session, session.begin():
            await session.execute(delete(Project).where(Project.id == project_id))
