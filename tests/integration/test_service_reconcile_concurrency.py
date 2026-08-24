import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import RuntimeType
from models.identity import Project
from models.outbox import OutboxEvent
from models.service import ModelService, ServiceReplica, ServingRuntime
from repositories.quotas import QuotaRepository
from repositories.services import ServiceRepository

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


async def test_concurrent_reconcilers_create_exact_desired_replica_count(
    live_database: Database,
) -> None:
    project_id = uuid.uuid4()
    service_id: uuid.UUID | None = None
    run_id = uuid.uuid4().hex
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Service reconcile race {run_id}",
                    slug=f"service-reconcile-race-{run_id}",
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)
            service = await ServiceRepository.create(
                session,
                project_id=project_id,
                name="concurrent-reconcile",
                model="fake/concurrent-model",
                runtime=ServingRuntime.FAKE,
                runtime_type=RuntimeType.FAKE,
                image=None,
                cpu_millicores=100,
                memory_mb=128,
                gpu_count=0,
                gpu_memory_mb=0,
                desired_replicas=2,
            )
            service_id = service.id

        start = asyncio.Event()

        async def contend() -> int:
            await start.wait()
            result = await ServiceReconciler(live_database).reconcile_service(
                service_id,
                project_id=project_id,
            )
            assert result is not None
            return result.replicas_created

        contenders = [asyncio.create_task(contend()) for _ in range(2)]
        start.set()
        created = await asyncio.gather(*contenders)

        async with live_database.session() as session:
            replicas = await ServiceRepository.list_replicas(session, service_id)
        assert sum(created) == 2
        assert len(replicas) == 2
        assert [(item.generation, item.ordinal) for item in replicas] == [(1, 0), (1, 1)]
    finally:
        async with live_database.session() as session, session.begin():
            aggregate_ids: list[uuid.UUID] = []
            if service_id is not None:
                aggregate_ids.append(service_id)
                aggregate_ids.extend(
                    await session.scalars(
                        select(ServiceReplica.id).where(ServiceReplica.service_id == service_id)
                    )
                )
                await session.execute(delete(ModelService).where(ModelService.id == service_id))
            if aggregate_ids:
                await session.execute(
                    delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(aggregate_ids))
                )
            await session.execute(delete(Project).where(Project.id == project_id))
