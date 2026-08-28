import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from core.database import Database
from core.enums import ModelAvailabilityStatus
from core.rbac import ProjectStatus
from models.identity import Project
from models.model_variant import LogicalModel
from models.routing import VendorCircuitState
from repositories.gateway_routing import GatewayRoute, GatewayRoutingRepository
from repositories.services import EndpointSelection

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


async def test_postgresql_concurrent_first_outcomes_create_one_circuit_row(
    live_database: Database,
) -> None:
    project_id = uuid.uuid4()
    logical_model_id = uuid.uuid4()
    run_id = uuid.uuid4().hex
    route = GatewayRoute(
        service_id=uuid.uuid4(),
        logical_model_id=logical_model_id,
        model_variant_id=uuid.uuid4(),
        selected_vendor="nvidia",
        upstream_model="physical/nvidia",
        gpu_count=1,
        selection=EndpointSelection(
            service_id=uuid.uuid4(),
            replica_id=uuid.uuid4(),
            generation=1,
            execution_id=uuid.uuid4(),
            endpoint_url="http://worker.invalid:8000",
        ),
    )
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Gateway circuit race {run_id}",
                    slug=f"gateway-circuit-race-{run_id}",
                    status=ProjectStatus.ACTIVE,
                )
            )
            session.add(
                LogicalModel(
                    id=logical_model_id,
                    project_id=project_id,
                    name=f"gateway-race-{run_id}",
                    public_name=f"Gateway race {run_id}",
                    status=ModelAvailabilityStatus.READY,
                )
            )

        start = asyncio.Event()

        async def record_failure() -> None:
            await start.wait()
            async with live_database.session() as session, session.begin():
                await GatewayRoutingRepository.record_outcome(
                    session,
                    route=route,
                    project_id=project_id,
                    success=False,
                    error_code="UPSTREAM_DISCONNECTED",
                    failure_threshold=100,
                    cooldown_seconds=30,
                )

        contenders = [asyncio.create_task(record_failure()) for _ in range(8)]
        start.set()
        async with asyncio.timeout(10):
            await asyncio.gather(*contenders)

        async with live_database.session() as session:
            states = list(
                await session.scalars(
                    select(VendorCircuitState).where(
                        VendorCircuitState.project_id == project_id,
                        VendorCircuitState.logical_model_id == logical_model_id,
                        VendorCircuitState.vendor == "nvidia",
                    )
                )
            )
        assert len(states) == 1
        assert states[0].failure_count == 8
        assert states[0].version == 8
    finally:
        async with live_database.session() as session, session.begin():
            await session.execute(delete(Project).where(Project.id == project_id))
