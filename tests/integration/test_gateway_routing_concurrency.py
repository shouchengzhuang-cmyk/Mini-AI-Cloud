import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

import repositories.services as services_module
from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    ModelAvailabilityStatus,
    RuntimeType,
)
from core.rbac import ProjectStatus
from core.runtime_profiles import RuntimeProfileCatalog
from models.admission import AdmissionEvent
from models.identity import Project
from models.model_variant import LogicalModel
from models.routing import VendorCircuitState
from models.service import ModelService, ServingRuntime
from repositories.admission import AdmissionRepository
from repositories.gateway_model_names import GatewayModelNameConflictError
from repositories.gateway_routing import GatewayRoute, GatewayRoutingRepository
from repositories.model_variants import LogicalModelConflictError, LogicalModelRepository
from repositories.quotas import QuotaRepository, QuotaSnapshot
from repositories.services import EndpointSelection, ServiceRepository
from scheduler.admission import AdmissionRequest

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


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


async def test_postgresql_concurrent_gateway_name_claim_has_one_owner(
    live_database: Database,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4().hex
    shared_name = f"gateway-shared-{run_id}"
    start = asyncio.Barrier(2)
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Gateway namespace race {run_id}",
                    slug=f"gateway-namespace-race-{run_id}",
                    status=ProjectStatus.ACTIVE,
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)

        async def create_logical_model() -> str:
            await start.wait()
            try:
                async with live_database.session() as session, session.begin():
                    await LogicalModelRepository.create(
                        session,
                        project_id=project_id,
                        name=f"logical-{run_id}",
                        public_name=shared_name,
                        description=None,
                        metadata={},
                        created_by_user_id=None,
                    )
            except LogicalModelConflictError:
                return "conflict"
            return "logical"

        async def create_direct_service() -> str:
            await start.wait()
            try:
                async with live_database.session() as session, session.begin():
                    await ServiceRepository.create(
                        session,
                        project_id=project_id,
                        name=shared_name,
                        model="org/direct-model",
                        runtime=ServingRuntime.FAKE,
                        runtime_type=RuntimeType.DOCKER,
                        image=None,
                        cpu_millicores=100,
                        memory_mb=128,
                        gpu_count=0,
                        gpu_memory_mb=0,
                        desired_replicas=0,
                    )
            except GatewayModelNameConflictError:
                return "conflict"
            return "service"

        async with asyncio.timeout(10):
            outcomes = await asyncio.gather(create_logical_model(), create_direct_service())

        assert outcomes.count("conflict") == 1
        assert set(outcomes).intersection({"logical", "service"})
        async with live_database.session() as session:
            logical_count = len(
                list(
                    await session.scalars(
                        select(LogicalModel.id).where(
                            LogicalModel.project_id == project_id,
                            LogicalModel.public_name == shared_name,
                        )
                    )
                )
            )
            service_count = len(
                list(
                    await session.scalars(
                        select(ModelService.id).where(
                            ModelService.project_id == project_id,
                            ModelService.name == shared_name,
                        )
                    )
                )
            )
        assert logical_count + service_count == 1
    finally:
        async with live_database.session() as session, session.begin():
            await session.execute(delete(ModelService).where(ModelService.project_id == project_id))
            await session.execute(delete(Project).where(Project.id == project_id))


async def test_postgresql_logical_admission_locks_project_before_quota(
    live_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4().hex
    logical_quota_locked = asyncio.Event()
    release_logical = asyncio.Event()
    direct_project_locked = asyncio.Event()
    original_get_locked = QuotaRepository.get_locked
    original_service_namespace_lock = services_module.lock_gateway_model_namespace

    async def observe_quota_lock(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> QuotaSnapshot:
        quota = await original_get_locked(session, project_id=project_id)
        current_task = asyncio.current_task()
        if current_task is not None and current_task.get_name() == "logical":
            logical_quota_locked.set()
            await release_logical.wait()
        return quota

    async def observe_service_namespace_lock(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> Project | None:
        project = await original_service_namespace_lock(session, project_id=project_id)
        current_task = asyncio.current_task()
        if current_task is not None and current_task.get_name() == "direct":
            direct_project_locked.set()
        return project

    monkeypatch.setattr(QuotaRepository, "get_locked", observe_quota_lock)
    monkeypatch.setattr(
        services_module,
        "lock_gateway_model_namespace",
        observe_service_namespace_lock,
    )
    logical: asyncio.Task[bool] | None = None
    direct: asyncio.Task[None] | None = None
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Gateway lock order {run_id}",
                    slug=f"gateway-lock-order-{run_id}",
                    status=ProjectStatus.ACTIVE,
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)

        catalog = RuntimeProfileCatalog.from_path(
            REPOSITORY_ROOT / "runtime_profiles" / "manifest.json"
        )

        async def run_logical_admission() -> bool:
            async with live_database.session() as session, session.begin():
                result = await AdmissionRepository.admit_logical_model_service(
                    session,
                    catalog=catalog,
                    project_id=project_id,
                    service_id=uuid.uuid4(),
                    logical_model_id=uuid.uuid4(),
                    request=AdmissionRequest(
                        count=1,
                        allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
                        allowed_kinds=frozenset({AcceleratorKind.GPU}),
                        selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
                    ),
                    minimum_memory_mb=1,
                    desired_replicas=1,
                    requested_dtype="float16",
                )
            return result.allowed

        async def run_direct_create() -> None:
            async with live_database.session() as session, session.begin():
                await ServiceRepository.create(
                    session,
                    project_id=project_id,
                    name=f"direct-{run_id}",
                    model="org/direct-model",
                    runtime=ServingRuntime.FAKE,
                    runtime_type=RuntimeType.DOCKER,
                    image=None,
                    cpu_millicores=100,
                    memory_mb=128,
                    gpu_count=0,
                    gpu_memory_mb=0,
                    desired_replicas=0,
                )

        logical = asyncio.create_task(run_logical_admission(), name="logical")
        await asyncio.wait_for(logical_quota_locked.wait(), timeout=5)
        direct = asyncio.create_task(run_direct_create(), name="direct")
        await asyncio.sleep(0.25)
        assert direct_project_locked.is_set() is False
        release_logical.set()
        async with asyncio.timeout(10):
            logical_allowed, _ = await asyncio.gather(logical, direct)
        assert logical_allowed is False
        assert direct_project_locked.is_set() is True
    finally:
        release_logical.set()
        tasks = [task for task in (logical, direct) if task is not None and not task.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with live_database.session() as session, session.begin():
            await session.execute(
                delete(AdmissionEvent).where(AdmissionEvent.project_id == project_id)
            )
            await session.execute(delete(ModelService).where(ModelService.project_id == project_id))
            await session.execute(delete(Project).where(Project.id == project_id))
