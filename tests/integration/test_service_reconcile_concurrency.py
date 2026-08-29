import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from api.services.autoscaler import ServiceAutoscaler
from api.services.gateway import ServiceLoad
from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
    GatewayRoutingPolicy,
    ModelAvailabilityStatus,
    RuntimeType,
)
from models.identity import Project
from models.model_variant import LogicalModel, ModelVariant
from models.outbox import OutboxEvent
from models.service import ModelService, ServiceReplica, ServingRuntime
from repositories.quotas import QuotaRepository
from repositories.services import ServiceRepository

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"
DIGEST = "sha256:" + "a" * 64


class _StaticMetrics:
    def __init__(self, loads: dict[uuid.UUID, ServiceLoad]) -> None:
        self.loads = loads

    async def snapshot(self, service_id: uuid.UUID) -> ServiceLoad | None:
        return self.loads.get(service_id)


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


async def test_autoscaler_scans_past_locked_logical_model(
    live_database: Database,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4().hex
    logical_model_ids = (uuid.uuid4(), uuid.uuid4())
    variant_ids = (uuid.uuid4(), uuid.uuid4())
    service_ids = (uuid.uuid4(), uuid.uuid4())
    now = datetime.now(UTC)
    try:
        async with live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"Autoscaler lock scan {run_id}",
                    slug=f"autoscaler-lock-scan-{run_id}",
                )
            )
            await session.flush()
            await QuotaRepository.initialize(session, project_id=project_id)
            for index, (logical_model_id, variant_id, service_id) in enumerate(
                zip(logical_model_ids, variant_ids, service_ids, strict=True)
            ):
                session.add(
                    LogicalModel(
                        id=logical_model_id,
                        project_id=project_id,
                        name=f"autoscale-logical-{index}-{run_id}",
                        public_name=f"autoscale-public-{index}-{run_id}",
                        description=None,
                        status=ModelAvailabilityStatus.READY,
                        routing_policy=GatewayRoutingPolicy.BALANCED,
                        routing_cursor=0,
                        metadata_json={},
                        created_by_user_id=None,
                    )
                )
                session.add(
                    ModelVariant(
                        id=variant_id,
                        logical_model_id=logical_model_id,
                        name=f"autoscale-variant-{index}",
                        vendor=AcceleratorVendor.NVIDIA,
                        kind=AcceleratorKind.GPU,
                        runtime_profile_id="nvidia-vllm-k8s",
                        runtime_profile_version="2.0.0",
                        runtime_profile_digest=DIGEST,
                        artifact_source="test/autoscaler",
                        artifact_revision="test-revision",
                        artifact_digest=DIGEST,
                        architecture="transformer",
                        dtype="bfloat16",
                        quantization=None,
                        status=ModelAvailabilityStatus.READY,
                        status_reason=None,
                        metadata_json={},
                        created_by_user_id=None,
                    )
                )
                await session.flush()
                service = await ServiceRepository.create(
                    session,
                    service_id=service_id,
                    project_id=project_id,
                    name=f"autoscale-service-{index}",
                    model=f"test/autoscale-{index}",
                    runtime=ServingRuntime.VLLM,
                    runtime_type=RuntimeType.KUBERNETES,
                    image=f"example/vllm@{DIGEST}",
                    cpu_millicores=100,
                    memory_mb=128,
                    gpu_count=1,
                    gpu_memory_mb=40_960,
                    desired_replicas=2,
                    tensor_parallel_size=1,
                    autoscaling_enabled=True,
                    autoscaling_min_replicas=1,
                    autoscaling_max_replicas=4,
                    autoscaling_target_concurrency=8,
                    autoscaling_cooldown_seconds=0,
                    logical_model_id=logical_model_id,
                    model_variant_id=variant_id,
                    selected_vendor=AcceleratorVendor.NVIDIA,
                    selected_kind=AcceleratorKind.GPU.value,
                    selected_model="NVIDIA A100",
                    runtime_profile_id="nvidia-vllm-k8s",
                    runtime_profile_version="2.0.0",
                    runtime_profile_digest=DIGEST,
                    allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value,
                    accelerator_resource_name="nvidia.com/gpu",
                    selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY.value,
                    eligible_node_names=("gpu-node-a", "gpu-node-b"),
                )
                service.last_autoscale_checked_at = datetime(2000, 1, 1, tzinfo=UTC) + timedelta(
                    seconds=index
                )

        metrics = _StaticMetrics(
            {
                service_id: ServiceLoad(active_requests=0, observed_at=now)
                for service_id in service_ids
            }
        )
        autoscaler = ServiceAutoscaler(live_database, metrics, batch_size=1)
        candidates = autoscaler._candidates()
        try:
            first_candidate = await anext(candidates)
            assert first_candidate[0] == service_ids[0]
            async with live_database.session() as session, session.begin():
                moved_service = await session.get(
                    ModelService,
                    service_ids[0],
                    with_for_update=True,
                )
                assert moved_service is not None
                moved_service.last_autoscale_checked_at = now
            second_candidate = await anext(candidates)
            assert second_candidate[0] == service_ids[1]
        finally:
            await candidates.aclose()

        async with live_database.session() as session, session.begin():
            moved_service = await session.get(
                ModelService,
                service_ids[0],
                with_for_update=True,
            )
            assert moved_service is not None
            moved_service.last_autoscale_checked_at = datetime(2000, 1, 1, tzinfo=UTC)
        async with live_database.session() as blocker, blocker.begin():
            locked = await blocker.scalar(
                select(LogicalModel.id)
                .where(LogicalModel.id == logical_model_ids[0])
                .with_for_update()
            )
            assert locked == logical_model_ids[0]
            result = await asyncio.wait_for(autoscaler.run_once(), timeout=2)

        async with live_database.session() as session:
            services = list(
                await session.scalars(
                    select(ModelService)
                    .where(ModelService.id.in_(service_ids))
                    .order_by(ModelService.last_autoscale_checked_at.asc().nullsfirst())
                )
            )
        by_id = {service.id: service for service in services}
        assert result.examined == 1
        assert result.scaled == 1
        assert by_id[service_ids[0]].desired_replicas == 2
        assert by_id[service_ids[1]].desired_replicas == 1
    finally:
        async with live_database.session() as session, session.begin():
            await session.execute(delete(ModelService).where(ModelService.id.in_(service_ids)))
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(service_ids))
            )
            await session.execute(
                delete(LogicalModel).where(LogicalModel.id.in_(logical_model_ids))
            )
            await session.execute(delete(Project).where(Project.id == project_id))
