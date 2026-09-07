import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import cast

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.accelerators import AcceleratorRequest
from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, RuntimeType, TaskStatus
from core.rbac import ProjectStatus
from core.runtime_profiles import RuntimeProfileCatalog, runtime_profile_binding_id
from models.admission import AdmissionEvent
from models.identity import Project
from models.outbox import OutboxEvent
from models.scheduling import GPUDevice, PlacementAttempt, ReservationGPUDevice, ResourceReservation
from models.task import Task, TaskEvent
from models.usage import ProjectQuotaState
from models.worker import Worker
from repositories.admission import AdmissionRepository, InventoryDeviceSnapshot
from repositories.quotas import QuotaRepository, QuotaSnapshot
from repositories.scheduling import SchedulerCandidate, SchedulingRepository
from repositories.tasks import LEGACY_PROJECT_ID, TaskRepository
from repositories.workers import WorkerRepository
from scheduler.global_scheduler import GlobalScheduler

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"
REPOSITORY_ROOT = Path(__file__).parents[2]


@pytest_asyncio.fixture
async def scheduler_live_database() -> AsyncIterator[Database]:
    url = os.getenv("LIVE_DATABASE_URL", DEFAULT_LIVE_DATABASE_URL)
    try:
        database = Database(url)
        async with asyncio.timeout(2):
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
    except Exception as exc:
        if "database" in locals():
            await database.dispose()
        pytest.skip(f"live PostgreSQL is unavailable; set LIVE_DATABASE_URL ({type(exc).__name__})")

    try:
        yield database
    finally:
        await database.dispose()


async def _create_candidate(
    database: Database, *, queue_order: int, labels: dict[str, str] | None = None
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('candidate-snapshot')"],
            environment={},
            timeout_seconds=30,
            max_retries=0,
            cpu_limit=0.25,
            memory_limit_mb=64,
            labels=labels or {},
            network_enabled=False,
            gpu_count=0,
            priority=100,
            idempotency_key=None,
            request_hash=None,
        )
        task.queue_order = queue_order
        return task.id


async def _cleanup_candidates(database: Database, task_ids: list[uuid.UUID]) -> None:
    async with database.session() as session, session.begin():
        tasks = list(
            await session.scalars(select(Task).where(Task.id.in_(task_ids)).with_for_update())
        )
        for task in tasks:
            if task.status == TaskStatus.QUEUED:
                await QuotaRepository.release_queued(session, project_id=task.project_id)
        candidate_ids = [task.id for task in tasks]
        await session.execute(
            delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(candidate_ids))
        )
        await session.execute(delete(TaskEvent).where(TaskEvent.task_id.in_(candidate_ids)))
        await session.execute(delete(Task).where(Task.id.in_(candidate_ids)))


async def _cleanup_lock_order_regression(
    database: Database,
    *,
    project_id: uuid.UUID,
    worker_id: str,
) -> None:
    """Remove every row created by the isolated live lock-order regression."""

    async with database.session() as session, session.begin():
        task_ids = select(Task.id).where(Task.project_id == project_id)
        reservation_ids = select(ResourceReservation.id).where(
            ResourceReservation.task_id.in_(task_ids)
        )
        await session.execute(
            delete(ReservationGPUDevice).where(
                ReservationGPUDevice.reservation_id.in_(reservation_ids)
            )
        )
        await session.execute(
            delete(ResourceReservation).where(ResourceReservation.task_id.in_(task_ids))
        )
        await session.execute(delete(TaskEvent).where(TaskEvent.task_id.in_(task_ids)))
        await session.execute(
            delete(PlacementAttempt).where(PlacementAttempt.task_id.in_(task_ids))
        )
        await session.execute(
            delete(AdmissionEvent).where(AdmissionEvent.workload_id.in_(task_ids))
        )
        await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(task_ids)))
        await session.execute(delete(Task).where(Task.id.in_(task_ids)))
        await session.execute(delete(GPUDevice).where(GPUDevice.worker_id == worker_id))
        await session.execute(delete(Worker).where(Worker.id == worker_id))
        await session.execute(delete(Project).where(Project.id == project_id))


async def test_candidate_discovery_does_not_hide_ranked_tasks_between_schedulers(
    scheduler_live_database: Database,
) -> None:
    run_order = -(10**9)
    async with scheduler_live_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, LEGACY_PROJECT_ID)
        assert quota_state is not None
        queued_before = quota_state.queued_tasks
    task_ids = [
        await _create_candidate(scheduler_live_database, queue_order=run_order),
        await _create_candidate(scheduler_live_database, queue_order=run_order + 1),
    ]

    try:
        async with scheduler_live_database.session() as first_session, first_session.begin():
            first_candidates = await SchedulingRepository.choose_candidates(
                first_session,
                aging_interval_seconds=60,
                scan_limit=2,
            )
            first_ids = [candidate.task.id for candidate in first_candidates[:2]]

            async with scheduler_live_database.session() as second_session, second_session.begin():
                second_candidates = await SchedulingRepository.choose_candidates(
                    second_session,
                    aging_interval_seconds=60,
                    scan_limit=2,
                )
                second_ids = [candidate.task.id for candidate in second_candidates[:2]]

        assert first_ids == task_ids
        assert second_ids == task_ids
    finally:
        await _cleanup_candidates(scheduler_live_database, task_ids)

    async with scheduler_live_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, LEGACY_PROJECT_ID)
        assert quota_state is not None
        assert quota_state.queued_tasks == queued_before


async def test_global_scheduler_skips_preemption_contention_without_task_fallback_lock(
    scheduler_live_database: Database,
) -> None:
    task_id = await _create_candidate(
        scheduler_live_database,
        queue_order=-(10**9),
        labels={"scheduler-test-isolation": str(uuid.uuid4())},
    )
    scheduler = GlobalScheduler(
        scheduler_live_database.session_factory,
        scheduler_id="scheduler-preemption-contention-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
        preemption_enabled=True,
        preemption_min_delta=1,
        batch_size=1,
        candidate_scan_limit=1,
    )

    try:
        async with scheduler_live_database.session() as first_session:
            first_transaction = await first_session.begin()
            try:
                locked_task = await first_session.scalar(
                    select(Task).where(Task.id == task_id).with_for_update()
                )
                assert locked_task is not None
                result = await asyncio.wait_for(scheduler.run_once(), timeout=2)
                assert result.task_id == task_id
                assert result.reason == "preemption_contention"
                await first_transaction.rollback()
            finally:
                if first_transaction.is_active:
                    await first_transaction.rollback()

        async with scheduler_live_database.session() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            assert task.unschedulable_reason is None
            attempts = list(
                await session.scalars(
                    select(PlacementAttempt).where(PlacementAttempt.task_id == task_id)
                )
            )
            assert attempts == []
    finally:
        await _cleanup_candidates(scheduler_live_database, [task_id])


async def test_global_schedulers_keep_quota_after_worker_inventory_lock_order(
    scheduler_live_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the former quota -> inventory / worker -> quota lock cycle on PostgreSQL.

    The CPU scheduler retains the authoritative Task -> Worker fence until it
    reserves quota. The Kubernetes scheduler pauses immediately before its
    advisory inventory scan. Before the fix that scan was reached with the
    project quota row locked, creating a real PostgreSQL cycle. It must now be
    an unlocked snapshot; ``place`` reacquires and validates inventory only
    after it has acquired the authoritative Task and Worker fences.
    """

    run_id = uuid.uuid4()
    project_id = uuid.uuid4()
    worker_id = f"live-scheduler-lock-order-{run_id}"
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    profile = next(
        item for item in catalog.manifest.profiles if item.identity == "nvidia-vllm-k8s@2.0.0"
    )
    profile_binding_id = runtime_profile_binding_id(
        profile_id=profile.profile_id,
        profile_version=profile.profile_version,
        semantic_digest=profile.semantic_digest,
    )
    accelerator = AcceleratorRequest.model_validate(
        {
            "count": 1,
            "allowed_vendors": [AcceleratorVendor.NVIDIA.value],
            "allowed_kinds": [AcceleratorKind.GPU.value],
            "allowed_models": ["NVIDIA-A100"],
            "runtime_profile": profile.profile_id,
            "selection_policy": "nvidia-only",
        }
    )
    task_ids: dict[str, set[uuid.UUID]] = {"cpu": set(), "gpu": set()}
    active_lane: ContextVar[str | None] = ContextVar("scheduler_lane", default=None)
    gpu_inventory_ready = asyncio.Event()
    cpu_quota_ready = asyncio.Event()
    gpu_inventory_gate_used = False
    cpu_quota_gate_used = False

    def _lane_session_factory(lane: str) -> Callable[[], AsyncSession]:
        @asynccontextmanager
        async def factory() -> AsyncIterator[AsyncSession]:
            token = active_lane.set(lane)
            try:
                async with scheduler_live_database.session() as session:
                    yield session
            finally:
                active_lane.reset(token)

        return cast(Callable[[], AsyncSession], factory)

    original_choose_candidates = SchedulingRepository.choose_candidates

    async def choose_lane_candidates(
        session: AsyncSession,
        *,
        aging_interval_seconds: int,
        scan_limit: int = 128,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
    ) -> list[SchedulerCandidate]:
        candidates = await original_choose_candidates(
            session,
            aging_interval_seconds=aging_interval_seconds,
            scan_limit=scan_limit,
            excluded_task_ids=excluded_task_ids,
        )
        lane = active_lane.get()
        assert lane is not None
        return [candidate for candidate in candidates if candidate.task.id in task_ids[lane]]

    original_inventory = AdmissionRepository.list_healthy_inventory_devices

    async def coordinate_gpu_inventory(
        session: AsyncSession,
        *,
        vendors: Sequence[AcceleratorVendor],
        kinds: Sequence[AcceleratorKind],
        minimum_memory_mb: int = 0,
        runtime_type: RuntimeType | None = None,
        for_update: bool = False,
        include_unavailable: bool = False,
    ) -> list[InventoryDeviceSnapshot]:
        nonlocal gpu_inventory_gate_used
        if active_lane.get() == "gpu" and not gpu_inventory_gate_used:
            gpu_inventory_gate_used = True
            gpu_inventory_ready.set()
            await asyncio.wait_for(cpu_quota_ready.wait(), timeout=2)
        return await original_inventory(
            session,
            vendors=vendors,
            kinds=kinds,
            minimum_memory_mb=minimum_memory_mb,
            runtime_type=runtime_type,
            for_update=for_update,
            include_unavailable=include_unavailable,
        )

    original_get_locked = QuotaRepository.get_locked

    async def coordinate_cpu_quota_fence(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> QuotaSnapshot:
        nonlocal cpu_quota_gate_used
        if active_lane.get() == "cpu" and not cpu_quota_gate_used:
            cpu_quota_gate_used = True
            await asyncio.wait_for(gpu_inventory_ready.wait(), timeout=2)
            cpu_quota_ready.set()
        return await original_get_locked(session, project_id=project_id)

    monkeypatch.setattr(
        SchedulingRepository,
        "choose_candidates",
        staticmethod(choose_lane_candidates),
    )
    monkeypatch.setattr(
        AdmissionRepository,
        "list_healthy_inventory_devices",
        staticmethod(coordinate_gpu_inventory),
    )
    monkeypatch.setattr(
        QuotaRepository,
        "get_locked",
        staticmethod(coordinate_cpu_quota_fence),
    )

    try:
        async with scheduler_live_database.session() as session, session.begin():
            session.add(
                Project(
                    id=project_id,
                    name=f"live scheduler lock order {run_id}",
                    slug=f"live-lock-order-{run_id.hex}",
                    status=ProjectStatus.ACTIVE,
                )
            )
            worker = await WorkerRepository.register(
                session,
                worker_id=worker_id,
                hostname=f"{worker_id}.invalid",
                concurrency=4,
                cpu_count=8,
                memory_total_mb=16_384,
                docker_version="live-integration-test",
                labels={"live-test-run": str(run_id)},
                gpu_count=2,
                gpu_model="NVIDIA-A100",
                gpu_memory_mb=81_920,
                runtime_types=[RuntimeType.DOCKER.value, RuntimeType.KUBERNETES.value],
            )
            await session.flush()
            await WorkerRepository.replace_gpu_inventory(
                session,
                worker_id=worker.id,
                worker_session_id=worker.worker_session_id,
                devices=[
                    {
                        "uuid": f"GPU-{run_id}-{index}",
                        "index": index,
                        "vendor": AcceleratorVendor.NVIDIA.value,
                        "accelerator_kind": AcceleratorKind.GPU.value,
                        "model": "NVIDIA-A100",
                        "memory_total_mb": 40_960,
                        "memory_free_mb": 40_960,
                        "compute_capability": "8.0",
                        "runtime_profile_ids": [profile_binding_id],
                        "capabilities": ["streaming"],
                        "kubernetes_resource_name": "nvidia.com/gpu",
                        "kubernetes_node_name": worker.node_name,
                    }
                    for index in range(2)
                ],
            )
            for lane, runtime_type in (
                ("cpu", RuntimeType.DOCKER),
                ("gpu", RuntimeType.KUBERNETES),
            ):
                for index in range(2):
                    task = await TaskRepository.create_queued(
                        session,
                        image="alpine:3.21",
                        command=["true"],
                        environment={},
                        timeout_seconds=30,
                        max_retries=0,
                        cpu_limit=0.25,
                        memory_limit_mb=256,
                        labels={"live-test-run": str(run_id)},
                        network_enabled=False,
                        gpu_count=1 if runtime_type == RuntimeType.KUBERNETES else 0,
                        gpu_memory_mb=8_000 if runtime_type == RuntimeType.KUBERNETES else 0,
                        accelerator_request_json=(
                            accelerator.model_dump(mode="json")
                            if runtime_type == RuntimeType.KUBERNETES
                            else None
                        ),
                        runtime_type=runtime_type.value,
                        priority=100 - index,
                        idempotency_key=None,
                        request_hash=None,
                        project_id=project_id,
                    )
                    task_ids[lane].add(task.id)

        def _scheduler(lane: str, scheduler_id: str) -> GlobalScheduler:
            return GlobalScheduler(
                _lane_session_factory(lane),
                scheduler_id=scheduler_id,
                lease_seconds=30,
                policy="binpack",
                aging_interval_seconds=60,
                cpu_price_per_hour=0.05,
                memory_price_per_gb_hour=0.005,
                gpu_price_per_hour=1.0,
                batch_size=2,
                candidate_scan_limit=8,
                runtime_profile_catalog=catalog,
            )

        cpu_scheduler = _scheduler("cpu", "live-lock-order-cpu")
        gpu_scheduler = _scheduler("gpu", "live-lock-order-gpu")

        cpu_result, gpu_result = await asyncio.wait_for(
            asyncio.gather(cpu_scheduler.run_once(), gpu_scheduler.run_once()), timeout=5
        )

        assert cpu_result.placed_count == 2
        assert gpu_result.placed_count == 2
        async with scheduler_live_database.session() as session:
            tasks = list(
                await session.scalars(
                    select(Task).where(Task.id.in_(task_ids["cpu"] | task_ids["gpu"]))
                )
            )
            state = await session.get(ProjectQuotaState, project_id)
            reservations = list(
                await session.scalars(
                    select(ResourceReservation).where(
                        ResourceReservation.task_id.in_(task_ids["cpu"] | task_ids["gpu"]),
                        ResourceReservation.released_at.is_(None),
                    )
                )
            )

        assert {task.status for task in tasks} == {TaskStatus.ASSIGNED}
        assert state is not None
        assert state.queued_tasks == 0
        assert state.running_tasks == 4
        assert state.reserved_cpu_millicores == 1_000
        assert state.reserved_memory_mb == 1_024
        assert state.reserved_gpus == 2
        assert state.reserved_nvidia_gpus == 2
        assert state.reserved_ascend_npus == 0
        assert len(reservations) == 4
        assert sum(reservation.gpu_count for reservation in reservations) == 2
    finally:
        await _cleanup_lock_order_regression(
            scheduler_live_database,
            project_id=project_id,
            worker_id=worker_id,
        )
