import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable
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
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    RuntimeType,
    TaskStatus,
)
from core.rbac import ProjectStatus
from core.runtime_profiles import RuntimeProfileCatalog, runtime_profile_binding_id
from models.admission import AdmissionEvent
from models.identity import Project
from models.outbox import OutboxEvent
from models.scheduling import GPUDevice, PlacementAttempt, ReservationGPUDevice, ResourceReservation
from models.task import Task, TaskEvent
from models.usage import ProjectQuotaState
from models.worker import Worker
from repositories.admission import AdmissionRepository, BatchAdmissionSnapshot
from repositories.quotas import QuotaRepository, QuotaSnapshot
from repositories.scheduling import SchedulerCandidate, SchedulingRepository
from repositories.tasks import LEGACY_PROJECT_ID, TaskRepository
from repositories.workers import WorkerRepository
from scheduler.admission import AdmissionRequest
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
        await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(task_ids)))
        await session.execute(delete(Task).where(Task.id.in_(task_ids)))
        await session.execute(delete(GPUDevice).where(GPUDevice.worker_id == worker_id))
        await session.execute(delete(Worker).where(Worker.id == worker_id))
        await session.execute(delete(AdmissionEvent).where(AdmissionEvent.project_id == project_id))
        await session.flush()
        await session.execute(delete(Project).where(Project.id == project_id))


async def _cleanup_cross_worker_lock_order_regression(
    database: Database,
    *,
    project_ids: tuple[uuid.UUID, uuid.UUID],
    worker_ids: tuple[str, str],
) -> None:
    """Remove the rows owned by the isolated cross-worker live regression."""

    async with database.session() as session, session.begin():
        task_ids = select(Task.id).where(Task.project_id.in_(project_ids))
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
        await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(task_ids)))
        await session.execute(delete(Task).where(Task.id.in_(task_ids)))
        await session.execute(delete(GPUDevice).where(GPUDevice.worker_id.in_(worker_ids)))
        await session.execute(delete(Worker).where(Worker.id.in_(worker_ids)))
        await session.execute(
            delete(AdmissionEvent).where(AdmissionEvent.project_id.in_(project_ids))
        )
        await session.flush()
        await session.execute(delete(Project).where(Project.id.in_(project_ids)))


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

    A service-equivalent transaction takes the same authoritative quota then
    inventory locks as Kubernetes service admission. Two ``GlobalScheduler``
    instances with ``batch_size=2`` plus a worker-pull ``claim`` race it for
    CPU and Kubernetes GPU work. With the old batch or claim Worker/GPU ->
    quota order, one scheduler or claim transaction and the
    service transaction formed a PostgreSQL deadlock; canonical quota ->
    Worker/GPU ordering makes the schedulers wait at quota until service
    inventory admission commits.
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
    worker_pull_task_id: uuid.UUID | None = None
    active_lane: ContextVar[str | None] = ContextVar("scheduler_lane", default=None)
    service_quota_locked = asyncio.Event()
    quota_arrivals: set[str] = set()
    all_placement_actors_at_quota = asyncio.Event()

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

    monkeypatch.setattr(
        SchedulingRepository,
        "choose_candidates",
        staticmethod(choose_lane_candidates),
    )
    original_get_locked = QuotaRepository.get_locked

    async def coordinate_quota_arrival(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> QuotaSnapshot:
        actor = active_lane.get()
        if actor in {"cpu", "gpu", "claim"}:
            quota_arrivals.add(actor)
            if quota_arrivals == {"cpu", "gpu", "claim"}:
                all_placement_actors_at_quota.set()
        return await original_get_locked(session, project_id=project_id)

    monkeypatch.setattr(
        QuotaRepository,
        "get_locked",
        staticmethod(coordinate_quota_arrival),
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
                concurrency=5,
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
            worker_pull_task = await TaskRepository.create_queued(
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
                gpu_count=0,
                runtime_type=RuntimeType.DOCKER.value,
                priority=1,
                idempotency_key=None,
                request_hash=None,
                project_id=project_id,
            )
            worker_pull_task_id = worker_pull_task.id

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
        assert worker_pull_task_id is not None

        async def run_service_inventory_fence() -> None:
            async with scheduler_live_database.session() as session, session.begin():
                await QuotaRepository.get_locked(session, project_id=project_id)
                service_quota_locked.set()
                # Do not use timing: all authoritative placement actors must
                # be blocked at their quota fence before service requests
                # Worker/GPU inventory. A Worker/GPU -> quota regression
                # therefore establishes the inverse PostgreSQL lock cycle.
                await asyncio.wait_for(all_placement_actors_at_quota.wait(), timeout=2)
                inventory = await AdmissionRepository.list_healthy_inventory_devices(
                    session,
                    vendors=(AcceleratorVendor.NVIDIA,),
                    kinds=(AcceleratorKind.GPU,),
                    runtime_type=RuntimeType.KUBERNETES,
                    for_update=True,
                    include_unavailable=True,
                )
                assert len(inventory) == 2

        async def run_worker_pull_claim() -> tuple[Task, uuid.UUID]:
            token = active_lane.set("claim")
            try:
                async with scheduler_live_database.session() as session, session.begin():
                    return await TaskRepository.claim(
                        session,
                        task_id=worker_pull_task_id,
                        worker_id=worker_id,
                        lease_seconds=30,
                    )
            finally:
                active_lane.reset(token)

        service_fence = asyncio.create_task(
            run_service_inventory_fence(),
            name="service-quota-inventory-fence",
        )
        await asyncio.wait_for(service_quota_locked.wait(), timeout=2)
        _, cpu_result, gpu_result, (claimed_task, _) = await asyncio.wait_for(
            asyncio.gather(
                service_fence,
                cpu_scheduler.run_once(),
                gpu_scheduler.run_once(),
                run_worker_pull_claim(),
            ),
            timeout=5,
        )

        assert cpu_result.placed_count == 2
        assert gpu_result.placed_count == 2
        assert claimed_task.id == worker_pull_task_id
        async with scheduler_live_database.session() as session:
            tasks = list(
                await session.scalars(
                    select(Task).where(
                        Task.id.in_(task_ids["cpu"] | task_ids["gpu"] | {worker_pull_task_id})
                    )
                )
            )
            state = await session.get(ProjectQuotaState, project_id)
            reservations = list(
                await session.scalars(
                    select(ResourceReservation).where(
                        ResourceReservation.task_id.in_(
                            task_ids["cpu"] | task_ids["gpu"] | {worker_pull_task_id}
                        ),
                        ResourceReservation.released_at.is_(None),
                    )
                )
            )

        assert {task.status for task in tasks} == {TaskStatus.ASSIGNED}
        assert state is not None
        assert state.queued_tasks == 0
        assert state.running_tasks == 5
        assert state.reserved_cpu_millicores == 1_250
        assert state.reserved_memory_mb == 1_280
        assert state.reserved_gpus == 2
        assert state.reserved_nvidia_gpus == 2
        assert state.reserved_ascend_npus == 0
        assert len(reservations) == 5
        assert sum(reservation.gpu_count for reservation in reservations) == 2
    finally:
        await _cleanup_lock_order_regression(
            scheduler_live_database,
            project_id=project_id,
            worker_id=worker_id,
        )


async def test_kubernetes_placements_do_not_lock_unselected_workers(
    scheduler_live_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep two project-local placements from forming a Worker A <-> Worker B cycle.

    Each placement locks its own quota, Worker, and Kubernetes pool rows before
    the rendezvous. The old batch-capacity planner then locked every matching
    inventory row, so the two transactions deadlocked while trying to lock the
    other's Worker. The planner's global accounting snapshot must remain
    non-locking; only the selected pool is authoritative for this mutation.
    """

    run_id = uuid.uuid4()
    project_ids = (uuid.uuid4(), uuid.uuid4())
    worker_ids = (
        f"live-cross-worker-a-{run_id.hex}",
        f"live-cross-worker-b-{run_id.hex}",
    )
    task_ids: dict[str, uuid.UUID] = {}
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
            "selection_policy": AcceleratorSelectionPolicy.NVIDIA_ONLY.value,
        }
    )
    request = AdmissionRequest(
        count=1,
        allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
        allowed_kinds=frozenset({AcceleratorKind.GPU}),
        allowed_models=frozenset({"NVIDIA-A100"}),
        runtime_profile_id=profile.profile_id,
        selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
    )
    admissions: dict[str, BatchAdmissionSnapshot] = {}
    local_pool_locks: set[str] = set()
    both_local_pools_locked = asyncio.Event()

    try:
        async with scheduler_live_database.session() as session, session.begin():
            for index, project_id in enumerate(project_ids):
                session.add(
                    Project(
                        id=project_id,
                        name=f"cross worker lock order {run_id} {index}",
                        slug=f"cross-worker-lock-{run_id.hex}-{index}",
                        status=ProjectStatus.ACTIVE,
                    )
                )

            for index, (project_id, worker_id) in enumerate(
                zip(project_ids, worker_ids, strict=True)
            ):
                node_name = f"cross-worker-node-{index}-{run_id.hex}"
                worker = await WorkerRepository.register(
                    session,
                    worker_id=worker_id,
                    hostname=f"{worker_id}.invalid",
                    concurrency=1,
                    cpu_count=4,
                    memory_total_mb=8_192,
                    docker_version="live-integration-test",
                    labels={"live-test-run": str(run_id)},
                    gpu_count=1,
                    gpu_model="NVIDIA-A100",
                    gpu_memory_mb=40_960,
                    node_name=node_name,
                    runtime_types=[RuntimeType.KUBERNETES.value],
                )
                await session.flush()
                await WorkerRepository.replace_gpu_inventory(
                    session,
                    worker_id=worker.id,
                    worker_session_id=worker.worker_session_id,
                    devices=[
                        {
                            "uuid": f"GPU-{run_id}-{index}",
                            "index": 0,
                            "vendor": AcceleratorVendor.NVIDIA.value,
                            "accelerator_kind": AcceleratorKind.GPU.value,
                            "model": "NVIDIA-A100",
                            "memory_total_mb": 40_960,
                            "memory_free_mb": 40_960,
                            "compute_capability": "8.0",
                            "runtime_profile_ids": [profile_binding_id],
                            "capabilities": ["streaming"],
                            "kubernetes_resource_name": "nvidia.com/gpu",
                            "kubernetes_node_name": node_name,
                        }
                    ],
                )
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
                    gpu_count=1,
                    gpu_memory_mb=8_000,
                    accelerator_request_json=accelerator.model_dump(mode="json"),
                    runtime_type=RuntimeType.KUBERNETES.value,
                    priority=100,
                    idempotency_key=None,
                    request_hash=None,
                    project_id=project_id,
                )
                task_ids[worker_id] = task.id

        for worker_id in worker_ids:
            async with scheduler_live_database.session() as session, session.begin():
                queued_task = await TaskRepository.get(session, task_ids[worker_id])
                assert queued_task is not None
                result = await AdmissionRepository.admit_batch_task(
                    session,
                    catalog=catalog,
                    task=queued_task,
                    request=request,
                    allowed_worker_ids=frozenset({worker_id}),
                )
                assert result.snapshot is not None
                assert result.snapshot.worker_id == worker_id
                admissions[worker_id] = result.snapshot

        original_available = AdmissionRepository.available_batch_accelerators_for_pool

        async def rendezvous_before_global_accounting(
            session: AsyncSession,
            *,
            catalog: RuntimeProfileCatalog,
            worker_id: str,
            node_name: str,
            vendor: AcceleratorVendor,
            kind: AcceleratorKind,
            model: str,
            profile_id: str,
            profile_version: str,
            profile_digest: str,
            resource_name: str,
            minimum_memory_mb: int,
            required_capabilities: frozenset[str] = frozenset(),
        ) -> int:
            local_pool_locks.add(worker_id)
            if local_pool_locks == set(worker_ids):
                both_local_pools_locked.set()
            await asyncio.wait_for(both_local_pools_locked.wait(), timeout=2)
            return await original_available(
                session,
                catalog=catalog,
                worker_id=worker_id,
                node_name=node_name,
                vendor=vendor,
                kind=kind,
                model=model,
                profile_id=profile_id,
                profile_version=profile_version,
                profile_digest=profile_digest,
                resource_name=resource_name,
                minimum_memory_mb=minimum_memory_mb,
                required_capabilities=required_capabilities,
            )

        monkeypatch.setattr(
            AdmissionRepository,
            "available_batch_accelerators_for_pool",
            staticmethod(rendezvous_before_global_accounting),
        )

        async def place_on_selected_worker(worker_id: str) -> tuple[Task, uuid.UUID]:
            async with scheduler_live_database.session() as session, session.begin():
                return await SchedulingRepository.place(
                    session,
                    task_id=task_ids[worker_id],
                    worker_id=worker_id,
                    gpu_device_ids=(),
                    lease_seconds=30,
                    cpu_price_per_hour=0.05,
                    memory_price_per_gb_hour=0.005,
                    gpu_price_per_hour=1.0,
                    admission=admissions[worker_id],
                    runtime_profile_catalog=catalog,
                )

        placements = await asyncio.wait_for(
            asyncio.gather(*(place_on_selected_worker(worker_id) for worker_id in worker_ids)),
            timeout=5,
        )
        assert {task.id for task, _execution_id in placements} == set(task_ids.values())

        async with scheduler_live_database.session() as session:
            tasks = list(await session.scalars(select(Task).where(Task.id.in_(task_ids.values()))))
            workers = list(await session.scalars(select(Worker).where(Worker.id.in_(worker_ids))))
            states = [
                await session.get(ProjectQuotaState, project_id) for project_id in project_ids
            ]
            reservations = list(
                await session.scalars(
                    select(ResourceReservation).where(
                        ResourceReservation.task_id.in_(task_ids.values()),
                        ResourceReservation.released_at.is_(None),
                    )
                )
            )
            reservation_device_bindings = list(
                await session.scalars(
                    select(ReservationGPUDevice).where(
                        ReservationGPUDevice.reservation_id.in_(
                            [reservation.id for reservation in reservations]
                        )
                    )
                )
            )

        assert {task.status for task in tasks} == {TaskStatus.ASSIGNED}
        assert {task.worker_id for task in tasks} == set(worker_ids)
        assert all(task.gpu_device_ids == [] for task in tasks)
        assert {worker.id for worker in workers} == set(worker_ids)
        assert all(
            worker.running_tasks == 1
            and worker.reserved_gpus == 1
            and worker.reserved_cpu == 0.25
            and worker.reserved_memory_mb == 256
            for worker in workers
        )
        assert all(state is not None for state in states)
        assert all(
            state.queued_tasks == 0
            and state.running_tasks == 1
            and state.reserved_cpu_millicores == 250
            and state.reserved_memory_mb == 256
            and state.reserved_gpus == 1
            and state.reserved_nvidia_gpus == 1
            and state.reserved_ascend_npus == 0
            for state in states
            if state is not None
        )
        assert len(reservations) == 2
        assert reservation_device_bindings == []
    finally:
        await _cleanup_cross_worker_lock_order_regression(
            scheduler_live_database,
            project_ids=project_ids,
            worker_ids=worker_ids,
        )
