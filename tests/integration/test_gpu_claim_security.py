import uuid

import pytest
from sqlalchemy import delete, func, select

from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, AllocationAuthority, TaskStatus
from models.scheduling import ReservationGPUDevice, ResourceReservation
from models.usage import TaskExecution
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository
from scheduler import Scheduler
from scheduler.global_scheduler import GlobalScheduler

pytestmark = pytest.mark.integration


async def _register_worker(
    database: Database,
    worker_id: str,
    *,
    gpu_count: int,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.test",
            concurrency=2,
            cpu_count=4,
            memory_total_mb=4096,
            docker_version="test",
            labels={},
            gpu_count=gpu_count,
            gpu_model="NVIDIA-A100" if gpu_count else None,
            gpu_memory_mb=40_960 if gpu_count else 0,
        )
        return worker.worker_session_id


async def _replace_inventory(
    database: Database,
    worker_id: str,
    worker_session_id: uuid.UUID,
) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.replace_gpu_inventory(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            devices=[
                {
                    "uuid": "GPU-A100-0",
                    "index": 0,
                    "vendor": "nvidia",
                    "model": "NVIDIA-A100",
                    "memory_total_mb": 40_960,
                    "memory_free_mb": 40_960,
                    "compute_capability": "8.0",
                    "health": "healthy",
                    "fake": False,
                }
            ],
        )


async def _create_task(database: Database, *, gpu_count: int) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('ok')"],
            environment={},
            timeout_seconds=30,
            max_retries=0,
            cpu_limit=1.0,
            memory_limit_mb=128,
            labels={},
            network_enabled=False,
            gpu_count=gpu_count,
            gpu_memory_mb=8_000 if gpu_count else 0,
            gpu_model="NVIDIA-A100" if gpu_count else None,
            idempotency_key=None,
            request_hash=None,
        )
        return task.id


async def test_pull_scheduler_keeps_cpu_tasks_compatible_without_gpu_inventory(
    database: Database,
) -> None:
    await _register_worker(database, "legacy-cpu-worker", gpu_count=2)
    task_id = await _create_task(database, gpu_count=0)

    assignment = await Scheduler(database.session, lease_seconds=30).claim_for_worker(
        worker_id="legacy-cpu-worker"
    )

    assert assignment is not None
    assert assignment.task_id == task_id
    assert assignment.gpu_device_ids == ()


async def test_pull_scheduler_requires_concrete_gpu_inventory_and_reservation(
    database: Database,
) -> None:
    worker_id = "pull-gpu-worker"
    worker_session_id = await _register_worker(database, worker_id, gpu_count=1)
    task_id = await _create_task(database, gpu_count=1)
    scheduler = Scheduler(database.session, lease_seconds=30)

    assert await scheduler.claim_for_worker(worker_id=worker_id) is None
    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        reservation_count = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.task_id == task_id
                )
            )
            or 0
        )
    assert task is not None and task.status == TaskStatus.QUEUED
    assert task.worker_id is None
    assert reservation_count == 0

    await _replace_inventory(database, worker_id, worker_session_id)
    assignment = await scheduler.claim_for_worker(worker_id=worker_id)

    assert assignment is not None
    assert assignment.gpu_device_ids == ("GPU-A100-0",)
    async with database.session() as session:
        reservation = await session.scalar(
            select(ResourceReservation).where(ResourceReservation.task_id == task_id)
        )
        assert reservation is not None
        execution = await session.get(TaskExecution, reservation.execution_id)
        assert execution is not None
        active_device_links = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.reservation_id == reservation.id,
                    ReservationGPUDevice.released_at.is_(None),
                )
            )
            or 0
        )
    assert reservation.legacy_unbound is False
    assert reservation.allocation_authority == AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE
    assert reservation.requested_vendor == AcceleratorVendor.NVIDIA
    assert reservation.requested_kind == AcceleratorKind.GPU
    assert reservation.observed_device_ids_json == ["GPU-A100-0"]
    assert reservation.observed_vendor == AcceleratorVendor.NVIDIA
    assert reservation.observed_at is not None
    assert execution.allocation_authority == AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE
    assert execution.observed_device_ids_json == ["GPU-A100-0"]
    assert active_device_links == 1


async def test_global_scheduler_rejects_inventoryless_gpu_and_worker_skips_unbound_assignment(
    database: Database,
) -> None:
    worker_id = "global-gpu-worker"
    worker_session_id = await _register_worker(database, worker_id, gpu_count=1)
    task_id = await _create_task(database, gpu_count=1)
    global_scheduler = GlobalScheduler(
        database.session_factory,
        scheduler_id="gpu-security-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
    )

    rejected = await global_scheduler.run_once()
    assert rejected.placed is False
    assert rejected.reason == "gpu_model_mismatch"

    await _replace_inventory(database, worker_id, worker_session_id)
    placed = await global_scheduler.run_once()
    assert placed.placed is True
    assert placed.worker_id == worker_id

    async with database.session() as session, session.begin():
        reservation = await session.scalar(
            select(ResourceReservation).where(ResourceReservation.task_id == task_id)
        )
        assert reservation is not None
        active_device_links = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.reservation_id == reservation.id,
                    ReservationGPUDevice.released_at.is_(None),
                )
            )
            or 0
        )
        assert active_device_links == 1
        await session.execute(
            delete(ReservationGPUDevice).where(
                ReservationGPUDevice.reservation_id == reservation.id
            )
        )

    worker_scheduler = Scheduler(
        database.session,
        lease_seconds=30,
        mode="global",
        worker_session_id=worker_session_id,
    )
    assert await worker_scheduler.claim_for_worker(worker_id=worker_id) is None

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
    assert task is not None and task.status == TaskStatus.ASSIGNED
