import uuid
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from core.database import Database
from core.enums import TaskStatus
from models.base import utcnow
from models.scheduling import PreemptionPlan, ReservationGPUDevice, ResourceReservation
from models.usage import ProjectQuotaState
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository
from scheduler.global_scheduler import GlobalScheduler

pytestmark = pytest.mark.integration


async def _create_gpu_task(database: Database, *, priority: int, preemptible: bool) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('gpu')"],
            environment={},
            timeout_seconds=60,
            max_retries=0,
            cpu_limit=1.0,
            memory_limit_mb=256,
            labels={},
            network_enabled=False,
            gpu_count=1,
            gpu_memory_mb=8_000,
            gpu_model="NVIDIA-A100",
            priority=priority,
            preemptible=preemptible,
            idempotency_key=None,
            request_hash=None,
        )
        return task.id


async def _register_gpu_worker(database: Database) -> None:
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id="gpu-worker",
            hostname="gpu-worker.test",
            concurrency=2,
            cpu_count=4,
            memory_total_mb=8_192,
            docker_version="test",
            labels={},
            gpu_count=1,
            gpu_model="NVIDIA-A100",
            gpu_memory_mb=40_960,
        )
        await session.flush()
        await WorkerRepository.replace_gpu_inventory(
            session,
            worker_id=worker.id,
            worker_session_id=worker.worker_session_id,
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
                    "fake": True,
                }
            ],
        )


async def test_preemption_releases_gpu_only_after_fenced_worker_result(
    database: Database,
) -> None:
    await _register_gpu_worker(database)
    low_id = await _create_gpu_task(database, priority=10, preemptible=True)
    async with database.session() as session, session.begin():
        _low, low_execution_id = await TaskRepository.claim(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            lease_seconds=30,
        )
        await TaskRepository.mark_pulling(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )

    high_id = await _create_gpu_task(database, priority=100, preemptible=False)
    scheduler = GlobalScheduler(
        database.session_factory,
        scheduler_id="scheduler-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
        preemption_enabled=True,
        preemption_min_delta=10,
    )

    requested = await scheduler.run_once()
    assert requested.task_id == high_id
    assert requested.placed is False
    assert requested.reason == "preemption_in_progress"

    async with database.session() as session:
        low = await TaskRepository.get(session, low_id)
        active_reservations = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.execution_id == low_execution_id,
                    ResourceReservation.released_at.is_(None),
                )
            )
            or 0
        )
        active_gpu_links = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.released_at.is_(None)
                )
            )
            or 0
        )
    assert low is not None and low.status == TaskStatus.PREEMPTING
    assert low.cancel_requested is True
    assert active_reservations == 1
    assert active_gpu_links == 1

    async with database.session() as session, session.begin():
        result = await TaskRepository.finish_execution(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            target=TaskStatus.CANCELLED,
            exit_code=None,
            error_message="runtime stopped",
            retry_max_backoff_seconds=60,
            cpu_price_per_hour=0.05,
            memory_price_per_gb_hour=0.005,
            gpu_price_per_hour=1.0,
        )
    assert result.accepted is True
    assert result.status == TaskStatus.RETRYING
    assert result.retry_scheduled is True

    placed = await scheduler.run_once()
    assert placed.task_id == high_id
    assert placed.worker_id == "gpu-worker"
    assert placed.placed is True

    async with database.session() as session:
        low = await TaskRepository.get(session, low_id)
        high = await TaskRepository.get(session, high_id)
        plan = await session.scalar(
            select(PreemptionPlan).where(PreemptionPlan.victim_task_id == low_id)
        )
        quota_state = await session.get(
            ProjectQuotaState,
            uuid.UUID("00000000-0000-0000-0000-000000000001"),
        )
        active_gpu_links = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.released_at.is_(None)
                )
            )
            or 0
        )
    assert low is not None and low.status == TaskStatus.RETRYING
    assert low.preemption_count == 1
    assert high is not None and high.status == TaskStatus.ASSIGNED
    assert high.gpu_device_ids == ["GPU-A100-0"]
    assert plan is not None and plan.state == "completed"
    assert quota_state is not None
    assert (quota_state.queued_tasks, quota_state.running_tasks) == (1, 1)
    assert active_gpu_links == 1


async def test_preempting_victim_lease_expiry_completes_plan_and_requeues_victim(
    database: Database,
) -> None:
    await _register_gpu_worker(database)
    low_id = await _create_gpu_task(database, priority=10, preemptible=True)
    async with database.session() as session, session.begin():
        _low, low_execution_id = await TaskRepository.claim(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            lease_seconds=30,
        )
        await TaskRepository.mark_pulling(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )

    high_id = await _create_gpu_task(database, priority=100, preemptible=False)
    scheduler = GlobalScheduler(
        database.session_factory,
        scheduler_id="scheduler-lease-recovery-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
        preemption_enabled=True,
        preemption_min_delta=10,
    )
    requested = await scheduler.run_once()
    assert requested.reason == "preemption_in_progress"

    async with database.session() as session, session.begin():
        low = await TaskRepository.get(session, low_id, for_update=True)
        assert low is not None and low.status == TaskStatus.PREEMPTING
        low.lease_expires_at = utcnow() - timedelta(seconds=1)

    async with database.session() as session, session.begin():
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=0,
        )
    assert recovered == [low_id]

    placed = await scheduler.run_once()
    assert placed.task_id == high_id
    assert placed.worker_id == "gpu-worker"
    assert placed.placed is True

    async with database.session() as session:
        low = await TaskRepository.get(session, low_id)
        plan = await session.scalar(
            select(PreemptionPlan).where(PreemptionPlan.victim_task_id == low_id)
        )
        active_old_reservation = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.execution_id == low_execution_id,
                    ResourceReservation.released_at.is_(None),
                )
            )
            or 0
        )
    assert low is not None
    assert low.status == TaskStatus.RETRYING
    assert low.error_category == "PREEMPTED"
    assert low.worker_id is None
    assert low.execution_id is None
    assert plan is not None and plan.state == "completed"
    assert active_old_reservation == 0


async def test_user_cancel_wins_preemption_lease_recovery_without_requeue(
    database: Database,
) -> None:
    await _register_gpu_worker(database)
    low_id = await _create_gpu_task(database, priority=10, preemptible=True)
    async with database.session() as session, session.begin():
        _low, low_execution_id = await TaskRepository.claim(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            lease_seconds=30,
        )
        await TaskRepository.mark_pulling(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=low_id,
            worker_id="gpu-worker",
            execution_id=low_execution_id,
            lease_seconds=30,
        )

    high_id = await _create_gpu_task(database, priority=100, preemptible=False)
    scheduler = GlobalScheduler(
        database.session_factory,
        scheduler_id="scheduler-cancel-preemption-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
        preemption_enabled=True,
        preemption_min_delta=10,
    )
    assert (await scheduler.run_once()).reason == "preemption_in_progress"

    async with database.session() as session, session.begin():
        cancelled = await TaskRepository.cancel(session, low_id)
        assert cancelled is not None
        assert cancelled.status == TaskStatus.PREEMPTING
        assert cancelled.requeue_on_preempt is False
        cancelled.lease_expires_at = utcnow() - timedelta(seconds=1)

    async with database.session() as session, session.begin():
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=3,
        )
    assert recovered == [low_id]

    placed = await scheduler.run_once()
    assert placed.task_id == high_id and placed.placed is True
    async with database.session() as session:
        low = await TaskRepository.get(session, low_id)
        plan = await session.scalar(
            select(PreemptionPlan).where(PreemptionPlan.victim_task_id == low_id)
        )
    assert low is not None
    assert low.status == TaskStatus.CANCELLED
    assert low.retry_count == 0
    assert low.recovery_count == 0
    assert low.next_attempt_at is None
    assert plan is not None and plan.state == "completed"
