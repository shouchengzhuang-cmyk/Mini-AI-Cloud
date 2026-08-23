import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select

from core.database import Database
from core.enums import TaskStatus, WorkerStatus
from models.base import utcnow
from models.outbox import OutboxEvent
from models.worker import Worker
from repositories.tasks import ClaimRejected, TaskRepository
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration


async def _create_task(
    database: Database,
    *,
    cpu_limit: float = 1.0,
    memory_limit_mb: int = 128,
    gpu_count: int = 0,
    max_retries: int = 0,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('ok')"],
            environment={},
            timeout_seconds=30,
            max_retries=max_retries,
            cpu_limit=cpu_limit,
            memory_limit_mb=memory_limit_mb,
            labels={"runtime": "docker"},
            network_enabled=False,
            gpu_count=gpu_count,
            idempotency_key=None,
            request_hash=None,
        )
        task_id = task.id
    return task_id


async def _register_worker(
    database: Database,
    worker_id: str,
    *,
    concurrency: int = 4,
    cpu_count: int = 4,
    memory_total_mb: int = 4096,
    gpu_count: int = 0,
) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.test",
            concurrency=concurrency,
            cpu_count=cpu_count,
            memory_total_mb=memory_total_mb,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=gpu_count,
            gpu_model="test-gpu" if gpu_count else None,
            gpu_memory_mb=24_576 if gpu_count else 0,
        )


async def _claim_and_start(
    database: Database,
    task_id: uuid.UUID,
    worker_id: str,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        _task, execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_pulling(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            lease_seconds=30,
        )
    return execution_id


async def _finish_successfully(
    database: Database,
    task_id: uuid.UUID,
    worker_id: str,
    execution_id: uuid.UUID,
) -> None:
    async with database.session() as session, session.begin():
        result = await TaskRepository.finish_execution(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            target=TaskStatus.SUCCEEDED,
            exit_code=0,
            error_message=None,
            retry_max_backoff_seconds=60,
            cpu_price_per_hour=0.05,
            gpu_price_per_hour=1.0,
        )
    assert result.accepted is True
    assert result.status == TaskStatus.SUCCEEDED
    assert result.retry_scheduled is False


async def test_cancel_requested_overrides_late_success_result(database: Database) -> None:
    worker_id = "worker-cancel-finish"
    await _register_worker(database, worker_id)
    task_id = await _create_task(database, max_retries=3)
    execution_id = await _claim_and_start(database, task_id, worker_id)

    async with database.session() as session, session.begin():
        cancellation = await TaskRepository.cancel(session, task_id)
    assert cancellation is not None
    assert cancellation.status == TaskStatus.RUNNING
    assert cancellation.cancel_requested is True

    async with database.session() as session, session.begin():
        result = await TaskRepository.finish_execution(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            target=TaskStatus.SUCCEEDED,
            exit_code=0,
            error_message=None,
            retry_max_backoff_seconds=60,
            cpu_price_per_hour=0.05,
            gpu_price_per_hour=1.0,
        )

    assert result.accepted is True
    assert result.status == TaskStatus.CANCELLED
    assert result.retry_scheduled is False

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await WorkerRepository.get(session, worker_id)
        terminal_events = list(
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == task_id,
                    OutboxEvent.event_type == "task.terminal",
                )
            )
        )

    assert task is not None
    assert task.status == TaskStatus.CANCELLED
    assert task.error_message == "task was cancelled by user request"
    assert task.next_attempt_at is None
    assert task.retry_count == 0
    assert worker is not None
    assert worker.running_tasks == 0
    assert worker.reserved_cpu == 0
    assert worker.reserved_memory_mb == 0
    assert worker.reserved_gpus == 0
    assert [event.payload["status"] for event in terminal_events] == ["cancelled"]


async def test_cancel_requested_worker_death_converges_without_retry(
    database: Database,
) -> None:
    worker_id = "worker-cancel-expired"
    await _register_worker(database, worker_id)
    task_id = await _create_task(database, max_retries=5)
    await _claim_and_start(database, task_id, worker_id)

    async with database.session() as session, session.begin():
        cancellation = await TaskRepository.cancel(session, task_id)
        task = await TaskRepository.get(session, task_id, for_update=True)
        worker = await session.get(Worker, worker_id, with_for_update=True)
        assert cancellation is not None and task is not None and worker is not None
        task.lease_expires_at = utcnow() - timedelta(seconds=1)
        worker.last_heartbeat_at = utcnow() - timedelta(seconds=60)

    async with database.session() as session, session.begin():
        offline = await WorkerRepository.mark_stale_offline(
            session,
            offline_timeout_seconds=15,
        )
    assert offline == [worker_id]

    async with database.session() as session, session.begin():
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=3,
        )
    assert recovered == [task_id]

    async with database.session() as session, session.begin():
        released = await TaskRepository.release_due_retries(session, limit=10)
    assert released == []

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await WorkerRepository.get(session, worker_id)
        event_types = list(
            await session.scalars(
                select(OutboxEvent.event_type)
                .where(OutboxEvent.aggregate_id == task_id)
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )

    assert task is not None
    assert task.status == TaskStatus.CANCELLED
    assert task.worker_id is None
    assert task.execution_id is None
    assert task.lease_expires_at is None
    assert task.next_attempt_at is None
    assert task.retry_count == 0
    assert task.recovery_count == 0
    assert worker is not None
    assert worker.status == WorkerStatus.OFFLINE
    assert worker.running_tasks == 0
    assert worker.reserved_cpu == 0
    assert worker.reserved_memory_mb == 0
    assert worker.reserved_gpus == 0
    assert event_types.count("task.ready") == 1
    assert event_types.count("task.terminal") == 1


async def test_heartbeat_recovery_preserves_live_lease_reservations(
    database: Database,
) -> None:
    worker_id = "worker-heartbeat-recovery"
    await _register_worker(
        database,
        worker_id,
        cpu_count=2,
        memory_total_mb=512,
        gpu_count=1,
    )
    task_id = await _create_task(
        database,
        cpu_limit=1.5,
        memory_limit_mb=384,
        gpu_count=1,
    )
    execution_id = await _claim_and_start(database, task_id, worker_id)

    async with database.session() as session, session.begin():
        worker = await session.get(Worker, worker_id, with_for_update=True)
        assert worker is not None
        worker.last_heartbeat_at = utcnow() - timedelta(seconds=60)

    async with database.session() as session, session.begin():
        assert await WorkerRepository.mark_stale_offline(
            session,
            offline_timeout_seconds=15,
        ) == [worker_id]

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    assert worker is not None
    assert worker.status == WorkerStatus.OFFLINE
    assert (worker.running_tasks, worker.reserved_cpu) == (1, 1.5)
    assert (worker.reserved_memory_mb, worker.reserved_gpus) == (384, 1)

    async with database.session() as session, session.begin():
        await WorkerRepository.heartbeat(session, worker_id, running_tasks=1)

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    assert worker is not None
    assert worker.status == WorkerStatus.ONLINE
    assert (worker.running_tasks, worker.reserved_cpu) == (1, 1.5)
    assert (worker.reserved_memory_mb, worker.reserved_gpus) == (384, 1)

    contender_id = await _create_task(
        database,
        cpu_limit=1.0,
        memory_limit_mb=64,
    )
    async with database.session() as session, session.begin():
        with pytest.raises(ClaimRejected, match="does not satisfy task requirements"):
            await TaskRepository.claim(
                session,
                task_id=contender_id,
                worker_id=worker_id,
                lease_seconds=30,
            )

    await _finish_successfully(database, task_id, worker_id, execution_id)


async def test_reregistering_fixed_worker_id_preserves_live_reservations(
    database: Database,
) -> None:
    worker_id = "worker-fixed-id"
    await _register_worker(
        database,
        worker_id,
        cpu_count=2,
        memory_total_mb=512,
        gpu_count=1,
    )
    task_id = await _create_task(
        database,
        cpu_limit=1.5,
        memory_limit_mb=384,
        gpu_count=1,
    )
    await _claim_and_start(database, task_id, worker_id)

    await _register_worker(
        database,
        worker_id,
        cpu_count=2,
        memory_total_mb=512,
        gpu_count=1,
    )

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    assert worker is not None
    assert worker.status == WorkerStatus.ONLINE
    assert (worker.running_tasks, worker.reserved_cpu) == (1, 1.5)
    assert (worker.reserved_memory_mb, worker.reserved_gpus) == (384, 1)


async def test_aggregate_cpu_memory_and_gpu_reservations_prevent_overcommit(
    database: Database,
) -> None:
    worker_id = "worker-capacity"
    await _register_worker(
        database,
        worker_id,
        concurrency=10,
        cpu_count=4,
        memory_total_mb=1024,
        gpu_count=2,
    )
    reserved_task_id = await _create_task(
        database,
        cpu_limit=3.0,
        memory_limit_mb=800,
        gpu_count=1,
    )
    contenders = {
        "cpu": await _create_task(
            database,
            cpu_limit=2.0,
            memory_limit_mb=64,
            gpu_count=0,
        ),
        "memory": await _create_task(
            database,
            cpu_limit=0.25,
            memory_limit_mb=300,
            gpu_count=0,
        ),
        "gpu": await _create_task(
            database,
            cpu_limit=0.25,
            memory_limit_mb=64,
            gpu_count=2,
        ),
    }
    execution_id = await _claim_and_start(database, reserved_task_id, worker_id)

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    assert worker is not None
    assert worker.running_tasks == 1
    assert worker.reserved_cpu == 3.0
    assert worker.reserved_memory_mb == 800
    assert worker.reserved_gpus == 1

    for dimension, task_id in contenders.items():
        async with database.session() as session, session.begin():
            with pytest.raises(ClaimRejected, match="does not satisfy task requirements"):
                await TaskRepository.claim(
                    session,
                    task_id=task_id,
                    worker_id=worker_id,
                    lease_seconds=30,
                )

        async with database.session() as session:
            task = await TaskRepository.get(session, task_id)
        assert task is not None, dimension
        assert task.status == TaskStatus.QUEUED, dimension
        assert task.worker_id is None, dimension

    await _finish_successfully(database, reserved_task_id, worker_id, execution_id)

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    assert worker is not None
    assert worker.running_tasks == 0
    assert worker.reserved_cpu == 0
    assert worker.reserved_memory_mb == 0
    assert worker.reserved_gpus == 0

    # Once the terminal transition releases the aggregate reservation, a task
    # rejected only for the prior GPU reservation becomes claimable.
    async with database.session() as session, session.begin():
        claimed, _execution_id = await TaskRepository.claim(
            session,
            task_id=contenders["gpu"],
            worker_id=worker_id,
            lease_seconds=30,
        )
    assert claimed.status == TaskStatus.ASSIGNED


async def test_release_due_retries_cancels_legacy_cancel_requested_row(
    database: Database,
) -> None:
    task_id = await _create_task(database, max_retries=3)
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        # Model a row left by an older deployment that allowed this combination.
        task.status = TaskStatus.RETRYING
        task.cancel_requested = True
        task.retry_count = 1
        task.next_attempt_at = utcnow() - timedelta(seconds=1)

    async with database.session() as session, session.begin():
        released = await TaskRepository.release_due_retries(session, limit=10)
    assert released == [task_id]

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        event_types = list(
            await session.scalars(
                select(OutboxEvent.event_type)
                .where(OutboxEvent.aggregate_id == task_id)
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )

    assert task is not None
    assert task.status == TaskStatus.CANCELLED
    assert task.cancel_requested is True
    assert task.next_attempt_at is None
    assert event_types == ["task.ready", "task.terminal"]

    async with database.session() as session, session.begin():
        second_release = await TaskRepository.release_due_retries(session, limit=10)
    assert second_release == []
