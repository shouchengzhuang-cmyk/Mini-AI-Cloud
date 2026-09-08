import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import Database
from core.enums import RuntimeType, TaskStatus
from models.base import utcnow
from models.outbox import OutboxEvent
from models.worker import Worker
from repositories.quotas import QuotaRepository, QuotaSnapshot
from repositories.tasks import ClaimRejected, TaskRepository
from repositories.workers import WorkerRepository
from scheduler import AssignmentSource, Scheduler

pytestmark = pytest.mark.integration


async def _create_task(
    database: Database,
    *,
    max_retries: int = 0,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('ok')"],
            environment={"MODE": "test"},
            timeout_seconds=30,
            max_retries=max_retries,
            cpu_limit=1.0,
            memory_limit_mb=128,
            labels={"runtime": "docker"},
            network_enabled=False,
            gpu_count=0,
            idempotency_key=None,
            request_hash=None,
        )
        task_id = task.id
    return task_id


async def _register_worker(database: Database, worker_id: str) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.test",
            concurrency=1,
            cpu_count=4,
            memory_total_mb=4096,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def test_repository_create_query_list_and_cancel(database: Database) -> None:
    task_id = await _create_task(database)

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        queued = await TaskRepository.list_tasks(
            session,
            status=TaskStatus.QUEUED,
            worker_id=None,
            limit=10,
            offset=0,
        )
        ready_count = await session.scalar(
            select(func.count(OutboxEvent.id)).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.ready",
            )
        )

    assert task is not None
    assert task.status == TaskStatus.QUEUED
    assert task.command == ["python", "-c", "print('ok')"]
    assert [item.id for item in queued] == [task_id]
    assert ready_count == 1

    async with database.session() as session, session.begin():
        cancelled = await TaskRepository.cancel(session, task_id)

    assert cancelled is not None
    assert cancelled.status == TaskStatus.CANCELLED
    assert cancelled.cancel_requested is True

    async with database.session() as session:
        persisted = await TaskRepository.get(session, task_id)
        event_types = list(
            await session.scalars(
                select(OutboxEvent.event_type)
                .where(OutboxEvent.aggregate_id == task_id)
                .order_by(OutboxEvent.created_at)
            )
        )

    assert persisted is not None
    assert persisted.status == TaskStatus.CANCELLED
    assert event_types == ["task.ready", "task.terminal"]


async def test_atomic_claim_allows_only_one_of_multiple_contenders(
    database: Database,
) -> None:
    task_id = await _create_task(database)
    worker_ids = ("worker-a", "worker-b")
    for worker_id in worker_ids:
        await _register_worker(database, worker_id)

    start = asyncio.Event()

    async def contend(worker_id: str) -> tuple[str, uuid.UUID] | None:
        await start.wait()
        async with database.session() as session:
            # SQLite ignores SELECT FOR UPDATE. BEGIN IMMEDIATE supplies the write
            # serialization that PostgreSQL row locks provide in production.
            await session.execute(text("BEGIN IMMEDIATE"))
            try:
                _task, execution_id = await TaskRepository.claim(
                    session,
                    task_id=task_id,
                    worker_id=worker_id,
                    lease_seconds=30,
                )
            except ClaimRejected:
                await session.rollback()
                return None
            await session.commit()
            return worker_id, execution_id

    contenders = [asyncio.create_task(contend(worker_id)) for worker_id in worker_ids]
    start.set()
    results = await asyncio.gather(*contenders)
    winners = [result for result in results if result is not None]

    assert len(winners) == 1
    winner_id, execution_id = winners[0]
    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        workers = list(await session.scalars(select(Worker).order_by(Worker.id)))
        assignment_events = await session.scalar(
            select(func.count(OutboxEvent.id)).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.assigned",
            )
        )

    assert task is not None
    assert task.status == TaskStatus.ASSIGNED
    assert task.worker_id == winner_id
    assert task.execution_id == execution_id
    assert sum(worker.running_tasks for worker in workers) == 1
    assert assignment_events == 1


async def test_stale_execution_result_cannot_overwrite_new_owner(database: Database) -> None:
    task_id = await _create_task(database, max_retries=1)
    await _register_worker(database, "worker-a")
    await _register_worker(database, "worker-b")

    async with database.session() as session, session.begin():
        _task, stale_execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id="worker-a",
            lease_seconds=30,
        )

    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        assert task.assigned_at is not None
        task.lease_expires_at = task.assigned_at - timedelta(seconds=1)
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=1,
        )
        released = await TaskRepository.release_due_retries(session, limit=10)
    assert recovered == [task_id]
    assert released == [task_id]

    async with database.session() as session, session.begin():
        _task, current_execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id="worker-b",
            lease_seconds=30,
        )

    async with database.session() as session, session.begin():
        stale_result = await TaskRepository.finish_execution(
            session,
            task_id=task_id,
            worker_id="worker-a",
            execution_id=stale_execution_id,
            target=TaskStatus.SUCCEEDED,
            exit_code=0,
            error_message=None,
            retry_max_backoff_seconds=60,
            cpu_price_per_hour=0.05,
            gpu_price_per_hour=1.0,
        )

    assert stale_result.accepted is False
    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker_b = await WorkerRepository.get(session, "worker-b")

    assert task is not None
    assert task.status == TaskStatus.ASSIGNED
    assert task.worker_id == "worker-b"
    assert task.execution_id == current_execution_id
    assert worker_b is not None and worker_b.running_tasks == 1


async def test_recovery_fences_quota_before_legacy_worker_release(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id = "worker-legacy-recovery"
    task_id = await _create_task(database)
    await _register_worker(database, worker_id)

    async with database.session() as session, session.begin():
        _task, _execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=30,
        )
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        task.execution_id = None
        task.lease_expires_at = utcnow() - timedelta(seconds=1)
        project_id = task.project_id

    quota_fences: list[uuid.UUID] = []
    original_get_locked = QuotaRepository.get_locked

    async def record_quota_fence(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        reset_daily: bool = True,
    ) -> QuotaSnapshot:
        quota_fences.append(project_id)
        return await original_get_locked(
            session,
            project_id=project_id,
            reset_daily=reset_daily,
        )

    monkeypatch.setattr(QuotaRepository, "get_locked", record_quota_fence)

    async with database.session() as session, session.begin():
        original_session_get = session.get

        async def require_quota_before_worker_lock(
            entity: type[Any], ident: Any, **kwargs: Any
        ) -> Any:
            if entity is Worker and ident == worker_id and kwargs.get("with_for_update"):
                assert quota_fences == [project_id]
            return await original_session_get(entity, ident, **kwargs)

        monkeypatch.setattr(session, "get", require_quota_before_worker_lock)
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=1,
        )

    assert recovered == [task_id]

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await WorkerRepository.get(session, worker_id)

    assert task is not None
    assert task.status == TaskStatus.RETRYING
    assert task.worker_id is None
    assert task.execution_id is None
    assert task.lease_expires_at is None
    assert worker is not None
    assert worker.running_tasks == 0
    assert worker.reserved_cpu == 0
    assert worker.reserved_memory_mb == 0
    assert worker.reserved_gpus == 0


async def test_pull_claim_rechecks_worker_runtime_compatibility(database: Database) -> None:
    task_id = await _create_task(database)
    await _register_worker(database, "docker-only-worker")
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        task.runtime_type = RuntimeType.KUBERNETES

    async with database.session() as session, session.begin():
        with pytest.raises(ClaimRejected, match="requirements"):
            await TaskRepository.claim(
                session,
                task_id=task_id,
                worker_id="docker-only-worker",
                lease_seconds=30,
            )

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await WorkerRepository.get(session, "docker-only-worker")
    assert task is not None and task.status == TaskStatus.QUEUED
    assert worker is not None and worker.running_tasks == 0


async def test_scheduler_fallback_pages_past_incompatible_queue_prefix(
    database: Database,
) -> None:
    await _register_worker(database, "worker-local")
    compatible_id: uuid.UUID | None = None
    async with database.session() as session, session.begin():
        for index in range(101):
            task = await TaskRepository.create_queued(
                session,
                image="python:3.12-slim",
                command=["python", "-c", "print('ok')"],
                environment={},
                timeout_seconds=30,
                max_retries=0,
                cpu_limit=1.0,
                memory_limit_mb=128,
                labels={"runtime": "docker"} if index == 100 else {"runtime": "incompatible"},
                network_enabled=False,
                gpu_count=0,
                idempotency_key=None,
                request_hash=None,
            )
            if index == 100:
                compatible_id = task.id

    scheduler = Scheduler(database.session, lease_seconds=30, fallback_limit=10)
    assignment = await scheduler.claim_for_worker(worker_id="worker-local")

    assert compatible_id is not None
    assert assignment is not None
    assert assignment.task_id == compatible_id
    assert assignment.source == AssignmentSource.DATABASE_FALLBACK
