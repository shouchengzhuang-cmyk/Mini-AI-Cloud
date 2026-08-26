import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, cast

import pytest
from sqlalchemy import select

from api.services.outbox import OutboxDispatcher
from api.services.reaper import Reaper
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus, WorkerStatus
from core.redis import READY_GROUP, READY_STREAM, RedisQueue
from models.base import utcnow
from models.outbox import OutboxEvent
from models.worker import Worker
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration


async def test_redis_pending_entry_is_reclaimed_acknowledged_and_deleted(
    redis_queue: RedisQueue,
) -> None:
    task_id = uuid.uuid4()
    await redis_queue.ensure_ready_group()
    await redis_queue.publish_outbox(
        event_id=uuid.uuid4(),
        event_type="task.ready",
        payload={"task_id": str(task_id)},
    )

    delivered = await redis_queue.read_ready(consumer="worker-a", count=1, block_ms=1)
    reclaimed = await redis_queue.reclaim_ready(consumer="worker-b", min_idle_ms=0, count=1)

    assert delivered and delivered[0][1] == task_id
    assert reclaimed and reclaimed[0][1] == task_id
    await redis_queue.acknowledge_ready(reclaimed[0][0])
    assert await redis_queue.client.xrange(READY_STREAM) == []

    await redis_queue.client.xgroup_destroy(READY_STREAM, READY_GROUP)
    assert await redis_queue.reclaim_ready(consumer="worker-c", min_idle_ms=0, count=1) == []
    groups = await redis_queue.client.xinfo_groups(READY_STREAM)
    assert any(group["name"] == READY_GROUP for group in groups)


async def _create_task(database: Database) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="alpine:3.21",
            command=["echo", "ok"],
            environment={},
            timeout_seconds=30,
            max_retries=0,
            cpu_limit=1.0,
            memory_limit_mb=64,
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
            hostname="worker.test",
            concurrency=2,
            cpu_count=4,
            memory_total_mb=4096,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def test_reaper_marks_worker_offline_and_requeues_expired_lease(
    database: Database,
) -> None:
    task_id = await _create_task(database)
    await _register_worker(database, "worker-a")
    async with database.session() as session, session.begin():
        await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id="worker-a",
            lease_seconds=30,
        )
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        worker = await session.get(Worker, "worker-a", with_for_update=True)
        assert task is not None and worker is not None
        task.lease_expires_at = utcnow() - timedelta(seconds=1)
        worker.last_heartbeat_at = utcnow() - timedelta(seconds=60)

    settings = Settings(
        database_url=str(database.engine.url),
        control_plane_enabled=False,
        worker_offline_timeout=15,
        max_recovery_attempts=1,
        recovery_cleanup_grace_seconds=0,
        batch_size=10,
    )
    offline, recovered, retries = await Reaper(database, settings).run_once()

    assert (offline, recovered, retries) == (1, 1, 1)
    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await WorkerRepository.get(session, "worker-a")

    assert task is not None
    assert task.status == TaskStatus.QUEUED
    assert task.worker_id is None
    assert task.execution_id is None
    assert task.lease_expires_at is None
    assert task.recovery_count == 1
    assert worker is not None
    assert worker.status == WorkerStatus.OFFLINE
    assert worker.running_tasks == 0


async def test_reaper_bounds_mass_worker_offline_transition(database: Database) -> None:
    worker_ids = tuple(f"mass-offline-{index}" for index in range(3))
    for worker_id in worker_ids:
        await _register_worker(database, worker_id)
    async with database.session() as session, session.begin():
        workers = list(await session.scalars(select(Worker).where(Worker.id.in_(worker_ids))))
        for worker in workers:
            worker.last_heartbeat_at = utcnow() - timedelta(seconds=60)

    settings = Settings(
        database_url=str(database.engine.url),
        control_plane_enabled=False,
        worker_offline_timeout=15,
        batch_size=2,
    )
    reaper = Reaper(database, settings)

    assert await reaper.run_once() == (2, 0, 0)
    async with database.session() as session:
        first_statuses = list(
            await session.scalars(select(Worker.status).where(Worker.id.in_(worker_ids)))
        )
    assert first_statuses.count(WorkerStatus.OFFLINE) == 2
    assert first_statuses.count(WorkerStatus.ONLINE) == 1

    assert await reaper.run_once() == (1, 0, 0)
    async with database.session() as session:
        final_statuses = list(
            await session.scalars(select(Worker.status).where(Worker.id.in_(worker_ids)))
        )
    assert final_statuses == [WorkerStatus.OFFLINE] * 3


async def test_outbox_dispatch_is_at_least_once_after_publish_before_mark(
    database: Database, redis_queue: RedisQueue
) -> None:
    task_id = await _create_task(database)
    async with database.session() as session:
        event = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.ready",
            )
        )
        assert event is not None
        event_id = event.id
        event_type = event.event_type
        payload = dict(event.payload)

    # Simulate a process dying after Redis accepted XADD but before PostgreSQL
    # recorded processed_at. The next dispatcher must publish the same event again.
    await redis_queue.publish_outbox(
        event_id=event_id,
        event_type=event_type,
        payload=payload,
    )
    processed = await OutboxDispatcher(database, redis_queue).dispatch_once()
    assert processed == 1

    entries = await redis_queue.client.xrange(READY_STREAM)
    matching = [fields for _message_id, fields in entries if fields["event_id"] == str(event_id)]
    assert len(matching) == 2
    async with database.session() as session:
        persisted = await session.get(OutboxEvent, event_id)
    assert persisted is not None and persisted.processed_at is not None


class _FailOnceQueue:
    def __init__(self) -> None:
        self.calls = 0

    async def publish_outbox(
        self,
        *,
        event_id: uuid.UUID,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> str:
        del event_id, event_type, payload
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("redis unavailable")
        return "1-0"


async def test_outbox_failure_is_recorded_and_retried(database: Database) -> None:
    task_id = await _create_task(database)
    queue = _FailOnceQueue()
    dispatcher = OutboxDispatcher(database, cast(RedisQueue, queue), batch_size=10)

    assert await dispatcher.dispatch_once() == 0
    async with database.session() as session, session.begin():
        event = await session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.ready",
            )
            .with_for_update()
        )
        assert event is not None
        assert event.attempts == 1
        assert event.last_error == "redis unavailable"
        assert event.processed_at is None
        assert event.locked_by is None
        event.available_at = utcnow() - timedelta(seconds=1)

    assert await dispatcher.dispatch_once() == 1
    assert queue.calls == 2
    async with database.session() as session:
        persisted = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_id == task_id,
                OutboxEvent.event_type == "task.ready",
            )
        )
    assert persisted is not None
    assert persisted.processed_at is not None
    assert persisted.last_error is None
