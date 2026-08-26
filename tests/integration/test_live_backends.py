import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from sqlalchemy import delete, select, text

from core.database import Database
from core.enums import TaskStatus
from models.outbox import OutboxEvent
from models.task import Task, TaskLog
from models.usage import UsageLedger
from models.worker import Worker
from repositories.scheduling import SchedulingRepository
from repositories.tasks import ClaimRejected, TaskRepository
from repositories.workers import WorkerRepository

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"
DEFAULT_LIVE_REDIS_URL = "redis://127.0.0.1:6379/0"


@pytest_asyncio.fixture
async def live_database() -> AsyncIterator[Database]:
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


@pytest_asyncio.fixture
async def live_redis() -> AsyncIterator[Redis]:
    url = os.getenv("LIVE_REDIS_URL", DEFAULT_LIVE_REDIS_URL)
    client = Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=2,
    )
    try:
        async with asyncio.timeout(2):
            await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"live Redis is unavailable; set LIVE_REDIS_URL ({type(exc).__name__})")

    try:
        yield client
    finally:
        await client.aclose()


async def _register_live_worker(database: Database, worker_id: str, labels: dict[str, str]) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.invalid",
            concurrency=1,
            cpu_count=2,
            memory_total_mb=1024,
            docker_version="live-integration-test",
            labels=labels,
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def _cleanup_live_database_rows(
    database: Database, *, task_id: uuid.UUID, worker_ids: tuple[str, str]
) -> None:
    async with database.session() as session, session.begin():
        await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id == task_id))
        await session.execute(delete(TaskLog).where(TaskLog.task_id == task_id))
        await session.execute(delete(UsageLedger).where(UsageLedger.task_id == task_id))
        await session.execute(delete(Task).where(Task.id == task_id))
        await session.execute(delete(Worker).where(Worker.id.in_(worker_ids)))


async def test_live_postgresql_atomic_claim_has_one_winner(live_database: Database) -> None:
    run_id = uuid.uuid4()
    task_id = uuid.uuid4()
    worker_ids = (f"live-claim-a-{run_id}", f"live-claim-b-{run_id}")
    labels = {"live-test-run": str(run_id)}

    try:
        for worker_id in worker_ids:
            await _register_live_worker(live_database, worker_id, labels)

        async with live_database.session() as session, session.begin():
            task = await TaskRepository.create_queued(
                session,
                image="alpine:3.21",
                command=["true"],
                environment={},
                timeout_seconds=30,
                max_retries=0,
                cpu_limit=0.25,
                memory_limit_mb=64,
                labels=labels,
                network_enabled=False,
                gpu_count=0,
                idempotency_key=None,
                request_hash=None,
            )
            task_id = task.id

        start = asyncio.Event()

        async def contend(worker_id: str) -> tuple[str, uuid.UUID] | None:
            await start.wait()
            try:
                async with live_database.session() as session, session.begin():
                    _task, execution_id = await TaskRepository.claim(
                        session,
                        task_id=task_id,
                        worker_id=worker_id,
                        lease_seconds=30,
                    )
                    # Keep this test's assigned event away from the concurrently
                    # running Compose dispatcher, then delete it during cleanup.
                    for item in session.new:
                        if isinstance(item, OutboxEvent) and item.aggregate_id == task_id:
                            item.available_at = datetime.now(UTC) + timedelta(days=1)
                return worker_id, execution_id
            except ClaimRejected:
                return None

        contenders = [asyncio.create_task(contend(worker_id)) for worker_id in worker_ids]
        start.set()
        results = await asyncio.gather(*contenders)
        winners = [result for result in results if result is not None]

        assert len(winners) == 1
        winner_id, execution_id = winners[0]
        async with live_database.session() as session:
            persisted_task = await session.scalar(select(Task).where(Task.id == task_id))
            workers = list(await session.scalars(select(Worker).where(Worker.id.in_(worker_ids))))

        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.ASSIGNED
        assert persisted_task.worker_id == winner_id
        assert persisted_task.execution_id == execution_id
        assert sum(worker.running_tasks for worker in workers) == 1
        assert sum(worker.reserved_cpu for worker in workers) == pytest.approx(0.25)
        async with live_database.session() as session, session.begin():
            result = await TaskRepository.finish_execution(
                session,
                task_id=task_id,
                worker_id=winner_id,
                execution_id=execution_id,
                target=TaskStatus.CANCELLED,
                exit_code=None,
                error_message="live test cleanup",
                retry_max_backoff_seconds=60,
                cpu_price_per_hour=0.05,
                memory_price_per_gb_hour=0.005,
                gpu_price_per_hour=1.0,
            )
        assert result.accepted
    finally:
        await _cleanup_live_database_rows(live_database, task_id=task_id, worker_ids=worker_ids)


async def test_live_postgresql_global_candidate_lanes_compile_and_execute(
    live_database: Database,
) -> None:
    async with live_database.session() as session, session.begin():
        candidates = await SchedulingRepository.choose_candidates(
            session,
            aging_interval_seconds=60,
            scan_limit=1,
        )

    assert len(candidates) <= 3


async def test_live_redis_consumer_group_reclaim_ack_and_delete(live_redis: Redis) -> None:
    run_id = uuid.uuid4().hex
    stream = f"mini-ai-cloud:live-test:{run_id}"
    group = f"live-group-{run_id}"
    consumer_a = f"consumer-a-{run_id}"
    consumer_b = f"consumer-b-{run_id}"

    try:
        message_id = await live_redis.xadd(stream, {"run_id": run_id})
        await live_redis.xgroup_create(stream, group, id="0")

        delivered = await live_redis.xreadgroup(
            group,
            consumer_a,
            {stream: ">"},
            count=1,
            block=1000,
        )
        assert delivered == [[stream, [(message_id, {"run_id": run_id})]]]
        assert (await live_redis.xpending(stream, group))["pending"] == 1

        reclaimed = await live_redis.xautoclaim(
            stream,
            group,
            consumer_b,
            0,
            "0-0",
            count=1,
        )
        assert reclaimed[1] == [(message_id, {"run_id": run_id})]
        assert await live_redis.xack(stream, group, message_id) == 1
        assert (await live_redis.xpending(stream, group))["pending"] == 0
        assert await live_redis.xdel(stream, message_id) == 1
        assert await live_redis.xlen(stream) == 0
    finally:
        await live_redis.delete(stream)
