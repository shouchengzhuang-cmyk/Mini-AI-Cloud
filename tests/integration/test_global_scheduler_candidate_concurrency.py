import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from core.database import Database
from core.enums import TaskStatus
from models.outbox import OutboxEvent
from models.scheduling import PlacementAttempt
from models.task import Task, TaskEvent
from models.usage import ProjectQuotaState
from repositories.quotas import QuotaRepository
from repositories.scheduling import SchedulingRepository
from repositories.tasks import LEGACY_PROJECT_ID, TaskRepository
from scheduler.global_scheduler import GlobalScheduler

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"


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


async def _create_candidate(database: Database, *, queue_order: int) -> uuid.UUID:
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
            labels={},
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
    task_id = await _create_candidate(scheduler_live_database, queue_order=-(10**9))
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
