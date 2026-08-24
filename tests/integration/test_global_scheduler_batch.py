from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update

from core.database import Database
from core.enums import RuntimeType, TaskStatus, WorkerStatus
from core.rbac import ProjectStatus
from models.identity import Project
from models.scheduling import PlacementAttempt
from models.task import Task
from models.usage import ProjectQuotaState, TaskExecution
from models.worker import Worker
from repositories.quotas import QuotaRepository
from repositories.scheduling import PlacementConflict, SchedulingRepository
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository
from scheduler.global_scheduler import GlobalScheduler

pytestmark = pytest.mark.integration

LEGACY_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


def _scheduler(database: Database, *, batch_size: int = 16) -> GlobalScheduler:
    return GlobalScheduler(
        database.session_factory,
        scheduler_id="scheduler-batch-test",
        lease_seconds=30,
        policy="binpack",
        aging_interval_seconds=60,
        cpu_price_per_hour=0.05,
        memory_price_per_gb_hour=0.005,
        gpu_price_per_hour=1.0,
        batch_size=batch_size,
    )


async def _register_worker(
    database: Database,
    *,
    concurrency: int = 8,
    cpu_count: int = 8,
    memory_total_mb: int = 8_192,
) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id="batch-worker",
            hostname="batch-worker.test",
            concurrency=concurrency,
            cpu_count=cpu_count,
            memory_total_mb=memory_total_mb,
            docker_version="test",
            labels={},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def _create_task(
    database: Database,
    *,
    priority: int,
    memory_mb: int = 256,
    project_id: uuid.UUID = LEGACY_PROJECT_ID,
    queue_order: int | None = None,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('scheduled')"],
            environment={},
            timeout_seconds=60,
            max_retries=0,
            cpu_limit=1.0,
            memory_limit_mb=memory_mb,
            labels={},
            network_enabled=False,
            gpu_count=0,
            priority=priority,
            idempotency_key=None,
            request_hash=None,
            project_id=project_id,
        )
        if queue_order is not None:
            task.queue_order = queue_order
        return task.id


async def test_unschedulable_high_priority_task_does_not_block_viable_task(
    database: Database,
) -> None:
    await _register_worker(database, concurrency=2, memory_total_mb=1_024)
    high_id = await _create_task(database, priority=100, memory_mb=2_048)
    low_id = await _create_task(database, priority=50, memory_mb=256)

    result = await _scheduler(database).run_once()

    assert result.task_id == high_id
    assert result.placed is False
    assert result.reason == "insufficient_memory"
    assert result.attempted_count == 2
    assert result.placed_count == 1
    async with database.session() as session:
        high = await TaskRepository.get(session, high_id)
        low = await TaskRepository.get(session, low_id)
    assert high is not None and high.status == TaskStatus.QUEUED
    assert high.unschedulable_reason == "insufficient_memory"
    assert low is not None and low.status == TaskStatus.ASSIGNED


async def test_batch_size_places_multiple_tasks_but_bounds_each_tick(database: Database) -> None:
    await _register_worker(database, concurrency=4)
    task_ids = [await _create_task(database, priority=50) for _ in range(3)]

    result = await _scheduler(database, batch_size=2).run_once()

    assert result.attempted_count == 2
    assert result.placed_count == 2
    async with database.session() as session:
        statuses = list(await session.scalars(select(Task.status).where(Task.id.in_(task_ids))))
    assert statuses.count(TaskStatus.ASSIGNED) == 2
    assert statuses.count(TaskStatus.QUEUED) == 1


async def test_batch_refreshes_drf_share_after_each_placement(database: Database) -> None:
    second_project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=second_project_id,
                name="DRF peer project",
                slug="drf-peer-project",
                status=ProjectStatus.ACTIVE,
            )
        )
    await _register_worker(database, concurrency=4)
    first_project_ids = [
        await _create_task(database, priority=50, queue_order=queue_order) for queue_order in (0, 1)
    ]
    second_project_ids = [
        await _create_task(
            database,
            priority=50,
            project_id=second_project_id,
            queue_order=queue_order,
        )
        for queue_order in (100, 101)
    ]

    result = await _scheduler(database, batch_size=4).run_once()

    assert result.placed_count == 4
    async with database.session() as session:
        attempts = list(
            await session.scalars(
                select(PlacementAttempt)
                .where(PlacementAttempt.outcome == "placed")
                .order_by(PlacementAttempt.created_at, PlacementAttempt.id)
            )
        )
    assert [attempt.task_id for attempt in attempts[:2]] == [
        first_project_ids[0],
        second_project_ids[0],
    ]


async def test_quota_race_rolls_back_one_placement_and_continues_batch(
    database: Database,
) -> None:
    second_project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=second_project_id,
                name="Second project",
                slug="second-project",
                status=ProjectStatus.ACTIVE,
            )
        )
    await _register_worker(database, concurrency=3)
    first_id = await _create_task(database, priority=100)
    quota_blocked_id = await _create_task(database, priority=90)
    async with database.session() as session, session.begin():
        await QuotaRepository.replace(
            session,
            project_id=LEGACY_PROJECT_ID,
            max_queued_tasks=None,
            max_running_tasks=1,
            max_cpu_millicores=None,
            max_memory_mb=None,
            max_gpus=None,
            max_services=None,
            max_service_replicas=None,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )
    other_id = await _create_task(database, priority=80, project_id=second_project_id)

    result = await _scheduler(database, batch_size=3).run_once()

    assert result.task_id == first_id
    assert result.placed is True
    assert result.attempted_count == 3
    assert result.placed_count == 2
    async with database.session() as session:
        blocked = await TaskRepository.get(session, quota_blocked_id)
        other = await TaskRepository.get(session, other_id)
        blocked_executions = int(
            await session.scalar(
                select(func.count(TaskExecution.id)).where(
                    TaskExecution.task_id == quota_blocked_id
                )
            )
            or 0
        )
        worker = await session.get(Worker, "batch-worker")
    assert blocked is not None and blocked.status == TaskStatus.QUEUED
    assert blocked.unschedulable_reason == "project_quota_exceeded"
    assert blocked_executions == 0
    assert other is not None and other.status == TaskStatus.ASSIGNED
    assert worker is not None and worker.running_tasks == 2


async def test_authoritative_placement_rechecks_runtime_compatibility(
    database: Database,
) -> None:
    await _register_worker(database)
    task_id = await _create_task(database, priority=50)
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        task.runtime_type = RuntimeType.KUBERNETES

    async with database.session() as session, session.begin():
        with pytest.raises(PlacementConflict, match="runtime"):
            await SchedulingRepository.place(
                session,
                task_id=task_id,
                worker_id="batch-worker",
                gpu_device_ids=(),
                lease_seconds=30,
                cpu_price_per_hour=0.05,
                memory_price_per_gb_hour=0.005,
                gpu_price_per_hour=1.0,
            )

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        worker = await session.get(Worker, "batch-worker")
    assert task is not None and task.status == TaskStatus.QUEUED
    assert worker is not None and worker.running_tasks == 0


async def test_authoritative_placement_refreshes_worker_fence_from_database(
    database: Database,
) -> None:
    await _register_worker(database)
    task_id = await _create_task(database, priority=50)
    async with database.session() as session, session.begin():
        stale_worker = await session.get(Worker, "batch-worker")
        assert stale_worker is not None and stale_worker.status == WorkerStatus.ONLINE
        await session.execute(
            update(Worker)
            .where(Worker.id == "batch-worker")
            .values(status=WorkerStatus.DRAINING)
            .execution_options(synchronize_session=False)
        )
        # Model a scheduler identity-map snapshot that predates an independently
        # committed drain or worker re-registration.
        assert stale_worker.status == WorkerStatus.ONLINE

        with pytest.raises(PlacementConflict, match="unavailable"):
            await SchedulingRepository.place(
                session,
                task_id=task_id,
                worker_id="batch-worker",
                gpu_device_ids=(),
                lease_seconds=30,
                cpu_price_per_hour=0.05,
                memory_price_per_gb_hour=0.005,
                gpu_price_per_hour=1.0,
            )


async def test_placement_conflict_is_recorded_and_does_not_abort_batch(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _register_worker(database, concurrency=2)
    racing_id = await _create_task(database, priority=100)
    next_id = await _create_task(database, priority=50)
    original_place = SchedulingRepository.place

    async def race_once(*args: Any, **kwargs: Any) -> tuple[Task, uuid.UUID]:
        if kwargs["task_id"] == racing_id:
            raise PlacementConflict("simulated concurrent scheduler")
        return await original_place(*args, **kwargs)

    monkeypatch.setattr(SchedulingRepository, "place", staticmethod(race_once))

    result = await _scheduler(database, batch_size=2).run_once()

    assert result.task_id == racing_id
    assert result.reason == "placement_conflict"
    assert result.attempted_count == 2
    assert result.placed_count == 1
    async with database.session() as session:
        racing = await TaskRepository.get(session, racing_id)
        next_task = await TaskRepository.get(session, next_id)
    assert racing is not None and racing.status == TaskStatus.QUEUED
    assert next_task is not None and next_task.status == TaskStatus.ASSIGNED


async def test_candidate_pool_applies_drf_beyond_raw_priority_scan_limit(
    database: Database,
) -> None:
    project_a = uuid.uuid4()
    project_b = uuid.uuid4()
    queued_at = datetime.now(UTC)
    async with database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=project_a,
                    name="Busy project",
                    slug="busy-project",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=project_b,
                    name="Idle project",
                    slug="idle-project",
                    status=ProjectStatus.ACTIVE,
                ),
                ProjectQuotaState(
                    project_id=project_a,
                    queued_tasks=129,
                    running_tasks=1,
                    reserved_cpu_millicores=1_000,
                    reserved_memory_mb=256,
                    reserved_gpus=0,
                    service_count=0,
                    service_replicas=0,
                    artifact_bytes=0,
                    accounting_date=date.today(),
                ),
                ProjectQuotaState(
                    project_id=project_b,
                    queued_tasks=1,
                    running_tasks=0,
                    reserved_cpu_millicores=0,
                    reserved_memory_mb=0,
                    reserved_gpus=0,
                    service_count=0,
                    service_replicas=0,
                    artifact_bytes=0,
                    accounting_date=date.today(),
                ),
            ]
        )
        for queue_order in range(129):
            session.add(
                _queued_task(
                    project_id=project_a,
                    queued_at=queued_at,
                    priority=50,
                    queue_order=queue_order,
                )
            )
        fair_task = _queued_task(
            project_id=project_b,
            queued_at=queued_at,
            priority=50,
            queue_order=10_000,
        )
        session.add(fair_task)
        await session.flush()
        fair_task_id = fair_task.id
    await _register_worker(database)

    async with database.session() as session, session.begin():
        candidate = await SchedulingRepository.choose_next_candidate(
            session, aging_interval_seconds=60, scan_limit=128
        )

    assert candidate is not None
    assert candidate.task.id == fair_task_id
    assert candidate.project_dominant_share == 0.0


async def test_candidate_pool_applies_aging_beyond_raw_priority_scan_limit(
    database: Database,
) -> None:
    now = datetime.now(UTC)
    async with database.session() as session, session.begin():
        for queue_order in range(129):
            session.add(
                _queued_task(
                    project_id=LEGACY_PROJECT_ID,
                    queued_at=now,
                    priority=90,
                    queue_order=queue_order,
                )
            )
        aged_task = _queued_task(
            project_id=LEGACY_PROJECT_ID,
            queued_at=now - timedelta(hours=2),
            priority=0,
            queue_order=10_000,
        )
        session.add(aged_task)
        await session.flush()
        aged_task_id = aged_task.id

    async with database.session() as session, session.begin():
        candidate = await SchedulingRepository.choose_next_candidate(
            session, aging_interval_seconds=60, scan_limit=128
        )

    assert candidate is not None
    assert candidate.task.id == aged_task_id
    assert candidate.effective_priority == 100


def _queued_task(
    *,
    project_id: uuid.UUID,
    queued_at: datetime,
    priority: int,
    queue_order: int,
) -> Task:
    return Task(
        project_id=project_id,
        image="python:3.12-slim",
        command=["true"],
        environment={},
        status=TaskStatus.QUEUED,
        created_at=queued_at,
        queued_at=queued_at,
        priority=priority,
        queue_order=queue_order,
    )
