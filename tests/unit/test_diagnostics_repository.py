import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest_asyncio
from sqlalchemy import event, insert, select

from core.database import Database
from core.enums import TaskStatus, WorkerStatus
from core.rbac import ProjectStatus
from models import Base
from models.identity import Project
from models.outbox import OutboxEvent
from models.scheduling import ResourceReservation
from models.task import Task
from models.usage import ProjectQuota, ProjectQuotaState, TaskExecution
from models.worker import Worker
from repositories.diagnostics import DiagnosticsRepository


@pytest_asyncio.fixture
async def diagnostics_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'diagnostics.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield database
    finally:
        await database.dispose()


async def test_diagnostics_are_read_only_and_project_scoped(
    diagnostics_database: Database,
) -> None:
    now = datetime.now(UTC)
    project_id = uuid.uuid4()
    other_project_id = uuid.uuid4()
    expired_task_id = uuid.uuid4()
    unschedulable_task_id = uuid.uuid4()
    retry_task_id = uuid.uuid4()
    normal_task_id = uuid.uuid4()
    other_task_id = uuid.uuid4()
    running_without_lease_id = uuid.uuid4()
    lease_without_worker_id = uuid.uuid4()
    terminal_lease_task_id = uuid.uuid4()
    other_running_without_lease_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    async with diagnostics_database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=project_id,
                    name="Diagnostics",
                    slug="diagnostics",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=other_project_id,
                    name="Other Diagnostics",
                    slug="other-diagnostics",
                    status=ProjectStatus.ACTIVE,
                ),
                Worker(
                    id="worker-offline",
                    worker_session_id=uuid.uuid4(),
                    hostname="offline",
                    status=WorkerStatus.OFFLINE,
                    started_at=now - timedelta(hours=1),
                    last_heartbeat_at=now - timedelta(minutes=5),
                    running_tasks=1,
                    concurrency=1,
                    reserved_memory_mb=-1,
                    cpu_count=4,
                    memory_total_mb=8192,
                ),
                Worker(
                    id="worker-online",
                    worker_session_id=uuid.uuid4(),
                    hostname="online",
                    status=WorkerStatus.ONLINE,
                    started_at=now,
                    last_heartbeat_at=now,
                    running_tasks=0,
                    concurrency=1,
                    cpu_count=4,
                    memory_total_mb=8192,
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                Task(
                    id=expired_task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.RUNNING,
                    worker_id="worker-offline",
                    execution_id=execution_id,
                    lease_expires_at=now - timedelta(minutes=2),
                ),
                Task(
                    id=unschedulable_task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                    queued_at=now - timedelta(minutes=3),
                    unschedulable_reason="no_matching_gpu",
                ),
                Task(
                    id=retry_task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.RETRYING,
                    next_attempt_at=now - timedelta(minutes=3),
                ),
                Task(
                    id=normal_task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                    queued_at=now,
                ),
                Task(
                    id=other_task_id,
                    project_id=other_project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                    queued_at=now - timedelta(minutes=3),
                    unschedulable_reason="other_project_reason",
                ),
                Task(
                    id=running_without_lease_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.RUNNING,
                    worker_id="worker-online",
                ),
                Task(
                    id=lease_without_worker_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.RUNNING,
                    lease_expires_at=now + timedelta(minutes=2),
                ),
                Task(
                    id=terminal_lease_task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.SUCCEEDED,
                    worker_id="worker-online",
                    lease_expires_at=now + timedelta(minutes=2),
                    finished_at=now,
                ),
                Task(
                    id=other_running_without_lease_id,
                    project_id=other_project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.RUNNING,
                    worker_id="worker-online",
                ),
            ]
        )
        await session.flush()
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=expired_task_id,
                project_id=project_id,
                worker_id="worker-offline",
                worker_session_id=uuid.uuid4(),
                attempt=0,
                status="running",
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1"),
                assigned_at=now - timedelta(minutes=10),
                runtime_type="docker",
            )
        )
        await session.flush()
        session.add(
            ResourceReservation(
                project_id=project_id,
                task_id=expired_task_id,
                execution_id=execution_id,
                worker_id="worker-offline",
                worker_session_id=uuid.uuid4(),
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                created_at=now - timedelta(minutes=10),
            )
        )
        session.add_all(
            [
                OutboxEvent(
                    aggregate_id=expired_task_id,
                    aggregate_type="task",
                    event_type="task.assigned",
                    payload={"task_id": str(expired_task_id)},
                    created_at=now - timedelta(minutes=4),
                    available_at=now - timedelta(minutes=4),
                    attempts=2,
                ),
                OutboxEvent(
                    aggregate_id=other_task_id,
                    aggregate_type="task",
                    event_type="task.ready",
                    payload={"task_id": str(other_task_id)},
                    created_at=now - timedelta(minutes=4),
                    available_at=now - timedelta(minutes=4),
                ),
                OutboxEvent(
                    aggregate_id=expired_task_id,
                    aggregate_type="task",
                    event_type="task.running",
                    payload={"task_id": str(expired_task_id)},
                    created_at=now - timedelta(minutes=2),
                    available_at=now - timedelta(minutes=2),
                    processed_at=now - timedelta(minutes=1),
                    locked_by=uuid.uuid4(),
                    locked_until=now + timedelta(minutes=1),
                ),
                OutboxEvent(
                    aggregate_id=other_task_id,
                    aggregate_type="task",
                    event_type="task.running",
                    payload={"task_id": str(other_task_id)},
                    created_at=now - timedelta(minutes=2),
                    available_at=now - timedelta(minutes=2),
                    processed_at=now - timedelta(minutes=1),
                    locked_by=uuid.uuid4(),
                ),
            ]
        )

    async with diagnostics_database.session() as session:
        snapshot = await DiagnosticsRepository.snapshot(
            session,
            project_id=project_id,
            worker_offline_timeout_seconds=30,
            stuck_after_seconds=60,
        )

    assert snapshot.project_id == project_id
    assert snapshot.queued_tasks == 2
    assert snapshot.online_workers == 1
    assert snapshot.outbox.scope == "project_events"
    assert snapshot.outbox.pending_events == 1
    assert snapshot.outbox.ready_events == 1
    assert snapshot.outbox.retrying_events == 1
    assert snapshot.outbox.lag_seconds >= 239
    assert snapshot.offline_workers_total == 1
    assert snapshot.offline_workers[0].worker_id == "worker-offline"
    assert snapshot.stuck_tasks_total == 3
    assert {item.reason for item in snapshot.stuck_tasks} == {
        "expired_lease",
        "overdue_retry",
        "unschedulable",
    }
    assert all(item.task_id != other_task_id for item in snapshot.stuck_tasks)
    assert snapshot.active_reservations_total == 1
    assert snapshot.active_reservations[0].task_id == expired_task_id
    checks = {check.name: check for check in snapshot.consistency.checks}
    assert snapshot.consistency.status == "issues"
    assert snapshot.consistency.complete is False
    assert checks["running_task_without_lease"].total == 1
    assert checks["running_task_without_lease"].issues[0].task_id == running_without_lease_id
    assert checks["lease_without_worker"].total == 1
    assert checks["lease_without_worker"].issues[0].task_id == lease_without_worker_id
    assert checks["reservation_without_task"].status == "clean"
    assert checks["terminal_task_with_active_reservation"].status == "clean"
    assert checks["terminal_task_with_lease"].total == 1
    assert checks["processed_outbox_inconsistency"].total == 1
    assert checks["negative_capacity"].total == 1
    assert checks["orphan_container"].status == "not_observable"
    assert checks["orphan_pod"].status == "not_observable"
    assert all(
        issue.task_id != other_running_without_lease_id
        for check in snapshot.consistency.checks
        for issue in check.issues
    )


async def test_conservative_repair_releases_terminal_reservation_and_lease_idempotently(
    diagnostics_database: Database,
) -> None:
    now = datetime.now(UTC)
    project_id = uuid.uuid4()
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    worker_session_id = uuid.uuid4()
    async with diagnostics_database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name="Repair",
                slug="repair",
                status=ProjectStatus.ACTIVE,
            )
        )
        session.add(
            Worker(
                id="repair-worker",
                worker_session_id=worker_session_id,
                hostname="repair-worker",
                status=WorkerStatus.ONLINE,
                started_at=now,
                last_heartbeat_at=now,
                running_tasks=1,
                concurrency=1,
                reserved_cpu=1.0,
                reserved_memory_mb=256,
                reserved_gpus=0,
                cpu_count=4,
                memory_total_mb=8192,
            )
        )
        await session.flush()
        session.add_all(
            [
                ProjectQuota(project_id=project_id),
                ProjectQuotaState(
                    project_id=project_id,
                    running_tasks=1,
                    reserved_cpu_millicores=1000,
                    reserved_memory_mb=256,
                    reserved_gpus=0,
                    accounting_date=now.date(),
                ),
                Task(
                    id=task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.SUCCEEDED,
                    worker_id="repair-worker",
                    execution_id=execution_id,
                    lease_expires_at=now + timedelta(minutes=1),
                    finished_at=now,
                ),
            ]
        )
        await session.flush()
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=task_id,
                project_id=project_id,
                worker_id="repair-worker",
                worker_session_id=worker_session_id,
                attempt=0,
                status="running",
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1"),
                assigned_at=now - timedelta(minutes=1),
                runtime_type="docker",
            )
        )
        await session.flush()
        session.add(
            ResourceReservation(
                project_id=project_id,
                task_id=task_id,
                execution_id=execution_id,
                worker_id="repair-worker",
                worker_session_id=worker_session_id,
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                created_at=now - timedelta(minutes=1),
            )
        )

    async with diagnostics_database.session() as session, session.begin():
        first = await DiagnosticsRepository.repair_conservative(
            session,
            project_id=project_id,
        )
    assert first.candidates_total == 2
    assert first.repaired_total == 2
    assert first.skipped_total == 0

    async with diagnostics_database.session() as session:
        task = await session.get(Task, task_id)
        reservation = await session.scalar(
            select(ResourceReservation).where(ResourceReservation.task_id == task_id)
        )
        worker = await session.get(Worker, "repair-worker")
        quota_state = await session.get(ProjectQuotaState, project_id)
        execution = await session.get(TaskExecution, execution_id)
        assert task is not None and task.lease_expires_at is None
        assert reservation is not None and reservation.released_at is not None
        assert reservation.release_reason == "doctor_terminal_task"
        assert worker is not None
        assert worker.running_tasks == 0
        assert worker.reserved_cpu == 0
        assert worker.reserved_memory_mb == 0
        assert quota_state is not None and quota_state.running_tasks == 0
        assert quota_state.reserved_cpu_millicores == 0
        assert execution is not None and execution.status == TaskStatus.SUCCEEDED.value

    async with diagnostics_database.session() as session, session.begin():
        second = await DiagnosticsRepository.repair_conservative(
            session,
            project_id=project_id,
        )
    assert second.candidates_total == 0
    assert second.repaired_total == 0


async def test_reservation_without_task_is_reported_but_never_auto_repaired(
    diagnostics_database: Database,
) -> None:
    project_id = uuid.uuid4()
    reservation_id = uuid.uuid4()
    missing_task_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with diagnostics_database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name="Orphan reservation",
                slug="orphan-reservation",
                status=ProjectStatus.ACTIVE,
            )
        )

    async with diagnostics_database.engine.connect() as connection:
        await connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        await connection.execute(
            insert(ResourceReservation),
            {
                "id": reservation_id,
                "project_id": project_id,
                "task_id": missing_task_id,
                "execution_id": uuid.uuid4(),
                "worker_id": "missing-worker",
                "worker_session_id": uuid.uuid4(),
                "cpu_millicores": 1000,
                "memory_mb": 256,
                "gpu_count": 0,
                "state": "active",
                "legacy_unbound": False,
                "created_at": now,
                "version": 1,
            },
        )
        await connection.commit()
        await connection.exec_driver_sql("PRAGMA foreign_keys=ON")

    async with diagnostics_database.session() as session:
        snapshot = await DiagnosticsRepository.snapshot(
            session,
            project_id=project_id,
            worker_offline_timeout_seconds=30,
            stuck_after_seconds=60,
        )
    check = next(
        item for item in snapshot.consistency.checks if item.name == "reservation_without_task"
    )
    assert check.total == 1
    assert check.issues[0].resource_id == str(reservation_id)
    assert check.issues[0].task_id == missing_task_id
    assert check.issues[0].repairable is False

    async with diagnostics_database.session() as session, session.begin():
        repair = await DiagnosticsRepository.repair_conservative(
            session,
            project_id=project_id,
        )
    assert repair.candidates_total == 0
    async with diagnostics_database.session() as session:
        assert await session.get(ResourceReservation, reservation_id) is not None
