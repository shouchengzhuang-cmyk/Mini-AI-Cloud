import uuid
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from core.database import Database
from core.enums import ErrorCategory, ErrorCode, TaskStatus, WorkerStatus
from models.base import utcnow
from models.usage import TaskExecution, UsageLedger
from models.worker import Worker
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration


async def _create_and_claim(
    database: Database,
    *,
    retry_on_exit_codes: list[int],
) -> tuple[uuid.UUID, uuid.UUID]:
    worker_id = f"worker-retry-{uuid.uuid4().hex[:8]}"
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
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "raise SystemExit(1)"],
            environment={},
            timeout_seconds=30,
            max_retries=2,
            retry_backoff="linear",
            retry_base_seconds=2.0,
            retry_max_seconds=10.0,
            retry_on_exit_codes=retry_on_exit_codes,
            cpu_limit=1.0,
            memory_limit_mb=128,
            labels={"runtime": "docker"},
            network_enabled=False,
            gpu_count=0,
            idempotency_key=None,
            request_hash=None,
        )
        task_id = task.id

    async with database.session() as session, session.begin():
        _task, execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=30,
        )
        return task_id, execution_id


async def _finish(
    database: Database,
    *,
    task_id: uuid.UUID,
    execution_id: uuid.UUID,
    category: ErrorCategory,
    code: ErrorCode | None,
    exit_code: int | None,
) -> None:
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id)
        assert task is not None and task.worker_id is not None
        result = await TaskRepository.finish_execution(
            session,
            task_id=task_id,
            worker_id=task.worker_id,
            execution_id=execution_id,
            target=TaskStatus.FAILED,
            exit_code=exit_code,
            error_message="classified failure",
            error_category=category,
            error_code=code,
            retry_max_backoff_seconds=60,
            cpu_price_per_hour=0.05,
            gpu_price_per_hour=1.0,
        )
        assert result.accepted is True


async def test_user_error_retries_only_for_configured_exit_code(database: Database) -> None:
    task_id, execution_id = await _create_and_claim(database, retry_on_exit_codes=[1, 137])

    await _finish(
        database,
        task_id=task_id,
        execution_id=execution_id,
        category=ErrorCategory.USER_ERROR,
        code=None,
        exit_code=2,
    )

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        execution = await session.get(TaskExecution, execution_id)
    assert task is not None and execution is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 0
    assert task.error_category == ErrorCategory.USER_ERROR.value
    assert execution.error_category == ErrorCategory.USER_ERROR.value
    assert execution.error_message == "classified failure"


async def test_infrastructure_error_retries_without_exit_code_match(database: Database) -> None:
    task_id, execution_id = await _create_and_claim(database, retry_on_exit_codes=[])

    await _finish(
        database,
        task_id=task_id,
        execution_id=execution_id,
        category=ErrorCategory.INFRA_ERROR,
        code=ErrorCode.IMAGE_PULL_FAILED,
        exit_code=None,
    )

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        execution = await session.get(TaskExecution, execution_id)
    assert task is not None and execution is not None
    assert task.status == TaskStatus.RETRYING
    assert task.retry_count == 1
    assert task.error_code == ErrorCode.IMAGE_PULL_FAILED.value
    assert execution.error_category == ErrorCategory.INFRA_ERROR.value
    assert execution.error_code == ErrorCode.IMAGE_PULL_FAILED.value


async def test_new_attempt_does_not_reuse_prior_runtime_usage_window(
    database: Database,
) -> None:
    task_id, first_execution_id = await _create_and_claim(database, retry_on_exit_codes=[])
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id)
        assert task is not None and task.worker_id is not None
        worker_id = task.worker_id
        await TaskRepository.mark_pulling(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=first_execution_id,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=first_execution_id,
            lease_seconds=30,
        )

    await _finish(
        database,
        task_id=task_id,
        execution_id=first_execution_id,
        category=ErrorCategory.INFRA_ERROR,
        code=ErrorCode.CONTAINER_START_FAILED,
        exit_code=None,
    )
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        task.next_attempt_at = utcnow() - timedelta(seconds=1)
        assert await TaskRepository.release_due_retries(session, limit=10) == [task_id]

    async with database.session() as session, session.begin():
        task, second_execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id=worker_id,
            lease_seconds=30,
        )
        assert task.started_at is None
        assert task.duration_ms is None
        assert task.cpu_seconds is None
        assert task.gpu_seconds is None
        assert task.wall_time_seconds is None
        assert task.estimated_cost is None

    await _finish(
        database,
        task_id=task_id,
        execution_id=second_execution_id,
        category=ErrorCategory.INFRA_ERROR,
        code=ErrorCode.CONTAINER_START_FAILED,
        exit_code=None,
    )

    async with database.session() as session:
        second_execution = await session.get(TaskExecution, second_execution_id)
        ledger_count = int(
            await session.scalar(
                select(func.count(UsageLedger.id)).where(UsageLedger.task_id == task_id)
            )
            or 0
        )
    assert second_execution is not None and second_execution.started_at is None
    assert ledger_count == 1


async def test_oom_retries_only_when_exit_137_is_configured(database: Database) -> None:
    task_id, execution_id = await _create_and_claim(database, retry_on_exit_codes=[137])

    await _finish(
        database,
        task_id=task_id,
        execution_id=execution_id,
        category=ErrorCategory.RESOURCE_ERROR,
        code=ErrorCode.OOM_KILLED,
        exit_code=137,
    )

    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
    assert task is not None
    assert task.status == TaskStatus.RETRYING
    assert task.retry_count == 1
    assert task.error_category == ErrorCategory.RESOURCE_ERROR.value
    assert task.error_code == ErrorCode.OOM_KILLED.value


@pytest.mark.parametrize(
    ("worker_status", "expected_code"),
    [
        (WorkerStatus.ONLINE, ErrorCode.LEASE_EXPIRED),
        (WorkerStatus.OFFLINE, ErrorCode.WORKER_LOST),
    ],
)
async def test_lease_recovery_persists_infrastructure_error_code(
    database: Database,
    worker_status: WorkerStatus,
    expected_code: ErrorCode,
) -> None:
    task_id, execution_id = await _create_and_claim(database, retry_on_exit_codes=[])
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None and task.worker_id is not None
        worker = await session.get(Worker, task.worker_id, with_for_update=True)
        assert worker is not None
        worker.status = worker_status
        task.lease_expires_at = utcnow() - timedelta(seconds=1)

    async with database.session() as session, session.begin():
        recovered = await TaskRepository.recover_expired(
            session,
            limit=10,
            max_recovery_attempts=0,
        )

    assert recovered == []
    async with database.session() as session:
        task = await TaskRepository.get(session, task_id)
        execution = await session.get(TaskExecution, execution_id)
    assert task is not None and execution is not None
    assert task.status == TaskStatus.FAILED
    assert task.error_category == ErrorCategory.INFRA_ERROR.value
    assert task.error_code == expected_code.value
    assert execution.error_category == ErrorCategory.INFRA_ERROR.value
    assert execution.error_code == expected_code.value
