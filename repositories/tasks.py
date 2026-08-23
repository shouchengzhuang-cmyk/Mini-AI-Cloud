from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import ACTIVE_TASK_STATUSES, LogStream, TaskStatus, WorkerStatus
from core.state_machine import ensure_transition
from models.base import utcnow
from models.task import Task, TaskLog
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.outbox import OutboxRepository


class ClaimRejected(RuntimeError):
    """The task or worker no longer satisfies an atomic claim."""


class StaleExecutionError(RuntimeError):
    """A worker attempted to update a task with an expired execution token."""


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    accepted: bool
    status: TaskStatus | None
    retry_scheduled: bool = False


def retry_delay_seconds(retry_count: int, maximum: float = 60.0) -> float:
    if retry_count < 1:
        raise ValueError("retry_count must be at least 1")
    return min(float(2 ** (retry_count - 1)), maximum)


def _transition(task: Task, target: TaskStatus, now: datetime | None = None) -> None:
    ensure_transition(task.status, target)
    changed_at = now or utcnow()
    task.status = target
    task.version += 1
    if target == TaskStatus.QUEUED:
        task.queued_at = changed_at
        task.finished_at = None
    elif target == TaskStatus.ASSIGNED:
        task.assigned_at = changed_at
    elif target == TaskStatus.RUNNING:
        task.started_at = changed_at
    elif target in {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMED_OUT,
    }:
        task.finished_at = changed_at
    elif target == TaskStatus.RETRYING:
        task.finished_at = None


class TaskRepository:
    @staticmethod
    async def get(
        session: AsyncSession, task_id: uuid.UUID, *, for_update: bool = False
    ) -> Task | None:
        query = select(Task).where(Task.id == task_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_by_idempotency_key(session: AsyncSession, idempotency_key: str) -> Task | None:
        return await session.scalar(select(Task).where(Task.idempotency_key == idempotency_key))

    @staticmethod
    async def create_queued(
        session: AsyncSession,
        *,
        image: str,
        command: list[str],
        environment: dict[str, str],
        timeout_seconds: int,
        max_retries: int,
        cpu_limit: float,
        memory_limit_mb: int,
        labels: dict[str, str],
        network_enabled: bool,
        gpu_count: int,
        idempotency_key: str | None,
        request_hash: str | None,
    ) -> Task:
        now = await database_utcnow(session)
        task = Task(
            image=image,
            command=command,
            environment=environment,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            cpu_limit=cpu_limit,
            memory_limit_mb=memory_limit_mb,
            labels=labels,
            network_enabled=network_enabled,
            gpu_count=gpu_count,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            status=TaskStatus.PENDING,
            created_at=now,
        )
        session.add(task)
        await session.flush()
        _transition(task, TaskStatus.QUEUED, now)
        OutboxRepository.add(
            session,
            aggregate_id=task.id,
            event_type="task.ready",
            payload={"task_id": str(task.id)},
            available_at=now,
        )
        return task

    @staticmethod
    async def list_tasks(
        session: AsyncSession,
        *,
        status: TaskStatus | None,
        worker_id: str | None,
        limit: int,
        offset: int,
    ) -> list[Task]:
        query = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
        if status is not None:
            query = query.where(Task.status == status)
        if worker_id is not None:
            query = query.where(Task.worker_id == worker_id)
        return list(await session.scalars(query))

    @staticmethod
    async def list_queued_ids(session: AsyncSession, limit: int) -> list[uuid.UUID]:
        return list(
            await session.scalars(
                select(Task.id)
                .where(Task.status == TaskStatus.QUEUED)
                .order_by(Task.queued_at)
                .limit(limit)
            )
        )

    @staticmethod
    async def list_queued_candidates_for_worker(
        session: AsyncSession,
        *,
        worker: Worker,
        limit: int,
        offset: int,
    ) -> tuple[list[uuid.UUID], int]:
        available_cpu = max(0.0, worker.cpu_count - worker.reserved_cpu)
        available_memory = max(0, worker.memory_total_mb - worker.reserved_memory_mb)
        available_gpus = max(0, worker.gpu_count - worker.reserved_gpus)
        page = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.status == TaskStatus.QUEUED,
                    Task.cancel_requested.is_(False),
                    Task.cpu_limit <= available_cpu,
                    Task.memory_limit_mb <= available_memory,
                    Task.gpu_count <= available_gpus,
                )
                .order_by(Task.queued_at, Task.id)
                .offset(offset)
                .limit(limit)
            )
        )
        return (
            [task.id for task in page if _labels_match(task.labels, worker.labels)],
            len(page),
        )

    @staticmethod
    async def list_logs(
        session: AsyncSession, task_id: uuid.UUID, *, offset: int, limit: int
    ) -> list[TaskLog]:
        return list(
            await session.scalars(
                select(TaskLog)
                .where(TaskLog.task_id == task_id, TaskLog.sequence > offset)
                .order_by(TaskLog.sequence)
                .limit(limit)
            )
        )

    @staticmethod
    async def append_log(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        execution_id: uuid.UUID | None,
        stream: LogStream,
        content: str,
    ) -> TaskLog:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None:
            raise LookupError(f"task {task_id} does not exist")
        if execution_id is not None and (
            task.execution_id != execution_id or task.status not in ACTIVE_TASK_STATUSES
        ):
            raise StaleExecutionError("cannot append logs for a stale or terminal execution")
        task.log_sequence += 1
        task.version += 1
        now = await database_utcnow(session)
        log = TaskLog(
            task_id=task_id,
            execution_id=execution_id,
            stream=stream,
            sequence=task.log_sequence,
            content=content,
            timestamp=now,
        )
        session.add(log)
        await session.flush()
        return log

    @staticmethod
    async def candidate_workers(session: AsyncSession, task: Task) -> list[Worker]:
        workers = list(
            await session.scalars(
                select(Worker)
                .where(
                    Worker.status == WorkerStatus.ONLINE,
                    Worker.running_tasks < Worker.concurrency,
                    Worker.reserved_cpu + task.cpu_limit <= Worker.cpu_count,
                    Worker.reserved_memory_mb + task.memory_limit_mb <= Worker.memory_total_mb,
                    Worker.reserved_gpus + task.gpu_count <= Worker.gpu_count,
                )
                .order_by(Worker.running_tasks, Worker.last_heartbeat_at.desc())
            )
        )
        return [worker for worker in workers if _labels_match(task.labels, worker.labels)]

    @staticmethod
    async def claim(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        lease_seconds: float,
    ) -> tuple[Task, uuid.UUID]:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.status != TaskStatus.QUEUED or task.cancel_requested:
            raise ClaimRejected("task is no longer queued")
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None or worker.status != WorkerStatus.ONLINE:
            raise ClaimRejected("worker is not online")
        if worker.running_tasks >= worker.concurrency:
            raise ClaimRejected("worker has no free concurrency slot")
        if not _worker_can_run_task(worker, task):
            raise ClaimRejected("worker does not satisfy task requirements")

        execution_id = uuid.uuid4()
        now = await database_utcnow(session)
        _transition(task, TaskStatus.ASSIGNED, now)
        task.worker_id = worker.id
        task.execution_id = execution_id
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        task.cancel_requested = False
        task.error_message = None
        worker.running_tasks += 1
        worker.reserved_cpu += task.cpu_limit
        worker.reserved_memory_mb += task.memory_limit_mb
        worker.reserved_gpus += task.gpu_count
        worker.version += 1
        OutboxRepository.add(
            session,
            aggregate_id=task.id,
            event_type="task.assigned",
            payload={
                "task_id": str(task.id),
                "worker_id": worker.id,
                "execution_id": str(execution_id),
                "queue_wait_seconds": max(
                    0.0,
                    (now - _as_utc(task.queued_at)).total_seconds(),
                )
                if task.queued_at is not None
                else 0.0,
            },
            available_at=now,
        )
        return task, execution_id

    @staticmethod
    async def mark_pulling(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
    ) -> Task:
        task = await TaskRepository._owned_task(
            session, task_id=task_id, worker_id=worker_id, execution_id=execution_id
        )
        if task.status != TaskStatus.ASSIGNED:
            raise StaleExecutionError("task is no longer assigned to this execution")
        now = await database_utcnow(session)
        _transition(task, TaskStatus.PULLING, now)
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        return task

    @staticmethod
    async def mark_running(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
    ) -> Task:
        task = await TaskRepository._owned_task(
            session, task_id=task_id, worker_id=worker_id, execution_id=execution_id
        )
        if task.status != TaskStatus.PULLING:
            raise StaleExecutionError("task is no longer pulling for this execution")
        now = await database_utcnow(session)
        _transition(task, TaskStatus.RUNNING, now)
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        return task

    @staticmethod
    async def renew_lease(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
    ) -> bool:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if (
            task is None
            or task.worker_id != worker_id
            or task.execution_id != execution_id
            or task.status not in ACTIVE_TASK_STATUSES
        ):
            return False
        now = await database_utcnow(session)
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        task.version += 1
        return True

    @staticmethod
    async def cancellation_requested(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
    ) -> bool:
        task = await TaskRepository.get(session, task_id)
        if task is None or task.worker_id != worker_id or task.execution_id != execution_id:
            raise StaleExecutionError("execution token is stale")
        return task.cancel_requested or task.status == TaskStatus.CANCELLED

    @staticmethod
    async def finish_execution(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        target: TaskStatus,
        exit_code: int | None,
        error_message: str | None,
        retry_max_backoff_seconds: float,
        cpu_price_per_hour: float,
        gpu_price_per_hour: float,
    ) -> ExecutionResult:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.worker_id != worker_id or task.execution_id != execution_id:
            return ExecutionResult(accepted=False, status=None)
        if task.status not in ACTIVE_TASK_STATUSES:
            return ExecutionResult(accepted=False, status=task.status)

        worker = await session.get(Worker, worker_id, with_for_update=True)
        now = await database_utcnow(session)
        if task.cancel_requested:
            target = TaskStatus.CANCELLED
            error_message = "task was cancelled by user request"
        _transition(task, target, now)
        task.exit_code = exit_code
        task.error_message = error_message
        task.lease_expires_at = None
        if task.started_at is not None:
            start = _as_utc(task.started_at)
            duration = max(0.0, (now - start).total_seconds())
            task.duration_ms = round(duration * 1000)
            task.wall_time_seconds = duration
            task.cpu_seconds = duration * task.cpu_limit
            task.gpu_seconds = duration * task.gpu_count
            task.estimated_cost = (
                task.cpu_seconds * cpu_price_per_hour / 3600
                + task.gpu_seconds * gpu_price_per_hour / 3600
            )

        if worker is not None:
            _release_worker_capacity(worker, task)

        retry_scheduled = False
        if (
            target in {TaskStatus.FAILED, TaskStatus.TIMED_OUT}
            and task.retry_count < task.max_retries
        ):
            task.retry_count += 1
            _transition(task, TaskStatus.RETRYING, now)
            task.next_attempt_at = now + timedelta(
                seconds=retry_delay_seconds(task.retry_count, retry_max_backoff_seconds)
            )
            task.worker_id = None
            task.execution_id = None
            task.cancel_requested = False
            retry_scheduled = True
        else:
            OutboxRepository.add(
                session,
                aggregate_id=task.id,
                event_type="task.terminal",
                payload={
                    "task_id": str(task.id),
                    "status": task.status.value,
                    "duration_seconds": task.wall_time_seconds,
                },
                available_at=now,
            )
        return ExecutionResult(accepted=True, status=task.status, retry_scheduled=retry_scheduled)

    @staticmethod
    async def cancel(session: AsyncSession, task_id: uuid.UUID) -> Task | None:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None:
            return None
        if task.status in {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMED_OUT,
        }:
            return task
        if task.status == TaskStatus.RUNNING:
            task.cancel_requested = True
            task.version += 1
            return task

        previous_worker_id = task.worker_id
        now = await database_utcnow(session)
        _transition(task, TaskStatus.CANCELLED, now)
        task.cancel_requested = True
        task.lease_expires_at = None
        task.execution_id = None
        if previous_worker_id is not None:
            worker = await session.get(Worker, previous_worker_id, with_for_update=True)
            if worker is not None:
                _release_worker_capacity(worker, task)
        OutboxRepository.add(
            session,
            aggregate_id=task.id,
            event_type="task.terminal",
            payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
            available_at=now,
        )
        return task

    @staticmethod
    async def release_due_retries(session: AsyncSession, limit: int) -> list[uuid.UUID]:
        now = await database_utcnow(session)
        tasks = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.status == TaskStatus.RETRYING,
                    or_(Task.next_attempt_at.is_(None), Task.next_attempt_at <= now),
                )
                .order_by(Task.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        for task in tasks:
            if task.cancel_requested:
                _transition(task, TaskStatus.CANCELLED, now)
                task.next_attempt_at = None
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                    available_at=now,
                )
                continue
            _transition(task, TaskStatus.QUEUED, now)
            task.next_attempt_at = None
            task.cancel_requested = False
            OutboxRepository.add(
                session,
                aggregate_id=task.id,
                event_type="task.ready",
                payload={"task_id": str(task.id)},
                available_at=now,
            )
        return [task.id for task in tasks]

    @staticmethod
    async def recover_expired(
        session: AsyncSession,
        *,
        limit: int,
        max_recovery_attempts: int,
        recovery_cleanup_grace_seconds: float = 0.0,
    ) -> list[uuid.UUID]:
        now = await database_utcnow(session)
        tasks = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.status.in_(ACTIVE_TASK_STATUSES),
                    Task.lease_expires_at.is_not(None),
                    Task.lease_expires_at < now,
                )
                .order_by(Task.lease_expires_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        recovered: list[uuid.UUID] = []
        for task in tasks:
            worker_id = task.worker_id
            task.lease_expires_at = None
            task.execution_id = None
            if worker_id is not None:
                worker = await session.get(Worker, worker_id, with_for_update=True)
                if worker is not None:
                    _release_worker_capacity(worker, task)
            task.worker_id = None
            if task.cancel_requested:
                _transition(task, TaskStatus.CANCELLED, now)
                task.error_message = "task was cancelled before its execution lease expired"
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                    available_at=now,
                )
                recovered.append(task.id)
                continue

            _transition(task, TaskStatus.FAILED, now)
            task.error_message = "execution lease expired; worker ownership was revoked"
            if task.recovery_count < max_recovery_attempts:
                task.recovery_count += 1
                _transition(task, TaskStatus.RETRYING, now)
                task.next_attempt_at = now + timedelta(seconds=recovery_cleanup_grace_seconds)
                task.cancel_requested = False
                recovered.append(task.id)
            else:
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.FAILED.value},
                    available_at=now,
                )
        return recovered

    @staticmethod
    async def counts_by_status(session: AsyncSession) -> dict[TaskStatus, int]:
        rows = await session.execute(select(Task.status, func.count(Task.id)).group_by(Task.status))
        return {status: count for status, count in rows}

    @staticmethod
    async def _owned_task(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
    ) -> Task:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.worker_id != worker_id or task.execution_id != execution_id:
            raise StaleExecutionError("execution token is stale")
        return task


def _worker_can_run_task(worker: Worker, task: Task) -> bool:
    return (
        worker.reserved_cpu + task.cpu_limit <= worker.cpu_count
        and worker.reserved_memory_mb + task.memory_limit_mb <= worker.memory_total_mb
        and worker.reserved_gpus + task.gpu_count <= worker.gpu_count
        and _labels_match(task.labels, worker.labels)
    )


def _release_worker_capacity(worker: Worker, task: Task) -> None:
    worker.running_tasks = max(0, worker.running_tasks - 1)
    worker.reserved_cpu = max(0.0, worker.reserved_cpu - task.cpu_limit)
    worker.reserved_memory_mb = max(0, worker.reserved_memory_mb - task.memory_limit_mb)
    worker.reserved_gpus = max(0, worker.reserved_gpus - task.gpu_count)
    worker.version += 1


def _labels_match(required: dict[str, str], offered: dict[str, str]) -> bool:
    return all(offered.get(key) == value for key, value in required.items())


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
