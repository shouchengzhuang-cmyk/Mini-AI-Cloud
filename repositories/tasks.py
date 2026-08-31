from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from api.pagination import CursorKey
from core.enums import (
    ACTIVE_TASK_STATUSES,
    FINAL_TASK_STATUSES,
    AcceleratorKind,
    AcceleratorVendor,
    AllocationAuthority,
    ErrorCategory,
    ErrorCode,
    LogStream,
    RetryBackoff,
    TaskStatus,
    WorkerStatus,
    WorkloadType,
)
from core.state_machine import ensure_transition
from models.artifact import TaskDependency
from models.base import utcnow
from models.scheduling import (
    GPUDevice,
    PreemptionPlan,
    ReservationGPUDevice,
    ResourceReservation,
)
from models.task import Task, TaskEvent, TaskLog
from models.usage import ProjectQuotaState, TaskExecution
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.outbox import OutboxRepository
from repositories.quotas import QuotaExceededError, QuotaRepository
from repositories.reservations import ReservationRepository

LEGACY_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


class ClaimRejected(RuntimeError):
    """The task or worker no longer satisfies an atomic claim."""


class StaleExecutionError(RuntimeError):
    """A worker attempted to update a task with an expired execution token."""


class DependencyValidationError(ValueError):
    """A submitted dependency is missing, cross-project, or otherwise invalid."""


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    accepted: bool
    status: TaskStatus | None
    retry_scheduled: bool = False


@dataclass(frozen=True, slots=True)
class RecoverableRuntimeExecution:
    task_id: uuid.UUID
    execution_id: uuid.UUID
    worker_session_id: uuid.UUID
    runtime_log_cursor_bytes: int


def retry_delay_seconds(
    retry_count: int,
    maximum: float = 60.0,
    *,
    backoff: RetryBackoff | str = RetryBackoff.EXPONENTIAL,
    base_seconds: float = 1.0,
) -> float:
    if retry_count < 1:
        raise ValueError("retry_count must be at least 1")
    if base_seconds <= 0:
        raise ValueError("base_seconds must be greater than zero")
    if maximum <= 0:
        raise ValueError("maximum must be greater than zero")
    mode = RetryBackoff(backoff)
    if mode == RetryBackoff.FIXED:
        delay = base_seconds
    elif mode == RetryBackoff.LINEAR:
        delay = base_seconds * retry_count
    else:
        delay = base_seconds * (2 ** (retry_count - 1))
    return min(float(delay), maximum)


def should_retry_failure(
    *,
    error_category: ErrorCategory | str | None,
    exit_code: int | None,
    retry_on_exit_codes: list[int],
) -> bool:
    """Apply retry semantics without consuming or mutating an attempt counter.

    Infrastructure, internal, and timeout failures are assumed transient. User
    and resource failures require an explicit exit-code match. Cancellation and
    preemption are handled outside the user retry budget.
    """

    if error_category is None:
        # Phase I callers did not submit a taxonomy; preserve their retry behavior.
        return True
    category = ErrorCategory(error_category)
    if category in {
        ErrorCategory.INFRA_ERROR,
        ErrorCategory.INTERNAL_ERROR,
        ErrorCategory.TIMEOUT,
    }:
        return True
    if category in {ErrorCategory.USER_ERROR, ErrorCategory.RESOURCE_ERROR}:
        return exit_code is not None and exit_code in retry_on_exit_codes
    return False


def _transition(
    session: AsyncSession,
    task: Task,
    target: TaskStatus,
    now: datetime | None = None,
    *,
    event_type: str = "task.status_changed",
) -> None:
    previous = task.status
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
        TaskStatus.PREEMPTED,
    }:
        task.finished_at = changed_at
    elif target == TaskStatus.RETRYING:
        task.finished_at = None
    session.add(
        TaskEvent(
            project_id=task.project_id,
            task_id=task.id,
            event_type=event_type,
            sequence=task.version,
            from_status=previous.value,
            status=target.value,
            execution_id=task.execution_id,
            worker_id=task.worker_id,
            details={},
            created_at=changed_at,
        )
    )


def _prepare_for_assignment(task: Task) -> None:
    """Clear per-attempt state before minting a new execution token.

    Historical attempt data remains in ``TaskExecution`` and ``UsageLedger``.
    Reusing these fields would make a failure before runtime start inherit the
    previous attempt's usage window and settle it a second time.
    """

    task.started_at = None
    task.finished_at = None
    task.exit_code = None
    task.error_message = None
    task.failure_category = None
    task.error_category = None
    task.error_code = None
    task.duration_ms = None
    task.cpu_seconds = None
    task.gpu_seconds = None
    task.wall_time_seconds = None
    task.estimated_cost = None
    task.runtime_handle = None
    task.gpu_device_ids = []
    task.cancel_requested = False
    task.next_attempt_at = None
    task.unschedulable_reason = None


def _accelerator_request_vendors(
    request: dict[str, object] | None,
    *,
    gpu_count: int,
) -> tuple[AcceleratorVendor, ...] | None:
    if gpu_count == 0 or request is None:
        return None
    raw_vendors = request.get("allowed_vendors")
    if not isinstance(raw_vendors, list) or not raw_vendors:
        return None
    return tuple(AcceleratorVendor(str(value)) for value in raw_vendors)


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
    async def transfer_recoverable_kubernetes_executions(
        session: AsyncSession,
        *,
        worker_id: str,
        new_worker_session_id: uuid.UUID,
        lease_seconds: float,
        observations: list[dict[str, str | None]],
        kubernetes_cleanup_grace_seconds: float = 0.0,
    ) -> list[RecoverableRuntimeExecution]:
        """CAS-transfer persisted Job executions to a newly registered Worker session."""

        recovered: list[RecoverableRuntimeExecution] = []
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None or worker.worker_session_id != new_worker_session_id:
            return recovered
        for observed in observations:
            try:
                task_id = uuid.UUID(observed["task_id"] or "")
                execution_id = uuid.UUID(observed["execution_id"] or "")
                runtime_worker_session_id = uuid.UUID(observed["worker_session_id"] or "")
                controller_session_id = uuid.UUID(observed["controller_session_id"] or "")
            except (TypeError, ValueError):
                continue
            if controller_session_id != new_worker_session_id:
                continue
            task = await TaskRepository.get(session, task_id, for_update=True)
            execution = await session.get(TaskExecution, execution_id, with_for_update=True)
            reservation = await session.scalar(
                select(ResourceReservation)
                .where(
                    ResourceReservation.execution_id == execution_id,
                    ResourceReservation.released_at.is_(None),
                )
                .with_for_update()
            )
            persisted = task.runtime_handle if task is not None else None
            if not isinstance(persisted, dict):
                continue
            runtime_log_cursor_bytes = persisted.get("runtime_log_cursor_bytes", 0)
            if (
                not isinstance(runtime_log_cursor_bytes, int)
                or isinstance(runtime_log_cursor_bytes, bool)
                or runtime_log_cursor_bytes < 0
            ):
                continue
            expected = {
                "runtime_type": "kubernetes",
                "runtime_namespace": observed["namespace"],
                "runtime_resource_kind": "job",
                "runtime_resource_name": observed["resource_name"],
                "runtime_resource_uid": observed["resource_uid"],
                "runtime_worker_session_id": str(runtime_worker_session_id),
                "runtime_spec_hash": observed["spec_hash"],
            }
            if (
                task is None
                or execution is None
                or reservation is None
                or task.status not in ACTIVE_TASK_STATUSES
                or task.worker_id != worker_id
                or task.execution_id != execution_id
                or task.runtime_type.value != "kubernetes"
                or execution.task_id != task_id
                or execution.worker_id != worker_id
                or execution.runtime_worker_session_id != runtime_worker_session_id
                or reservation.worker_id != worker_id
                or execution.worker_session_id is None
                or reservation.worker_session_id != execution.worker_session_id
                or any(persisted.get(key) != value for key, value in expected.items())
                or execution.runtime_namespace != observed["namespace"]
                or execution.runtime_resource_kind != "job"
                or execution.runtime_resource_name != observed["resource_name"]
                or execution.runtime_resource_uid != observed["resource_uid"]
                or execution.runtime_spec_hash != observed["spec_hash"]
            ):
                continue
            observed_pod_name = observed.get("observed_pod_name")
            observed_pod_uid = observed.get("observed_pod_uid")
            if (
                (observed_pod_name is None) != (observed_pod_uid is None)
                or (
                    persisted.get("observed_pod_name") is not None
                    and (
                        persisted.get("observed_pod_name") != observed_pod_name
                        or persisted.get("observed_pod_uid") != observed_pod_uid
                    )
                )
                or (
                    execution.observed_pod_name is not None
                    and (
                        execution.observed_pod_name != observed_pod_name
                        or execution.observed_pod_uid != observed_pod_uid
                    )
                )
            ):
                continue
            now = await database_utcnow(session)
            execution.worker_session_id = new_worker_session_id
            reservation.worker_session_id = new_worker_session_id
            task.lease_expires_at = _runtime_lease_expiry(
                task,
                now,
                lease_seconds=lease_seconds,
                kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
            )
            task.version += 1
            recovered.append(
                RecoverableRuntimeExecution(
                    task_id=task_id,
                    execution_id=execution_id,
                    worker_session_id=new_worker_session_id,
                    runtime_log_cursor_bytes=runtime_log_cursor_bytes,
                )
            )
        return recovered

    @staticmethod
    async def get_by_idempotency_key(
        session: AsyncSession,
        idempotency_key: str,
        *,
        project_id: uuid.UUID = LEGACY_PROJECT_ID,
    ) -> Task | None:
        return await session.scalar(
            select(Task).where(
                Task.project_id == project_id,
                Task.idempotency_key == idempotency_key,
            )
        )

    @staticmethod
    async def get_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        task_id: uuid.UUID,
        for_update: bool = False,
    ) -> Task | None:
        query = select(Task).where(Task.project_id == project_id, Task.id == task_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

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
        accelerator_request_json: dict[str, object] | None = None,
        idempotency_key: str | None,
        request_hash: str | None,
        project_id: uuid.UUID = LEGACY_PROJECT_ID,
        submitted_by_user_id: uuid.UUID | None = None,
        created_by_api_key_id: uuid.UUID | None = None,
        runtime_type: str = "docker",
        workload_type: str = WorkloadType.BATCH_JOB.value,
        priority: int = 50,
        preemptible: bool = False,
        gpu_memory_mb: int = 0,
        gpu_model: str | None = None,
        tolerations: list[dict[str, str]] | None = None,
        depends_on: list[uuid.UUID] | None = None,
        dependency_failure_policy: str = "cancel",
        retry_backoff: str = RetryBackoff.EXPONENTIAL.value,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 60.0,
        retry_on_exit_codes: list[int] | None = None,
    ) -> Task:
        dependency_ids = tuple(depends_on or ())
        if len(dependency_ids) != len(set(dependency_ids)):
            raise DependencyValidationError("depends_on task IDs must be unique")
        if dependency_failure_policy not in {"block", "cancel"}:
            raise DependencyValidationError("unsupported dependency failure policy")
        dependency_tasks = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.project_id == project_id,
                    Task.id.in_(dependency_ids),
                )
                .order_by(Task.id)
                .with_for_update()
            )
        )
        if len(dependency_tasks) != len(dependency_ids):
            raise DependencyValidationError("every dependency must exist in the submitting project")
        quota = await QuotaRepository.initialize(session, project_id=project_id)
        QuotaRepository.ensure_task_can_fit(
            quota,
            cpu_millicores=round(cpu_limit * 1000),
            memory_mb=memory_limit_mb,
            gpu_count=gpu_count,
            accelerator_vendors=_accelerator_request_vendors(
                accelerator_request_json,
                gpu_count=gpu_count,
            ),
        )
        await QuotaRepository.admit_queued(session, project_id=project_id)
        now = await database_utcnow(session)
        task = Task(
            project_id=project_id,
            submitted_by_user_id=submitted_by_user_id,
            created_by_api_key_id=created_by_api_key_id,
            image=image,
            workload_type=WorkloadType(workload_type),
            command=command,
            environment=environment,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff=RetryBackoff(retry_backoff).value,
            retry_base_seconds=retry_base_seconds,
            retry_max_seconds=retry_max_seconds,
            retry_on_exit_codes=list(
                [1, 137] if retry_on_exit_codes is None else retry_on_exit_codes
            ),
            cpu_limit=cpu_limit,
            cpu_millicores=round(cpu_limit * 1000),
            memory_limit_mb=memory_limit_mb,
            labels=labels,
            network_enabled=network_enabled,
            gpu_count=gpu_count,
            accelerator_request_json=accelerator_request_json,
            gpu_memory_mb=gpu_memory_mb,
            gpu_model=gpu_model,
            runtime_type=runtime_type,
            priority=priority,
            preemptible=preemptible,
            tolerations=tolerations or [],
            network_mode="internet" if network_enabled else "none",
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            status=TaskStatus.PENDING,
            created_at=now,
        )
        session.add(task)
        await session.flush()
        for dependency_id in dependency_ids:
            session.add(
                TaskDependency(
                    task_id=task.id,
                    depends_on_task_id=dependency_id,
                    job_group_id=None,
                    failure_policy=dependency_failure_policy,
                )
            )
        session.add(
            TaskEvent(
                project_id=task.project_id,
                task_id=task.id,
                event_type="task.created",
                sequence=0,
                from_status=None,
                status=TaskStatus.PENDING.value,
                details={},
                created_at=now,
            )
        )
        dependency_failed = any(
            dependency.status in FINAL_TASK_STATUSES and dependency.status != TaskStatus.SUCCEEDED
            for dependency in dependency_tasks
        )
        dependencies_succeeded = all(
            dependency.status == TaskStatus.SUCCEEDED for dependency in dependency_tasks
        )
        if dependency_failed and dependency_failure_policy == "cancel":
            _transition(
                session,
                task,
                TaskStatus.CANCELLED,
                now,
                event_type="task.dependency_cancelled",
            )
            task.error_message = "a prerequisite task did not succeed"
            task.failure_category = ErrorCategory.CANCELLED.value
            task.error_category = ErrorCategory.CANCELLED.value
            await QuotaRepository.release_queued(session, project_id=project_id)
            OutboxRepository.add(
                session,
                aggregate_id=task.id,
                event_type="task.terminal",
                payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                available_at=now,
            )
        elif dependencies_succeeded:
            _transition(session, task, TaskStatus.QUEUED, now, event_type="task.queued")
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
    async def list_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        status: TaskStatus | None,
        worker_id: str | None,
        limit: int,
        offset: int,
        after: CursorKey | None = None,
    ) -> list[Task]:
        query = select(Task).where(Task.project_id == project_id)
        if status is not None:
            query = query.where(Task.status == status)
        if worker_id is not None:
            query = query.where(Task.worker_id == worker_id)
        if after is not None:
            query = query.where(
                or_(
                    Task.created_at < after.created_at,
                    (Task.created_at == after.created_at) & (Task.id < after.item_id),
                )
            )
        return list(
            await session.scalars(
                query.order_by(Task.created_at.desc(), Task.id.desc())
                .limit(limit)
                .offset(0 if after is not None else offset)
            )
        )

    @staticmethod
    async def list_queued_ids(session: AsyncSession, limit: int) -> list[uuid.UUID]:
        return list(
            await session.scalars(
                select(Task.id)
                .where(
                    Task.status == TaskStatus.QUEUED,
                    ~_unsatisfied_dependencies(Task.id),
                )
                .order_by(Task.queued_at)
                .limit(limit)
            )
        )

    @staticmethod
    async def dependencies_ready(session: AsyncSession, task_id: uuid.UUID) -> bool:
        return not bool(await session.scalar(select(_unsatisfied_dependencies(task_id))))

    @staticmethod
    async def resolve_dependency_readiness(
        session: AsyncSession,
        *,
        limit: int,
    ) -> list[uuid.UUID]:
        """Promote ready DAG tasks and cancel dependants according to edge policy.

        The scan is PostgreSQL-fenced and safe to run from every API replica. Queued
        tasks are included because a dependency may have been attached through the
        JobGroup API after the task was originally submitted.
        """

        dependent_ids = select(TaskDependency.task_id).distinct()
        tasks = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.id.in_(dependent_ids),
                    Task.status.in_({TaskStatus.PENDING, TaskStatus.QUEUED}),
                )
                .order_by(Task.created_at, Task.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        if not tasks:
            return []

        dependency_task = aliased(Task)
        dependency_rows = (
            await session.execute(
                select(
                    TaskDependency.task_id,
                    TaskDependency.failure_policy,
                    dependency_task.status,
                )
                .join(
                    dependency_task,
                    dependency_task.id == TaskDependency.depends_on_task_id,
                )
                .where(TaskDependency.task_id.in_([task.id for task in tasks]))
                .order_by(TaskDependency.task_id, TaskDependency.depends_on_task_id)
            )
        ).all()
        rows_by_task: dict[uuid.UUID, list[tuple[str, TaskStatus]]] = {
            task.id: [] for task in tasks
        }
        for task_id, failure_policy, dependency_status in dependency_rows:
            rows_by_task[task_id].append((failure_policy, dependency_status))

        changed: list[uuid.UUID] = []
        resolved_at: datetime | None = None
        for task in tasks:
            rows = rows_by_task[task.id]
            if not rows:
                continue
            if all(status == TaskStatus.SUCCEEDED for _policy, status in rows):
                if task.status == TaskStatus.PENDING:
                    if resolved_at is None:
                        resolved_at = await database_utcnow(session)
                    _transition(
                        session,
                        task,
                        TaskStatus.QUEUED,
                        resolved_at,
                        event_type="task.dependencies_satisfied",
                    )
                    OutboxRepository.add(
                        session,
                        aggregate_id=task.id,
                        event_type="task.ready",
                        payload={"task_id": str(task.id)},
                        available_at=resolved_at,
                    )
                    changed.append(task.id)
                continue

            failed_rows = [
                (policy, status)
                for policy, status in rows
                if status in FINAL_TASK_STATUSES and status != TaskStatus.SUCCEEDED
            ]
            if failed_rows and any(policy == "cancel" for policy, _status in failed_rows):
                if resolved_at is None:
                    resolved_at = await database_utcnow(session)
                _transition(
                    session,
                    task,
                    TaskStatus.CANCELLED,
                    resolved_at,
                    event_type="task.dependency_cancelled",
                )
                task.cancel_requested = True
                task.error_message = "a prerequisite task did not succeed"
                task.failure_category = ErrorCategory.CANCELLED.value
                task.error_category = ErrorCategory.CANCELLED.value
                await QuotaRepository.release_queued(session, project_id=task.project_id)
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                    available_at=resolved_at,
                )
                changed.append(task.id)
        return changed

    @staticmethod
    async def list_queued_candidates_for_worker(
        session: AsyncSession,
        *,
        worker: Worker,
        limit: int,
        offset: int,
    ) -> tuple[list[uuid.UUID], int]:
        available_cpu = max(
            0.0,
            worker.cpu_allocatable_millicores / 1000 - worker.reserved_cpu,
        )
        available_memory = max(
            0,
            worker.memory_allocatable_mb - worker.reserved_memory_mb,
        )
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
                    ~_unsatisfied_dependencies(Task.id),
                )
                .order_by(Task.queued_at, Task.id)
                .offset(offset)
                .limit(limit)
            )
        )
        return (
            [task.id for task in page if _worker_can_run_task(worker, task)],
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
    async def list_logs_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        task_id: uuid.UUID,
        offset: int,
        limit: int,
    ) -> list[TaskLog]:
        return list(
            await session.scalars(
                select(TaskLog)
                .join(Task, Task.id == TaskLog.task_id)
                .where(
                    Task.project_id == project_id,
                    TaskLog.task_id == task_id,
                    TaskLog.sequence > offset,
                )
                .order_by(TaskLog.sequence)
                .limit(limit)
            )
        )

    @staticmethod
    async def list_events_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        task_id: uuid.UUID,
        limit: int = 1000,
    ) -> list[TaskEvent]:
        return list(
            await session.scalars(
                select(TaskEvent)
                .where(
                    TaskEvent.project_id == project_id,
                    TaskEvent.task_id == task_id,
                )
                .order_by(TaskEvent.sequence)
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
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> TaskLog:
        task: Task | None
        if worker_session_id is not None:
            if worker_id is None or execution_id is None:
                raise ValueError("worker_id and execution_id are required with worker_session_id")
            task = await TaskRepository._owned_task(
                session,
                task_id=task_id,
                worker_id=worker_id,
                execution_id=execution_id,
                worker_session_id=worker_session_id,
            )
        else:
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
                )
                .order_by(Worker.running_tasks, Worker.last_heartbeat_at.desc())
            )
        )
        return [worker for worker in workers if _worker_can_run_task(worker, task)]

    @staticmethod
    async def claim(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        lease_seconds: float,
        worker_session_id: uuid.UUID | None = None,
        cpu_price_per_hour: float = 0.05,
        memory_price_per_gb_hour: float = 0.005,
        gpu_price_per_hour: float = 1.0,
    ) -> tuple[Task, uuid.UUID]:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.status != TaskStatus.QUEUED or task.cancel_requested:
            raise ClaimRejected("task is no longer queued")
        if not await TaskRepository.dependencies_ready(session, task.id):
            raise ClaimRejected("task dependencies are not ready")
        worker = await session.scalar(
            select(Worker)
            .where(Worker.id == worker_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        if worker is None or worker.status != WorkerStatus.ONLINE:
            raise ClaimRejected("worker is not online")
        if worker_session_id is not None and worker.worker_session_id != worker_session_id:
            raise ClaimRejected("worker session is stale")
        if worker.running_tasks >= worker.concurrency:
            raise ClaimRejected("worker has no free concurrency slot")
        if not _worker_can_run_task(worker, task):
            raise ClaimRejected("worker does not satisfy task requirements")

        devices: list[GPUDevice] = []
        if task.gpu_count:
            allocated_ids = select(ReservationGPUDevice.gpu_device_id).where(
                ReservationGPUDevice.released_at.is_(None)
            )
            device_query = (
                select(GPUDevice)
                .where(
                    GPUDevice.worker_id == worker.id,
                    GPUDevice.health == "healthy",
                    GPUDevice.memory_free_mb >= task.gpu_memory_mb,
                    GPUDevice.vendor == AcceleratorVendor.NVIDIA.value,
                    GPUDevice.accelerator_kind == AcceleratorKind.GPU.value,
                    ~GPUDevice.id.in_(allocated_ids),
                )
                .order_by(
                    GPUDevice.memory_free_mb,
                    GPUDevice.memory_total_mb,
                    GPUDevice.device_uuid,
                )
                .limit(task.gpu_count)
                .with_for_update(skip_locked=True)
            )
            if task.gpu_model is not None:
                device_query = device_query.where(GPUDevice.model == task.gpu_model)
            devices = list(await session.scalars(device_query))
            if len(devices) != task.gpu_count:
                raise ClaimRejected("worker concrete GPU inventory is insufficient")

        execution_id = uuid.uuid4()
        now = await database_utcnow(session)
        _prepare_for_assignment(task)
        _transition(session, task, TaskStatus.ASSIGNED, now, event_type="task.assigned")
        task.worker_id = worker.id
        task.execution_id = execution_id
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        worker.running_tasks += 1
        worker.reserved_cpu += task.cpu_limit
        worker.reserved_memory_mb += task.memory_limit_mb
        worker.reserved_gpus += task.gpu_count
        worker.version += 1
        if devices:
            task.gpu_device_ids = [device.device_uuid for device in devices]

        previous_attempt = int(
            await session.scalar(
                select(func.max(TaskExecution.attempt)).where(TaskExecution.task_id == task.id)
            )
            or 0
        )
        execution = TaskExecution(
            id=execution_id,
            task_id=task.id,
            project_id=task.project_id,
            worker_id=worker.id,
            worker_session_id=worker.worker_session_id,
            attempt=previous_attempt + 1,
            status=TaskStatus.ASSIGNED.value,
            cpu_millicores=task.cpu_millicores,
            memory_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            gpu_model=task.gpu_model,
            allocation_authority=AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value,
            requested_vendor=(AcceleratorVendor.NVIDIA.value if task.gpu_count else None),
            requested_kind=(AcceleratorKind.GPU.value if task.gpu_count else None),
            observed_device_ids_json=(
                [device.device_uuid for device in devices] if devices else None
            ),
            observed_vendor=(AcceleratorVendor.NVIDIA.value if devices else None),
            observed_at=(now if devices else None),
            cpu_price_per_hour=Decimal(str(cpu_price_per_hour)),
            memory_price_per_gb_hour=Decimal(str(memory_price_per_gb_hour)),
            gpu_price_per_hour=Decimal(str(gpu_price_per_hour)),
            assigned_at=now,
            runtime_type=task.runtime_type.value,
        )
        session.add(execution)
        await session.flush()
        reservation = ResourceReservation(
            project_id=task.project_id,
            task_id=task.id,
            execution_id=execution_id,
            worker_id=worker.id,
            worker_session_id=worker.worker_session_id,
            cpu_millicores=task.cpu_millicores,
            memory_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            legacy_unbound=False,
            allocation_authority=AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value,
            requested_vendor=(AcceleratorVendor.NVIDIA.value if task.gpu_count else None),
            requested_kind=(AcceleratorKind.GPU.value if task.gpu_count else None),
            observed_device_ids_json=(
                [device.device_uuid for device in devices] if devices else None
            ),
            observed_vendor=(AcceleratorVendor.NVIDIA.value if devices else None),
            observed_at=(now if devices else None),
            created_at=now,
        )
        session.add(reservation)
        await session.flush()
        for device in devices:
            session.add(
                ReservationGPUDevice(
                    reservation_id=reservation.id,
                    gpu_device_id=device.id,
                    created_at=now,
                )
            )
        await session.flush()
        await ReservationRepository.assert_exact_device_binding(session, reservation)
        await QuotaRepository.reserve_execution(
            session,
            project_id=task.project_id,
            cpu_millicores=task.cpu_millicores,
            memory_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            accelerator_vendor=(AcceleratorVendor.NVIDIA if task.gpu_count else None),
        )
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
    async def take_global_assignment(
        session: AsyncSession,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        lease_seconds: float,
    ) -> Task | None:
        active_device_count = (
            select(func.count(ReservationGPUDevice.id))
            .where(
                ReservationGPUDevice.reservation_id == ResourceReservation.id,
                ReservationGPUDevice.released_at.is_(None),
            )
            .correlate(ResourceReservation)
            .scalar_subquery()
        )
        task = await session.scalar(
            select(Task)
            .join(
                ResourceReservation,
                ResourceReservation.execution_id == Task.execution_id,
            )
            .join(Worker, Worker.id == Task.worker_id)
            .where(
                Task.worker_id == worker_id,
                Task.status == TaskStatus.ASSIGNED,
                ResourceReservation.worker_session_id == worker_session_id,
                ResourceReservation.released_at.is_(None),
                ResourceReservation.gpu_count == Task.gpu_count,
                or_(Task.gpu_count == 0, active_device_count == Task.gpu_count),
                Worker.worker_session_id == worker_session_id,
            )
            .order_by(Task.assigned_at, Task.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if task is None:
            return None
        now = await database_utcnow(session)
        _transition(session, task, TaskStatus.PREPARING, now)
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        return task

    @staticmethod
    async def mark_pulling(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
        worker_session_id: uuid.UUID | None = None,
    ) -> Task:
        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        if task.status in {TaskStatus.PULLING, TaskStatus.STARTING, TaskStatus.RUNNING}:
            now = await database_utcnow(session)
            task.lease_expires_at = now + timedelta(seconds=lease_seconds)
            task.version += 1
            return task
        if task.status not in {TaskStatus.ASSIGNED, TaskStatus.PREPARING}:
            raise StaleExecutionError("task is no longer assigned to this execution")
        now = await database_utcnow(session)
        _transition(session, task, TaskStatus.PULLING, now)
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        return task

    @staticmethod
    async def record_runtime_handle(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        runtime_type: str,
        resource_kind: str,
        resource_name: str,
        namespace: str | None,
        resource_uid: str | None,
        runtime_worker_session_id: uuid.UUID | None,
        spec_hash: str | None,
        observed_pod_name: str | None,
        observed_pod_uid: str | None,
        worker_session_id: uuid.UUID | None = None,
        kubernetes_cleanup_grace_seconds: float = 0.0,
    ) -> Task:
        """Persist a runtime observation behind the full execution/session fence."""

        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        if execution is None or execution.task_id != task_id or execution.worker_id != worker_id:
            raise StaleExecutionError("runtime handle execution record is stale")
        if worker_session_id is not None and execution.worker_session_id != worker_session_id:
            raise StaleExecutionError("runtime handle controller session is stale")
        if execution.runtime_type != runtime_type:
            raise StaleExecutionError("runtime handle type does not match the execution")
        if (observed_pod_name is None) != (observed_pod_uid is None):
            raise ValueError("observed Pod name and UID must be recorded together")
        if execution.observed_pod_name is not None and (
            execution.observed_pod_name != observed_pod_name
            or execution.observed_pod_uid != observed_pod_uid
        ):
            raise StaleExecutionError("observed Kubernetes Pod identity is immutable")
        existing_handle = task.runtime_handle
        if (
            isinstance(existing_handle, dict)
            and existing_handle.get("observed_pod_name") is not None
            and (
                existing_handle.get("observed_pod_name") != observed_pod_name
                or existing_handle.get("observed_pod_uid") != observed_pod_uid
            )
        ):
            raise StaleExecutionError("persisted Kubernetes Pod identity is immutable")
        if runtime_type == "kubernetes":
            if kubernetes_cleanup_grace_seconds < 0:
                raise ValueError("Kubernetes runtime cleanup grace must not be negative")
            if (
                resource_kind != "job"
                or not namespace
                or not resource_name
                or not resource_uid
                or runtime_worker_session_id is None
                or not spec_hash
            ):
                raise ValueError("Kubernetes runtime handle has incomplete Job fencing identity")
            if (
                execution.runtime_worker_session_id is not None
                and execution.runtime_worker_session_id != runtime_worker_session_id
            ):
                raise StaleExecutionError("Kubernetes Job creation session is immutable")

        persisted: dict[str, object] = {
            "runtime_type": runtime_type,
            "runtime_namespace": namespace,
            "runtime_resource_kind": resource_kind,
            "runtime_resource_name": resource_name,
            "runtime_resource_uid": resource_uid,
            "runtime_worker_session_id": (
                str(runtime_worker_session_id) if runtime_worker_session_id is not None else None
            ),
            "observed_pod_name": observed_pod_name,
            "observed_pod_uid": observed_pod_uid,
            "runtime_spec_hash": spec_hash,
            "runtime_log_cursor_bytes": (
                existing_handle.get("runtime_log_cursor_bytes", 0)
                if isinstance(existing_handle, dict)
                else 0
            ),
        }
        task.runtime_handle = persisted
        task.version += 1
        if runtime_type == "kubernetes":
            # A durable Job may outlive an abrupt worker loss.  Install its
            # complete execution lease now, rather than waiting for graceful
            # shutdown, so retry recovery cannot overlap that Job.
            now = await database_utcnow(session)
            task.lease_expires_at = _runtime_lease_expiry(
                task,
                now,
                lease_seconds=0,
                kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
            )
        execution.runtime_object_id = resource_name
        if runtime_type == "kubernetes":
            execution.runtime_namespace = namespace
            execution.runtime_resource_kind = resource_kind
            execution.runtime_resource_name = resource_name
            execution.runtime_resource_uid = resource_uid
            execution.runtime_worker_session_id = runtime_worker_session_id
            execution.observed_pod_name = observed_pod_name
            execution.observed_pod_uid = observed_pod_uid
            execution.runtime_spec_hash = spec_hash
        return task

    @staticmethod
    async def advance_runtime_log_cursor(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        cursor_bytes: int,
        resource_name: str,
        resource_uid: str,
        spec_hash: str,
        worker_session_id: uuid.UUID,
    ) -> Task:
        """Advance a Kubernetes log cursor in the log-write transaction."""

        if cursor_bytes < 0:
            raise ValueError("runtime log cursor must not be negative")
        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        persisted = task.runtime_handle
        if (
            execution is None
            or execution.task_id != task_id
            or execution.worker_id != worker_id
            or execution.worker_session_id != worker_session_id
            or execution.runtime_type != "kubernetes"
            or execution.runtime_resource_name != resource_name
            or execution.runtime_resource_uid != resource_uid
            or execution.runtime_spec_hash != spec_hash
            or not isinstance(persisted, dict)
            or persisted.get("runtime_type") != "kubernetes"
            or persisted.get("runtime_resource_name") != resource_name
            or persisted.get("runtime_resource_uid") != resource_uid
            or persisted.get("runtime_spec_hash") != spec_hash
        ):
            raise StaleExecutionError("runtime log cursor execution fence is stale")
        current = persisted.get("runtime_log_cursor_bytes", 0)
        if not isinstance(current, int) or isinstance(current, bool) or current < 0:
            raise StaleExecutionError("persisted runtime log cursor is invalid")
        if cursor_bytes < current:
            raise StaleExecutionError("runtime log cursor must be monotonic")
        if cursor_bytes == current:
            return task
        updated = dict(persisted)
        updated["runtime_log_cursor_bytes"] = cursor_bytes
        task.runtime_handle = updated
        task.version += 1
        return task

    @staticmethod
    async def runtime_cleanup_owned(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID,
        runtime_type: str,
        resource_name: str,
        resource_uid: str | None,
        spec_hash: str | None,
    ) -> bool:
        """Gate destructive runtime operations on current DB controller ownership."""

        worker = await session.get(Worker, worker_id)
        execution = await session.get(TaskExecution, execution_id)
        reservation = await session.scalar(
            select(ResourceReservation).where(ResourceReservation.execution_id == execution_id)
        )
        if (
            worker is None
            or execution is None
            or reservation is None
            or worker.worker_session_id != worker_session_id
            or execution.task_id != task_id
            or execution.worker_id != worker_id
            or execution.worker_session_id != worker_session_id
            or reservation.worker_id != worker_id
            or reservation.worker_session_id != worker_session_id
            or execution.runtime_type != runtime_type
            or execution.runtime_object_id != resource_name
        ):
            return False
        if runtime_type == "kubernetes":
            return (
                execution.runtime_resource_kind == "job"
                and execution.runtime_resource_name == resource_name
                and execution.runtime_resource_uid == resource_uid
                and execution.runtime_spec_hash == spec_hash
            )
        return True

    @staticmethod
    async def mark_running(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
        worker_session_id: uuid.UUID | None = None,
        kubernetes_cleanup_grace_seconds: float = 0.0,
    ) -> Task:
        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        if task.status == TaskStatus.RUNNING:
            now = await database_utcnow(session)
            task.lease_expires_at = _runtime_lease_expiry(
                task,
                now,
                lease_seconds=lease_seconds,
                kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
            )
            task.version += 1
            return task
        if task.status not in {TaskStatus.PULLING, TaskStatus.STARTING}:
            raise StaleExecutionError("task is no longer starting for this execution")
        now = await database_utcnow(session)
        _transition(session, task, TaskStatus.RUNNING, now)
        task.lease_expires_at = _runtime_lease_expiry(
            task,
            now,
            lease_seconds=lease_seconds,
            kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
        )
        return task

    @staticmethod
    async def preserve_kubernetes_handoff_lease(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID,
        cleanup_grace_seconds: int,
    ) -> Task:
        """Keep a running Kubernetes Job exclusive while its Worker restarts.

        Kubernetes enforces a task timeout with ``activeDeadlineSeconds``. A
        normal Worker lease is much shorter, so a graceful rollout must extend
        that lease through the Job's lifetime before relinquishing it. Otherwise
        the generic reaper can schedule a second execution while the original
        Pod is still running.
        """

        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        if task.runtime_type.value != "kubernetes" or task.runtime_handle is None:
            raise StaleExecutionError("Kubernetes handoff runtime handle is no longer owned")
        if cleanup_grace_seconds < 0:
            raise ValueError("Kubernetes handoff cleanup grace must not be negative")
        now = await database_utcnow(session)
        task.lease_expires_at = now + timedelta(
            seconds=task.timeout_seconds + cleanup_grace_seconds
        )
        task.version += 1
        return task

    @staticmethod
    async def mark_starting(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
        worker_session_id: uuid.UUID | None = None,
        kubernetes_cleanup_grace_seconds: float = 0.0,
    ) -> Task:
        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
        if task.status in {TaskStatus.STARTING, TaskStatus.RUNNING}:
            now = await database_utcnow(session)
            task.lease_expires_at = _runtime_lease_expiry(
                task,
                now,
                lease_seconds=lease_seconds,
                kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
            )
            task.version += 1
            return task
        if task.status != TaskStatus.PULLING:
            raise StaleExecutionError("task is no longer pulling for this execution")
        now = await database_utcnow(session)
        _transition(session, task, TaskStatus.STARTING, now)
        task.lease_expires_at = _runtime_lease_expiry(
            task,
            now,
            lease_seconds=lease_seconds,
            kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
        )
        return task

    @staticmethod
    async def renew_lease(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        lease_seconds: float,
        worker_session_id: uuid.UUID | None = None,
        kubernetes_cleanup_grace_seconds: float = 0.0,
    ) -> bool:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if (
            task is None
            or task.worker_id != worker_id
            or task.execution_id != execution_id
            or task.status not in ACTIVE_TASK_STATUSES
        ):
            return False
        if worker_session_id is not None:
            if not await TaskRepository._session_owns_execution(
                session,
                worker_id=worker_id,
                execution_id=execution_id,
                worker_session_id=worker_session_id,
            ):
                return False
        now = await database_utcnow(session)
        task.lease_expires_at = _runtime_lease_expiry(
            task,
            now,
            lease_seconds=lease_seconds,
            kubernetes_cleanup_grace_seconds=kubernetes_cleanup_grace_seconds,
        )
        task.version += 1
        return True

    @staticmethod
    async def cancellation_requested(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        task = await TaskRepository._owned_task(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        )
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
        memory_price_per_gb_hour: float = 0.005,
        error_category: ErrorCategory | str | None = None,
        error_code: ErrorCode | str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> ExecutionResult:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.worker_id != worker_id or task.execution_id != execution_id:
            return ExecutionResult(accepted=False, status=None)
        if worker_session_id is not None and not await TaskRepository._session_owns_execution(
            session,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        ):
            return ExecutionResult(accepted=False, status=None)
        if task.status not in ACTIVE_TASK_STATUSES:
            return ExecutionResult(accepted=False, status=task.status)

        worker = await session.scalar(
            select(Worker)
            .where(Worker.id == worker_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        now = await database_utcnow(session)
        preemption_requested = task.status == TaskStatus.PREEMPTING
        user_cancelled_preemption = preemption_requested and task.failure_category == "CANCELLED"
        if task.cancel_requested and (not preemption_requested or user_cancelled_preemption):
            target = TaskStatus.CANCELLED
            error_message = "task was cancelled by user request"
            error_category = ErrorCategory.CANCELLED
            error_code = None
        elif preemption_requested:
            target = TaskStatus.PREEMPTED
            error_message = "task was stopped to admit a higher-priority workload"
            error_category = ErrorCategory.PREEMPTED
            error_code = None
        elif error_category is None:
            if target == TaskStatus.TIMED_OUT:
                error_category = ErrorCategory.TIMEOUT
            elif target == TaskStatus.CANCELLED:
                error_category = ErrorCategory.CANCELLED
            elif target == TaskStatus.PREEMPTED:
                error_category = ErrorCategory.PREEMPTED
            elif target == TaskStatus.FAILED:
                error_category = (
                    ErrorCategory.USER_ERROR
                    if exit_code is not None
                    else ErrorCategory.INTERNAL_ERROR
                )
        _transition(session, task, target, now, event_type=f"task.{target.value}")
        task.exit_code = exit_code
        task.error_message = error_message
        category_value = _enum_value(error_category)
        code_value = _enum_value(error_code)
        task.failure_category = category_value
        task.error_category = category_value
        task.error_code = code_value
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

        released = await ReservationRepository.release_and_settle(
            session,
            task=task,
            execution_id=execution_id,
            final_status=target.value,
            now=now,
            release_reason=target.value,
        )
        if not released and worker is not None:
            # Compatibility for an in-flight Phase I execution encountered
            # before the reservation backfill migration completed.
            _release_worker_capacity(worker, task)
        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        if execution is not None:
            execution.error_category = category_value
            execution.error_code = code_value
            execution.error_message = error_message

        if preemption_requested:
            await _complete_preemption_plans(session, task_id=task.id, now=now)

        retry_scheduled = False
        retry_admitted = False
        if target == TaskStatus.PREEMPTED and task.requeue_on_preempt:
            retry_admitted = await _admit_retry(session, task.project_id)
        elif (
            target in {TaskStatus.FAILED, TaskStatus.TIMED_OUT}
            and task.retry_count < task.max_retries
            and should_retry_failure(
                error_category=error_category,
                exit_code=exit_code,
                retry_on_exit_codes=task.retry_on_exit_codes,
            )
        ):
            retry_admitted = await _admit_retry(session, task.project_id)
        if (
            target in {TaskStatus.FAILED, TaskStatus.TIMED_OUT}
            and task.retry_count < task.max_retries
            and should_retry_failure(
                error_category=error_category,
                exit_code=exit_code,
                retry_on_exit_codes=task.retry_on_exit_codes,
            )
            and retry_admitted
        ):
            task.retry_count += 1
            _transition(session, task, TaskStatus.RETRYING, now)
            task.next_attempt_at = now + timedelta(
                seconds=retry_delay_seconds(
                    task.retry_count,
                    min(task.retry_max_seconds, retry_max_backoff_seconds),
                    backoff=task.retry_backoff,
                    base_seconds=task.retry_base_seconds,
                )
            )
            task.worker_id = None
            task.execution_id = None
            task.cancel_requested = False
            retry_scheduled = True
        elif target == TaskStatus.PREEMPTED and task.requeue_on_preempt and retry_admitted:
            _transition(session, task, TaskStatus.RETRYING, now)
            task.next_attempt_at = now
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
                    "cpu_seconds": task.cpu_seconds,
                    "gpu_seconds": task.gpu_seconds,
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
            TaskStatus.PREEMPTED,
        }:
            return task
        if task.status == TaskStatus.PREEMPTING:
            task.cancel_requested = True
            task.requeue_on_preempt = False
            task.failure_category = ErrorCategory.CANCELLED.value
            task.error_category = ErrorCategory.CANCELLED.value
            task.error_code = None
            task.version += 1
            return task
        if task.status in ACTIVE_TASK_STATUSES and task.execution_id is not None:
            task.cancel_requested = True
            if task.status != TaskStatus.STOPPING:
                _transition(
                    session,
                    task,
                    TaskStatus.STOPPING,
                    await database_utcnow(session),
                    event_type="task.stop_requested",
                )
            return task

        previous_status = task.status
        previous_worker_id = task.worker_id
        now = await database_utcnow(session)
        _transition(session, task, TaskStatus.CANCELLED, now)
        task.cancel_requested = True
        task.failure_category = ErrorCategory.CANCELLED.value
        task.error_category = ErrorCategory.CANCELLED.value
        task.error_code = None
        task.lease_expires_at = None
        task.execution_id = None
        if previous_status in {
            TaskStatus.PENDING,
            TaskStatus.QUEUED,
            TaskStatus.SCHEDULING,
            TaskStatus.RETRYING,
        }:
            quota_state = await session.get(ProjectQuotaState, task.project_id)
            if quota_state is not None:
                await QuotaRepository.release_queued(session, project_id=task.project_id)
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
    async def cancel_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        task_id: uuid.UUID,
    ) -> Task | None:
        task = await TaskRepository.get_for_project(
            session, project_id=project_id, task_id=task_id, for_update=True
        )
        if task is None:
            return None
        # ``cancel`` locks the same row again in the current transaction and
        # then applies the single authoritative state transition.
        return await TaskRepository.cancel(session, task_id)

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
                _transition(session, task, TaskStatus.CANCELLED, now)
                task.next_attempt_at = None
                task.failure_category = ErrorCategory.CANCELLED.value
                task.error_category = ErrorCategory.CANCELLED.value
                task.error_code = None
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                    available_at=now,
                )
                continue
            _transition(session, task, TaskStatus.QUEUED, now)
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
            execution_id = task.execution_id
            preemption_requested = task.status == TaskStatus.PREEMPTING
            user_cancelled_preemption = (
                preemption_requested and task.failure_category == ErrorCategory.CANCELLED.value
            )
            cancellation_requested = task.cancel_requested and (
                not preemption_requested or user_cancelled_preemption
            )
            if cancellation_requested:
                recovered_status = TaskStatus.CANCELLED
            elif preemption_requested:
                recovered_status = TaskStatus.PREEMPTED
            else:
                recovered_status = TaskStatus.FAILED
            worker = (
                await session.get(Worker, worker_id, with_for_update=True)
                if worker_id is not None
                else None
            )
            recovery_code = (
                ErrorCode.WORKER_LOST
                if worker is None or worker.status == WorkerStatus.OFFLINE
                else ErrorCode.LEASE_EXPIRED
            )
            task.lease_expires_at = None
            released = False
            if execution_id is not None:
                released = await ReservationRepository.release_and_settle(
                    session,
                    task=task,
                    execution_id=execution_id,
                    final_status=recovered_status.value,
                    now=now,
                    release_reason="execution_lease_expired",
                )
            if not released and worker_id is not None:
                worker = await session.get(Worker, worker_id, with_for_update=True)
                if worker is not None:
                    _release_worker_capacity(worker, task)
            task.execution_id = None
            task.worker_id = None
            if preemption_requested:
                await _complete_preemption_plans(session, task_id=task.id, now=now)
            if cancellation_requested:
                _transition(session, task, TaskStatus.CANCELLED, now)
                task.error_message = "task was cancelled before its execution lease expired"
                task.failure_category = ErrorCategory.CANCELLED.value
                task.error_category = ErrorCategory.CANCELLED.value
                task.error_code = None
                if execution_id is not None:
                    execution = await session.get(TaskExecution, execution_id, with_for_update=True)
                    if execution is not None:
                        execution.error_category = ErrorCategory.CANCELLED.value
                        execution.error_code = None
                        execution.error_message = task.error_message
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.terminal",
                    payload={"task_id": str(task.id), "status": TaskStatus.CANCELLED.value},
                    available_at=now,
                )
                recovered.append(task.id)
                continue

            if preemption_requested:
                _transition(
                    session,
                    task,
                    TaskStatus.PREEMPTED,
                    now,
                    event_type="task.preempted",
                )
                task.error_message = (
                    "preempted execution lease expired; worker ownership was revoked"
                )
                task.failure_category = ErrorCategory.PREEMPTED.value
                task.error_category = ErrorCategory.PREEMPTED.value
                task.error_code = None
                if execution_id is not None:
                    execution = await session.get(TaskExecution, execution_id, with_for_update=True)
                    if execution is not None:
                        execution.error_category = ErrorCategory.PREEMPTED.value
                        execution.error_code = None
                        execution.error_message = task.error_message
                if task.requeue_on_preempt and await _admit_retry(session, task.project_id):
                    _transition(session, task, TaskStatus.RETRYING, now)
                    task.next_attempt_at = now + timedelta(seconds=recovery_cleanup_grace_seconds)
                    task.cancel_requested = False
                    recovered.append(task.id)
                else:
                    if task.requeue_on_preempt:
                        task.error_message += "; retry suppressed by project queue quota"
                    OutboxRepository.add(
                        session,
                        aggregate_id=task.id,
                        event_type="task.terminal",
                        payload={
                            "task_id": str(task.id),
                            "status": TaskStatus.PREEMPTED.value,
                        },
                        available_at=now,
                    )
                continue

            _transition(session, task, TaskStatus.FAILED, now)
            task.error_message = "execution lease expired; worker ownership was revoked"
            task.failure_category = ErrorCategory.INFRA_ERROR.value
            task.error_category = ErrorCategory.INFRA_ERROR.value
            task.error_code = recovery_code.value
            if execution_id is not None:
                execution = await session.get(TaskExecution, execution_id, with_for_update=True)
                if execution is not None:
                    execution.error_category = ErrorCategory.INFRA_ERROR.value
                    execution.error_code = recovery_code.value
                    execution.error_message = task.error_message
            if task.recovery_count < max_recovery_attempts:
                if await _admit_retry(session, task.project_id):
                    task.recovery_count += 1
                    _transition(session, task, TaskStatus.RETRYING, now)
                    task.next_attempt_at = now + timedelta(seconds=recovery_cleanup_grace_seconds)
                    task.cancel_requested = False
                    recovered.append(task.id)
                else:
                    task.error_message += "; retry suppressed by project queue quota"
                    OutboxRepository.add(
                        session,
                        aggregate_id=task.id,
                        event_type="task.terminal",
                        payload={"task_id": str(task.id), "status": TaskStatus.FAILED.value},
                        available_at=now,
                    )
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
        worker_session_id: uuid.UUID | None = None,
    ) -> Task:
        task = await TaskRepository.get(session, task_id, for_update=True)
        if task is None or task.worker_id != worker_id or task.execution_id != execution_id:
            raise StaleExecutionError("execution token is stale")
        if worker_session_id is not None and not await TaskRepository._session_owns_execution(
            session,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=worker_session_id,
        ):
            raise StaleExecutionError("worker session is stale")
        return task

    @staticmethod
    async def _session_owns_execution(
        session: AsyncSession,
        *,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID,
    ) -> bool:
        worker = await session.scalar(
            select(Worker)
            .where(Worker.id == worker_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        if worker is None or worker.worker_session_id != worker_session_id:
            return False
        reservation = await session.scalar(
            select(ResourceReservation)
            .where(
                ResourceReservation.execution_id == execution_id,
                ResourceReservation.worker_id == worker_id,
                ResourceReservation.worker_session_id == worker_session_id,
                ResourceReservation.released_at.is_(None),
            )
            .with_for_update()
        )
        return reservation is not None


def _unsatisfied_dependencies(task_id: Any) -> ColumnElement[bool]:
    dependency_task = aliased(Task)
    return (
        select(TaskDependency.task_id)
        .join(
            dependency_task,
            dependency_task.id == TaskDependency.depends_on_task_id,
        )
        .where(
            TaskDependency.task_id == task_id,
            dependency_task.status != TaskStatus.SUCCEEDED,
        )
        .exists()
    )


def _worker_can_run_task(worker: Worker, task: Task) -> bool:
    return (
        worker.status == WorkerStatus.ONLINE
        and not worker.overcommitted
        and task.runtime_type.value in worker.runtime_types
        and round(worker.reserved_cpu * 1000) + task.cpu_millicores
        <= worker.cpu_allocatable_millicores
        and worker.reserved_memory_mb + task.memory_limit_mb <= worker.memory_allocatable_mb
        and worker.reserved_gpus + task.gpu_count <= worker.gpu_count
        and _labels_match(task.labels, worker.labels)
        and _taints_tolerated(worker.taints, task.tolerations)
    )


def _release_worker_capacity(worker: Worker, task: Task) -> None:
    worker.running_tasks = max(0, worker.running_tasks - 1)
    worker.reserved_cpu = max(0.0, worker.reserved_cpu - task.cpu_limit)
    worker.reserved_memory_mb = max(0, worker.reserved_memory_mb - task.memory_limit_mb)
    worker.reserved_gpus = max(0, worker.reserved_gpus - task.gpu_count)
    worker.version += 1


def _labels_match(required: dict[str, str], offered: dict[str, str]) -> bool:
    return all(offered.get(key) == value for key, value in required.items())


def _taints_tolerated(
    taints: list[dict[str, str]],
    tolerations: list[dict[str, str]],
) -> bool:
    for taint in taints:
        if taint.get("effect", "NoSchedule") != "NoSchedule":
            continue
        if not any(
            tolerance.get("key") == taint.get("key")
            and tolerance.get("value", taint.get("value")) == taint.get("value")
            for tolerance in tolerations
        ):
            return False
    return True


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _runtime_lease_expiry(
    task: Task,
    now: datetime,
    *,
    lease_seconds: float,
    kubernetes_cleanup_grace_seconds: float,
) -> datetime:
    if kubernetes_cleanup_grace_seconds < 0:
        raise ValueError("Kubernetes runtime cleanup grace must not be negative")
    ordinary_expiry = now + timedelta(seconds=lease_seconds)
    if task.runtime_type.value != "kubernetes" or task.runtime_handle is None:
        return ordinary_expiry
    started_at = _as_utc(task.started_at) if task.started_at is not None else now
    job_expiry = started_at + timedelta(
        seconds=task.timeout_seconds + kubernetes_cleanup_grace_seconds
    )
    return max(ordinary_expiry, job_expiry)


def _enum_value(value: ErrorCategory | ErrorCode | str | None) -> str | None:
    if isinstance(value, ErrorCategory | ErrorCode):
        return value.value
    return value


async def _admit_retry(session: AsyncSession, project_id: uuid.UUID) -> bool:
    state = await session.get(ProjectQuotaState, project_id)
    if state is None:
        # Compatibility with an execution created before the Phase II quota
        # migration. New tasks always initialize quota state transactionally.
        return True
    try:
        await QuotaRepository.admit_queued(session, project_id=project_id)
    except QuotaExceededError:
        return False
    return True


async def _complete_preemption_plans(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    now: datetime,
) -> None:
    plans = list(
        await session.scalars(
            select(PreemptionPlan)
            .where(
                PreemptionPlan.victim_task_id == task_id,
                PreemptionPlan.state == "requested",
            )
            .with_for_update()
        )
    )
    for plan in plans:
        plan.state = "completed"
        plan.completed_at = now
