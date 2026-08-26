import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.enums import ACTIVE_TASK_STATUSES, FINAL_TASK_STATUSES, TaskStatus, WorkerStatus
from models.artifact import Artifact, Dataset, JobGroup
from models.outbox import OutboxEvent
from models.scheduling import ResourceReservation
from models.service import ModelService, ServiceReplica
from models.task import Task
from models.usage import ProjectQuotaState
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.quotas import QuotaInvariantViolation, QuotaNotFoundError
from repositories.reservations import ReservationRepository, ResourceInvariantViolation


@dataclass(frozen=True, slots=True)
class OutboxLagDiagnostic:
    scope: str
    pending_events: int
    ready_events: int
    retrying_events: int
    oldest_ready_at: datetime | None
    lag_seconds: float


@dataclass(frozen=True, slots=True)
class OfflineWorkerDiagnostic:
    worker_id: str
    status: WorkerStatus
    last_heartbeat_at: datetime
    stale_for_seconds: float
    running_tasks: int


@dataclass(frozen=True, slots=True)
class StuckTaskDiagnostic:
    task_id: uuid.UUID
    status: TaskStatus
    reason: str
    worker_id: str | None
    lease_expires_at: datetime | None
    stuck_for_seconds: float
    unschedulable_reason: str | None


@dataclass(frozen=True, slots=True)
class ActiveReservationDiagnostic:
    reservation_id: uuid.UUID
    task_id: uuid.UUID
    execution_id: uuid.UUID
    worker_id: str
    cpu_millicores: int
    memory_mb: int
    gpu_count: int
    created_at: datetime
    age_seconds: float


@dataclass(frozen=True, slots=True)
class ConsistencyIssueDiagnostic:
    resource_type: str
    resource_id: str
    task_id: uuid.UUID | None
    reason: str
    repairable: bool = False


@dataclass(frozen=True, slots=True)
class ConsistencyCheckDiagnostic:
    name: str
    status: str
    total: int | None
    issues: tuple[ConsistencyIssueDiagnostic, ...] = ()
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ConsistencyDiagnostic:
    status: str
    complete: bool
    issues_total: int
    checks: tuple[ConsistencyCheckDiagnostic, ...]


@dataclass(frozen=True, slots=True)
class RepairActionDiagnostic:
    check: str
    resource_type: str
    resource_id: str
    action: str
    outcome: str
    reason: str


@dataclass(frozen=True, slots=True)
class ConservativeRepairResult:
    project_id: uuid.UUID | None
    observed_at: datetime
    candidates_total: int
    repaired_total: int
    skipped_total: int
    actions: tuple[RepairActionDiagnostic, ...]


@dataclass(frozen=True, slots=True)
class DiagnosticSnapshot:
    project_id: uuid.UUID | None
    observed_at: datetime
    queued_tasks: int
    online_workers: int
    outbox: OutboxLagDiagnostic
    offline_workers_total: int
    offline_workers: tuple[OfflineWorkerDiagnostic, ...]
    stuck_tasks_total: int
    stuck_tasks: tuple[StuckTaskDiagnostic, ...]
    active_reservations_total: int
    active_reservations: tuple[ActiveReservationDiagnostic, ...]
    consistency: ConsistencyDiagnostic


class DiagnosticsRepository:
    """Read-only operational evidence for a project administrator."""

    @staticmethod
    async def snapshot(
        session: AsyncSession,
        *,
        project_id: uuid.UUID | None,
        worker_offline_timeout_seconds: float,
        stuck_after_seconds: float,
        limit: int = 100,
    ) -> DiagnosticSnapshot:
        if worker_offline_timeout_seconds <= 0:
            raise ValueError("worker_offline_timeout_seconds must be greater than zero")
        if stuck_after_seconds <= 0:
            raise ValueError("stuck_after_seconds must be greater than zero")
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")

        now = await database_utcnow(session)
        offline_cutoff = now - timedelta(seconds=worker_offline_timeout_seconds)
        stuck_cutoff = now - timedelta(seconds=stuck_after_seconds)

        task_scope = [] if project_id is None else [Task.project_id == project_id]
        queued_tasks = int(
            await session.scalar(
                select(func.count(Task.id)).where(
                    Task.status == TaskStatus.QUEUED,
                    *task_scope,
                )
            )
            or 0
        )
        online_workers = int(
            await session.scalar(
                select(func.count(Worker.id)).where(
                    Worker.status == WorkerStatus.ONLINE,
                    Worker.last_heartbeat_at >= offline_cutoff,
                )
            )
            or 0
        )

        outbox = await _outbox_lag(session, project_id=project_id, now=now)
        offline_workers_total, offline_workers = await _offline_workers(
            session,
            now=now,
            cutoff=offline_cutoff,
            limit=limit,
        )
        stuck_tasks_total, stuck_tasks = await _stuck_tasks(
            session,
            project_id=project_id,
            now=now,
            cutoff=stuck_cutoff,
            limit=limit,
        )
        active_reservations_total, active_reservations = await _active_reservations(
            session,
            project_id=project_id,
            now=now,
            limit=limit,
        )
        consistency = await _consistency_checks(
            session,
            project_id=project_id,
            limit=limit,
        )
        return DiagnosticSnapshot(
            project_id=project_id,
            observed_at=now,
            queued_tasks=queued_tasks,
            online_workers=online_workers,
            outbox=outbox,
            offline_workers_total=offline_workers_total,
            offline_workers=offline_workers,
            stuck_tasks_total=stuck_tasks_total,
            stuck_tasks=stuck_tasks,
            active_reservations_total=active_reservations_total,
            active_reservations=active_reservations,
            consistency=consistency,
        )

    @staticmethod
    async def repair_conservative(
        session: AsyncSession,
        *,
        project_id: uuid.UUID | None,
        limit: int = 100,
    ) -> ConservativeRepairResult:
        """Repair only terminal-state database leaks under row locks.

        This deliberately does not call a runtime, stop a container or Pod, infer a
        missing lease, delete an orphan, or rewrite capacity. Repeating the method is
        safe: repaired reservations and cleared leases no longer match its predicates.
        """

        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")

        now = await database_utcnow(session)
        actions: list[RepairActionDiagnostic] = []
        reservation_filters: list[ColumnElement[bool]] = [
            ResourceReservation.released_at.is_(None),
            Task.status.in_(FINAL_TASK_STATUSES),
        ]
        if project_id is not None:
            reservation_filters.extend(
                [
                    ResourceReservation.project_id == project_id,
                    Task.project_id == project_id,
                ]
            )
        reservation_rows = list(
            (
                await session.execute(
                    select(ResourceReservation, Task)
                    .join(Task, Task.id == ResourceReservation.task_id)
                    .where(*reservation_filters)
                    .order_by(ResourceReservation.created_at, ResourceReservation.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for reservation, task in reservation_rows:
            try:
                async with session.begin_nested():
                    released = await ReservationRepository.release_and_settle(
                        session,
                        task=task,
                        execution_id=reservation.execution_id,
                        final_status=task.status.value,
                        now=now,
                        release_reason="doctor_terminal_task",
                    )
                    if not released:
                        raise ResourceInvariantViolation("reservation is no longer active")
            except (
                QuotaInvariantViolation,
                QuotaNotFoundError,
                ResourceInvariantViolation,
            ):
                actions.append(
                    RepairActionDiagnostic(
                        check="terminal_task_with_active_reservation",
                        resource_type="reservation",
                        resource_id=str(reservation.id),
                        action="release_reservation",
                        outcome="skipped",
                        reason="resource accounting could not be proven safe",
                    )
                )
            else:
                actions.append(
                    RepairActionDiagnostic(
                        check="terminal_task_with_active_reservation",
                        resource_type="reservation",
                        resource_id=str(reservation.id),
                        action="release_reservation",
                        outcome="repaired",
                        reason="terminal task reservation released and accounting settled",
                    )
                )

        lease_filters: list[ColumnElement[bool]] = [
            Task.status.in_(FINAL_TASK_STATUSES),
            Task.lease_expires_at.is_not(None),
        ]
        if project_id is not None:
            lease_filters.append(Task.project_id == project_id)
        terminal_leases = list(
            await session.scalars(
                select(Task)
                .where(*lease_filters)
                .order_by(Task.finished_at, Task.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        for task in terminal_leases:
            task.lease_expires_at = None
            task.version += 1
            actions.append(
                RepairActionDiagnostic(
                    check="terminal_task_with_lease",
                    resource_type="task",
                    resource_id=str(task.id),
                    action="clear_lease",
                    outcome="repaired",
                    reason="terminal task lease cleared without contacting the runtime",
                )
            )

        repaired_total = sum(action.outcome == "repaired" for action in actions)
        skipped_total = sum(action.outcome == "skipped" for action in actions)
        return ConservativeRepairResult(
            project_id=project_id,
            observed_at=now,
            candidates_total=len(actions),
            repaired_total=repaired_total,
            skipped_total=skipped_total,
            actions=tuple(actions),
        )


async def _outbox_lag(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    now: datetime,
) -> OutboxLagDiagnostic:
    filters: list[ColumnElement[bool]] = [OutboxEvent.processed_at.is_(None)]
    if project_id is not None:
        filters.append(_project_outbox_event(project_id))
    pending_events = int(
        await session.scalar(select(func.count(OutboxEvent.id)).where(*filters)) or 0
    )
    ready_filters = [*filters, OutboxEvent.available_at <= now]
    ready_events = int(
        await session.scalar(select(func.count(OutboxEvent.id)).where(*ready_filters)) or 0
    )
    retrying_events = int(
        await session.scalar(
            select(func.count(OutboxEvent.id)).where(
                *filters,
                OutboxEvent.attempts > 0,
            )
        )
        or 0
    )
    oldest_ready_at = await session.scalar(
        select(func.min(OutboxEvent.available_at)).where(*ready_filters)
    )
    normalized_oldest = _as_utc(oldest_ready_at) if oldest_ready_at is not None else None
    return OutboxLagDiagnostic(
        scope="all_events" if project_id is None else "project_events",
        pending_events=pending_events,
        ready_events=ready_events,
        retrying_events=retrying_events,
        oldest_ready_at=normalized_oldest,
        lag_seconds=_seconds_since(now, normalized_oldest),
    )


def _project_outbox_event(project_id: uuid.UUID) -> ColumnElement[bool]:
    project_text = str(project_id)
    task_event = and_(
        OutboxEvent.aggregate_type == "task",
        exists(
            select(Task.id).where(
                Task.id == OutboxEvent.aggregate_id,
                Task.project_id == project_id,
            )
        ),
    )
    service_event = and_(
        OutboxEvent.aggregate_type.in_(("service", "model_service")),
        exists(
            select(ModelService.id).where(
                ModelService.id == OutboxEvent.aggregate_id,
                ModelService.project_id == project_id,
            )
        ),
    )
    replica_event = and_(
        OutboxEvent.aggregate_type == "service_replica",
        exists(
            select(ServiceReplica.id)
            .join(ModelService, ModelService.id == ServiceReplica.service_id)
            .where(
                ServiceReplica.id == OutboxEvent.aggregate_id,
                ModelService.project_id == project_id,
            )
        ),
    )
    artifact_event = and_(
        OutboxEvent.aggregate_type == "artifact",
        exists(
            select(Artifact.id).where(
                Artifact.id == OutboxEvent.aggregate_id,
                Artifact.project_id == project_id,
            )
        ),
    )
    dataset_event = and_(
        OutboxEvent.aggregate_type == "dataset",
        exists(
            select(Dataset.id).where(
                Dataset.id == OutboxEvent.aggregate_id,
                Dataset.project_id == project_id,
            )
        ),
    )
    job_group_event = and_(
        OutboxEvent.aggregate_type == "job_group",
        exists(
            select(JobGroup.id).where(
                JobGroup.id == OutboxEvent.aggregate_id,
                JobGroup.project_id == project_id,
            )
        ),
    )
    known_aggregate_types = (
        "task",
        "service",
        "model_service",
        "service_replica",
        "artifact",
        "dataset",
        "job_group",
    )
    project_payload_event = and_(
        OutboxEvent.aggregate_type.not_in(known_aggregate_types),
        OutboxEvent.payload["project_id"].as_string() == project_text,
    )
    return or_(
        task_event,
        service_event,
        replica_event,
        artifact_event,
        dataset_event,
        job_group_event,
        project_payload_event,
    )


async def _offline_workers(
    session: AsyncSession,
    *,
    now: datetime,
    cutoff: datetime,
    limit: int,
) -> tuple[int, tuple[OfflineWorkerDiagnostic, ...]]:
    condition = or_(
        Worker.status == WorkerStatus.OFFLINE,
        Worker.last_heartbeat_at < cutoff,
    )
    total = int(await session.scalar(select(func.count(Worker.id)).where(condition)) or 0)
    workers = list(
        await session.scalars(
            select(Worker)
            .where(condition)
            .order_by(Worker.last_heartbeat_at, Worker.id)
            .limit(limit)
        )
    )
    return total, tuple(
        OfflineWorkerDiagnostic(
            worker_id=worker.id,
            status=worker.status,
            last_heartbeat_at=_as_utc(worker.last_heartbeat_at),
            stale_for_seconds=_seconds_since(now, _as_utc(worker.last_heartbeat_at)),
            running_tasks=worker.running_tasks,
        )
        for worker in workers
    )


async def _stuck_tasks(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    now: datetime,
    cutoff: datetime,
    limit: int,
) -> tuple[int, tuple[StuckTaskDiagnostic, ...]]:
    expired_lease = and_(
        Task.status.in_(ACTIVE_TASK_STATUSES),
        Task.lease_expires_at.is_not(None),
        Task.lease_expires_at < now,
    )
    overdue_retry = and_(
        Task.status == TaskStatus.RETRYING,
        Task.next_attempt_at.is_not(None),
        Task.next_attempt_at < cutoff,
    )
    unschedulable = and_(
        Task.status == TaskStatus.QUEUED,
        Task.unschedulable_reason.is_not(None),
        Task.queued_at.is_not(None),
        Task.queued_at < cutoff,
    )
    filters = [or_(expired_lease, overdue_retry, unschedulable)]
    if project_id is not None:
        filters.append(Task.project_id == project_id)
    total = int(await session.scalar(select(func.count(Task.id)).where(*filters)) or 0)
    tasks = list(
        await session.scalars(
            select(Task).where(*filters).order_by(Task.created_at, Task.id).limit(limit)
        )
    )
    return total, tuple(_stuck_task(task, now=now, cutoff=cutoff) for task in tasks)


def _stuck_task(task: Task, *, now: datetime, cutoff: datetime) -> StuckTaskDiagnostic:
    lease_expires_at = _as_utc(task.lease_expires_at) if task.lease_expires_at else None
    next_attempt_at = _as_utc(task.next_attempt_at) if task.next_attempt_at else None
    queued_at = _as_utc(task.queued_at) if task.queued_at else None
    if (
        task.status in ACTIVE_TASK_STATUSES
        and lease_expires_at is not None
        and lease_expires_at < now
    ):
        reason = "expired_lease"
        reference = lease_expires_at
    elif task.status == TaskStatus.RETRYING and next_attempt_at is not None:
        reason = "overdue_retry"
        reference = next_attempt_at
    else:
        reason = "unschedulable"
        reference = queued_at or cutoff
    return StuckTaskDiagnostic(
        task_id=task.id,
        status=task.status,
        reason=reason,
        worker_id=task.worker_id,
        lease_expires_at=lease_expires_at,
        stuck_for_seconds=_seconds_since(now, reference),
        unschedulable_reason=task.unschedulable_reason,
    )


async def _active_reservations(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    now: datetime,
    limit: int,
) -> tuple[int, tuple[ActiveReservationDiagnostic, ...]]:
    filters: list[ColumnElement[bool]] = [ResourceReservation.released_at.is_(None)]
    if project_id is not None:
        filters.append(ResourceReservation.project_id == project_id)
    total = int(
        await session.scalar(select(func.count(ResourceReservation.id)).where(*filters)) or 0
    )
    reservations = list(
        await session.scalars(
            select(ResourceReservation)
            .where(*filters)
            .order_by(ResourceReservation.created_at, ResourceReservation.id)
            .limit(limit)
        )
    )
    return total, tuple(
        ActiveReservationDiagnostic(
            reservation_id=reservation.id,
            task_id=reservation.task_id,
            execution_id=reservation.execution_id,
            worker_id=reservation.worker_id,
            cpu_millicores=reservation.cpu_millicores,
            memory_mb=reservation.memory_mb,
            gpu_count=reservation.gpu_count,
            created_at=_as_utc(reservation.created_at),
            age_seconds=_seconds_since(now, _as_utc(reservation.created_at)),
        )
        for reservation in reservations
    )


async def _consistency_checks(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyDiagnostic:
    checks = (
        await _running_tasks_without_lease(session, project_id=project_id, limit=limit),
        await _leases_without_worker(session, project_id=project_id, limit=limit),
        await _reservations_without_task(session, project_id=project_id, limit=limit),
        await _terminal_tasks_with_reservation(session, project_id=project_id, limit=limit),
        await _terminal_tasks_with_lease(session, project_id=project_id, limit=limit),
        ConsistencyCheckDiagnostic(
            name="orphan_container",
            status="not_observable",
            total=None,
            reason=(
                "container inventories are node-local and the control plane has no "
                "cross-worker runtime inventory"
            ),
        ),
        ConsistencyCheckDiagnostic(
            name="orphan_pod",
            status="not_observable",
            total=None,
            reason=(
                "cluster-wide Pod inventory is not available through the persisted "
                "control-plane state"
            ),
        ),
        await _processed_outbox_inconsistencies(
            session,
            project_id=project_id,
            limit=limit,
        ),
        await _negative_capacities(session, project_id=project_id, limit=limit),
    )
    issues_total = sum(check.total or 0 for check in checks if check.status == "issues")
    complete = all(check.status != "not_observable" for check in checks)
    if issues_total:
        status = "issues"
    elif not complete:
        status = "incomplete"
    else:
        status = "clean"
    return ConsistencyDiagnostic(
        status=status,
        complete=complete,
        issues_total=issues_total,
        checks=checks,
    )


async def _running_tasks_without_lease(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    filters: list[ColumnElement[bool]] = [
        Task.status == TaskStatus.RUNNING,
        Task.lease_expires_at.is_(None),
    ]
    if project_id is not None:
        filters.append(Task.project_id == project_id)
    total = int(await session.scalar(select(func.count(Task.id)).where(*filters)) or 0)
    tasks = list(
        await session.scalars(
            select(Task).where(*filters).order_by(Task.created_at, Task.id).limit(limit)
        )
    )
    return _issue_check(
        "running_task_without_lease",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="task",
                resource_id=str(task.id),
                task_id=task.id,
                reason="running task has no lease expiry",
            )
            for task in tasks
        ),
    )


async def _leases_without_worker(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    worker_exists = exists(select(Worker.id).where(Worker.id == Task.worker_id))
    filters: list[ColumnElement[bool]] = [
        Task.lease_expires_at.is_not(None),
        or_(Task.worker_id.is_(None), ~worker_exists),
    ]
    if project_id is not None:
        filters.append(Task.project_id == project_id)
    total = int(await session.scalar(select(func.count(Task.id)).where(*filters)) or 0)
    tasks = list(
        await session.scalars(
            select(Task).where(*filters).order_by(Task.created_at, Task.id).limit(limit)
        )
    )
    return _issue_check(
        "lease_without_worker",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="task",
                resource_id=str(task.id),
                task_id=task.id,
                reason="task lease has no persisted worker",
                repairable=task.status in FINAL_TASK_STATUSES,
            )
            for task in tasks
        ),
    )


async def _reservations_without_task(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    filters: list[ColumnElement[bool]] = [Task.id.is_(None)]
    if project_id is not None:
        filters.append(ResourceReservation.project_id == project_id)
    base = (
        select(ResourceReservation)
        .select_from(ResourceReservation)
        .outerjoin(Task, Task.id == ResourceReservation.task_id)
        .where(*filters)
    )
    total = int(
        await session.scalar(
            select(func.count(ResourceReservation.id))
            .select_from(ResourceReservation)
            .outerjoin(Task, Task.id == ResourceReservation.task_id)
            .where(*filters)
        )
        or 0
    )
    reservations = list(
        await session.scalars(base.order_by(ResourceReservation.created_at).limit(limit))
    )
    return _issue_check(
        "reservation_without_task",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="reservation",
                resource_id=str(reservation.id),
                task_id=reservation.task_id,
                reason="reservation references a missing task",
            )
            for reservation in reservations
        ),
    )


async def _terminal_tasks_with_reservation(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    filters: list[ColumnElement[bool]] = [
        ResourceReservation.released_at.is_(None),
        Task.status.in_(FINAL_TASK_STATUSES),
    ]
    if project_id is not None:
        filters.extend(
            [
                Task.project_id == project_id,
                ResourceReservation.project_id == project_id,
            ]
        )
    total = int(
        await session.scalar(
            select(func.count(ResourceReservation.id))
            .join(Task, Task.id == ResourceReservation.task_id)
            .where(*filters)
        )
        or 0
    )
    rows = list(
        (
            await session.execute(
                select(ResourceReservation, Task)
                .join(Task, Task.id == ResourceReservation.task_id)
                .where(*filters)
                .order_by(ResourceReservation.created_at, ResourceReservation.id)
                .limit(limit)
            )
        ).all()
    )
    return _issue_check(
        "terminal_task_with_active_reservation",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="reservation",
                resource_id=str(reservation.id),
                task_id=task.id,
                reason=f"{task.status.value} task still has an active reservation",
                repairable=True,
            )
            for reservation, task in rows
        ),
    )


async def _terminal_tasks_with_lease(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    filters: list[ColumnElement[bool]] = [
        Task.status.in_(FINAL_TASK_STATUSES),
        Task.lease_expires_at.is_not(None),
    ]
    if project_id is not None:
        filters.append(Task.project_id == project_id)
    total = int(await session.scalar(select(func.count(Task.id)).where(*filters)) or 0)
    tasks = list(
        await session.scalars(
            select(Task).where(*filters).order_by(Task.finished_at, Task.id).limit(limit)
        )
    )
    return _issue_check(
        "terminal_task_with_lease",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="task",
                resource_id=str(task.id),
                task_id=task.id,
                reason=f"{task.status.value} task retains a lease expiry",
                repairable=True,
            )
            for task in tasks
        ),
    )


async def _processed_outbox_inconsistencies(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    filters: list[ColumnElement[bool]] = [
        OutboxEvent.processed_at.is_not(None),
        or_(
            OutboxEvent.locked_by.is_not(None),
            OutboxEvent.locked_until.is_not(None),
            OutboxEvent.last_error.is_not(None),
            OutboxEvent.processed_at < OutboxEvent.created_at,
            OutboxEvent.processed_at < OutboxEvent.available_at,
        ),
    ]
    if project_id is not None:
        filters.append(_project_outbox_event(project_id))
    total = int(await session.scalar(select(func.count(OutboxEvent.id)).where(*filters)) or 0)
    events = list(
        await session.scalars(
            select(OutboxEvent)
            .where(*filters)
            .order_by(OutboxEvent.processed_at, OutboxEvent.id)
            .limit(limit)
        )
    )
    return _issue_check(
        "processed_outbox_inconsistency",
        total,
        tuple(
            ConsistencyIssueDiagnostic(
                resource_type="outbox_event",
                resource_id=str(event.id),
                task_id=event.aggregate_id if event.aggregate_type == "task" else None,
                reason="processed outbox event retains failed/locked or impossible timestamps",
            )
            for event in events
        ),
    )


async def _negative_capacities(
    session: AsyncSession,
    *,
    project_id: uuid.UUID | None,
    limit: int,
) -> ConsistencyCheckDiagnostic:
    worker_fields = {
        "running_tasks": Worker.running_tasks,
        "concurrency": Worker.concurrency,
        "reserved_cpu": Worker.reserved_cpu,
        "reserved_memory_mb": Worker.reserved_memory_mb,
        "reserved_gpus": Worker.reserved_gpus,
        "cpu_count": Worker.cpu_count,
        "cpu_total_millicores": Worker.cpu_total_millicores,
        "cpu_allocatable_millicores": Worker.cpu_allocatable_millicores,
        "memory_total_mb": Worker.memory_total_mb,
        "memory_allocatable_mb": Worker.memory_allocatable_mb,
        "gpu_count": Worker.gpu_count,
        "gpu_memory_mb": Worker.gpu_memory_mb,
    }
    worker_negative = or_(*(column < 0 for column in worker_fields.values()))
    worker_filters: list[ColumnElement[bool]] = [worker_negative]
    if project_id is not None:
        worker_filters.append(
            or_(
                exists(
                    select(Task.id).where(
                        Task.worker_id == Worker.id,
                        Task.project_id == project_id,
                    )
                ),
                exists(
                    select(ResourceReservation.id).where(
                        ResourceReservation.worker_id == Worker.id,
                        ResourceReservation.project_id == project_id,
                    )
                ),
            )
        )
    workers_total = int(
        await session.scalar(select(func.count(Worker.id)).where(*worker_filters)) or 0
    )
    workers = list(
        await session.scalars(
            select(Worker).where(*worker_filters).order_by(Worker.id).limit(limit)
        )
    )

    quota_fields = {
        "queued_tasks": ProjectQuotaState.queued_tasks,
        "running_tasks": ProjectQuotaState.running_tasks,
        "reserved_cpu_millicores": ProjectQuotaState.reserved_cpu_millicores,
        "reserved_memory_mb": ProjectQuotaState.reserved_memory_mb,
        "reserved_gpus": ProjectQuotaState.reserved_gpus,
        "service_count": ProjectQuotaState.service_count,
        "service_replicas": ProjectQuotaState.service_replicas,
        "artifact_bytes": ProjectQuotaState.artifact_bytes,
        "daily_reserved_cost": ProjectQuotaState.daily_reserved_cost,
        "daily_settled_cost": ProjectQuotaState.daily_settled_cost,
    }
    quota_filters: list[ColumnElement[bool]] = [
        or_(*(column < 0 for column in quota_fields.values()))
    ]
    if project_id is not None:
        quota_filters.append(ProjectQuotaState.project_id == project_id)
    quotas_total = int(
        await session.scalar(select(func.count(ProjectQuotaState.project_id)).where(*quota_filters))
        or 0
    )
    remaining = max(0, limit - len(workers))
    quota_states = list(
        await session.scalars(
            select(ProjectQuotaState)
            .where(*quota_filters)
            .order_by(ProjectQuotaState.project_id)
            .limit(remaining)
        )
    )

    issues = [
        ConsistencyIssueDiagnostic(
            resource_type="worker",
            resource_id=worker.id,
            task_id=None,
            reason=(
                "negative fields: "
                + ", ".join(name for name in worker_fields if getattr(worker, name) < 0)
            ),
        )
        for worker in workers
    ]
    issues.extend(
        ConsistencyIssueDiagnostic(
            resource_type="project_quota_state",
            resource_id=str(state.project_id),
            task_id=None,
            reason=(
                "negative fields: "
                + ", ".join(name for name in quota_fields if getattr(state, name) < 0)
            ),
        )
        for state in quota_states
    )
    return _issue_check("negative_capacity", workers_total + quotas_total, tuple(issues))


def _issue_check(
    name: str,
    total: int,
    issues: tuple[ConsistencyIssueDiagnostic, ...],
) -> ConsistencyCheckDiagnostic:
    return ConsistencyCheckDiagnostic(
        name=name,
        status="issues" if total else "clean",
        total=total,
        issues=issues,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _seconds_since(now: datetime, value: datetime | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, (now - value).total_seconds())
