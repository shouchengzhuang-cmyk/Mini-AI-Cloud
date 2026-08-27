from __future__ import annotations

import uuid
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Float, Integer, Select, case, cast, func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    AllocationAuthority,
    TaskStatus,
    WorkerStatus,
)
from core.state_machine import ensure_transition
from models.scheduling import (
    GPUDevice,
    PlacementAttempt,
    PreemptionPlan,
    ReservationGPUDevice,
    ResourceReservation,
)
from models.task import Task, TaskEvent
from models.usage import ProjectQuotaState, TaskExecution
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.outbox import OutboxRepository
from repositories.quotas import QuotaRepository
from repositories.reservations import ReservationRepository
from repositories.tasks import TaskRepository, _prepare_for_assignment, _unsatisfied_dependencies
from scheduler.policies import (
    GPUDeviceSnapshot,
    TaskSnapshot,
    WorkerSnapshot,
    dominant_share,
    effective_priority,
    evaluate_snapshot,
)


class PlacementConflict(RuntimeError):
    """A concurrent scheduler changed a placement input."""


@dataclass(frozen=True, slots=True)
class SchedulerCandidate:
    task: Task
    snapshot: TaskSnapshot
    effective_priority: int
    project_dominant_share: float


@dataclass(frozen=True, slots=True)
class PreemptionDecision:
    incoming_task_id: uuid.UUID
    worker_id: str
    victim_task_ids: tuple[uuid.UUID, ...]
    already_requested: bool = False


def _candidate_conditions(
    excluded_task_ids: frozenset[uuid.UUID],
) -> tuple[ColumnElement[bool], ...]:
    conditions: tuple[ColumnElement[bool], ...] = (
        Task.status == TaskStatus.QUEUED,
        Task.cancel_requested.is_(False),
        ~_unsatisfied_dependencies(Task.id),
    )
    if excluded_task_ids:
        conditions += (Task.id.not_in(excluded_task_ids),)
    return conditions


def _effective_priority_expression(
    *,
    now: datetime,
    aging_interval_seconds: int,
    dialect_name: str,
) -> ColumnElement[int]:
    """Mirror ``scheduler.policies.effective_priority`` in supported SQL dialects."""

    if aging_interval_seconds < 1:
        raise ValueError("aging_interval_seconds must be at least one")
    if dialect_name == "sqlite":
        elapsed_seconds = (func.julianday(literal(now)) - func.julianday(Task.queued_at)) * 86_400.0
    else:
        # PostgreSQL is the production dialect. SQL-standard EXTRACT also gives
        # useful behavior for compatible diagnostic databases.
        elapsed_seconds = func.extract("epoch", literal(now) - Task.queued_at)
    nonnegative_elapsed = case(
        (Task.queued_at.is_(None), 0.0),
        (elapsed_seconds > 0, elapsed_seconds),
        else_=0.0,
    )
    aging_steps = cast(func.floor(nonnegative_elapsed / aging_interval_seconds), Integer)
    uncapped_priority = Task.priority + aging_steps
    return case((uncapped_priority >= 100, 100), else_=uncapped_priority)


class SchedulingRepository:
    @staticmethod
    def candidate_query(
        scan_limit: int,
        *,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
        effective_priority_expression: ColumnElement[int] | None = None,
    ) -> Select[tuple[Task]]:
        """Return a bounded effective-priority lane.

        ``choose_candidates`` supplies the database-specific aging expression.
        The oldest-first fallback keeps this query independently useful to
        diagnostics and compile-only tests without hiding aging behind a raw
        priority cutoff.
        """

        conditions = _candidate_conditions(excluded_task_ids)
        policy_order: tuple[Any, ...]
        if effective_priority_expression is None:
            policy_order = (
                Task.queued_at,
                Task.priority.desc(),
            )
        else:
            policy_order = (effective_priority_expression.desc(),)
        return (
            select(Task)
            .where(*conditions)
            .order_by(
                Task.unschedulable_reason.is_not(None),
                *policy_order,
                Task.queue_order,
                Task.queued_at,
                Task.id,
            )
            .limit(scan_limit)
            .with_for_update(of=Task, skip_locked=True)
        )

    @staticmethod
    def priority_candidate_query(
        scan_limit: int,
        *,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
    ) -> Select[tuple[Task]]:
        """Return the bounded raw-priority lane used alongside the safety lanes."""

        return (
            select(Task)
            .where(*_candidate_conditions(excluded_task_ids))
            .order_by(
                Task.unschedulable_reason.is_not(None),
                Task.priority.desc(),
                Task.queue_order,
                Task.queued_at,
                Task.id,
            )
            .limit(scan_limit)
            .with_for_update(of=Task, skip_locked=True)
        )

    @staticmethod
    def project_fair_candidate_query(
        scan_limit: int,
        *,
        cluster_cpu_millicores: int,
        cluster_memory_mb: int,
        cluster_gpus: int,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
        effective_priority_expression: ColumnElement[int] | None = None,
    ) -> Select[tuple[Task]]:
        """Return one queue head per least-served project, with bounded locks."""

        project_order: tuple[Any, ...]
        if effective_priority_expression is None:
            project_order = (Task.priority.desc(),)
        else:
            project_order = (effective_priority_expression.desc(),)
        ranked = (
            select(
                Task.id.label("task_id"),
                func.row_number()
                .over(
                    partition_by=Task.project_id,
                    order_by=(
                        Task.unschedulable_reason.is_not(None),
                        *project_order,
                        Task.queue_order,
                        Task.queued_at,
                        Task.id,
                    ),
                )
                .label("project_rank"),
            )
            .where(*_candidate_conditions(excluded_task_ids))
            .subquery()
        )
        cpu_share = cast(func.coalesce(ProjectQuotaState.reserved_cpu_millicores, 0), Float) / max(
            1, cluster_cpu_millicores
        )
        memory_share = cast(func.coalesce(ProjectQuotaState.reserved_memory_mb, 0), Float) / max(
            1, cluster_memory_mb
        )
        gpu_share = (
            cast(func.coalesce(ProjectQuotaState.reserved_gpus, 0), Float) / cluster_gpus
            if cluster_gpus
            else case(
                (func.coalesce(ProjectQuotaState.reserved_gpus, 0) > 0, 1.0),
                else_=0.0,
            )
        )
        cpu_or_memory_share = case(
            (cpu_share >= memory_share, cpu_share),
            else_=memory_share,
        )
        dominant_share_expression = case(
            (cpu_or_memory_share >= gpu_share, cpu_or_memory_share),
            else_=gpu_share,
        )
        fair_order: tuple[Any, ...]
        if effective_priority_expression is None:
            fair_order = (Task.priority.desc(), dominant_share_expression)
        else:
            fair_order = (effective_priority_expression.desc(), dominant_share_expression)
        return (
            select(Task)
            .join(ranked, ranked.c.task_id == Task.id)
            .outerjoin(ProjectQuotaState, ProjectQuotaState.project_id == Task.project_id)
            .where(ranked.c.project_rank == 1)
            .order_by(
                Task.unschedulable_reason.is_not(None),
                *fair_order,
                Task.queue_order,
                Task.queued_at,
                Task.id,
            )
            .limit(scan_limit)
            .with_for_update(of=Task, skip_locked=True)
        )

    @staticmethod
    async def choose_candidates(
        session: AsyncSession,
        *,
        aging_interval_seconds: int,
        scan_limit: int = 128,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
    ) -> list[SchedulerCandidate]:
        """Build an exact policy order from three independently bounded lanes."""

        if scan_limit < 1:
            raise ValueError("scan_limit must be at least one")

        now = await database_utcnow(session)
        total_cpu, total_memory, total_gpus = (
            await session.execute(
                select(
                    func.coalesce(func.sum(Worker.cpu_allocatable_millicores), 0),
                    func.coalesce(func.sum(Worker.memory_allocatable_mb), 0),
                    func.coalesce(func.sum(Worker.gpu_count), 0),
                ).where(Worker.status == WorkerStatus.ONLINE)
            )
        ).one()
        cluster_cpu = int(total_cpu)
        cluster_memory = int(total_memory)
        cluster_gpus = int(total_gpus)
        dialect_name = session.get_bind().dialect.name
        effective_priority_expression = _effective_priority_expression(
            now=now,
            aging_interval_seconds=aging_interval_seconds,
            dialect_name=dialect_name,
        )

        queries = (
            SchedulingRepository.candidate_query(
                scan_limit,
                excluded_task_ids=excluded_task_ids,
                effective_priority_expression=effective_priority_expression,
            ),
            SchedulingRepository.priority_candidate_query(
                scan_limit, excluded_task_ids=excluded_task_ids
            ),
            SchedulingRepository.project_fair_candidate_query(
                scan_limit,
                cluster_cpu_millicores=cluster_cpu,
                cluster_memory_mb=cluster_memory,
                cluster_gpus=cluster_gpus,
                excluded_task_ids=excluded_task_ids,
                effective_priority_expression=effective_priority_expression,
            ),
        )
        tasks_by_id: dict[uuid.UUID, Task] = {}
        for query in queries:
            for task in await session.scalars(query):
                tasks_by_id[task.id] = task
        tasks = list(tasks_by_id.values())
        if not tasks:
            return []

        states = {
            state.project_id: state
            for state in await session.scalars(
                select(ProjectQuotaState).where(
                    ProjectQuotaState.project_id.in_({task.project_id for task in tasks})
                )
            )
        }

        candidates: list[SchedulerCandidate] = []
        for task in tasks:
            snapshot = task_snapshot(task)
            priority = effective_priority(
                snapshot, now=now, aging_interval_seconds=aging_interval_seconds
            )
            state = states.get(task.project_id)
            share = (
                dominant_share(
                    cpu_millicores=state.reserved_cpu_millicores,
                    memory_mb=state.reserved_memory_mb,
                    gpus=state.reserved_gpus,
                    cluster_cpu_millicores=cluster_cpu,
                    cluster_memory_mb=cluster_memory,
                    cluster_gpus=cluster_gpus,
                )
                if state is not None
                else 0.0
            )
            candidates.append(
                SchedulerCandidate(
                    task=task,
                    snapshot=snapshot,
                    effective_priority=priority,
                    project_dominant_share=share,
                )
            )
        candidates.sort(
            key=lambda item: (
                item.task.unschedulable_reason is not None,
                -item.effective_priority,
                item.project_dominant_share,
                item.snapshot.queue_order,
                item.snapshot.id,
            )
        )
        return candidates

    @staticmethod
    async def choose_next_candidate(
        session: AsyncSession,
        *,
        aging_interval_seconds: int,
        scan_limit: int = 128,
        excluded_task_ids: frozenset[uuid.UUID] = frozenset(),
    ) -> SchedulerCandidate | None:
        candidates = await SchedulingRepository.choose_candidates(
            session,
            aging_interval_seconds=aging_interval_seconds,
            scan_limit=scan_limit,
            excluded_task_ids=excluded_task_ids,
        )
        return candidates[0] if candidates else None

    @staticmethod
    async def worker_snapshots(session: AsyncSession) -> list[WorkerSnapshot]:
        workers = list(
            await session.scalars(
                select(Worker).where(Worker.status == WorkerStatus.ONLINE).order_by(Worker.id)
            )
        )
        if not workers:
            return []
        devices = list(
            await session.scalars(
                select(GPUDevice)
                .where(
                    GPUDevice.worker_id.in_([worker.id for worker in workers]),
                    GPUDevice.vendor == AcceleratorVendor.NVIDIA.value,
                    GPUDevice.accelerator_kind == AcceleratorKind.GPU.value,
                )
                .order_by(GPUDevice.worker_id, GPUDevice.memory_total_mb, GPUDevice.device_uuid)
            )
        )
        allocated = set(
            await session.scalars(
                select(ReservationGPUDevice.gpu_device_id).where(
                    ReservationGPUDevice.released_at.is_(None)
                )
            )
        )
        by_worker: dict[str, list[GPUDeviceSnapshot]] = {}
        for device in devices:
            by_worker.setdefault(device.worker_id, []).append(
                GPUDeviceSnapshot(
                    id=str(device.id),
                    uuid=device.device_uuid,
                    model=device.model,
                    memory_total_mb=device.memory_total_mb,
                    memory_free_mb=device.memory_free_mb,
                    healthy=device.health == "healthy",
                    allocated=device.id in allocated,
                )
            )
        return [worker_snapshot(worker, tuple(by_worker.get(worker.id, []))) for worker in workers]

    @staticmethod
    async def mark_unschedulable(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        reason: str,
    ) -> bool:
        """Persist an admission reason only while the task remains queued."""

        task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
        if task is None or task.status != TaskStatus.QUEUED or task.cancel_requested:
            return False
        if task.unschedulable_reason != reason:
            task.unschedulable_reason = reason
            task.version += 1
        return True

    @staticmethod
    async def place(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        worker_id: str,
        gpu_device_ids: tuple[str, ...],
        lease_seconds: float,
        cpu_price_per_hour: float,
        memory_price_per_gb_hour: float,
        gpu_price_per_hour: float,
    ) -> tuple[Task, uuid.UUID]:
        task = await session.scalar(
            select(Task)
            .where(Task.id == task_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        worker = await session.scalar(
            select(Worker)
            .where(Worker.id == worker_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        if task is None or task.status != TaskStatus.QUEUED or task.cancel_requested:
            raise PlacementConflict("task is no longer queued")
        if not await TaskRepository.dependencies_ready(session, task.id):
            raise PlacementConflict("task dependencies are not ready")
        if worker is None or worker.status != WorkerStatus.ONLINE or worker.overcommitted:
            raise PlacementConflict("worker is unavailable")
        if worker.running_tasks >= worker.concurrency:
            raise PlacementConflict("worker has no free execution slot")
        if worker.reserved_cpu * 1000 + task.cpu_millicores > worker.cpu_allocatable_millicores:
            raise PlacementConflict("worker CPU capacity changed")
        if worker.reserved_memory_mb + task.memory_limit_mb > worker.memory_allocatable_mb:
            raise PlacementConflict("worker memory capacity changed")

        requested_device_ids = tuple(uuid.UUID(value) for value in gpu_device_ids)
        devices: list[GPUDevice] = []
        if requested_device_ids:
            devices = list(
                await session.scalars(
                    select(GPUDevice)
                    .where(
                        GPUDevice.id.in_(requested_device_ids),
                        GPUDevice.worker_id == worker.id,
                        GPUDevice.health == "healthy",
                        GPUDevice.vendor == AcceleratorVendor.NVIDIA.value,
                        GPUDevice.accelerator_kind == AcceleratorKind.GPU.value,
                    )
                    .order_by(GPUDevice.id)
                    .execution_options(populate_existing=True)
                    .with_for_update()
                )
            )
            if len(devices) != task.gpu_count:
                raise PlacementConflict("GPU device inventory changed")
            active_allocations = int(
                await session.scalar(
                    select(func.count(ReservationGPUDevice.id)).where(
                        ReservationGPUDevice.gpu_device_id.in_(requested_device_ids),
                        ReservationGPUDevice.released_at.is_(None),
                    )
                )
                or 0
            )
            if active_allocations:
                raise PlacementConflict("a selected GPU was allocated concurrently")
        elif task.gpu_count:
            raise PlacementConflict("GPU task requires concrete device reservations")

        authoritative_worker = worker_snapshot(
            worker,
            tuple(
                GPUDeviceSnapshot(
                    id=str(device.id),
                    uuid=device.device_uuid,
                    model=device.model,
                    memory_total_mb=device.memory_total_mb,
                    memory_free_mb=device.memory_free_mb,
                    healthy=device.health == "healthy",
                    allocated=False,
                )
                for device in devices
            ),
        )
        rejection, _authoritative_device_ids = evaluate_snapshot(
            authoritative_worker,
            task_snapshot(task),
        )
        if rejection is not None:
            raise PlacementConflict(
                f"worker no longer satisfies task requirements: {rejection.value}"
            )

        now = await database_utcnow(session)
        previous_status: TaskStatus = task.status
        ensure_transition(task.status, TaskStatus.SCHEDULING)
        task.status = TaskStatus.SCHEDULING
        task.version += 1
        _record_task_event(
            session,
            task,
            previous_status=previous_status,
            event_type="task.scheduling",
            created_at=now,
        )
        previous_status = task.status
        ensure_transition(task.status, TaskStatus.ASSIGNED)
        _prepare_for_assignment(task)
        task.status = TaskStatus.ASSIGNED
        task.assigned_at = now
        execution_id = uuid.uuid4()
        task.worker_id = worker.id
        task.execution_id = execution_id
        task.lease_expires_at = now + timedelta(seconds=lease_seconds)
        task.gpu_device_ids = [device.device_uuid for device in devices]
        task.unschedulable_reason = None
        task.version += 1
        _record_task_event(
            session,
            task,
            previous_status=previous_status,
            event_type="task.assigned",
            created_at=now,
        )

        worker.running_tasks += 1
        worker.reserved_cpu += task.cpu_millicores / 1000
        worker.reserved_memory_mb += task.memory_limit_mb
        worker.reserved_gpus += task.gpu_count
        worker.version += 1

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
            allocation_authority=AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value,
            requested_vendor=(AcceleratorVendor.NVIDIA.value if task.gpu_count else None),
            requested_kind=(AcceleratorKind.GPU.value if task.gpu_count else None),
            observed_device_ids_json=(
                [device.device_uuid for device in devices] if devices else None
            ),
            observed_vendor=(AcceleratorVendor.NVIDIA.value if devices else None),
            observed_at=(now if devices else None),
            state="active",
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
        )

        OutboxRepository.add(
            session,
            aggregate_id=task.id,
            event_type="task.assigned",
            payload={
                "task_id": str(task.id),
                "worker_id": worker.id,
                "worker_session_id": str(worker.worker_session_id),
                "execution_id": str(execution_id),
                "gpu_device_ids": task.gpu_device_ids,
            },
            available_at=now,
        )
        return task, execution_id

    @staticmethod
    async def request_preemption(
        session: AsyncSession,
        *,
        candidate: SchedulerCandidate,
        workers: list[WorkerSnapshot],
        min_priority_delta: int,
    ) -> PreemptionDecision | None:
        """Request stop of the smallest deterministic lower-priority victim set.

        Resource reservations deliberately remain active here.  They are released
        only after the owning Worker has stopped the runtime object and commits the
        fenced ``preempted`` result.
        """

        existing = list(
            await session.scalars(
                select(PreemptionPlan)
                .where(
                    PreemptionPlan.incoming_task_id == candidate.task.id,
                    PreemptionPlan.state == "requested",
                )
                .order_by(PreemptionPlan.created_at, PreemptionPlan.id)
            )
        )
        if existing:
            return PreemptionDecision(
                incoming_task_id=candidate.task.id,
                worker_id=existing[0].worker_id,
                victim_task_ids=tuple(plan.victim_task_id for plan in existing),
                already_requested=True,
            )

        worker_by_id = {worker.id: worker for worker in workers}
        if not worker_by_id:
            return None
        maximum_victim_priority = candidate.effective_priority - min_priority_delta
        victims = list(
            await session.scalars(
                select(Task)
                .where(
                    Task.worker_id.in_(worker_by_id),
                    Task.status.in_(
                        {
                            TaskStatus.ASSIGNED,
                            TaskStatus.PREPARING,
                            TaskStatus.PULLING,
                            TaskStatus.STARTING,
                            TaskStatus.RUNNING,
                        }
                    ),
                    Task.preemptible.is_(True),
                    Task.cancel_requested.is_(False),
                    Task.priority <= maximum_victim_priority,
                )
                .order_by(Task.worker_id, Task.priority, Task.started_at.desc(), Task.id)
                .with_for_update(skip_locked=True)
            )
        )
        by_worker: dict[str, list[Task]] = {}
        for victim in victims:
            if victim.worker_id is not None:
                by_worker.setdefault(victim.worker_id, []).append(victim)

        feasible: list[tuple[tuple[int, int, str], WorkerSnapshot, tuple[Task, ...]]] = []
        for worker_id, worker_victims in by_worker.items():
            worker = worker_by_id[worker_id]
            simulated = worker
            selected: list[Task] = []
            freed_device_uuids: set[str] = set()
            for victim in worker_victims:
                selected.append(victim)
                freed_device_uuids.update(victim.gpu_device_ids or [])
                simulated = dataclass_replace(
                    simulated,
                    running_tasks=max(0, worker.running_tasks - len(selected)),
                    reserved_cpu_millicores=max(
                        0,
                        worker.reserved_cpu_millicores
                        - sum(item.cpu_millicores for item in selected),
                    ),
                    reserved_memory_mb=max(
                        0,
                        worker.reserved_memory_mb - sum(item.memory_limit_mb for item in selected),
                    ),
                    gpu_devices=tuple(
                        dataclass_replace(device, allocated=False)
                        if device.uuid in freed_device_uuids
                        else device
                        for device in worker.gpu_devices
                    ),
                )
                reason, _device_ids = evaluate_snapshot(simulated, candidate.snapshot)
                if reason is None:
                    score = (
                        sum(item.priority for item in selected),
                        len(selected),
                        worker.id,
                    )
                    feasible.append((score, worker, tuple(selected)))
                    break
        if not feasible:
            return None

        feasible.sort(key=lambda item: item[0])
        _score, worker, selected_victims = feasible[0]
        now = await database_utcnow(session)
        for victim in selected_victims:
            previous_status = victim.status
            ensure_transition(victim.status, TaskStatus.PREEMPTING)
            victim.status = TaskStatus.PREEMPTING
            victim.cancel_requested = True
            victim.preemption_count += 1
            victim.version += 1
            _record_task_event(
                session,
                victim,
                previous_status=previous_status,
                event_type="task.preemption_requested",
                created_at=now,
                details={"incoming_task_id": str(candidate.task.id)},
            )
            session.add(
                PreemptionPlan(
                    incoming_task_id=candidate.task.id,
                    victim_task_id=victim.id,
                    worker_id=worker.id,
                    state="requested",
                    created_at=now,
                )
            )
            OutboxRepository.add(
                session,
                aggregate_id=victim.id,
                event_type="task.preemption_requested",
                payload={
                    "task_id": str(victim.id),
                    "incoming_task_id": str(candidate.task.id),
                    "worker_id": worker.id,
                },
                available_at=now,
            )
        candidate.task.unschedulable_reason = "preemption_in_progress"
        candidate.task.version += 1
        return PreemptionDecision(
            incoming_task_id=candidate.task.id,
            worker_id=worker.id,
            victim_task_ids=tuple(victim.id for victim in selected_victims),
        )

    @staticmethod
    def record_attempt(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        scheduler_id: str,
        worker_id: str | None,
        policy: str,
        outcome: str,
        reason: str | None,
        effective_priority_value: int,
        detail: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        session.add(
            PlacementAttempt(
                task_id=task_id,
                scheduler_id=scheduler_id,
                worker_id=worker_id,
                policy=policy,
                outcome=outcome,
                reason=reason,
                detail=detail,
                effective_priority=effective_priority_value,
                created_at=created_at or datetime.now().astimezone(),
            )
        )


def task_snapshot(task: Task) -> TaskSnapshot:
    return TaskSnapshot(
        id=str(task.id),
        project_id=str(task.project_id),
        status=task.status,
        runtime_type=task.runtime_type.value,
        cpu_millicores=task.cpu_millicores,
        memory_mb=task.memory_limit_mb,
        gpu_count=task.gpu_count,
        gpu_memory_mb=task.gpu_memory_mb,
        gpu_model=task.gpu_model,
        labels=dict(task.labels),
        tolerations=tuple(dict(item) for item in task.tolerations),
        priority=task.priority,
        queued_at=task.queued_at,
        queue_order=task.queue_order,
    )


def worker_snapshot(worker: Worker, devices: tuple[GPUDeviceSnapshot, ...]) -> WorkerSnapshot:
    return WorkerSnapshot(
        id=worker.id,
        status=worker.status,
        runtime_types=frozenset(worker.runtime_types),
        running_tasks=worker.running_tasks,
        concurrency=worker.concurrency,
        cpu_allocatable_millicores=worker.cpu_allocatable_millicores,
        reserved_cpu_millicores=round(worker.reserved_cpu * 1000),
        memory_allocatable_mb=worker.memory_allocatable_mb,
        reserved_memory_mb=worker.reserved_memory_mb,
        labels=dict(worker.labels),
        taints=tuple(dict(item) for item in worker.taints),
        gpu_devices=devices,
        overcommitted=worker.overcommitted,
    )


def _record_task_event(
    session: AsyncSession,
    task: Task,
    *,
    previous_status: TaskStatus,
    event_type: str,
    created_at: datetime,
    details: dict[str, object] | None = None,
) -> None:
    session.add(
        TaskEvent(
            project_id=task.project_id,
            task_id=task.id,
            event_type=event_type,
            sequence=task.version,
            from_status=previous_status.value,
            status=task.status.value,
            execution_id=task.execution_id,
            worker_id=task.worker_id,
            details=details or {},
            created_at=created_at,
        )
    )
