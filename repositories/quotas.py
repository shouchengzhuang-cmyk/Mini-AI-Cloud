import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import AcceleratorVendor
from core.rbac import ProjectStatus
from models.identity import Project
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.clock import database_utcnow

ZERO_COST = Decimal("0")


class QuotaNotFoundError(LookupError):
    pass


class QuotaInvariantViolation(RuntimeError):
    pass


class QuotaExceededError(RuntimeError):
    def __init__(self, resource: str, *, limit: Decimal | int, requested: Decimal | int) -> None:
        super().__init__(f"project quota exceeded for {resource}: {requested} > {limit}")
        self.resource = resource
        self.limit = limit
        self.requested = requested


@dataclass(frozen=True, slots=True)
class QuotaSnapshot:
    quota: ProjectQuota
    state: ProjectQuotaState


class QuotaRepository:
    """Mutate project counters under row locks inside the caller's transaction.

    Limit and invariant checks run before mutation. Callers must not clamp or
    duplicate these counters; a raised exception intentionally aborts the
    surrounding task transition.
    """

    @staticmethod
    async def initialize(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> QuotaSnapshot:
        project = await session.scalar(
            select(Project).where(Project.id == project_id).with_for_update()
        )
        if project is None or project.status != ProjectStatus.ACTIVE:
            raise QuotaNotFoundError("active project does not exist")
        quota = await session.get(ProjectQuota, project_id, with_for_update=True)
        state = await session.get(ProjectQuotaState, project_id, with_for_update=True)
        now = await database_utcnow(session)
        if quota is None:
            quota = ProjectQuota(project_id=project_id, updated_at=now)
            session.add(quota)
        if state is None:
            state = ProjectQuotaState(
                project_id=project_id,
                accounting_date=now.date(),
                updated_at=now,
            )
            session.add(state)
        await session.flush()
        _assert_state_nonnegative(state)
        return QuotaSnapshot(quota=quota, state=state)

    @staticmethod
    async def get_locked(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        reset_daily: bool = True,
    ) -> QuotaSnapshot:
        quota = await session.get(ProjectQuota, project_id, with_for_update=True)
        state = await session.get(ProjectQuotaState, project_id, with_for_update=True)
        if quota is None or state is None:
            raise QuotaNotFoundError("project quota state does not exist")
        _assert_state_nonnegative(state)
        if reset_daily:
            now = await database_utcnow(session)
            if _roll_daily_state(state, now.date()):
                state.updated_at = now
        return QuotaSnapshot(quota=quota, state=state)

    @staticmethod
    async def replace(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        max_queued_tasks: int | None,
        max_running_tasks: int | None,
        max_cpu_millicores: int | None,
        max_memory_mb: int | None,
        max_gpus: int | None,
        max_services: int | None,
        max_service_replicas: int | None,
        max_artifact_bytes: int | None,
        daily_cost_limit: Decimal | None,
        max_nvidia_gpus: int | None = None,
        max_ascend_npus: int | None = None,
    ) -> QuotaSnapshot:
        values: dict[str, Decimal | int | None] = {
            "max_queued_tasks": max_queued_tasks,
            "max_running_tasks": max_running_tasks,
            "max_cpu_millicores": max_cpu_millicores,
            "max_memory_mb": max_memory_mb,
            "max_gpus": max_gpus,
            "max_nvidia_gpus": max_nvidia_gpus,
            "max_ascend_npus": max_ascend_npus,
            "max_services": max_services,
            "max_service_replicas": max_service_replicas,
            "max_artifact_bytes": max_artifact_bytes,
            "daily_cost_limit": daily_cost_limit,
        }
        for field, value in values.items():
            if value is not None and value < 0:
                raise ValueError(f"{field} must be non-negative")
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        quota = snapshot.quota
        state = snapshot.state
        _check_limit("queued_tasks", max_queued_tasks, state.queued_tasks)
        _check_limit(
            "running_tasks",
            max_running_tasks,
            state.running_tasks + state.service_replicas,
        )
        _check_limit(
            "cpu_millicores",
            max_cpu_millicores,
            state.reserved_cpu_millicores + state.service_reserved_cpu_millicores,
        )
        _check_limit(
            "memory_mb",
            max_memory_mb,
            state.reserved_memory_mb + state.service_reserved_memory_mb,
        )
        _check_limit("gpus", max_gpus, state.reserved_gpus + state.service_reserved_gpus)
        _check_limit(
            "nvidia_gpus",
            max_nvidia_gpus,
            state.reserved_nvidia_gpus + state.service_reserved_nvidia_gpus,
        )
        _check_limit(
            "ascend_npus",
            max_ascend_npus,
            state.reserved_ascend_npus + state.service_reserved_ascend_npus,
        )
        _check_limit("services", max_services, state.service_count)
        _check_limit("service_replicas", max_service_replicas, state.service_replicas)
        _check_limit("artifact_bytes", max_artifact_bytes, state.artifact_bytes)
        _check_limit(
            "daily_cost",
            daily_cost_limit,
            state.daily_reserved_cost + state.daily_settled_cost,
        )
        for field, value in values.items():
            setattr(quota, field, value)
        quota.version += 1
        quota.updated_at = await database_utcnow(session)
        return snapshot

    @staticmethod
    async def admit_queued(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> ProjectQuotaState:
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        prospective = snapshot.state.queued_tasks + 1
        _check_limit("queued_tasks", snapshot.quota.max_queued_tasks, prospective)
        snapshot.state.queued_tasks = prospective
        await _touch_state(session, snapshot.state)
        return snapshot.state

    @staticmethod
    def ensure_task_can_fit(
        snapshot: QuotaSnapshot,
        *,
        cpu_millicores: int,
        memory_mb: int,
        gpu_count: int,
        accelerator_vendor: AcceleratorVendor | str | None = None,
        accelerator_vendors: Sequence[AcceleratorVendor | str] | None = None,
    ) -> None:
        """Reject a task that can never fit inside its project's hard limits."""

        _validate_reservation(cpu_millicores, memory_mb, gpu_count, ZERO_COST)
        quota = snapshot.quota
        _check_limit("running_tasks", quota.max_running_tasks, 1)
        _check_limit("cpu_millicores", quota.max_cpu_millicores, cpu_millicores)
        _check_limit("memory_mb", quota.max_memory_mb, memory_mb)
        _check_limit("gpus", quota.max_gpus, gpu_count)
        if accelerator_vendors is not None:
            if accelerator_vendor is not None:
                raise ValueError("pass accelerator_vendor or accelerator_vendors, not both")
            vendors = tuple(
                _normalize_accelerator_vendor(value, gpu_count=gpu_count)
                for value in accelerator_vendors
            )
            if not vendors or any(vendor is None for vendor in vendors):
                raise ValueError("accelerator_vendors must contain supported vendors")
            limits = {
                AcceleratorVendor.NVIDIA: quota.max_nvidia_gpus,
                AcceleratorVendor.HUAWEI_ASCEND: quota.max_ascend_npus,
            }
            candidate_limits = [limits[vendor] for vendor in vendors if vendor is not None]
            if not any(limit is None or gpu_count <= limit for limit in candidate_limits):
                raise QuotaExceededError(
                    "vendor_accelerators",
                    limit=max(int(limit or 0) for limit in candidate_limits),
                    requested=gpu_count,
                )
            return
        vendor = _normalize_accelerator_vendor(accelerator_vendor, gpu_count=gpu_count)
        if vendor == AcceleratorVendor.NVIDIA:
            _check_limit("nvidia_gpus", quota.max_nvidia_gpus, gpu_count)
        elif vendor == AcceleratorVendor.HUAWEI_ASCEND:
            _check_limit("ascend_npus", quota.max_ascend_npus, gpu_count)

    @staticmethod
    async def release_queued(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> ProjectQuotaState:
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        if snapshot.state.queued_tasks < 1:
            raise QuotaInvariantViolation("queued task count would become negative")
        snapshot.state.queued_tasks -= 1
        await _touch_state(session, snapshot.state)
        return snapshot.state

    @staticmethod
    async def reserve_execution(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
        gpu_count: int,
        estimated_cost: Decimal = ZERO_COST,
        accelerator_vendor: AcceleratorVendor | str | None = None,
    ) -> ProjectQuotaState:
        """Atomically move one admitted task from queued to running."""

        _validate_reservation(cpu_millicores, memory_mb, gpu_count, estimated_cost)
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        quota = snapshot.quota
        state = snapshot.state
        vendor = _normalize_accelerator_vendor(accelerator_vendor, gpu_count=gpu_count)
        if state.queued_tasks < 1:
            raise QuotaInvariantViolation("cannot reserve an execution without a queued task")
        _check_limit(
            "running_tasks",
            quota.max_running_tasks,
            state.running_tasks + state.service_replicas + 1,
        )
        _check_limit(
            "cpu_millicores",
            quota.max_cpu_millicores,
            state.reserved_cpu_millicores + state.service_reserved_cpu_millicores + cpu_millicores,
        )
        _check_limit(
            "memory_mb",
            quota.max_memory_mb,
            state.reserved_memory_mb + state.service_reserved_memory_mb + memory_mb,
        )
        _check_limit(
            "gpus",
            quota.max_gpus,
            state.reserved_gpus + state.service_reserved_gpus + gpu_count,
        )
        prospective_nvidia = state.reserved_nvidia_gpus + (
            gpu_count if vendor == AcceleratorVendor.NVIDIA else 0
        )
        prospective_ascend = state.reserved_ascend_npus + (
            gpu_count if vendor == AcceleratorVendor.HUAWEI_ASCEND else 0
        )
        _check_limit(
            "nvidia_gpus",
            quota.max_nvidia_gpus,
            prospective_nvidia + state.service_reserved_nvidia_gpus,
        )
        _check_limit(
            "ascend_npus",
            quota.max_ascend_npus,
            prospective_ascend + state.service_reserved_ascend_npus,
        )
        _check_limit(
            "daily_cost",
            quota.daily_cost_limit,
            state.daily_settled_cost + state.daily_reserved_cost + estimated_cost,
        )
        state.queued_tasks -= 1
        state.running_tasks += 1
        state.reserved_cpu_millicores += cpu_millicores
        state.reserved_memory_mb += memory_mb
        state.reserved_nvidia_gpus = prospective_nvidia
        state.reserved_ascend_npus = prospective_ascend
        state.reserved_gpus = prospective_nvidia + prospective_ascend
        state.daily_reserved_cost += estimated_cost
        await _touch_state(session, state)
        return state

    @staticmethod
    async def replace_service_commitment(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        current_replicas: int,
        desired_replicas: int,
        cpu_millicores: int,
        memory_mb: int,
        gpu_count: int,
        accelerator_vendor: AcceleratorVendor | str | None = None,
    ) -> ProjectQuotaState:
        """Atomically replace one service's desired resource commitment.

        Service counters are derived from desired state, not reconciled replica
        rows. The caller must hold the service row lock when replacing an
        existing commitment so retries and concurrent scale requests supply one
        authoritative ``current_replicas`` value.
        """

        _validate_service_commitment(
            current_replicas=current_replicas,
            desired_replicas=desired_replicas,
            cpu_millicores=cpu_millicores,
            memory_mb=memory_mb,
            gpu_count=gpu_count,
        )
        try:
            snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        except QuotaNotFoundError:
            snapshot = await QuotaRepository.initialize(session, project_id=project_id)
        quota = snapshot.quota
        state = snapshot.state
        vendor = _normalize_accelerator_vendor(accelerator_vendor, gpu_count=gpu_count)
        replica_delta = desired_replicas - current_replicas
        service_delta = int(desired_replicas > 0) - int(current_replicas > 0)
        prospective_service_count = state.service_count + service_delta
        prospective_replicas = state.service_replicas + replica_delta
        prospective_cpu = state.service_reserved_cpu_millicores + (cpu_millicores * replica_delta)
        prospective_memory = state.service_reserved_memory_mb + memory_mb * replica_delta
        accelerator_delta = gpu_count * replica_delta
        prospective_nvidia = state.service_reserved_nvidia_gpus + (
            accelerator_delta if vendor == AcceleratorVendor.NVIDIA else 0
        )
        prospective_ascend = state.service_reserved_ascend_npus + (
            accelerator_delta if vendor == AcceleratorVendor.HUAWEI_ASCEND else 0
        )
        prospective_gpus = prospective_nvidia + prospective_ascend
        if (
            min(
                prospective_service_count,
                prospective_replicas,
                prospective_cpu,
                prospective_memory,
                prospective_gpus,
                prospective_nvidia,
                prospective_ascend,
            )
            < 0
        ):
            raise QuotaInvariantViolation(
                "service quota state is below the current desired commitment"
            )

        _check_limit("services", quota.max_services, prospective_service_count)
        _check_limit("service_replicas", quota.max_service_replicas, prospective_replicas)
        _check_limit(
            "running_tasks",
            quota.max_running_tasks,
            state.running_tasks + prospective_replicas,
        )
        _check_limit(
            "cpu_millicores",
            quota.max_cpu_millicores,
            state.reserved_cpu_millicores + prospective_cpu,
        )
        _check_limit(
            "memory_mb",
            quota.max_memory_mb,
            state.reserved_memory_mb + prospective_memory,
        )
        _check_limit("gpus", quota.max_gpus, state.reserved_gpus + prospective_gpus)
        _check_limit(
            "nvidia_gpus",
            quota.max_nvidia_gpus,
            state.reserved_nvidia_gpus + prospective_nvidia,
        )
        _check_limit(
            "ascend_npus",
            quota.max_ascend_npus,
            state.reserved_ascend_npus + prospective_ascend,
        )

        if replica_delta == 0:
            return state
        state.service_count = prospective_service_count
        state.service_replicas = prospective_replicas
        state.service_reserved_cpu_millicores = prospective_cpu
        state.service_reserved_memory_mb = prospective_memory
        state.service_reserved_gpus = prospective_gpus
        state.service_reserved_nvidia_gpus = prospective_nvidia
        state.service_reserved_ascend_npus = prospective_ascend
        await _touch_state(session, state)
        return state

    @staticmethod
    async def release(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
        gpu_count: int,
        reserved_cost: Decimal = ZERO_COST,
        settled_cost: Decimal = ZERO_COST,
        reservation_accounting_date: date | None = None,
        accelerator_vendor: AcceleratorVendor | str | None = None,
    ) -> ProjectQuotaState:
        """Release one execution, raising instead of hiding duplicate release.

        Pass the reservation's creation date when reserving a non-zero daily
        cost so an execution crossing the database-accounting day does not
        subtract yesterday's reservation from today's counters.
        """

        _validate_reservation(cpu_millicores, memory_mb, gpu_count, reserved_cost)
        if settled_cost < 0:
            raise ValueError("settled_cost must be non-negative")
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        state = snapshot.state
        vendor = _normalize_accelerator_vendor(accelerator_vendor, gpu_count=gpu_count)
        typed_reserved = 0
        if vendor == AcceleratorVendor.NVIDIA:
            typed_reserved = state.reserved_nvidia_gpus
        elif vendor == AcceleratorVendor.HUAWEI_ASCEND:
            typed_reserved = state.reserved_ascend_npus
        if (
            state.running_tasks < 1
            or state.reserved_cpu_millicores < cpu_millicores
            or state.reserved_memory_mb < memory_mb
            or state.reserved_gpus < gpu_count
            or typed_reserved < gpu_count
        ):
            raise QuotaInvariantViolation("project quota state is below the execution reservation")
        release_cost_today = (
            reservation_accounting_date is None
            or reservation_accounting_date == state.accounting_date
        )
        if release_cost_today and state.daily_reserved_cost < reserved_cost:
            raise QuotaInvariantViolation("daily reserved cost would become negative")
        state.running_tasks -= 1
        state.reserved_cpu_millicores -= cpu_millicores
        state.reserved_memory_mb -= memory_mb
        if vendor == AcceleratorVendor.NVIDIA:
            state.reserved_nvidia_gpus -= gpu_count
        elif vendor == AcceleratorVendor.HUAWEI_ASCEND:
            state.reserved_ascend_npus -= gpu_count
        state.reserved_gpus = state.reserved_nvidia_gpus + state.reserved_ascend_npus
        if release_cost_today:
            state.daily_reserved_cost -= reserved_cost
        state.daily_settled_cost += settled_cost
        await _touch_state(session, state)
        return state


def _check_limit(
    resource: str,
    limit: Decimal | int | None,
    requested: Decimal | int,
) -> None:
    if limit is not None and requested > limit:
        raise QuotaExceededError(resource, limit=limit, requested=requested)


def _validate_reservation(
    cpu_millicores: int,
    memory_mb: int,
    gpu_count: int,
    cost: Decimal,
) -> None:
    if cpu_millicores < 0 or memory_mb < 0 or gpu_count < 0 or cost < 0:
        raise ValueError("reservation resources and cost must be non-negative")


def _assert_state_nonnegative(state: ProjectQuotaState) -> None:
    numeric_values: dict[str, Decimal | int] = {
        "queued_tasks": state.queued_tasks,
        "running_tasks": state.running_tasks,
        "reserved_cpu_millicores": state.reserved_cpu_millicores,
        "reserved_memory_mb": state.reserved_memory_mb,
        "reserved_gpus": state.reserved_gpus,
        "reserved_nvidia_gpus": state.reserved_nvidia_gpus,
        "reserved_ascend_npus": state.reserved_ascend_npus,
        "service_count": state.service_count,
        "service_replicas": state.service_replicas,
        "service_reserved_cpu_millicores": state.service_reserved_cpu_millicores,
        "service_reserved_memory_mb": state.service_reserved_memory_mb,
        "service_reserved_gpus": state.service_reserved_gpus,
        "service_reserved_nvidia_gpus": state.service_reserved_nvidia_gpus,
        "service_reserved_ascend_npus": state.service_reserved_ascend_npus,
        "artifact_bytes": state.artifact_bytes,
        "daily_reserved_cost": state.daily_reserved_cost,
        "daily_settled_cost": state.daily_settled_cost,
    }
    invalid = [field for field, value in numeric_values.items() if value < 0]
    if invalid:
        raise QuotaInvariantViolation(
            f"project quota state has negative counters: {', '.join(invalid)}"
        )
    if state.reserved_gpus != state.reserved_nvidia_gpus + state.reserved_ascend_npus:
        raise QuotaInvariantViolation("task accelerator aggregate does not match typed counters")
    if (
        state.service_reserved_gpus
        != state.service_reserved_nvidia_gpus + state.service_reserved_ascend_npus
    ):
        raise QuotaInvariantViolation("service accelerator aggregate does not match typed counters")


def _normalize_accelerator_vendor(
    value: AcceleratorVendor | str | None,
    *,
    gpu_count: int,
) -> AcceleratorVendor | None:
    if gpu_count == 0:
        return None
    if value is None:
        return AcceleratorVendor.NVIDIA
    try:
        return AcceleratorVendor(value)
    except ValueError as exc:
        raise ValueError(f"unsupported accelerator vendor: {value}") from exc


def _roll_daily_state(state: ProjectQuotaState, current_date: date) -> bool:
    if state.accounting_date != current_date:
        state.accounting_date = current_date
        state.daily_reserved_cost = ZERO_COST
        state.daily_settled_cost = ZERO_COST
        state.version += 1
        return True
    return False


async def _touch_state(session: AsyncSession, state: ProjectQuotaState) -> None:
    _assert_state_nonnegative(state)
    state.version += 1
    state.updated_at = await database_utcnow(session)


def _validate_service_commitment(
    *,
    current_replicas: int,
    desired_replicas: int,
    cpu_millicores: int,
    memory_mb: int,
    gpu_count: int,
) -> None:
    if (
        current_replicas < 0
        or desired_replicas < 0
        or cpu_millicores < 0
        or memory_mb < 0
        or gpu_count < 0
    ):
        raise ValueError("service replicas and resources must be non-negative")
