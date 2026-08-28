import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import AcceleratorVendor, AllocationAuthority
from models.scheduling import GPUDevice, ReservationGPUDevice, ResourceReservation
from models.task import Task
from models.usage import TaskExecution, UsageLedger
from models.worker import Worker
from repositories.quotas import QuotaRepository


class ResourceInvariantViolation(RuntimeError):
    """Persistent resource accounting no longer matches its reservation truth."""


class AllocationObservationConflict(RuntimeError):
    """An observed allocation conflicts with its persisted request or authority."""


class ReservationRepository:
    @staticmethod
    async def get_active_for_execution(
        session: AsyncSession, execution_id: uuid.UUID, *, for_update: bool = False
    ) -> ResourceReservation | None:
        query = select(ResourceReservation).where(
            ResourceReservation.execution_id == execution_id,
            ResourceReservation.released_at.is_(None),
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def assert_exact_device_binding(
        session: AsyncSession,
        reservation: ResourceReservation,
    ) -> None:
        """Verify the concrete binding owned by a control-plane reservation."""

        if (
            reservation.gpu_count == 0
            or reservation.legacy_unbound
            or reservation.allocation_authority
            != AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value
        ):
            return
        rows = list(
            (
                await session.execute(
                    select(ReservationGPUDevice, GPUDevice)
                    .join(GPUDevice, GPUDevice.id == ReservationGPUDevice.gpu_device_id)
                    .where(
                        ReservationGPUDevice.reservation_id == reservation.id,
                        ReservationGPUDevice.released_at.is_(None),
                    )
                    .order_by(GPUDevice.device_uuid)
                )
            ).all()
        )
        if len(rows) != reservation.gpu_count:
            raise ResourceInvariantViolation(
                f"exact-device reservation {reservation.id} has {len(rows)} active bindings; "
                f"expected {reservation.gpu_count}"
            )
        device_ids = [device.device_uuid for _link, device in rows]
        vendors = {device.vendor for _link, device in rows}
        if vendors != {reservation.requested_vendor}:
            raise ResourceInvariantViolation(
                f"exact-device reservation {reservation.id} binding vendor does not match request"
            )
        if sorted(reservation.observed_device_ids_json or []) != sorted(device_ids):
            raise ResourceInvariantViolation(
                f"exact-device reservation {reservation.id} observation does not match bindings"
            )

    @staticmethod
    async def record_observed_allocation(
        session: AsyncSession,
        *,
        execution_id: uuid.UUID,
        vendor: AcceleratorVendor | str,
        device_ids: tuple[str, ...],
        observed_at: datetime,
    ) -> bool:
        """Persist a Device Plugin observation once in reservation and execution."""

        try:
            normalized_vendor = AcceleratorVendor(vendor).value
        except ValueError as exc:
            raise AllocationObservationConflict("unsupported observed accelerator vendor") from exc
        normalized_ids = tuple(device_id.strip() for device_id in device_ids)
        if any(not device_id for device_id in normalized_ids):
            raise AllocationObservationConflict("observed device IDs must not be blank")
        if len(normalized_ids) != len(set(normalized_ids)):
            raise AllocationObservationConflict("observed device IDs must be unique")

        reservation = await ReservationRepository.get_active_for_execution(
            session, execution_id, for_update=True
        )
        if reservation is None:
            raise AllocationObservationConflict("execution has no active reservation")
        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        if execution is None:
            raise ResourceInvariantViolation(
                f"execution {execution_id} is missing for active reservation"
            )
        authority = AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value
        if (
            reservation.allocation_authority != authority
            or execution.allocation_authority != authority
        ):
            raise AllocationObservationConflict(
                "only kubernetes_device_plugin allocations accept deferred observations"
            )
        if reservation.requested_vendor != normalized_vendor:
            raise AllocationObservationConflict("observed vendor does not match requested vendor")
        if len(normalized_ids) != reservation.gpu_count:
            raise AllocationObservationConflict(
                "observed device count does not match the accelerator reservation"
            )
        exact_links = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.reservation_id == reservation.id
                )
            )
            or 0
        )
        if exact_links:
            raise ResourceInvariantViolation(
                "kubernetes_device_plugin reservation must not own exact-device bindings"
            )

        expected_ids = list(normalized_ids)
        if reservation.observed_at is not None or execution.observed_at is not None:
            if (
                reservation.observed_vendor == normalized_vendor
                and reservation.observed_device_ids_json == expected_ids
                and execution.observed_vendor == normalized_vendor
                and execution.observed_device_ids_json == expected_ids
            ):
                return False
            raise AllocationObservationConflict(
                "allocation was already observed with different data"
            )

        reservation.observed_device_ids_json = expected_ids
        reservation.observed_vendor = normalized_vendor
        reservation.observed_at = observed_at
        reservation.version += 1
        execution.observed_device_ids_json = expected_ids
        execution.observed_vendor = normalized_vendor
        execution.observed_at = observed_at
        return True

    @staticmethod
    async def release_and_settle(
        session: AsyncSession,
        *,
        task: Task,
        execution_id: uuid.UUID,
        final_status: str,
        now: datetime,
        release_reason: str,
    ) -> bool:
        """Release exactly once and write one immutable usage ledger entry."""

        reservation = await ReservationRepository.get_active_for_execution(
            session, execution_id, for_update=True
        )
        if reservation is None:
            return False
        worker = await session.get(Worker, reservation.worker_id, with_for_update=True)
        if worker is not None:
            expected_cpu = reservation.cpu_millicores / 1000
            if (
                worker.running_tasks < 1
                or worker.reserved_cpu + 1e-9 < expected_cpu
                or worker.reserved_memory_mb < reservation.memory_mb
                or worker.reserved_gpus < reservation.gpu_count
            ):
                raise ResourceInvariantViolation(
                    f"worker {worker.id} aggregate capacity is below active reservation"
                )
            worker.running_tasks -= 1
            worker.reserved_cpu -= expected_cpu
            worker.reserved_memory_mb -= reservation.memory_mb
            worker.reserved_gpus -= reservation.gpu_count
            worker.version += 1

        links = list(
            await session.scalars(
                select(ReservationGPUDevice)
                .where(
                    ReservationGPUDevice.reservation_id == reservation.id,
                    ReservationGPUDevice.released_at.is_(None),
                )
                .with_for_update()
            )
        )
        for link in links:
            link.released_at = now
        reservation.state = "released"
        reservation.released_at = now
        reservation.release_reason = release_reason
        reservation.version += 1

        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        if execution is None:
            raise ResourceInvariantViolation(
                f"execution {execution_id} is missing for active reservation"
            )
        execution.status = final_status
        execution.finished_at = now
        if task.started_at is not None:
            execution.started_at = task.started_at
            existing_ledger = await session.scalar(
                select(UsageLedger.id).where(UsageLedger.execution_id == execution_id)
            )
            if existing_ledger is None:
                started_at = task.started_at
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=UTC)
                finished_at = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
                duration = Decimal(str(max(0.0, (finished_at - started_at).total_seconds())))
                cpu_seconds = duration * Decimal(execution.cpu_millicores) / Decimal(1000)
                memory_gb_seconds = duration * Decimal(execution.memory_mb) / Decimal(1024)
                gpu_seconds = duration * Decimal(execution.gpu_count)
                cost = (
                    cpu_seconds * execution.cpu_price_per_hour / Decimal(3600)
                    + memory_gb_seconds * execution.memory_price_per_gb_hour / Decimal(3600)
                    + gpu_seconds * execution.gpu_price_per_hour / Decimal(3600)
                )
                session.add(
                    UsageLedger(
                        project_id=reservation.project_id,
                        task_id=task.id,
                        execution_id=execution_id,
                        started_at=task.started_at,
                        finished_at=now,
                        cpu_seconds=cpu_seconds,
                        memory_gb_seconds=memory_gb_seconds,
                        gpu_seconds=gpu_seconds,
                        gpu_model=execution.gpu_model,
                        cost=cost,
                    )
                )
                task.estimated_cost = float(cost)
                await QuotaRepository.release(
                    session,
                    project_id=reservation.project_id,
                    cpu_millicores=reservation.cpu_millicores,
                    memory_mb=reservation.memory_mb,
                    gpu_count=reservation.gpu_count,
                    accelerator_vendor=reservation.requested_vendor,
                    settled_cost=cost,
                    reservation_accounting_date=reservation.created_at.date(),
                )
            else:
                await QuotaRepository.release(
                    session,
                    project_id=reservation.project_id,
                    cpu_millicores=reservation.cpu_millicores,
                    memory_mb=reservation.memory_mb,
                    gpu_count=reservation.gpu_count,
                    accelerator_vendor=reservation.requested_vendor,
                    reservation_accounting_date=reservation.created_at.date(),
                )
        else:
            await QuotaRepository.release(
                session,
                project_id=reservation.project_id,
                cpu_millicores=reservation.cpu_millicores,
                memory_mb=reservation.memory_mb,
                gpu_count=reservation.gpu_count,
                accelerator_vendor=reservation.requested_vendor,
                reservation_accounting_date=reservation.created_at.date(),
            )
        return True
