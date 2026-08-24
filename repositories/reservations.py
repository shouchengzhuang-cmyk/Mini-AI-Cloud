import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.scheduling import ReservationGPUDevice, ResourceReservation
from models.task import Task
from models.usage import TaskExecution, UsageLedger
from models.worker import Worker
from repositories.quotas import QuotaRepository


class ResourceInvariantViolation(RuntimeError):
    """Persistent resource accounting no longer matches its reservation truth."""


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
                    reservation_accounting_date=reservation.created_at.date(),
                )
        else:
            await QuotaRepository.release(
                session,
                project_id=reservation.project_id,
                cpu_millicores=reservation.cpu_millicores,
                memory_mb=reservation.memory_mb,
                gpu_count=reservation.gpu_count,
                reservation_accounting_date=reservation.created_at.date(),
            )
        return True
