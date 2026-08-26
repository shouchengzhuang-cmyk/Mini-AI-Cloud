from datetime import UTC, datetime

from sqlalchemy import func, select

from core.config import Settings
from core.database import Database
from core.enums import TaskStatus, WorkerStatus
from core.logging import get_logger
from core.metrics import (
    OUTBOX_OLDEST_AGE,
    OUTBOX_PENDING,
    SERVICE_REPLICAS,
    SERVICES_READY,
    TASKS_QUEUED,
    TASKS_RUNNING,
    WORKER_ALLOCATED,
    WORKER_ALLOCATED_CPU,
    WORKER_ALLOCATED_GPU,
    WORKER_ALLOCATED_MEMORY,
    WORKER_CAPACITY_CPU,
    WORKER_CAPACITY_GPU,
    WORKER_CAPACITY_MEMORY,
    WORKERS_ONLINE,
)
from models.outbox import OutboxEvent
from models.service import ModelService, ReplicaStatus, ServiceReplica, ServiceStatus
from models.task import Task
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.outbox import OutboxRepository
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository


class Reaper:
    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.logger = get_logger("reaper")

    async def recover_startup(self) -> int:
        async with self.database.session() as session, session.begin():
            await TaskRepository.release_due_retries(session, self.settings.batch_size)
            await TaskRepository.resolve_dependency_readiness(
                session,
                limit=self.settings.batch_size,
            )
            queued_without_event = list(
                await session.scalars(
                    select(Task)
                    .where(
                        Task.status == TaskStatus.QUEUED,
                        ~select(OutboxEvent.id)
                        .where(
                            OutboxEvent.aggregate_id == Task.id,
                            OutboxEvent.event_type == "task.ready",
                            OutboxEvent.processed_at.is_(None),
                        )
                        .exists(),
                    )
                    .limit(self.settings.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            emitted = 0
            for task in queued_without_event:
                if not await TaskRepository.dependencies_ready(session, task.id):
                    continue
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.ready",
                    payload={"task_id": str(task.id)},
                )
                emitted += 1
        return emitted

    async def run_once(self) -> tuple[int, int, int]:
        async with self.database.session() as session, session.begin():
            offline = await WorkerRepository.mark_stale_offline(
                session,
                offline_timeout_seconds=self.settings.worker_offline_timeout,
                limit=self.settings.batch_size,
            )
        async with self.database.session() as session, session.begin():
            recovered = await TaskRepository.recover_expired(
                session,
                limit=self.settings.batch_size,
                max_recovery_attempts=self.settings.max_recovery_attempts,
                recovery_cleanup_grace_seconds=self.settings.recovery_cleanup_grace_seconds,
            )
        async with self.database.session() as session, session.begin():
            retries = await TaskRepository.release_due_retries(session, self.settings.batch_size)
            await TaskRepository.resolve_dependency_readiness(
                session,
                limit=self.settings.batch_size,
            )
        if offline:
            self.logger.warning("workers marked offline", worker_ids=offline)
        if recovered:
            self.logger.warning(
                "expired task leases recovered", task_ids=[str(item) for item in recovered]
            )
        await self.refresh_gauges()
        return len(offline), len(recovered), len(retries)

    async def refresh_gauges(self) -> None:
        async with self.database.session() as session:
            now = await database_utcnow(session)
            counts = await TaskRepository.counts_by_status(session)
            online_row = (
                await session.execute(
                    select(
                        func.count(Worker.id),
                        func.coalesce(func.sum(Worker.cpu_allocatable_millicores), 0),
                        func.coalesce(func.sum(Worker.memory_allocatable_mb), 0),
                        func.coalesce(func.sum(Worker.gpu_count), 0),
                    ).where(Worker.status == WorkerStatus.ONLINE)
                )
            ).one()
            allocated_row = (
                await session.execute(
                    select(
                        func.coalesce(func.sum(Worker.reserved_cpu), 0),
                        func.coalesce(func.sum(Worker.reserved_memory_mb), 0),
                        func.coalesce(func.sum(Worker.reserved_gpus), 0),
                    )
                )
            ).one()
            replica_rows = list(
                await session.execute(
                    select(ServiceReplica.status, func.count(ServiceReplica.id)).group_by(
                        ServiceReplica.status
                    )
                )
            )
            services_ready = int(
                await session.scalar(
                    select(func.count(ModelService.id)).where(
                        ModelService.status == ServiceStatus.RUNNING
                    )
                )
                or 0
            )
            outbox_pending = int(
                await session.scalar(
                    select(func.count(OutboxEvent.id)).where(OutboxEvent.processed_at.is_(None))
                )
                or 0
            )
            oldest_available_outbox = await session.scalar(
                select(func.min(OutboxEvent.created_at)).where(
                    OutboxEvent.processed_at.is_(None),
                    OutboxEvent.available_at <= now,
                )
            )
        TASKS_QUEUED.set(counts.get(TaskStatus.QUEUED, 0))
        TASKS_RUNNING.set(counts.get(TaskStatus.RUNNING, 0))
        WORKERS_ONLINE.set(int(online_row[0]))

        capacity_cpu = int(online_row[1])
        capacity_memory = int(online_row[2])
        capacity_gpu = int(online_row[3])
        allocated_cpu = round(float(allocated_row[0]) * 1000)
        allocated_memory = int(allocated_row[1])
        allocated_gpu = int(allocated_row[2])
        WORKER_CAPACITY_CPU.set(capacity_cpu)
        WORKER_CAPACITY_MEMORY.set(capacity_memory)
        WORKER_CAPACITY_GPU.set(capacity_gpu)
        WORKER_ALLOCATED_CPU.set(allocated_cpu)
        WORKER_ALLOCATED_MEMORY.set(allocated_memory)
        WORKER_ALLOCATED_GPU.set(allocated_gpu)
        WORKER_ALLOCATED.labels("cpu_millicores").set(allocated_cpu)
        WORKER_ALLOCATED.labels("memory_mb").set(allocated_memory)
        WORKER_ALLOCATED.labels("gpu").set(allocated_gpu)

        replica_counts = {status: int(count) for status, count in replica_rows}
        for replica_status in ReplicaStatus:
            SERVICE_REPLICAS.labels(replica_status.value).set(replica_counts.get(replica_status, 0))
        SERVICES_READY.set(services_ready)
        OUTBOX_PENDING.set(outbox_pending)
        OUTBOX_OLDEST_AGE.set(
            0
            if oldest_available_outbox is None
            else max(0.0, (now - _as_utc(oldest_available_outbox)).total_seconds())
        )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
