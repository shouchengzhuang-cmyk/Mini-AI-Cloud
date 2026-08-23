from sqlalchemy import func, select

from core.config import Settings
from core.database import Database
from core.enums import TaskStatus, WorkerStatus
from core.logging import get_logger
from core.metrics import TASKS_QUEUED, TASKS_RUNNING, WORKERS_ONLINE
from models.outbox import OutboxEvent
from models.task import Task
from models.worker import Worker
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
            for task in queued_without_event:
                OutboxRepository.add(
                    session,
                    aggregate_id=task.id,
                    event_type="task.ready",
                    payload={"task_id": str(task.id)},
                )
        return len(queued_without_event)

    async def run_once(self) -> tuple[int, int, int]:
        async with self.database.session() as session, session.begin():
            offline = await WorkerRepository.mark_stale_offline(
                session,
                offline_timeout_seconds=self.settings.worker_offline_timeout,
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
            counts = await TaskRepository.counts_by_status(session)
            online = await session.scalar(
                select(func.count(Worker.id)).where(Worker.status == WorkerStatus.ONLINE)
            )
        TASKS_QUEUED.set(counts.get(TaskStatus.QUEUED, 0))
        TASKS_RUNNING.set(counts.get(TaskStatus.RUNNING, 0))
        WORKERS_ONLINE.set(online or 0)
