import uuid

from core.database import Database
from core.logging import get_logger
from core.metrics import (
    PROJECT_CPU_SECONDS,
    PROJECT_GPU_SECONDS,
    TASK_DURATION,
    TASK_PREEMPTIONS,
    TASK_QUEUE_WAIT,
    TASKS_CANCELLED,
    TASKS_FAILED,
    TASKS_SUCCEEDED,
)
from core.redis import RedisQueue
from models.outbox import OutboxEvent
from repositories.outbox import OutboxRepository


class OutboxDispatcher:
    def __init__(self, database: Database, queue: RedisQueue, *, batch_size: int = 100) -> None:
        self.database = database
        self.queue = queue
        self.batch_size = batch_size
        self.owner = uuid.uuid4()
        self.logger = get_logger("outbox_dispatcher")

    async def dispatch_once(self) -> int:
        async with self.database.session() as session, session.begin():
            events = await OutboxRepository.claim_batch(
                session, owner=self.owner, limit=self.batch_size
            )

        processed = 0
        for event in events:
            try:
                await self.queue.publish_outbox(
                    event_id=event.id,
                    event_type=event.event_type,
                    payload=event.payload,
                )
            except Exception as exc:
                await self._record_failure(event.id, exc)
                self.logger.warning(
                    "outbox publish failed",
                    event_id=str(event.id),
                    event_type=event.event_type,
                    error=str(exc),
                )
                continue

            async with self.database.session() as session, session.begin():
                published = await OutboxRepository.mark_processed(session, event.id, self.owner)
            if published is not None:
                processed += 1
                self._record_terminal_metric(published)
        return processed

    async def _record_failure(self, event_id: uuid.UUID, exc: Exception) -> None:
        async with self.database.session() as session, session.begin():
            await OutboxRepository.mark_failed(
                session, event_id=event_id, owner=self.owner, error=str(exc)
            )

    @staticmethod
    def _record_terminal_metric(event: OutboxEvent) -> None:
        if event.event_type == "task.preemption_requested":
            TASK_PREEMPTIONS.inc()
            return
        if event.event_type == "task.assigned":
            queue_wait = event.payload.get("queue_wait_seconds")
            if isinstance(queue_wait, int | float) and queue_wait >= 0:
                TASK_QUEUE_WAIT.observe(queue_wait)
            return
        if event.event_type != "task.terminal":
            return
        duration = event.payload.get("duration_seconds")
        if isinstance(duration, int | float) and duration >= 0:
            TASK_DURATION.observe(duration)
        cpu_seconds = event.payload.get("cpu_seconds")
        if isinstance(cpu_seconds, int | float) and cpu_seconds >= 0:
            PROJECT_CPU_SECONDS.inc(cpu_seconds)
        gpu_seconds = event.payload.get("gpu_seconds")
        if isinstance(gpu_seconds, int | float) and gpu_seconds >= 0:
            PROJECT_GPU_SECONDS.inc(gpu_seconds)
        status = event.payload.get("status")
        if status == "succeeded":
            TASKS_SUCCEEDED.inc()
        elif status == "cancelled":
            TASKS_CANCELLED.inc()
        elif status in {"failed", "timed_out"}:
            TASKS_FAILED.inc()
