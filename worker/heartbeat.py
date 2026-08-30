import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from core.config import Settings
from core.database import Database
from core.logging import get_logger
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository


@dataclass(slots=True)
class ActiveExecution:
    task_id: uuid.UUID
    execution_id: uuid.UUID
    runtime_type: str | None = None
    ownership_lost: asyncio.Event = field(default_factory=asyncio.Event)
    log_limit_exceeded: asyncio.Event = field(default_factory=asyncio.Event)
    runtime_handle_durable: asyncio.Event = field(default_factory=asyncio.Event)
    relinquish_requested: asyncio.Event = field(default_factory=asyncio.Event)


class Heartbeat:
    def __init__(
        self,
        database: Database,
        *,
        worker_id: str,
        active: dict[uuid.UUID, ActiveExecution],
        settings: Settings,
        worker_session_id: uuid.UUID | None = None,
        refresh_inventory: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.database = database
        self.worker_id = worker_id
        self.active = active
        self.settings = settings
        self.worker_session_id = worker_session_id
        self.refresh_inventory = refresh_inventory
        self.logger = get_logger("worker_heartbeat")

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.beat_once()
            except Exception as exc:
                self.logger.exception(
                    "heartbeat failed; active executions are fenced locally",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
                for execution in self.active.values():
                    execution.ownership_lost.set()
            try:
                interval = min(self.settings.heartbeat_interval, self.settings.lease_renew_interval)
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                continue

    async def beat_once(self) -> None:
        executions = list(self.active.values())
        for execution in executions:
            async with self.database.session() as session, session.begin():
                renewed = await TaskRepository.renew_lease(
                    session,
                    task_id=execution.task_id,
                    worker_id=self.worker_id,
                    execution_id=execution.execution_id,
                    lease_seconds=self.settings.task_lease_seconds,
                    worker_session_id=self.worker_session_id,
                    kubernetes_cleanup_grace_seconds=self.settings.kubernetes_cleanup_grace_seconds,
                )
            if not renewed:
                execution.ownership_lost.set()
                self.logger.warning(
                    "task lease ownership lost",
                    task_id=str(execution.task_id),
                    worker_id=self.worker_id,
                    execution_id=str(execution.execution_id),
                )

        async with self.database.session() as session, session.begin():
            worker = await WorkerRepository.heartbeat(
                session,
                self.worker_id,
                running_tasks=len(executions),
                worker_session_id=self.worker_session_id,
            )
        if worker is None:
            self.logger.error("worker registration disappeared", worker_id=self.worker_id)
            for execution in executions:
                execution.ownership_lost.set()
            return
        if self.refresh_inventory is not None:
            try:
                await self.refresh_inventory()
            except Exception as exc:
                # Inventory persistence is capacity evidence, not an execution-ownership
                # fence. The lease and worker heartbeat above have already succeeded, so
                # a transient refresh failure must not stop otherwise valid workloads.
                self.logger.exception(
                    "accelerator inventory refresh failed; active task leases remain valid",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
