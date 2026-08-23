from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from api.errors import ConflictError
from api.schemas.tasks import TaskCreate
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus
from core.logging import get_logger
from core.metrics import TASKS_CREATED
from models.task import Task, TaskLog
from repositories.tasks import TaskRepository


@dataclass(frozen=True, slots=True)
class CreateResult:
    task: Task
    created: bool


class TaskService:
    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.logger = get_logger("task_service")

    async def create(self, payload: TaskCreate, *, idempotency_key: str | None) -> CreateResult:
        key = _validate_idempotency_key(idempotency_key)
        timeout_seconds = payload.timeout_seconds or self.settings.default_task_timeout
        normalized_payload = payload.model_copy(update={"timeout_seconds": timeout_seconds})
        request_hash = _request_hash(normalized_payload)
        if timeout_seconds > self.settings.max_task_timeout:
            maximum = self.settings.max_task_timeout
            raise ConflictError(
                "TASK_TIMEOUT_LIMIT_EXCEEDED",
                f"timeout_seconds exceeds the configured maximum of {maximum}",
            )
        if payload.max_retries > self.settings.max_task_retries:
            raise ConflictError(
                "TASK_RETRY_LIMIT_EXCEEDED",
                f"max_retries exceeds the configured maximum of {self.settings.max_task_retries}",
            )

        try:
            async with self.database.session() as session, session.begin():
                if key is not None:
                    existing = await TaskRepository.get_by_idempotency_key(session, key)
                    if existing is not None:
                        return _resolve_existing(existing, request_hash)
                task = await TaskRepository.create_queued(
                    session,
                    image=payload.image,
                    command=list(payload.command),
                    environment=dict(payload.environment),
                    timeout_seconds=timeout_seconds,
                    max_retries=payload.max_retries,
                    cpu_limit=payload.cpu_limit,
                    memory_limit_mb=payload.memory_limit_mb,
                    labels=dict(payload.labels),
                    network_enabled=payload.network_enabled,
                    gpu_count=payload.gpu_count,
                    idempotency_key=key,
                    request_hash=request_hash if key is not None else None,
                )
        except IntegrityError:
            if key is None:
                raise
            async with self.database.session() as session:
                existing = await TaskRepository.get_by_idempotency_key(session, key)
                if existing is None:
                    raise
                return _resolve_existing(existing, request_hash)

        TASKS_CREATED.inc()
        self.logger.info(
            "task created",
            task_id=str(task.id),
            status=task.status.value,
            idempotent=False,
        )
        return CreateResult(task=task, created=True)

    async def get(self, task_id: uuid.UUID) -> Task | None:
        async with self.database.session() as session:
            return await TaskRepository.get(session, task_id)

    async def list_tasks(
        self,
        *,
        status: TaskStatus | None,
        worker_id: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        async with self.database.session() as session:
            items = await TaskRepository.list_tasks(
                session,
                status=status,
                worker_id=worker_id,
                limit=limit,
                offset=offset,
            )
            count_query = select(func.count(Task.id))
            if status is not None:
                count_query = count_query.where(Task.status == status)
            if worker_id is not None:
                count_query = count_query.where(Task.worker_id == worker_id)
            total = int(await session.scalar(count_query) or 0)
        return items, total

    async def cancel(self, task_id: uuid.UUID) -> Task | None:
        async with self.database.session() as session, session.begin():
            task = await TaskRepository.cancel(session, task_id)
        if task is not None:
            self.logger.info(
                "task cancellation requested",
                task_id=str(task.id),
                status=task.status.value,
                cancel_requested=task.cancel_requested,
            )
        return task

    async def logs(
        self, task_id: uuid.UUID, *, offset: int, limit: int
    ) -> tuple[list[TaskLog], int] | None:
        async with self.database.session() as session:
            task = await TaskRepository.get(session, task_id)
            if task is None:
                return None
            logs = await TaskRepository.list_logs(session, task_id, offset=offset, limit=limit)
            total = int(
                await session.scalar(
                    select(func.count(TaskLog.id)).where(TaskLog.task_id == task_id)
                )
                or 0
            )
        return logs, total


def _request_hash(payload: TaskCreate) -> str:
    normalized = json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def _validate_idempotency_key(value: str | None) -> str | None:
    if value is None:
        return None
    if not value or len(value) > 255 or any(ord(character) < 32 for character in value):
        raise ConflictError(
            "INVALID_IDEMPOTENCY_KEY",
            "Idempotency-Key must contain 1-255 printable characters",
        )
    return value


def _resolve_existing(existing: Task, request_hash: str) -> CreateResult:
    if existing.request_hash != request_hash:
        raise ConflictError(
            "IDEMPOTENCY_KEY_REUSED",
            "Idempotency-Key was already used with a different request payload",
            details={"task_id": str(existing.id)},
        )
    return CreateResult(task=existing, created=False)
