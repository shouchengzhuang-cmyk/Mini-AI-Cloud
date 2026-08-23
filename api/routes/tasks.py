import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse
from redis.exceptions import RedisError

from api.dependencies import get_app_settings, get_database, get_queue
from api.errors import ConflictError, NotFoundError
from api.schemas.common import PaginationMeta
from api.schemas.tasks import (
    TaskCreate,
    TaskCreated,
    TaskListResponse,
    TaskLogResponse,
    TaskLogsResponse,
    TaskResponse,
)
from api.services.tasks import TaskService
from core.config import Settings
from core.database import Database
from core.enums import FINAL_TASK_STATUSES, TaskStatus
from core.redis import RedisQueue
from repositories.tasks import TaskRepository

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


@router.post("", response_model=TaskCreated, status_code=status.HTTP_201_CREATED)
async def create_task(
    payload: TaskCreate,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=255)] = None,
) -> TaskCreated:
    result = await TaskService(database, settings).create(payload, idempotency_key=idempotency_key)
    return TaskCreated(id=result.task.id, status=result.task.status)


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    worker_id: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TaskListResponse:
    items, total = await TaskService(database, settings).list_tasks(
        status=task_status,
        worker_id=worker_id,
        limit=limit,
        offset=offset,
    )
    return TaskListResponse(
        items=[TaskResponse.model_validate(item) for item in items],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> TaskResponse:
    task = await TaskService(database, settings).get(task_id)
    if task is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    return TaskResponse.model_validate(task)


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> TaskResponse:
    task = await TaskService(database, settings).cancel(task_id)
    if task is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    if task.status == TaskStatus.SUCCEEDED:
        raise ConflictError("TASK_ALREADY_SUCCEEDED", "A succeeded task cannot be cancelled")
    return TaskResponse.model_validate(task)


@router.get("/{task_id}/logs", response_model=TaskLogsResponse)
async def get_task_logs(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> TaskLogsResponse:
    result = await TaskService(database, settings).logs(task_id, offset=offset, limit=limit)
    if result is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    logs, total = result
    return TaskLogsResponse(
        task_id=task_id,
        logs=[TaskLogResponse.model_validate(item) for item in logs],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{task_id}/logs/stream")
async def stream_task_logs(
    task_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    queue: Annotated[RedisQueue, Depends(get_queue)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    offset: Annotated[int, Query(ge=0)] = 0,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    async with database.session() as session:
        if await TaskRepository.get(session, task_id) is None:
            raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    try:
        sequence = max(offset, int(last_event_id or 0))
    except ValueError as exc:
        raise ConflictError("INVALID_LAST_EVENT_ID", "Last-Event-ID must be an integer") from exc

    async def events() -> AsyncIterator[str]:
        nonlocal sequence
        redis_cursor = "$"
        last_heartbeat = time.monotonic()
        while not await request.is_disconnected():
            async with database.session() as session:
                logs = await TaskRepository.list_logs(session, task_id, offset=sequence, limit=500)
                task = await TaskRepository.get(session, task_id)
            for item in logs:
                sequence = item.sequence
                data = TaskLogResponse.model_validate(item).model_dump(mode="json")
                yield _sse("log", json.dumps(data, ensure_ascii=False), event_id=str(sequence))
            if task is None:
                yield _sse("error", json.dumps({"code": "TASK_NOT_FOUND"}))
                return
            if task.status in FINAL_TASK_STATUSES and not logs:
                yield _sse(
                    "end",
                    json.dumps({"status": task.status.value, "last_sequence": sequence}),
                )
                return
            try:
                wakeups = await queue.wait_for_logs(
                    task_id=task_id, last_id=redis_cursor, block_ms=1000
                )
                if wakeups:
                    redis_cursor = wakeups[-1][0]
            except RedisError:
                await asyncio.sleep(0.25)
            if time.monotonic() - last_heartbeat >= settings.sse_heartbeat_seconds:
                yield ": ping\n\n"
                last_heartbeat = time.monotonic()

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse(event: str, data: str, *, event_id: str | None = None) -> str:
    parts = []
    if event_id is not None:
        parts.append(f"id: {event_id}")
    parts.extend((f"event: {event}", f"data: {data}", "", ""))
    return "\n".join(parts)
