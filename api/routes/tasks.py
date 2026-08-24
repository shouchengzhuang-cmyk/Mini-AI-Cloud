import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse
from redis.exceptions import RedisError
from sqlalchemy import distinct, func, select

from api.dependencies import (
    get_app_settings,
    get_database,
    get_principal,
    get_queue,
    require_api_permission,
)
from api.errors import ConflictError, NotFoundError
from api.pagination import encode_cursor
from api.routes._pagination import parse_list_cursor
from api.schemas.common import PaginationMeta
from api.schemas.tasks import (
    TaskCreate,
    TaskCreated,
    TaskEventResponse,
    TaskListResponse,
    TaskLogResponse,
    TaskLogsResponse,
    TaskResponse,
    TaskSchedulingResponse,
    TaskTimelineResponse,
)
from api.services.tasks import TaskService
from core.config import Settings
from core.database import Database
from core.enums import FINAL_TASK_STATUSES, TaskStatus
from core.rbac import Permission, Principal
from core.redis import RedisQueue
from models.scheduling import PlacementAttempt
from repositories.tasks import TaskRepository

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


@router.post("", response_model=TaskCreated, status_code=status.HTTP_201_CREATED)
async def create_task(
    payload: TaskCreate,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=255)] = None,
) -> TaskCreated:
    result = await TaskService(database, settings).create(
        payload, idempotency_key=idempotency_key, principal=principal
    )
    return TaskCreated(id=result.task.id, status=result.task.status)


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
    task_status: Annotated[TaskStatus | None, Query(alias="status")] = None,
    worker_id: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> TaskListResponse:
    after = parse_list_cursor(cursor=cursor, offset=offset)
    rows, total = await TaskService(database, settings).list_tasks(
        status=task_status,
        worker_id=worker_id,
        limit=limit + 1,
        offset=offset,
        after=after,
        principal=principal,
    )
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = encode_cursor(items[-1].created_at, items[-1].id) if has_more and items else None
    return TaskListResponse(
        items=[TaskResponse.model_validate(item) for item in items],
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset if cursor is None else 0,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> TaskResponse:
    task = await TaskService(database, settings).get(task_id, principal=principal)
    if task is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    return TaskResponse.model_validate(task)


@router.post("/{task_id}/cancel", response_model=TaskResponse)
async def cancel_task(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(get_principal)],
) -> TaskResponse:
    task = await TaskService(database, settings).cancel(task_id, principal=principal)
    if task is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    if task.status == TaskStatus.SUCCEEDED:
        raise ConflictError("TASK_ALREADY_SUCCEEDED", "A succeeded task cannot be cancelled")
    return TaskResponse.model_validate(task)


@router.get("/{task_id}/timeline", response_model=TaskTimelineResponse)
async def get_task_timeline(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> TaskTimelineResponse:
    if principal.project_id is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    async with database.session() as session:
        task = await TaskRepository.get_for_project(
            session,
            project_id=principal.project_id,
            task_id=task_id,
        )
        if task is None:
            raise NotFoundError("TASK_NOT_FOUND", "Task not found")
        events = await TaskRepository.list_events_for_project(
            session,
            project_id=principal.project_id,
            task_id=task_id,
        )
    return TaskTimelineResponse(
        task_id=task_id,
        events=[TaskEventResponse.model_validate(item) for item in events],
    )


@router.get(
    "/{task_id}/scheduling",
    response_model=TaskSchedulingResponse,
    summary="Explain recorded scheduler decisions without exposing worker details",
)
async def get_task_scheduling(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> TaskSchedulingResponse:
    if principal.project_id is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    async with database.session() as session:
        task = await TaskRepository.get_for_project(
            session,
            project_id=principal.project_id,
            task_id=task_id,
        )
        if task is None:
            raise NotFoundError("TASK_NOT_FOUND", "Task not found")

        attempts_total = int(
            await session.scalar(
                select(func.count(PlacementAttempt.id)).where(PlacementAttempt.task_id == task_id)
            )
            or 0
        )
        considered_workers = int(
            await session.scalar(
                select(func.count(distinct(PlacementAttempt.worker_id))).where(
                    PlacementAttempt.task_id == task_id,
                    PlacementAttempt.worker_id.is_not(None),
                )
            )
            or 0
        )
        grouped = list(
            await session.execute(
                select(
                    PlacementAttempt.outcome,
                    PlacementAttempt.reason,
                    func.count(PlacementAttempt.id),
                )
                .where(PlacementAttempt.task_id == task_id)
                .group_by(PlacementAttempt.outcome, PlacementAttempt.reason)
            )
        )
        latest_attempt_at = await session.scalar(
            select(func.max(PlacementAttempt.created_at)).where(PlacementAttempt.task_id == task_id)
        )

    outcomes: dict[str, int] = {}
    rejections: dict[str, int] = {}
    for outcome, reason, count in grouped:
        amount = int(count)
        outcomes[outcome] = outcomes.get(outcome, 0) + amount
        if outcome == "rejected":
            rejection = reason or "unspecified"
            rejections[rejection] = rejections.get(rejection, 0) + amount

    return TaskSchedulingResponse(
        task_id=task.id,
        state=_scheduling_state(task.status, task.unschedulable_reason),
        reason=task.unschedulable_reason,
        considered_workers=considered_workers,
        attempts_total=attempts_total,
        rejections=dict(sorted(rejections.items())),
        outcomes=dict(sorted(outcomes.items())),
        latest_attempt_at=latest_attempt_at,
    )


@router.get("/{task_id}/logs", response_model=TaskLogsResponse)
async def get_task_logs(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_LOG_READ))],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> TaskLogsResponse:
    result = await TaskService(database, settings).logs(
        task_id, offset=offset, limit=limit, principal=principal
    )
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
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_LOG_READ))],
    offset: Annotated[int, Query(ge=0)] = 0,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    async with database.session() as session:
        if (
            principal.project_id is None
            or await TaskRepository.get_for_project(
                session, project_id=principal.project_id, task_id=task_id
            )
            is None
        ):
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
                if principal.project_id is None:
                    return
                logs = await TaskRepository.list_logs_for_project(
                    session,
                    project_id=principal.project_id,
                    task_id=task_id,
                    offset=sequence,
                    limit=500,
                )
                task = await TaskRepository.get_for_project(
                    session, project_id=principal.project_id, task_id=task_id
                )
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


def _scheduling_state(task_status: TaskStatus, reason: str | None) -> str:
    if task_status == TaskStatus.QUEUED and reason:
        return "unschedulable"
    if task_status in {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RETRYING}:
        return "waiting"
    if task_status in {
        TaskStatus.SCHEDULING,
        TaskStatus.ASSIGNED,
        TaskStatus.PREPARING,
        TaskStatus.PULLING,
        TaskStatus.STARTING,
        TaskStatus.RUNNING,
        TaskStatus.PREEMPTING,
        TaskStatus.STOPPING,
    }:
        return "scheduled"
    return task_status.value
