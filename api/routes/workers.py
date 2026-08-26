from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select

from api.dependencies import get_database, require_api_permission
from api.errors import NotFoundError
from api.pagination import encode_text_cursor
from api.routes._pagination import parse_text_list_cursor
from api.schemas.common import PaginationMeta
from api.schemas.workers import WorkerListResponse, WorkerResponse
from core.database import Database
from core.rbac import Permission, Principal
from models.worker import Worker
from repositories.workers import WorkerRepository

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.get("", response_model=WorkerListResponse)
async def list_workers(
    database: Annotated[Database, Depends(get_database)],
    _principal: Annotated[Principal, Depends(require_api_permission(Permission.WORKER_READ))],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> WorkerListResponse:
    after = parse_text_list_cursor(cursor=cursor, offset=offset)
    async with database.session() as session:
        rows = await WorkerRepository.list_workers(
            session,
            limit=limit + 1,
            offset=offset,
            after=after,
        )
        total = int(await session.scalar(select(func.count(Worker.id))) or 0)
    has_more = len(rows) > limit
    workers = rows[:limit]
    next_cursor = (
        encode_text_cursor(workers[-1].started_at, workers[-1].id) if has_more and workers else None
    )
    return WorkerListResponse(
        items=[WorkerResponse.model_validate(worker) for worker in workers],
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset if cursor is None else 0,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: str,
    database: Annotated[Database, Depends(get_database)],
    _principal: Annotated[Principal, Depends(require_api_permission(Permission.WORKER_READ))],
) -> WorkerResponse:
    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    if worker is None:
        raise NotFoundError("WORKER_NOT_FOUND", "Worker not found")
    return WorkerResponse.model_validate(worker)
