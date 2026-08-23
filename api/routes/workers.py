from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select

from api.dependencies import get_database
from api.errors import NotFoundError
from api.schemas.common import PaginationMeta
from api.schemas.workers import WorkerListResponse, WorkerResponse
from core.database import Database
from models.worker import Worker
from repositories.workers import WorkerRepository

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


@router.get("", response_model=WorkerListResponse)
async def list_workers(
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> WorkerListResponse:
    async with database.session() as session:
        workers = await WorkerRepository.list_workers(session, limit=limit, offset=offset)
        total = int(await session.scalar(select(func.count(Worker.id))) or 0)
    return WorkerListResponse(
        items=[WorkerResponse.model_validate(worker) for worker in workers],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id: str, database: Annotated[Database, Depends(get_database)]
) -> WorkerResponse:
    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
    if worker is None:
        raise NotFoundError("WORKER_NOT_FOUND", "Worker not found")
    return WorkerResponse.model_validate(worker)
