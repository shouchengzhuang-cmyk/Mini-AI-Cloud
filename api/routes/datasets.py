import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.exc import IntegrityError

from api.dependencies import get_database, require_api_permission
from api.errors import ConflictError, NotFoundError
from api.schemas.common import PaginationMeta
from api.schemas.datasets import (
    DatasetCreate,
    DatasetListResponse,
    DatasetResponse,
    DatasetVersionCreate,
    DatasetVersionResponse,
)
from core.database import Database
from core.rbac import Permission, Principal, require_project_access
from repositories.datasets import (
    DatasetConflictError,
    DatasetNotFoundError,
    DatasetRepository,
    DatasetSummary,
)

router = APIRouter(prefix="/api/v1/projects/{project_id}/datasets", tags=["datasets"])


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    project_id: uuid.UUID,
    payload: DatasetCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> DatasetResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            summary = await DatasetRepository.create(
                session,
                project_id=project_id,
                name=payload.name,
                description=payload.description,
                artifact_id=payload.artifact_id,
                metadata=payload.metadata,
            )
    except IntegrityError as exc:
        raise ConflictError("DATASET_CONFLICT", "Dataset already exists") from exc
    except DatasetNotFoundError as exc:
        raise NotFoundError("DATASET_RESOURCE_NOT_FOUND", str(exc)) from exc
    except DatasetConflictError as exc:
        raise ConflictError("DATASET_CONFLICT", str(exc)) from exc
    return _dataset_response(summary)


@router.get("", response_model=DatasetListResponse)
async def list_datasets(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DatasetListResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            items = await DatasetRepository.list_summaries(
                session,
                project_id=project_id,
                limit=limit,
                offset=offset,
            )
            total = await DatasetRepository.count(session, project_id=project_id)
    except DatasetConflictError as exc:
        raise ConflictError("DATASET_INVARIANT_VIOLATION", str(exc)) from exc
    return DatasetListResponse(
        items=[_dataset_response(item) for item in items],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> DatasetResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            summary = await DatasetRepository.get_summary(
                session,
                project_id=project_id,
                dataset_id=dataset_id,
            )
    except DatasetNotFoundError as exc:
        raise NotFoundError("DATASET_NOT_FOUND", "Dataset not found") from exc
    except DatasetConflictError as exc:
        raise ConflictError("DATASET_INVARIANT_VIOLATION", str(exc)) from exc
    return _dataset_response(summary)


@router.post(
    "/{dataset_id}/versions",
    response_model=DatasetVersionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_dataset_version(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    payload: DatasetVersionCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> DatasetVersionResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            summary = await DatasetRepository.add_version(
                session,
                project_id=project_id,
                dataset_id=dataset_id,
                artifact_id=payload.artifact_id,
                metadata=payload.metadata,
            )
    except IntegrityError as exc:
        raise ConflictError("DATASET_VERSION_CONFLICT", "Dataset version conflict") from exc
    except DatasetNotFoundError as exc:
        raise NotFoundError("DATASET_RESOURCE_NOT_FOUND", str(exc)) from exc
    except DatasetConflictError as exc:
        raise ConflictError("DATASET_CONFLICT", str(exc)) from exc
    return DatasetVersionResponse.model_validate(summary.current)


@router.get("/{dataset_id}/versions", response_model=list[DatasetVersionResponse])
async def list_dataset_versions(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> list[DatasetVersionResponse]:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            versions = await DatasetRepository.list_versions(
                session,
                project_id=project_id,
                dataset_id=dataset_id,
            )
    except DatasetNotFoundError as exc:
        raise NotFoundError("DATASET_NOT_FOUND", "Dataset not found") from exc
    return [DatasetVersionResponse.model_validate(item) for item in versions]


def _require_same_project(principal: Principal, project_id: uuid.UUID) -> None:
    try:
        require_project_access(principal, project_id)
    except PermissionError as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc


def _dataset_response(summary: DatasetSummary) -> DatasetResponse:
    return DatasetResponse(
        id=summary.dataset.id,
        project_id=summary.dataset.project_id,
        name=summary.dataset.name,
        description=summary.dataset.description,
        current_version=summary.dataset.current_version,
        current_artifact_id=summary.current.artifact_id,
        current_metadata=summary.current.metadata_json,
        created_at=summary.dataset.created_at,
    )
