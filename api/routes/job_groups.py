import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import func, select

from api.dependencies import get_database, require_api_permission
from api.errors import ConflictError, NotFoundError
from api.schemas.common import PaginationMeta
from api.schemas.dag import (
    DependencyStateResponse,
    JobGroupCreate,
    JobGroupListResponse,
    JobGroupResponse,
    ReadyTasksResponse,
    TaskDependencyCreate,
    TaskDependencyResponse,
)
from core.database import Database
from core.rbac import Permission, Principal, require_project_access
from models.artifact import JobGroup
from repositories.dag import (
    DAGConflictError,
    DAGCycleError,
    DAGNotFoundError,
    DAGRepository,
    DependencyResolution,
    DependencySpec,
    JobGroupSummary,
)

router = APIRouter(prefix="/api/v1/projects/{project_id}/job-groups", tags=["job-groups"])


@router.post("", response_model=JobGroupResponse, status_code=status.HTTP_201_CREATED)
async def create_job_group(
    project_id: uuid.UUID,
    payload: JobGroupCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> JobGroupResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            group = await DAGRepository.create_group(
                session,
                project_id=project_id,
                name=payload.name,
                retry_policy=payload.retry_policy,
                dependencies=[_dependency_spec(item) for item in payload.dependencies],
            )
            summary = await DAGRepository.summarize_group(
                session,
                project_id=project_id,
                group_id=group.id,
            )
    except DAGCycleError as exc:
        raise ConflictError("DAG_CYCLE", str(exc)) from exc
    except DAGConflictError as exc:
        raise ConflictError("DAG_CONFLICT", str(exc)) from exc
    except DAGNotFoundError as exc:
        raise NotFoundError("DAG_RESOURCE_NOT_FOUND", "Project or task not found") from exc
    return _group_response(summary)


@router.get("", response_model=JobGroupListResponse)
async def list_job_groups(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobGroupListResponse:
    _require_same_project(principal, project_id)
    async with database.session() as session:
        groups = await DAGRepository.list_groups(
            session,
            project_id=project_id,
            limit=limit,
            offset=offset,
        )
        summaries = await DAGRepository.summarize_groups(
            session,
            project_id=project_id,
            groups=groups,
        )
        total = int(
            await session.scalar(
                select(func.count(JobGroup.id)).where(JobGroup.project_id == project_id)
            )
            or 0
        )
    return JobGroupListResponse(
        items=[_group_response(summary) for summary in summaries],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/{group_id}", response_model=JobGroupResponse)
async def get_job_group(
    project_id: uuid.UUID,
    group_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> JobGroupResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            summary = await DAGRepository.summarize_group(
                session,
                project_id=project_id,
                group_id=group_id,
            )
    except DAGNotFoundError as exc:
        raise NotFoundError("JOB_GROUP_NOT_FOUND", "Job group not found") from exc
    return _group_response(summary)


@router.get("/{group_id}/dependencies", response_model=list[TaskDependencyResponse])
async def list_dependencies(
    project_id: uuid.UUID,
    group_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> list[TaskDependencyResponse]:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            dependencies = await DAGRepository.list_dependencies(
                session,
                project_id=project_id,
                group_id=group_id,
            )
    except DAGNotFoundError as exc:
        raise NotFoundError("JOB_GROUP_NOT_FOUND", "Job group not found") from exc
    return [TaskDependencyResponse.model_validate(item) for item in dependencies]


@router.post(
    "/{group_id}/dependencies",
    response_model=TaskDependencyResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_dependency(
    project_id: uuid.UUID,
    group_id: uuid.UUID,
    payload: TaskDependencyCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> TaskDependencyResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            dependency = await DAGRepository.add_dependency(
                session,
                project_id=project_id,
                group_id=group_id,
                dependency=_dependency_spec(payload),
            )
    except DAGCycleError as exc:
        raise ConflictError("DAG_CYCLE", str(exc)) from exc
    except DAGConflictError as exc:
        raise ConflictError("DAG_CONFLICT", str(exc)) from exc
    except DAGNotFoundError as exc:
        raise NotFoundError("DAG_RESOURCE_NOT_FOUND", "Job group or task not found") from exc
    return TaskDependencyResponse.model_validate(dependency)


@router.get(
    "/{group_id}/tasks/{task_id}/dependency-state",
    response_model=DependencyStateResponse,
)
async def get_dependency_state(
    project_id: uuid.UUID,
    group_id: uuid.UUID,
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> DependencyStateResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            resolution = await DAGRepository.dependency_state(
                session,
                project_id=project_id,
                group_id=group_id,
                task_id=task_id,
            )
    except DAGNotFoundError as exc:
        raise NotFoundError("DAG_RESOURCE_NOT_FOUND", "Job group or task not found") from exc
    return _resolution_response(resolution)


@router.get("/{group_id}/ready-tasks", response_model=ReadyTasksResponse)
async def list_ready_tasks(
    project_id: uuid.UUID,
    group_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> ReadyTasksResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session:
            resolutions = await DAGRepository.ready_tasks(
                session,
                project_id=project_id,
                group_id=group_id,
            )
    except DAGNotFoundError as exc:
        raise NotFoundError("JOB_GROUP_NOT_FOUND", "Job group not found") from exc
    return ReadyTasksResponse(
        job_group_id=group_id,
        items=[_resolution_response(item) for item in resolutions],
    )


def _require_same_project(principal: Principal, project_id: uuid.UUID) -> None:
    try:
        require_project_access(principal, project_id)
    except PermissionError as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc


def _dependency_spec(payload: TaskDependencyCreate) -> DependencySpec:
    return DependencySpec(
        task_id=payload.task_id,
        depends_on_task_id=payload.depends_on_task_id,
        failure_policy=payload.failure_policy,
    )


def _resolution_response(resolution: DependencyResolution) -> DependencyStateResponse:
    return DependencyStateResponse(
        task_id=resolution.task_id,
        task_status=resolution.task_status,
        dependency_state=resolution.state,
        dependency_ids=list(resolution.dependency_ids),
        waiting_on_task_ids=list(resolution.waiting_on_task_ids),
        failed_dependency_ids=list(resolution.failed_dependency_ids),
    )


def _group_response(summary: JobGroupSummary) -> JobGroupResponse:
    return JobGroupResponse(
        id=summary.group.id,
        project_id=summary.group.project_id,
        name=summary.group.name,
        status=summary.status,
        retry_policy=summary.group.retry_policy,
        task_count=summary.task_count,
        ready_tasks=summary.ready_tasks,
        waiting_tasks=summary.waiting_tasks,
        blocked_tasks=summary.blocked_tasks,
        cancelled_tasks=summary.cancelled_tasks,
        succeeded_tasks=summary.succeeded_tasks,
        failed_tasks=summary.failed_tasks,
        created_at=summary.group.created_at,
        finished_at=summary.finished_at,
    )
