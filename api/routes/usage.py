import uuid
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_database, require_api_permission
from api.errors import APIError, ConflictError, NotFoundError
from api.schemas.usage import (
    CostResponse,
    CurrencyCostResponse,
    GPUUsageResponse,
    ProjectQuotaLimitsResponse,
    ProjectQuotaResponse,
    ProjectQuotaStateResponse,
    ProjectQuotaUpdate,
    ServingUsageResponse,
    UsageResponse,
)
from core.database import Database
from core.rbac import Permission, Principal, require_project_access
from repositories.quotas import (
    QuotaExceededError,
    QuotaNotFoundError,
    QuotaRepository,
    QuotaSnapshot,
)
from repositories.usage import UsageAggregate, UsageRepository

router = APIRouter(prefix="/api/v1/projects/{project_id}", tags=["usage"])


@router.get("/quota", response_model=ProjectQuotaResponse)
async def get_project_quota(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.QUOTA_MANAGE))],
) -> ProjectQuotaResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
    except QuotaNotFoundError as exc:
        raise NotFoundError("PROJECT_QUOTA_NOT_FOUND", "Project quota not found") from exc
    return _quota_response(snapshot)


@router.put("/quota", response_model=ProjectQuotaResponse)
async def replace_project_quota(
    project_id: uuid.UUID,
    payload: ProjectQuotaUpdate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.QUOTA_MANAGE))],
) -> ProjectQuotaResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            snapshot = await QuotaRepository.replace(
                session,
                project_id=project_id,
                **payload.model_dump(),
            )
    except QuotaNotFoundError as exc:
        raise NotFoundError("PROJECT_QUOTA_NOT_FOUND", "Project quota not found") from exc
    except QuotaExceededError as exc:
        raise ConflictError(
            "QUOTA_BELOW_CURRENT_USAGE",
            "Quota cannot be set below current project usage",
            details={
                "resource": exc.resource,
                "limit": str(exc.limit),
                "current": str(exc.requested),
            },
        ) from exc
    return _quota_response(snapshot)


@router.get("/usage", response_model=UsageResponse)
async def get_project_usage(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.USAGE_READ))],
    from_time: Annotated[datetime, Query(alias="from")],
    to_time: Annotated[datetime, Query(alias="to")],
) -> UsageResponse:
    _require_same_project(principal, project_id)
    _validate_window(from_time, to_time)
    async with database.session() as session:
        aggregate = await UsageRepository.aggregate_settled(
            session,
            project_id=project_id,
            from_time=from_time,
            to_time=to_time,
        )
    return _usage_response(aggregate)


@router.get("/cost", response_model=CostResponse)
async def get_project_cost(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.COST_READ))],
    from_time: Annotated[datetime, Query(alias="from")],
    to_time: Annotated[datetime, Query(alias="to")],
) -> CostResponse:
    _require_same_project(principal, project_id)
    _validate_window(from_time, to_time)
    async with database.session() as session:
        aggregate = await UsageRepository.aggregate_settled(
            session,
            project_id=project_id,
            from_time=from_time,
            to_time=to_time,
        )
    return CostResponse(
        project_id=aggregate.project_id,
        from_time=aggregate.from_time,
        to_time=aggregate.to_time,
        execution_count=aggregate.execution_count,
        costs=[CurrencyCostResponse.model_validate(cost) for cost in aggregate.costs],
    )


def _require_same_project(principal: Principal, project_id: uuid.UUID) -> None:
    try:
        require_project_access(principal, project_id)
    except PermissionError as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc


def _validate_window(from_time: datetime, to_time: datetime) -> None:
    if (
        from_time.tzinfo is None
        or from_time.utcoffset() is None
        or to_time.tzinfo is None
        or to_time.utcoffset() is None
    ):
        raise APIError(
            422,
            "INVALID_USAGE_WINDOW",
            "Usage window timestamps must include a timezone",
        )
    if to_time <= from_time or to_time - from_time > timedelta(days=366):
        raise APIError(
            422,
            "INVALID_USAGE_WINDOW",
            "Usage window must be positive and at most 366 days",
        )


def _quota_response(snapshot: QuotaSnapshot) -> ProjectQuotaResponse:
    return ProjectQuotaResponse(
        project_id=snapshot.quota.project_id,
        limits=ProjectQuotaLimitsResponse.model_validate(snapshot.quota),
        state=ProjectQuotaStateResponse.model_validate(snapshot.state),
    )


def _usage_response(aggregate: UsageAggregate) -> UsageResponse:
    return UsageResponse(
        project_id=aggregate.project_id,
        from_time=aggregate.from_time,
        to_time=aggregate.to_time,
        execution_count=aggregate.execution_count,
        cpu_seconds=aggregate.cpu_seconds,
        memory_gb_seconds=aggregate.memory_gb_seconds,
        gpu_seconds=aggregate.gpu_seconds,
        gpu_breakdown=[GPUUsageResponse.model_validate(item) for item in aggregate.gpu_breakdown],
        costs=[CurrencyCostResponse.model_validate(cost) for cost in aggregate.costs],
        serving=ServingUsageResponse(
            request_count=aggregate.serving_request_count,
            requests_with_reported_token_usage=aggregate.serving_requests_with_token_usage,
            reported_input_tokens=aggregate.input_tokens,
            reported_output_tokens=aggregate.output_tokens,
            reported_total_tokens=aggregate.total_tokens,
            allocated_gpu_seconds=aggregate.serving_allocated_gpu_seconds,
            replica_gpu_seconds=aggregate.serving_replica_gpu_seconds,
        ),
    )
