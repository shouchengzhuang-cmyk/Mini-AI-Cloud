import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.exc import IntegrityError

from api.dependencies import get_database, require_api_permission
from api.errors import ConflictError, NotFoundError
from api.pagination import encode_cursor
from api.routes._pagination import parse_list_cursor
from api.schemas.common import PaginationMeta
from api.schemas.services import (
    ServiceCreate,
    ServiceListResponse,
    ServiceReplicaListResponse,
    ServiceReplicaResponse,
    ServiceResponse,
    ServiceScale,
)
from core.database import Database
from core.rbac import Permission, Principal
from models.service import ModelService, ServiceStatus, ServingRuntime
from repositories.quotas import QuotaExceededError
from repositories.registry import ImagePolicyRepository
from repositories.services import ServiceCounts, ServiceRepository

router = APIRouter(prefix="/api/v1/services", tags=["services"])


@router.post("", response_model=ServiceResponse, status_code=status.HTTP_201_CREATED)
async def create_service(
    payload: ServiceCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_MANAGE))],
) -> ServiceResponse:
    project_id = _principal_project_id(principal)
    try:
        async with database.session() as session, session.begin():
            image = payload.image
            if image is None and payload.runtime != ServingRuntime.FAKE:
                raise ConflictError(
                    "SERVICE_IMAGE_REQUIRED",
                    "A container image is required for this serving runtime",
                )
            if image is not None:
                try:
                    decision = await ImagePolicyRepository.evaluate(
                        session,
                        project_id=project_id,
                        image=image,
                    )
                except ValueError as exc:
                    raise ConflictError("INVALID_IMAGE_REFERENCE", str(exc)) from exc
                if not decision.allowed:
                    raise ConflictError(
                        "IMAGE_POLICY_DENIED",
                        "The project image policy rejected this service image",
                        details={"reason": decision.reason},
                    )
                image = decision.canonical_image
            service = await ServiceRepository.create(
                session,
                project_id=project_id,
                name=payload.name,
                model=payload.model,
                runtime=payload.runtime,
                runtime_type=payload.runtime_type,
                image=image,
                cpu_millicores=payload.cpu_millicores,
                memory_mb=payload.memory_mb,
                gpu_count=payload.gpu_count,
                gpu_memory_mb=payload.gpu_memory_mb,
                desired_replicas=payload.replicas,
                autoscaling_enabled=(
                    payload.autoscaling.enabled if payload.autoscaling is not None else False
                ),
                autoscaling_min_replicas=(
                    payload.autoscaling.min_replicas if payload.autoscaling is not None else 1
                ),
                autoscaling_max_replicas=(
                    payload.autoscaling.max_replicas if payload.autoscaling is not None else 4
                ),
                autoscaling_target_concurrency=(
                    payload.autoscaling.target_concurrency if payload.autoscaling is not None else 8
                ),
                autoscaling_cooldown_seconds=(
                    payload.autoscaling.cooldown_seconds if payload.autoscaling is not None else 60
                ),
            )
            await ServiceRepository.reconcile_locked(session, service)
            counts = (await ServiceRepository.counts_for_service_ids(session, [service.id]))[
                service.id
            ]
    except QuotaExceededError as exc:
        raise _service_quota_conflict(exc) from exc
    except IntegrityError as exc:
        if not _is_service_name_conflict(exc):
            raise
        raise ConflictError(
            "SERVICE_NAME_ALREADY_EXISTS",
            "A service with this name already exists in the project",
        ) from exc
    return _service_response(service, counts)


@router.get("", response_model=ServiceListResponse)
async def list_services(
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_READ))],
    service_status: Annotated[ServiceStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> ServiceListResponse:
    project_id = _principal_project_id(principal)
    after = parse_list_cursor(cursor=cursor, offset=offset)
    async with database.session() as session:
        rows = await ServiceRepository.list_services(
            session,
            project_id=project_id,
            status=service_status,
            limit=limit + 1,
            offset=offset,
            after=after,
        )
        total = await ServiceRepository.count_services(
            session, project_id=project_id, status=service_status
        )
        has_more = len(rows) > limit
        services = rows[:limit]
        counts = await ServiceRepository.counts_for_service_ids(
            session, [service.id for service in services]
        )
    next_cursor = (
        encode_cursor(services[-1].created_at, services[-1].id) if has_more and services else None
    )
    return ServiceListResponse(
        items=[_service_response(service, counts[service.id]) for service in services],
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset if cursor is None else 0,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{service_id}", response_model=ServiceResponse)
async def get_service(
    service_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_READ))],
) -> ServiceResponse:
    project_id = _principal_project_id(principal)
    async with database.session() as session:
        service = await ServiceRepository.get(session, service_id, project_id=project_id)
        if service is None:
            raise NotFoundError("SERVICE_NOT_FOUND", "Service not found")
        counts = (await ServiceRepository.counts_for_service_ids(session, [service.id]))[service.id]
    return _service_response(service, counts)


@router.post("/{service_id}/scale", response_model=ServiceResponse)
async def scale_service(
    service_id: uuid.UUID,
    payload: ServiceScale,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_MANAGE))],
) -> ServiceResponse:
    project_id = _principal_project_id(principal)
    try:
        async with database.session() as session, session.begin():
            service = await ServiceRepository.set_desired_replicas(
                session,
                service_id=service_id,
                project_id=project_id,
                desired_replicas=payload.replicas,
            )
            if service is None:
                raise NotFoundError("SERVICE_NOT_FOUND", "Service not found")
            await ServiceRepository.reconcile_locked(session, service)
            counts = (await ServiceRepository.counts_for_service_ids(session, [service.id]))[
                service.id
            ]
    except QuotaExceededError as exc:
        raise _service_quota_conflict(exc) from exc
    return _service_response(service, counts)


@router.post("/{service_id}/stop", response_model=ServiceResponse)
async def stop_service(
    service_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_MANAGE))],
) -> ServiceResponse:
    return await scale_service(service_id, ServiceScale(replicas=0), database, principal)


@router.get("/{service_id}/replicas", response_model=ServiceReplicaListResponse)
async def list_service_replicas(
    service_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MODEL_READ))],
) -> ServiceReplicaListResponse:
    project_id = _principal_project_id(principal)
    async with database.session() as session:
        service = await ServiceRepository.get(session, service_id, project_id=project_id)
        if service is None:
            raise NotFoundError("SERVICE_NOT_FOUND", "Service not found")
        replicas = await ServiceRepository.list_replicas(session, service_id)
    return ServiceReplicaListResponse(
        service_id=service_id,
        items=[ServiceReplicaResponse.model_validate(replica) for replica in replicas],
    )


def _principal_project_id(principal: Principal) -> uuid.UUID:
    if principal.project_id is None:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found")
    return principal.project_id


def _service_response(service: ModelService, counts: ServiceCounts) -> ServiceResponse:
    return ServiceResponse(
        id=service.id,
        project_id=service.project_id,
        name=service.name,
        model=service.model,
        runtime=service.runtime,
        runtime_type=service.runtime_type,
        image=service.image,
        cpu_millicores=service.cpu_millicores,
        memory_mb=service.memory_mb,
        gpu_count=service.gpu_count,
        gpu_memory_mb=service.gpu_memory_mb,
        desired_replicas=service.desired_replicas,
        actual_replicas=counts.actual_replicas,
        healthy_replicas=counts.healthy_replicas,
        autoscaling={
            "enabled": service.autoscaling_enabled,
            "min_replicas": service.autoscaling_min_replicas,
            "max_replicas": service.autoscaling_max_replicas,
            "target_concurrency": service.autoscaling_target_concurrency,
            "cooldown_seconds": service.autoscaling_cooldown_seconds,
        },
        last_scaled_at=service.last_scaled_at,
        generation=service.generation,
        status=service.status,
        error_message=service.error_message,
        created_at=service.created_at,
        updated_at=service.updated_at,
        stopped_at=service.stopped_at,
        version=service.version,
    )


def _is_service_name_conflict(exc: IntegrityError) -> bool:
    diagnostic = getattr(exc.orig, "diag", None)
    if getattr(diagnostic, "constraint_name", None) == "uq_model_services_project_name":
        return True
    return "model_services.project_id, model_services.name" in str(exc.orig)


def _service_quota_conflict(exc: QuotaExceededError) -> ConflictError:
    return ConflictError(
        "PROJECT_QUOTA_EXCEEDED",
        "Project quota rejected the requested service capacity",
        details={
            "resource": exc.resource,
            "limit": str(exc.limit),
            "requested": str(exc.requested),
        },
    )
