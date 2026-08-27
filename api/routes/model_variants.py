import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.exc import IntegrityError

from api.dependencies import get_database, get_principal, get_runtime_profile_catalog
from api.errors import APIError, ConflictError, NotFoundError
from api.schemas.common import MessageResponse, PaginationMeta
from api.schemas.model_variants import (
    LogicalModelCreate,
    LogicalModelListResponse,
    LogicalModelResponse,
    LogicalModelStatusEventResponse,
    LogicalModelStatusHistoryResponse,
    LogicalModelStatusUpdate,
    ModelVariantCreate,
    ModelVariantListResponse,
    ModelVariantResponse,
    ModelVariantStatusUpdate,
)
from core.database import Database
from core.rbac import (
    Permission,
    PermissionDenied,
    Principal,
    ProjectAccessDenied,
    require_permission,
    require_project_access,
)
from core.runtime_profiles import RuntimeProfileCatalog, RuntimeProfileCompatibilityError
from models.model_variant import LogicalModel, LogicalModelStatusEvent, ModelVariant
from repositories.model_variants import (
    LogicalModelNotFoundError,
    LogicalModelRepository,
    ModelVariantInvariantError,
    ModelVariantNotFoundError,
    ModelVariantRepository,
)

router = APIRouter(
    prefix="/api/v1/projects/{project_id}",
    tags=["model-variants"],
    dependencies=[Depends(get_principal)],
)


@router.post(
    "/logical-models",
    response_model=LogicalModelResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_logical_model(
    project_id: uuid.UUID,
    payload: LogicalModelCreate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> LogicalModelResponse:
    principal = _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        async with database.session() as session, session.begin():
            model = await LogicalModelRepository.create(
                session,
                project_id=project_id,
                name=payload.name,
                public_name=payload.public_name,
                description=payload.description,
                metadata=payload.metadata,
                created_by_user_id=principal.user_id,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from error
    except IntegrityError as error:
        raise ConflictError(
            "LOGICAL_MODEL_NAME_ALREADY_EXISTS",
            "A logical model with this name already exists in the project",
        ) from error
    return _logical_model_response(model)


@router.get("/logical-models", response_model=LogicalModelListResponse)
async def list_logical_models(
    project_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1_000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> LogicalModelListResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    async with database.session() as session:
        models = await LogicalModelRepository.list(
            session,
            project_id=project_id,
            limit=limit,
            offset=offset,
        )
        total = await LogicalModelRepository.count(session, project_id=project_id)
    return LogicalModelListResponse(
        items=[_logical_model_response(model) for model in models],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get(
    "/logical-models/{logical_model_id}",
    response_model=LogicalModelResponse,
)
async def get_logical_model(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> LogicalModelResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    async with database.session() as session:
        model = await LogicalModelRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
        )
    if model is None:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found")
    return _logical_model_response(model)


@router.put(
    "/logical-models/{logical_model_id}/status",
    response_model=LogicalModelResponse,
)
async def set_logical_model_status(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    payload: LogicalModelStatusUpdate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> LogicalModelResponse:
    principal = _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        async with database.session() as session, session.begin():
            model = await LogicalModelRepository.set_status(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                status=payload.status,
                reason=payload.reason,
                created_by_user_id=principal.user_id,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    except ModelVariantInvariantError as error:
        raise ConflictError("LOGICAL_MODEL_INVARIANT", str(error)) from error
    return _logical_model_response(model)


@router.get(
    "/logical-models/{logical_model_id}/status-history",
    response_model=LogicalModelStatusHistoryResponse,
)
async def get_logical_model_status_history(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1_000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> LogicalModelStatusHistoryResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    try:
        async with database.session() as session:
            events = await LogicalModelRepository.status_history(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                limit=limit,
                offset=offset,
            )
            total = await LogicalModelRepository.status_history_count(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    return LogicalModelStatusHistoryResponse(
        items=[_status_event_response(event) for event in events],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.delete(
    "/logical-models/{logical_model_id}",
    response_model=MessageResponse,
)
async def delete_logical_model(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> MessageResponse:
    _authorize(request, project_id, Permission.MODEL_MANAGE)
    async with database.session() as session, session.begin():
        deleted = await LogicalModelRepository.delete(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
        )
    if not deleted:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found")
    return MessageResponse(message="Logical model deleted")


@router.post(
    "/logical-models/{logical_model_id}/variants",
    response_model=ModelVariantResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_model_variant(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    payload: ModelVariantCreate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    profiles: Annotated[RuntimeProfileCatalog, Depends(get_runtime_profile_catalog)],
) -> ModelVariantResponse:
    principal = _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        profiles.resolve_compatible(
            profile_id=payload.runtime_profile_id,
            profile_version=payload.runtime_profile_version,
            semantic_digest=payload.runtime_profile_digest,
            vendor=payload.vendor,
            kind=payload.kind,
            architecture=payload.architecture,
            dtype=payload.dtype,
        )
        async with database.session() as session, session.begin():
            variant = await ModelVariantRepository.create(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                name=payload.name,
                vendor=payload.vendor,
                kind=payload.kind,
                runtime_profile_id=payload.runtime_profile_id,
                runtime_profile_version=payload.runtime_profile_version,
                runtime_profile_digest=payload.runtime_profile_digest,
                artifact_source=payload.artifact_source,
                artifact_revision=payload.artifact_revision,
                artifact_digest=payload.artifact_digest,
                architecture=payload.architecture,
                dtype=payload.dtype,
                quantization=payload.quantization,
                status=payload.status,
                status_reason=payload.status_reason,
                metadata=payload.metadata,
                created_by_user_id=principal.user_id,
            )
    except RuntimeProfileCompatibilityError as error:
        raise ConflictError("MODEL_VARIANT_INCOMPATIBLE", str(error)) from error
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    except IntegrityError as error:
        raise ConflictError(
            "MODEL_VARIANT_NAME_ALREADY_EXISTS",
            "A variant with this name already exists for the logical model",
        ) from error
    return _model_variant_response(variant)


@router.get(
    "/logical-models/{logical_model_id}/variants",
    response_model=ModelVariantListResponse,
)
async def list_model_variants(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1_000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ModelVariantListResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    try:
        async with database.session() as session:
            variants = await ModelVariantRepository.list(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                limit=limit,
                offset=offset,
            )
            total = await ModelVariantRepository.count(
                session,
                logical_model_id=logical_model_id,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    return ModelVariantListResponse(
        items=[_model_variant_response(variant) for variant in variants],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get(
    "/logical-models/{logical_model_id}/variants/{variant_id}",
    response_model=ModelVariantResponse,
)
async def get_model_variant(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    variant_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> ModelVariantResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    async with database.session() as session:
        variant = await ModelVariantRepository.get(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            variant_id=variant_id,
        )
    if variant is None:
        raise NotFoundError("MODEL_VARIANT_NOT_FOUND", "Model variant not found")
    return _model_variant_response(variant)


@router.put(
    "/logical-models/{logical_model_id}/variants/{variant_id}/status",
    response_model=ModelVariantResponse,
)
async def set_model_variant_status(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    variant_id: uuid.UUID,
    payload: ModelVariantStatusUpdate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> ModelVariantResponse:
    _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        async with database.session() as session, session.begin():
            variant = await ModelVariantRepository.set_status(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                variant_id=variant_id,
                status=payload.status,
                reason=payload.reason,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    except ModelVariantNotFoundError as error:
        raise NotFoundError("MODEL_VARIANT_NOT_FOUND", "Model variant not found") from error
    except ModelVariantInvariantError as error:
        raise ConflictError("LOGICAL_MODEL_INVARIANT", str(error)) from error
    return _model_variant_response(variant)


@router.delete(
    "/logical-models/{logical_model_id}/variants/{variant_id}",
    response_model=MessageResponse,
)
async def delete_model_variant(
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    variant_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> MessageResponse:
    _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        async with database.session() as session, session.begin():
            deleted = await ModelVariantRepository.delete(
                session,
                project_id=project_id,
                logical_model_id=logical_model_id,
                variant_id=variant_id,
            )
    except LogicalModelNotFoundError as error:
        raise NotFoundError("LOGICAL_MODEL_NOT_FOUND", "Logical model not found") from error
    except ModelVariantInvariantError as error:
        raise ConflictError("LOGICAL_MODEL_INVARIANT", str(error)) from error
    if not deleted:
        raise NotFoundError("MODEL_VARIANT_NOT_FOUND", "Model variant not found")
    return MessageResponse(message="Model variant deleted")


def _authorize(
    request: Request,
    project_id: uuid.UUID,
    permission: Permission,
) -> Principal:
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, Principal):
        raise APIError(401, "AUTHENTICATION_REQUIRED", "Authentication required")
    try:
        require_project_access(principal, project_id)
    except ProjectAccessDenied as error:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from error
    try:
        require_permission(principal, permission)
    except PermissionDenied as error:
        raise APIError(403, "PERMISSION_DENIED", "Permission denied") from error
    return principal


def _logical_model_response(model: LogicalModel) -> LogicalModelResponse:
    return LogicalModelResponse(
        id=model.id,
        project_id=model.project_id,
        name=model.name,
        public_name=model.public_name,
        description=model.description,
        status=model.status,
        metadata=model.metadata_json,
        created_by_user_id=model.created_by_user_id,
        created_at=model.created_at,
        updated_at=model.updated_at,
        version=model.version,
    )


def _model_variant_response(variant: ModelVariant) -> ModelVariantResponse:
    return ModelVariantResponse(
        id=variant.id,
        logical_model_id=variant.logical_model_id,
        name=variant.name,
        vendor=variant.vendor,
        accelerator_kind=variant.kind,
        runtime_profile_id=variant.runtime_profile_id,
        runtime_profile_version=variant.runtime_profile_version,
        runtime_profile_digest=variant.runtime_profile_digest,
        artifact_source=variant.artifact_source,
        artifact_revision=variant.artifact_revision,
        artifact_digest=variant.artifact_digest,
        architecture=variant.architecture,
        dtype=variant.dtype,
        quantization=variant.quantization,
        status=variant.status,
        status_reason=variant.status_reason,
        metadata=variant.metadata_json,
        created_by_user_id=variant.created_by_user_id,
        created_at=variant.created_at,
        updated_at=variant.updated_at,
        version=variant.version,
    )


def _status_event_response(event: LogicalModelStatusEvent) -> LogicalModelStatusEventResponse:
    return LogicalModelStatusEventResponse.model_validate(event)
