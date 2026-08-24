import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy.exc import IntegrityError

from api.dependencies import get_app_settings, get_database, get_principal
from api.errors import APIError, ConflictError, NotFoundError, ServiceUnavailableError
from api.schemas.common import MessageResponse, PaginationMeta
from api.schemas.registry import (
    ImageEvaluationRequest,
    ImageEvaluationResponse,
    ImagePolicyResponse,
    ImagePolicyRuleResponse,
    ImagePolicyUpdate,
    RegisteredModelCreate,
    RegisteredModelListResponse,
    RegisteredModelResponse,
    SecretCreate,
    SecretListResponse,
    SecretResponse,
    SecretRotate,
)
from core.config import Settings
from core.database import Database
from core.rbac import (
    Permission,
    PermissionDenied,
    Principal,
    ProjectAccessDenied,
    require_permission,
    require_project_access,
)
from core.secrets import SecretCipher, SecretKeyConfigurationError
from models.registry import RegisteredModel
from repositories.registry import (
    ImagePolicyRepository,
    RegisteredModelRepository,
    RegistryNotFoundError,
    StoredImagePolicy,
)
from repositories.secrets import (
    SecretNotFoundError,
    SecretRepository,
    SecretRevokedError,
)

router = APIRouter(
    prefix="/api/v1/projects/{project_id}",
    tags=["registry"],
    dependencies=[Depends(get_principal)],
)


@router.post(
    "/models",
    response_model=RegisteredModelResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_registered_model(
    project_id: uuid.UUID,
    payload: RegisteredModelCreate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> RegisteredModelResponse:
    principal = _authorize(request, project_id, Permission.MODEL_MANAGE)
    try:
        async with database.session() as session, session.begin():
            model = await RegisteredModelRepository.create(
                session,
                project_id=project_id,
                name=payload.name,
                provider=payload.provider,
                source=payload.source,
                revision=payload.revision,
                size_bytes=payload.size_bytes,
                gpu_memory_mb=payload.required_gpu_memory_mb,
                architecture=payload.architecture,
                metadata=payload.metadata,
                created_by_user_id=principal.user_id,
            )
    except IntegrityError as exc:
        raise ConflictError(
            "MODEL_NAME_ALREADY_EXISTS",
            "A registered model with this name already exists in the project",
        ) from exc
    return _model_response(model)


@router.get("/models", response_model=RegisteredModelListResponse)
async def list_registered_models(
    project_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> RegisteredModelListResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    async with database.session() as session:
        models = await RegisteredModelRepository.list(
            session,
            project_id=project_id,
            limit=limit,
            offset=offset,
        )
        total = await RegisteredModelRepository.count(session, project_id=project_id)
    return RegisteredModelListResponse(
        items=[_model_response(model) for model in models],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/models/{model_id}", response_model=RegisteredModelResponse)
async def get_registered_model(
    project_id: uuid.UUID,
    model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> RegisteredModelResponse:
    _authorize(request, project_id, Permission.MODEL_READ)
    async with database.session() as session:
        model = await RegisteredModelRepository.get(
            session,
            project_id=project_id,
            model_id=model_id,
        )
    if model is None:
        raise NotFoundError("MODEL_NOT_FOUND", "Registered model not found")
    return _model_response(model)


@router.delete("/models/{model_id}", response_model=MessageResponse)
async def delete_registered_model(
    project_id: uuid.UUID,
    model_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> MessageResponse:
    _authorize(request, project_id, Permission.MODEL_MANAGE)
    async with database.session() as session, session.begin():
        deleted = await RegisteredModelRepository.delete(
            session,
            project_id=project_id,
            model_id=model_id,
        )
    if not deleted:
        raise NotFoundError("MODEL_NOT_FOUND", "Registered model not found")
    return MessageResponse(message="Registered model deleted")


@router.post(
    "/secrets",
    response_model=SecretResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_secret(
    project_id: uuid.UUID,
    payload: SecretCreate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> SecretResponse:
    principal = _authorize(request, project_id, Permission.SECRET_MANAGE)
    cipher = _secret_cipher(settings)
    try:
        async with database.session() as session, session.begin():
            secret = await SecretRepository.create(
                session,
                project_id=project_id,
                name=payload.name,
                value=payload.value.get_secret_value(),
                cipher=cipher,
                description=payload.description,
                created_by_user_id=principal.user_id,
            )
    except IntegrityError as exc:
        raise ConflictError(
            "SECRET_NAME_ALREADY_EXISTS",
            "A secret with this name already exists in the project",
        ) from exc
    return SecretResponse.model_validate(secret)


@router.get("/secrets", response_model=SecretListResponse)
async def list_secrets(
    project_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> SecretListResponse:
    _authorize(request, project_id, Permission.SECRET_MANAGE)
    async with database.session() as session:
        secrets = await SecretRepository.list(
            session,
            project_id=project_id,
            limit=limit,
            offset=offset,
        )
        total = await SecretRepository.count(session, project_id=project_id)
    return SecretListResponse(
        items=[SecretResponse.model_validate(secret) for secret in secrets],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.get("/secrets/{secret_id}", response_model=SecretResponse)
async def get_secret(
    project_id: uuid.UUID,
    secret_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> SecretResponse:
    _authorize(request, project_id, Permission.SECRET_MANAGE)
    async with database.session() as session:
        secret = await SecretRepository.get(
            session,
            project_id=project_id,
            secret_id=secret_id,
        )
    if secret is None:
        raise NotFoundError("SECRET_NOT_FOUND", "Secret not found")
    return SecretResponse.model_validate(secret)


@router.post("/secrets/{secret_id}/rotate", response_model=SecretResponse)
async def rotate_secret(
    project_id: uuid.UUID,
    secret_id: uuid.UUID,
    payload: SecretRotate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> SecretResponse:
    _authorize(request, project_id, Permission.SECRET_MANAGE)
    cipher = _secret_cipher(settings)
    try:
        async with database.session() as session, session.begin():
            secret = await SecretRepository.rotate(
                session,
                project_id=project_id,
                secret_id=secret_id,
                value=payload.value.get_secret_value(),
                cipher=cipher,
            )
    except SecretNotFoundError as exc:
        raise NotFoundError("SECRET_NOT_FOUND", "Secret not found") from exc
    except SecretRevokedError as exc:
        raise ConflictError("SECRET_REVOKED", "A revoked secret cannot be rotated") from exc
    return SecretResponse.model_validate(secret)


@router.delete("/secrets/{secret_id}", response_model=SecretResponse)
async def revoke_secret(
    project_id: uuid.UUID,
    secret_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> SecretResponse:
    _authorize(request, project_id, Permission.SECRET_MANAGE)
    async with database.session() as session, session.begin():
        secret = await SecretRepository.revoke(
            session,
            project_id=project_id,
            secret_id=secret_id,
        )
    if secret is None:
        raise NotFoundError("SECRET_NOT_FOUND", "Secret not found")
    return SecretResponse.model_validate(secret)


@router.get("/image-policy", response_model=ImagePolicyResponse)
async def get_image_policy(
    project_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> ImagePolicyResponse:
    _authorize(request, project_id, Permission.IMAGE_POLICY_MANAGE)
    async with database.session() as session:
        policy = await ImagePolicyRepository.get(session, project_id=project_id)
    if policy is None:
        raise NotFoundError("IMAGE_POLICY_NOT_FOUND", "Image policy not configured")
    return _policy_response(policy)


@router.put("/image-policy", response_model=ImagePolicyResponse)
async def replace_image_policy(
    project_id: uuid.UUID,
    payload: ImagePolicyUpdate,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> ImagePolicyResponse:
    _authorize(request, project_id, Permission.IMAGE_POLICY_MANAGE)
    try:
        async with database.session() as session, session.begin():
            policy = await ImagePolicyRepository.replace(
                session,
                project_id=project_id,
                default_action=payload.default_action,
                require_digest=payload.require_digest,
                rules=[rule.to_core() for rule in payload.rules],
            )
    except RegistryNotFoundError as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc
    return _policy_response(policy)


@router.post("/image-policy/evaluate", response_model=ImageEvaluationResponse)
async def evaluate_image(
    project_id: uuid.UUID,
    payload: ImageEvaluationRequest,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
) -> ImageEvaluationResponse:
    _authorize(request, project_id, Permission.TASK_CREATE)
    async with database.session() as session:
        decision = await ImagePolicyRepository.evaluate(
            session,
            project_id=project_id,
            image=payload.image,
        )
    return ImageEvaluationResponse(
        allowed=decision.allowed,
        canonical_image=decision.canonical_image,
        reason=decision.reason,
        matched_rule_id=decision.matched_rule_id,
    )


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
    except ProjectAccessDenied as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc
    try:
        require_permission(principal, permission)
    except PermissionDenied as exc:
        raise APIError(403, "PERMISSION_DENIED", "Permission denied") from exc
    return principal


def _secret_cipher(settings: Settings) -> SecretCipher:
    try:
        return SecretCipher.from_settings(settings)
    except SecretKeyConfigurationError as exc:
        raise ServiceUnavailableError(
            "SECRET_KEY_NOT_CONFIGURED",
            "Secret encryption is not configured",
        ) from exc


def _model_response(model: RegisteredModel) -> RegisteredModelResponse:
    return RegisteredModelResponse(
        id=model.id,
        project_id=model.project_id,
        name=model.name,
        provider=model.provider,
        source=model.source,
        revision=model.revision,
        size_bytes=model.size_bytes,
        required_gpu_memory_mb=model.gpu_memory_mb,
        architecture=model.architecture,
        metadata=model.metadata_json,
        created_by_user_id=model.created_by_user_id,
        created_at=model.created_at,
    )


def _policy_response(stored: StoredImagePolicy) -> ImagePolicyResponse:
    return ImagePolicyResponse(
        project_id=stored.policy.project_id,
        default_action=stored.policy.default_action,
        require_digest=stored.policy.require_digest,
        updated_at=stored.policy.updated_at,
        rules=[ImagePolicyRuleResponse.model_validate(rule) for rule in stored.rules],
    )
