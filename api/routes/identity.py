import hmac
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, status
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from api.dependencies import (
    get_app_settings,
    get_database,
    get_principal,
    require_api_permission,
)
from api.errors import APIError, ConflictError, NotFoundError, ServiceUnavailableError
from api.schemas.common import PaginationMeta
from api.schemas.identity import (
    ApiKeyCreate,
    ApiKeyCreated,
    ApiKeyResponse,
    BootstrapRequest,
    BootstrapResponse,
    MembershipCreate,
    MembershipResponse,
    MembershipUpdate,
    PrincipalResponse,
    ProjectCreate,
    ProjectCreated,
    ProjectListResponse,
    ProjectResponse,
    UserCreate,
    UserResponse,
)
from core.config import Settings
from core.database import Database
from core.enums import ProjectRole
from core.rbac import (
    MembershipStatus,
    Permission,
    Principal,
    PrincipalKind,
    can_issue_api_key_for_role,
    can_manage_membership_roles,
    require_project_access,
)
from core.security import hash_password_async
from models.identity import ApiKey, User
from repositories.identity import (
    ApiKeyRepository,
    IdentityNotFoundError,
    LastProjectOwnerError,
    MembershipNotActiveError,
    MembershipRepository,
    ProjectRepository,
    UserRepository,
)
from repositories.quotas import QuotaRepository

router = APIRouter(prefix="/api/v1", tags=["identity"])


@router.post("/bootstrap", response_model=BootstrapResponse, status_code=status.HTTP_201_CREATED)
async def bootstrap(
    payload: BootstrapRequest,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    bootstrap_token: Annotated[str | None, Header(alias="X-Bootstrap-Token")] = None,
) -> BootstrapResponse:
    if not settings.bootstrap_enabled:
        raise APIError(403, "BOOTSTRAP_DISABLED", "Bootstrap is disabled")
    if settings.bootstrap_token and (
        bootstrap_token is None
        or not hmac.compare_digest(bootstrap_token, settings.bootstrap_token)
    ):
        raise APIError(403, "INVALID_BOOTSTRAP_TOKEN", "The bootstrap token is invalid")
    password_hash = await hash_password_async(payload.user.password.get_secret_value())
    try:
        async with database.session() as session, session.begin():
            if session.get_bind().dialect.name == "postgresql":
                await session.execute(text("LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE"))
            if int(await session.scalar(select(func.count(User.id))) or 0):
                raise ConflictError("BOOTSTRAP_ALREADY_COMPLETED", "A user already exists")
            user = await UserRepository.create(
                session,
                username=payload.user.username,
                email=payload.user.email,
                password_hash=password_hash,
            )
            project, membership = await ProjectRepository.create_with_owner(
                session,
                name=payload.project.name,
                slug=payload.project.slug,
                owner_user_id=user.id,
            )
            await QuotaRepository.initialize(session, project_id=project.id)
            issued = await ApiKeyRepository.issue(
                session,
                project_id=project.id,
                user_id=user.id,
                name=payload.api_key_name,
                hmac_key=_api_key_hmac_key(settings),
                hash_key_id="v1",
                created_by_user_id=user.id,
            )
    except IntegrityError as exc:
        raise ConflictError(
            "BOOTSTRAP_ALREADY_COMPLETED", "Bootstrap was already completed"
        ) from exc
    return BootstrapResponse(
        user=UserResponse.model_validate(user),
        project=ProjectResponse.model_validate(project),
        membership=MembershipResponse.model_validate(membership),
        api_key=ApiKeyCreated.model_validate(
            {**ApiKeyResponse.model_validate(issued.api_key).model_dump(), "api_key": issued.token}
        ),
    )


@router.get("/auth/whoami", response_model=PrincipalResponse)
async def whoami(principal: Annotated[Principal, Depends(get_principal)]) -> PrincipalResponse:
    return PrincipalResponse.model_validate(principal)


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: UserCreate,
    database: Annotated[Database, Depends(get_database)],
    _principal: Annotated[Principal, Depends(require_api_permission(Permission.MEMBERSHIP_MANAGE))],
) -> UserResponse:
    password_hash = await hash_password_async(payload.password.get_secret_value())
    try:
        async with database.session() as session, session.begin():
            user = await UserRepository.create(
                session,
                username=payload.username,
                email=payload.email,
                password_hash=password_hash,
            )
    except IntegrityError as exc:
        raise ConflictError(
            "USER_ALREADY_EXISTS", "A user with this username or email already exists"
        ) from exc
    return UserResponse.model_validate(user)


@router.post("/projects", response_model=ProjectCreated, status_code=status.HTTP_201_CREATED)
async def create_project(
    payload: ProjectCreate,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(get_principal)],
) -> ProjectCreated:
    if principal.kind != PrincipalKind.API_KEY or principal.user_id is None:
        raise APIError(403, "PERMISSION_DENIED", "An authenticated user is required")
    try:
        async with database.session() as session, session.begin():
            project, _membership = await ProjectRepository.create_with_owner(
                session,
                name=payload.name,
                slug=payload.slug,
                owner_user_id=principal.user_id,
            )
            await QuotaRepository.initialize(session, project_id=project.id)
            issued = await ApiKeyRepository.issue(
                session,
                project_id=project.id,
                user_id=principal.user_id,
                name=payload.api_key_name,
                hmac_key=_api_key_hmac_key(settings),
                hash_key_id="v1",
                created_by_user_id=principal.user_id,
            )
    except IdentityNotFoundError as exc:
        raise APIError(403, "USER_DISABLED", "The authenticated user is not active") from exc
    except IntegrityError as exc:
        raise ConflictError(
            "PROJECT_SLUG_ALREADY_EXISTS", "A project with this slug already exists"
        ) from exc
    return ProjectCreated.model_validate(
        {
            **ProjectResponse.model_validate(project).model_dump(),
            "api_key": {
                **ApiKeyResponse.model_validate(issued.api_key).model_dump(),
                "api_key": issued.token,
            },
        }
    )


@router.get("/projects/current", response_model=ProjectResponse)
async def current_project(
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(get_principal)],
) -> ProjectResponse:
    if principal.project_id is None:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found")
    async with database.session() as session:
        project = await ProjectRepository.get(session, principal.project_id)
    if project is None:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found")
    return ProjectResponse.model_validate(project)


@router.get("/projects", response_model=ProjectListResponse)
async def list_projects(
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(get_principal)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ProjectListResponse:
    if principal.kind != PrincipalKind.API_KEY or principal.user_id is None:
        raise APIError(403, "PERMISSION_DENIED", "An authenticated user is required")
    async with database.session() as session:
        projects = await ProjectRepository.list_for_user(
            session,
            principal.user_id,
            limit=limit,
            offset=offset,
        )
        total = await ProjectRepository.count_for_user(session, principal.user_id)
    return ProjectListResponse(
        items=[ProjectResponse.model_validate(project) for project in projects],
        pagination=PaginationMeta(total=total, limit=limit, offset=offset),
    )


@router.post(
    "/projects/{project_id}/members",
    response_model=MembershipResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_member(
    project_id: uuid.UUID,
    payload: MembershipCreate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MEMBERSHIP_MANAGE))],
) -> MembershipResponse:
    _require_same_project(principal, project_id)
    if principal.user_id is None:
        raise APIError(403, "PERMISSION_DENIED", "An authenticated user is required")
    try:
        async with database.session() as session, session.begin():
            await ProjectRepository.get(session, project_id, for_update=True)
            existing = await MembershipRepository.get(
                session,
                project_id=project_id,
                user_id=payload.user_id,
                for_update=True,
            )
            current_role = (
                existing.role
                if existing is not None and existing.status == MembershipStatus.ACTIVE
                else None
            )
            _require_membership_authority(
                principal,
                current_role=current_role,
                requested_role=payload.role,
            )
            membership = await MembershipRepository.add_or_restore(
                session,
                project_id=project_id,
                user_id=payload.user_id,
                role=payload.role,
                created_by_user_id=principal.user_id,
            )
    except IdentityNotFoundError as exc:
        raise NotFoundError("MEMBERSHIP_TARGET_NOT_FOUND", "Project or user not found") from exc
    return MembershipResponse.model_validate(membership)


@router.patch("/projects/{project_id}/members/{user_id}", response_model=MembershipResponse)
async def change_member_role(
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    payload: MembershipUpdate,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MEMBERSHIP_MANAGE))],
) -> MembershipResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            await ProjectRepository.get(session, project_id, for_update=True)
            existing = await MembershipRepository.get(
                session,
                project_id=project_id,
                user_id=user_id,
                for_update=True,
            )
            if existing is None or existing.status != MembershipStatus.ACTIVE:
                raise MembershipNotActiveError("active project membership does not exist")
            _require_membership_authority(
                principal,
                current_role=existing.role,
                requested_role=payload.role,
            )
            membership = await MembershipRepository.change_role(
                session, project_id=project_id, user_id=user_id, role=payload.role
            )
    except (MembershipNotActiveError, IdentityNotFoundError) as exc:
        raise NotFoundError("MEMBERSHIP_NOT_FOUND", "Membership not found") from exc
    except LastProjectOwnerError as exc:
        raise ConflictError("LAST_PROJECT_OWNER", str(exc)) from exc
    return MembershipResponse.model_validate(membership)


@router.delete("/projects/{project_id}/members/{user_id}", response_model=MembershipResponse)
async def remove_member(
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.MEMBERSHIP_MANAGE))],
) -> MembershipResponse:
    _require_same_project(principal, project_id)
    try:
        async with database.session() as session, session.begin():
            await ProjectRepository.get(session, project_id, for_update=True)
            existing = await MembershipRepository.get(
                session,
                project_id=project_id,
                user_id=user_id,
                for_update=True,
            )
            if existing is None or existing.status != MembershipStatus.ACTIVE:
                raise MembershipNotActiveError("active project membership does not exist")
            _require_membership_authority(principal, current_role=existing.role)
            membership = await MembershipRepository.remove(
                session, project_id=project_id, user_id=user_id
            )
    except (MembershipNotActiveError, IdentityNotFoundError) as exc:
        raise NotFoundError("MEMBERSHIP_NOT_FOUND", "Membership not found") from exc
    except LastProjectOwnerError as exc:
        raise ConflictError("LAST_PROJECT_OWNER", str(exc)) from exc
    return MembershipResponse.model_validate(membership)


@router.post(
    "/projects/{project_id}/api-keys",
    response_model=ApiKeyCreated,
    status_code=status.HTTP_201_CREATED,
)
async def issue_api_key(
    project_id: uuid.UUID,
    payload: ApiKeyCreate,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.API_KEY_MANAGE))],
) -> ApiKeyCreated:
    _require_same_project(principal, project_id)
    if principal.user_id is None:
        raise APIError(403, "PERMISSION_DENIED", "An authenticated user is required")
    target_user_id = payload.user_id or principal.user_id
    try:
        async with database.session() as session, session.begin():
            await ProjectRepository.get(session, project_id, for_update=True)
            target_membership = await MembershipRepository.get(
                session,
                project_id=project_id,
                user_id=target_user_id,
                for_update=True,
            )
            if target_membership is None or target_membership.status != MembershipStatus.ACTIVE:
                raise MembershipNotActiveError("API key user is not an active project member")
            if not can_issue_api_key_for_role(principal, target_membership.role):
                raise APIError(
                    403,
                    "PERMISSION_DENIED",
                    "API keys cannot be issued for a role above the caller's authority",
                )
            issued = await ApiKeyRepository.issue(
                session,
                project_id=project_id,
                user_id=target_user_id,
                name=payload.name,
                hmac_key=_api_key_hmac_key(settings),
                hash_key_id="v1",
                created_by_user_id=principal.user_id,
                expires_at=payload.expires_at,
            )
    except (MembershipNotActiveError, IdentityNotFoundError) as exc:
        raise NotFoundError("MEMBERSHIP_NOT_FOUND", "Membership not found") from exc
    except ValueError as exc:
        raise APIError(422, "INVALID_API_KEY", "API key request is invalid") from exc
    return ApiKeyCreated.model_validate(
        {**ApiKeyResponse.model_validate(issued.api_key).model_dump(), "api_key": issued.token}
    )


@router.get("/projects/{project_id}/api-keys", response_model=list[ApiKeyResponse])
async def list_api_keys(
    project_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.API_KEY_MANAGE))],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[ApiKeyResponse]:
    _require_same_project(principal, project_id)
    async with database.session() as session:
        keys = await ApiKeyRepository.list_for_project(
            session, project_id, limit=limit, offset=offset
        )
    return [ApiKeyResponse.model_validate(item) for item in keys]


@router.delete("/projects/{project_id}/api-keys/{api_key_id}", response_model=ApiKeyResponse)
async def revoke_api_key(
    project_id: uuid.UUID,
    api_key_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.API_KEY_MANAGE))],
) -> ApiKeyResponse:
    _require_same_project(principal, project_id)
    async with database.session() as session, session.begin():
        api_key = await session.scalar(
            select(ApiKey).where(ApiKey.id == api_key_id, ApiKey.project_id == project_id)
        )
        if api_key is None:
            raise NotFoundError("API_KEY_NOT_FOUND", "API key not found")
        revoked = await ApiKeyRepository.revoke(session, api_key_id)
    if revoked is None:
        raise NotFoundError("API_KEY_NOT_FOUND", "API key not found")
    return ApiKeyResponse.model_validate(revoked)


def _require_same_project(principal: Principal, project_id: uuid.UUID) -> None:
    try:
        require_project_access(principal, project_id)
    except PermissionError as exc:
        # Use not-found semantics so project UUIDs cannot be probed across tenants.
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc


def _require_membership_authority(
    principal: Principal,
    *,
    current_role: ProjectRole | None = None,
    requested_role: ProjectRole | None = None,
) -> None:
    if not can_manage_membership_roles(
        principal,
        current_role=current_role,
        requested_role=requested_role,
    ):
        raise APIError(
            403,
            "PERMISSION_DENIED",
            "Membership roles cannot be managed above the caller's authority",
        )


def _api_key_hmac_key(settings: Settings) -> bytes:
    key = settings.api_key_pepper.encode("utf-8")
    if len(key) < 32:
        raise ServiceUnavailableError(
            "API_KEY_PEPPER_INVALID", "API key authentication is not configured"
        )
    return key
