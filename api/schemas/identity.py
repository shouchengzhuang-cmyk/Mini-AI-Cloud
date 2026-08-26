import uuid
from datetime import datetime

from pydantic import Field, SecretStr, StrictStr, field_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.rbac import (
    MembershipStatus,
    PrincipalKind,
    ProjectRole,
    ProjectStatus,
    UserStatus,
)
from core.security import normalize_email, normalize_project_slug, normalize_username


class UserCreate(RequestModel):
    username: StrictStr = Field(min_length=3, max_length=64)
    email: StrictStr = Field(min_length=3, max_length=320)
    password: SecretStr = Field(min_length=12, max_length=1024, repr=False)

    @field_validator("username")
    @classmethod
    def validate_username(cls, value: str) -> str:
        normalize_username(value)
        return value.strip()

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        normalize_email(value)
        return value.strip()

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: SecretStr) -> SecretStr:
        password = value.get_secret_value()
        if "\x00" in password:
            raise ValueError("password must not contain NUL")
        if len(password.encode("utf-8")) > 1024:
            raise ValueError("password must be at most 1024 encoded bytes")
        return value


class UserResponse(ResponseModel):
    id: uuid.UUID
    username: str
    email: str
    status: UserStatus
    created_at: datetime
    updated_at: datetime
    password_changed_at: datetime
    version: int = Field(ge=1)


class ProjectCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    slug: StrictStr = Field(min_length=2, max_length=63)
    api_key_name: StrictStr = Field(default="initial", min_length=1, max_length=128)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if not name or "\x00" in name:
            raise ValueError("project name must not be blank or contain NUL")
        return name

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return normalize_project_slug(value)


class ProjectResponse(ResponseModel):
    id: uuid.UUID
    name: str
    slug: str
    status: ProjectStatus
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    version: int = Field(ge=1)


class ProjectListResponse(ResponseModel):
    items: list[ProjectResponse]
    pagination: PaginationMeta


class MembershipCreate(RequestModel):
    user_id: uuid.UUID
    role: ProjectRole = ProjectRole.MEMBER


class MembershipUpdate(RequestModel):
    role: ProjectRole


class MembershipResponse(ResponseModel):
    project_id: uuid.UUID
    user_id: uuid.UUID
    role: ProjectRole
    status: MembershipStatus
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    removed_at: datetime | None
    version: int = Field(ge=1)


class ApiKeyCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    user_id: uuid.UUID | None = None
    expires_at: datetime | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if not name or "\x00" in name:
            raise ValueError("API key name must not be blank or contain NUL")
        return name

    @field_validator("expires_at")
    @classmethod
    def validate_expiration_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("API key expiration must include a timezone")
        return value


class ApiKeyResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    user_id: uuid.UUID
    name: str
    key_prefix: str
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    expires_at: datetime | None
    revoked_at: datetime | None
    last_used_at: datetime | None
    version: int = Field(ge=1)


class ApiKeyCreated(ApiKeyResponse):
    api_key: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}$",
        repr=False,
    )


class ProjectCreated(ProjectResponse):
    api_key: ApiKeyCreated


class PrincipalResponse(ResponseModel):
    kind: PrincipalKind
    project_id: uuid.UUID | None
    user_id: uuid.UUID | None
    api_key_id: uuid.UUID | None
    role: ProjectRole | None
    key_prefix: str | None


class BootstrapRequest(RequestModel):
    user: UserCreate
    project: ProjectCreate
    api_key_name: StrictStr = Field(default="bootstrap", min_length=1, max_length=128)


class BootstrapResponse(ResponseModel):
    user: UserResponse
    project: ProjectResponse
    membership: MembershipResponse
    api_key: ApiKeyCreated
