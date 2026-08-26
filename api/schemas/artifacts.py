import re
import uuid
from datetime import datetime
from typing import Literal

from pydantic import Field, StrictInt, StrictStr, field_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.artifacts import ArtifactState, normalize_sha256

_CONTENT_TYPE_PATTERN = re.compile(r"^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$")


class ArtifactCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=255)
    content_type: StrictStr = Field(default="application/octet-stream", max_length=255)
    size_bytes: StrictInt = Field(ge=0)
    sha256: StrictStr = Field(min_length=64, max_length=64)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if (
            not name
            or name in {".", ".."}
            or any(character in name for character in ("/", "\\", "\x00", "\r", "\n"))
            or any(ord(character) < 32 for character in name)
        ):
            raise ValueError("artifact name contains unsafe characters")
        return name

    @field_validator("content_type")
    @classmethod
    def validate_content_type(cls, value: str) -> str:
        content_type = value.strip().casefold()
        if not _CONTENT_TYPE_PATTERN.fullmatch(content_type):
            raise ValueError("content_type must be a simple media type without parameters")
        return content_type

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return normalize_sha256(value)


class ArtifactFinalize(RequestModel):
    size_bytes: StrictInt = Field(ge=0)
    sha256: StrictStr = Field(min_length=64, max_length=64)

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return normalize_sha256(value)


class ArtifactResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    state: ArtifactState
    backend: Literal["local", "s3"]
    content_type: str | None
    size_bytes: int | None = Field(ge=0)
    sha256: str | None
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    verified_at: datetime | None
    deleted_at: datetime | None
    failure_reason: str | None


class ArtifactListResponse(ResponseModel):
    items: list[ArtifactResponse]
    pagination: PaginationMeta


class ArtifactTransferResponse(ResponseModel):
    method: Literal["GET", "PUT"]
    url: str = Field(min_length=1, max_length=16_384)
    headers: dict[str, str] = Field(default_factory=dict)
    expires_at: datetime | None
    authorization: Literal["presigned", "api"]
