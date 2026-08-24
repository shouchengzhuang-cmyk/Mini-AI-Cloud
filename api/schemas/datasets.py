import json
import uuid
from datetime import datetime

from pydantic import Field, StrictStr, field_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel


def _validated_metadata(value: dict[str, object]) -> dict[str, object]:
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > 16_384:
        raise ValueError("metadata must be at most 16384 encoded bytes")
    return value


class DatasetCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=255)
    description: StrictStr | None = Field(default=None, max_length=4096)
    artifact_id: uuid.UUID
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if (
            not normalized
            or normalized in {".", ".."}
            or any(character in normalized for character in ("/", "\\", "\x00", "\r", "\n"))
            or any(ord(character) < 32 for character in normalized)
        ):
            raise ValueError("dataset name contains unsafe characters")
        return normalized

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, object]) -> dict[str, object]:
        return _validated_metadata(value)


class DatasetVersionCreate(RequestModel):
    artifact_id: uuid.UUID
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, object]) -> dict[str, object]:
        return _validated_metadata(value)


class DatasetVersionResponse(ResponseModel):
    dataset_id: uuid.UUID
    version: int = Field(ge=1)
    artifact_id: uuid.UUID
    metadata: dict[str, object] = Field(validation_alias="metadata_json")
    created_at: datetime


class DatasetResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    description: str | None
    current_version: int = Field(ge=1)
    current_artifact_id: uuid.UUID
    current_metadata: dict[str, object]
    created_at: datetime


class DatasetListResponse(ResponseModel):
    items: list[DatasetResponse]
    pagination: PaginationMeta
