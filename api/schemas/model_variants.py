import json
import re
import uuid
from datetime import datetime
from typing import Self

from pydantic import Field, StrictStr, field_validator, model_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.accelerators import vendor_kind_is_compatible
from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    GatewayRoutingPolicy,
    ModelAvailabilityStatus,
)

_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PROFILE_ID = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_SEMANTIC_VERSION = re.compile(r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMPATIBILITY_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]{0,254}$")


def _validated_metadata(value: dict[str, object]) -> dict[str, object]:
    try:
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as error:
        raise ValueError("metadata must be JSON serializable") from error
    if len(encoded.encode("utf-8")) > 16_384:
        raise ValueError("metadata must be at most 16384 encoded bytes")
    return value


def _normalized_reference(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized or any(
        character.isspace() or ord(character) < 32 for character in normalized
    ):
        raise ValueError(f"{field} must not contain whitespace or control characters")
    return normalized


class LogicalModelCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    public_name: StrictStr = Field(min_length=1, max_length=255)
    routing_policy: GatewayRoutingPolicy = GatewayRoutingPolicy.BALANCED
    description: StrictStr | None = Field(default=None, max_length=2_000)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not _RESOURCE_NAME.fullmatch(normalized):
            raise ValueError("logical model name contains unsupported characters")
        return normalized

    @field_validator("public_name")
    @classmethod
    def validate_public_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or any(ord(character) < 32 for character in normalized):
            raise ValueError("public_name must not be blank or contain control characters")
        return normalized

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if any(ord(character) < 32 and character not in "\n\t" for character in normalized):
            raise ValueError("description contains unsupported control characters")
        return normalized or None

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, object]) -> dict[str, object]:
        return _validated_metadata(value)


class LogicalModelStatusUpdate(RequestModel):
    status: ModelAvailabilityStatus
    reason: StrictStr = Field(min_length=1, max_length=2_000)

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or any(
            ord(character) < 32 and character != "\t" for character in normalized
        ):
            raise ValueError("reason must not be blank or contain control characters")
        return normalized


class LogicalModelRoutingPolicyUpdate(RequestModel):
    routing_policy: GatewayRoutingPolicy


class LogicalModelResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    public_name: str
    description: str | None
    status: ModelAvailabilityStatus
    routing_policy: GatewayRoutingPolicy
    metadata: dict[str, object]
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    version: int = Field(ge=1)


class LogicalModelListResponse(ResponseModel):
    items: list[LogicalModelResponse]
    pagination: PaginationMeta


class ModelVariantCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    runtime_profile_id: StrictStr = Field(min_length=3, max_length=64)
    runtime_profile_version: StrictStr = Field(min_length=5, max_length=32)
    runtime_profile_digest: StrictStr = Field(min_length=71, max_length=71)
    artifact_source: StrictStr = Field(min_length=1, max_length=1_024)
    artifact_revision: StrictStr = Field(min_length=1, max_length=255)
    artifact_digest: StrictStr = Field(min_length=71, max_length=71)
    architecture: StrictStr = Field(min_length=1, max_length=255)
    dtype: StrictStr = Field(min_length=1, max_length=64)
    quantization: StrictStr | None = Field(default=None, min_length=1, max_length=128)
    status: ModelAvailabilityStatus = ModelAvailabilityStatus.DISABLED
    status_reason: StrictStr | None = Field(default=None, max_length=2_000)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not _RESOURCE_NAME.fullmatch(normalized):
            raise ValueError("model variant name contains unsupported characters")
        return normalized

    @field_validator("runtime_profile_id")
    @classmethod
    def validate_profile_id(cls, value: str) -> str:
        normalized = value.strip()
        if not _PROFILE_ID.fullmatch(normalized):
            raise ValueError("runtime_profile_id is malformed")
        return normalized

    @field_validator("runtime_profile_version")
    @classmethod
    def validate_profile_version(cls, value: str) -> str:
        normalized = value.strip()
        if not _SEMANTIC_VERSION.fullmatch(normalized):
            raise ValueError("runtime_profile_version must be a stable semantic version")
        return normalized

    @field_validator("runtime_profile_digest", "artifact_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        normalized = value.strip().casefold()
        if not _DIGEST.fullmatch(normalized):
            raise ValueError("digests must use sha256 followed by 64 lowercase hex characters")
        return normalized

    @field_validator("artifact_source", "artifact_revision")
    @classmethod
    def validate_artifact_reference(cls, value: str, info: object) -> str:
        field_name = getattr(info, "field_name", "artifact reference")
        return _normalized_reference(value, str(field_name))

    @field_validator("architecture", "dtype", "quantization")
    @classmethod
    def validate_compatibility_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not _COMPATIBILITY_NAME.fullmatch(normalized):
            raise ValueError("compatibility names contain unsupported characters")
        return normalized

    @field_validator("status_reason")
    @classmethod
    def validate_status_reason(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or any(
            ord(character) < 32 and character != "\t" for character in normalized
        ):
            raise ValueError("status_reason must not be blank or contain control characters")
        return normalized

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, object]) -> dict[str, object]:
        return _validated_metadata(value)

    @model_validator(mode="after")
    def validate_variant_contract(self) -> Self:
        if not vendor_kind_is_compatible(self.vendor, self.kind):
            raise ValueError(f"{self.vendor.value} is not compatible with kind={self.kind.value}")
        if self.status is ModelAvailabilityStatus.DEGRADED and self.status_reason is None:
            raise ValueError("degraded model variants require status_reason")
        return self


class ModelVariantStatusUpdate(RequestModel):
    status: ModelAvailabilityStatus
    reason: StrictStr = Field(min_length=1, max_length=2_000)

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        return LogicalModelStatusUpdate.validate_reason(value)


class ModelVariantResponse(ResponseModel):
    id: uuid.UUID
    logical_model_id: uuid.UUID
    name: str
    vendor: AcceleratorVendor
    kind: AcceleratorKind = Field(validation_alias="accelerator_kind")
    runtime_profile_id: str
    runtime_profile_version: str
    runtime_profile_digest: str
    artifact_source: str
    artifact_revision: str
    artifact_digest: str
    architecture: str
    dtype: str
    quantization: str | None
    status: ModelAvailabilityStatus
    status_reason: str | None
    metadata: dict[str, object]
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime
    version: int = Field(ge=1)


class ModelVariantListResponse(ResponseModel):
    items: list[ModelVariantResponse]
    pagination: PaginationMeta


class LogicalModelStatusEventResponse(ResponseModel):
    id: uuid.UUID
    logical_model_id: uuid.UUID
    from_status: ModelAvailabilityStatus | None
    to_status: ModelAvailabilityStatus
    reason: str
    model_version: int = Field(ge=1)
    created_by_user_id: uuid.UUID | None
    created_at: datetime


class LogicalModelStatusHistoryResponse(ResponseModel):
    items: list[LogicalModelStatusEventResponse]
    pagination: PaginationMeta
