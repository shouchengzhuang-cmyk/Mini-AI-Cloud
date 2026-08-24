import json
import re
import uuid
from datetime import datetime
from typing import Self

from pydantic import (
    Field,
    SecretStr,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.image_policy import (
    ImagePolicyAction,
    ImageRule,
    canonicalize_image_reference,
)
from models.service import ServingRuntime

_RESOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SECRET_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")


class RegisteredModelRuntimeDefaults(RequestModel):
    """Typed, allowlisted defaults copied into a model service at deploy time."""

    gpu_model: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    tensor_parallel_size: StrictInt | None = Field(default=None, ge=1, le=64)
    dtype: StrictStr = Field(
        default="auto",
        pattern=r"^(auto|half|float16|bfloat16|float|float32)$",
    )
    gpu_memory_utilization: StrictFloat = Field(default=0.9, gt=0, le=1)
    max_model_len: StrictInt | None = Field(default=None, ge=1, le=16_777_216)

    @field_validator("gpu_model")
    @classmethod
    def validate_gpu_model(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or any(ord(character) < 32 for character in normalized):
            raise ValueError("gpu_model must not be blank or contain control characters")
        return normalized


class RegisteredModelCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    provider: StrictStr = Field(min_length=1, max_length=64)
    source: StrictStr = Field(min_length=1, max_length=1024)
    revision: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    runtime: ServingRuntime = ServingRuntime.VLLM
    default_gpu_count: StrictInt = Field(default=0, ge=0, le=64)
    runtime_defaults: RegisteredModelRuntimeDefaults = Field(
        default_factory=RegisteredModelRuntimeDefaults
    )
    size_bytes: StrictInt | None = Field(default=None, ge=0, le=2_147_483_647)
    required_gpu_memory_mb: StrictInt | None = Field(default=None, ge=0, le=1_048_576)
    architecture: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    metadata: dict[str, object] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not _RESOURCE_NAME.fullmatch(normalized):
            raise ValueError("model name contains unsupported characters")
        return normalized

    @field_validator("provider", "source", "revision", "architecture")
    @classmethod
    def validate_reference(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or any(
            character.isspace() or ord(character) < 32 for character in normalized
        ):
            raise ValueError("model references must not contain whitespace or controls")
        return normalized

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, object]) -> dict[str, object]:
        try:
            encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata must be JSON serializable") from exc
        if len(encoded.encode("utf-8")) > 16_384:
            raise ValueError("metadata must be at most 16384 encoded bytes")
        return value

    @model_validator(mode="after")
    def validate_runtime_defaults(self) -> "RegisteredModelCreate":
        expected_tensor_parallel_size = max(1, self.default_gpu_count)
        if self.runtime == ServingRuntime.FAKE:
            if self.default_gpu_count != 0 or self.runtime_defaults.gpu_model is not None:
                raise ValueError("fake registered models cannot require GPU resources")
            expected_tensor_parallel_size = 1
        if self.runtime_defaults.tensor_parallel_size is None:
            self.runtime_defaults.tensor_parallel_size = expected_tensor_parallel_size
        elif self.runtime_defaults.tensor_parallel_size != expected_tensor_parallel_size:
            raise ValueError(
                "runtime_defaults.tensor_parallel_size must equal max(1, default_gpu_count)"
            )
        return self


class RegisteredModelResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    provider: str
    source: str
    revision: str | None
    runtime: ServingRuntime
    default_gpu_count: int = Field(ge=0, le=64)
    runtime_defaults: RegisteredModelRuntimeDefaults
    size_bytes: int | None = Field(ge=0)
    required_gpu_memory_mb: int | None = Field(ge=0)
    architecture: str | None
    metadata: dict[str, object]
    created_by_user_id: uuid.UUID | None
    created_at: datetime


class RegisteredModelListResponse(ResponseModel):
    items: list[RegisteredModelResponse]
    pagination: PaginationMeta


class SecretCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    value: SecretStr = Field(min_length=1, max_length=65_536, repr=False)
    description: StrictStr | None = Field(default=None, max_length=2_000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if not _SECRET_NAME.fullmatch(name):
            raise ValueError("secret name contains unsupported characters")
        return name

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: SecretStr) -> SecretStr:
        plaintext = value.get_secret_value()
        if "\x00" in plaintext or len(plaintext.encode("utf-8")) > 65_536:
            raise ValueError("secret value must be at most 65536 encoded bytes without NUL")
        return value

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        description = value.strip()
        if "\x00" in description:
            raise ValueError("secret description must not contain NUL")
        return description or None


class SecretRotate(RequestModel):
    value: SecretStr = Field(min_length=1, max_length=65_536, repr=False)

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: SecretStr) -> SecretStr:
        return SecretCreate.validate_value(value)


class SecretResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    description: str | None
    current_version: int = Field(ge=1)
    revoked_at: datetime | None
    created_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class SecretListResponse(ResponseModel):
    items: list[SecretResponse]
    pagination: PaginationMeta


class ImagePolicyRuleInput(RequestModel):
    action: ImagePolicyAction
    registry: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    repository_glob: StrictStr = Field(min_length=1, max_length=512)
    tag_glob: StrictStr | None = Field(default=None, min_length=1, max_length=128)
    digest: StrictStr | None = Field(default=None, min_length=71, max_length=71)
    priority: StrictInt = Field(default=100, ge=0, le=1_000_000)

    @model_validator(mode="after")
    def validate_rule(self) -> Self:
        rule = self.to_core()
        self.registry = rule.registry
        self.repository_glob = rule.repository_glob
        self.tag_glob = rule.tag_glob
        self.digest = rule.digest
        return self

    def to_core(self) -> ImageRule:
        return ImageRule(
            action=self.action,
            registry=self.registry,
            repository_glob=self.repository_glob,
            tag_glob=self.tag_glob,
            digest=self.digest,
            priority=self.priority,
        )


class ImagePolicyUpdate(RequestModel):
    default_action: ImagePolicyAction = ImagePolicyAction.DENY
    require_digest: bool = True
    rules: list[ImagePolicyRuleInput] = Field(default_factory=list, max_length=256)


class ImagePolicyRuleResponse(ResponseModel):
    id: uuid.UUID
    action: ImagePolicyAction
    registry: str | None = Field(validation_alias="registry_host")
    repository_glob: str
    tag_glob: str | None
    digest: str | None
    priority: int = Field(ge=0)


class ImagePolicyResponse(ResponseModel):
    project_id: uuid.UUID
    default_action: ImagePolicyAction
    require_digest: bool
    updated_at: datetime
    rules: list[ImagePolicyRuleResponse]


class ImageEvaluationRequest(RequestModel):
    image: StrictStr = Field(min_length=1, max_length=512)

    @field_validator("image")
    @classmethod
    def validate_image(cls, value: str) -> str:
        canonicalize_image_reference(value)
        return value.strip()


class ImageEvaluationResponse(ResponseModel):
    allowed: bool
    canonical_image: str
    reason: str
    matched_rule_id: uuid.UUID | None
