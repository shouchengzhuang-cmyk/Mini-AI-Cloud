import re
import uuid
from datetime import datetime
from typing import Literal

from pydantic import (
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.enums import RuntimeType
from models.service import (
    ReplicaHealth,
    ReplicaStatus,
    ServiceStatus,
    ServingRuntime,
)

_SERVICE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
VLLMDType = Literal["auto", "half", "float16", "bfloat16", "float", "float32"]


class AutoscalingConfig(RequestModel):
    enabled: StrictBool = True
    min_replicas: StrictInt = Field(default=1, ge=0, le=1000)
    max_replicas: StrictInt = Field(default=4, ge=1, le=1000)
    target_concurrency: StrictInt = Field(default=8, ge=1, le=1_000_000)
    cooldown_seconds: StrictInt = Field(default=60, ge=0, le=86_400)

    @model_validator(mode="after")
    def validate_bounds(self) -> "AutoscalingConfig":
        if self.max_replicas < self.min_replicas:
            raise ValueError("max_replicas must be greater than or equal to min_replicas")
        return self


class ServiceCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=128)
    model: StrictStr | None = Field(default=None, min_length=1, max_length=512)
    registered_model_id: uuid.UUID | None = None
    model_revision: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    runtime: ServingRuntime = ServingRuntime.VLLM
    runtime_type: RuntimeType = RuntimeType.DOCKER
    image: StrictStr | None = Field(default=None, min_length=1, max_length=512)
    cpu_millicores: StrictInt = Field(default=1000, ge=1, le=1_024_000)
    memory_mb: StrictInt = Field(default=1024, ge=16, le=1_048_576)
    gpu_count: StrictInt = Field(default=0, ge=0, le=64)
    gpu_memory_mb: StrictInt = Field(default=0, ge=0, le=1_048_576)
    gpu_model: StrictStr | None = Field(default=None, min_length=1, max_length=255)
    tensor_parallel_size: StrictInt | None = Field(default=None, ge=1, le=64)
    dtype: VLLMDType = "auto"
    gpu_memory_utilization: StrictFloat = Field(default=0.9, gt=0, le=1)
    max_model_len: StrictInt | None = Field(default=None, ge=1, le=16_777_216)
    replicas: StrictInt = Field(default=1, ge=0, le=1000)
    autoscaling: AutoscalingConfig | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if not _SERVICE_NAME.fullmatch(name):
            raise ValueError(
                "name must start alphanumeric and contain only letters, digits, '.', '_' or '-'"
            )
        return name

    @field_validator("model", "model_revision", "image")
    @classmethod
    def validate_reference(cls, value: str | None) -> str | None:
        if value is None:
            return None
        reference = value.strip()
        if not reference or any(
            character.isspace() or ord(character) < 32 for character in reference
        ):
            raise ValueError(
                "model and image references must not contain whitespace or control characters"
            )
        return reference

    @field_validator("gpu_model")
    @classmethod
    def validate_gpu_model(cls, value: str | None) -> str | None:
        if value is None:
            return None
        model = value.strip()
        if not model or any(ord(character) < 32 for character in model):
            raise ValueError("gpu_model must not be blank or contain control characters")
        return model

    @model_validator(mode="after")
    def validate_initial_replica_count(self) -> "ServiceCreate":
        if self.model is None and self.registered_model_id is None:
            raise ValueError("model or registered_model_id is required")

        # Registry-backed requests are validated again after the project-scoped
        # registry defaults have been resolved into a complete service snapshot.
        if self.registered_model_id is not None:
            return self._validate_autoscaling_bounds()

        if self.runtime == ServingRuntime.FAKE:
            if self.runtime_type != RuntimeType.FAKE:
                raise ValueError("fake serving runtime requires runtime_type='fake'")
            if self.gpu_count or self.gpu_memory_mb or self.gpu_model is not None:
                raise ValueError("fake serving runtime does not accept GPU resources")
            if self.tensor_parallel_size not in {None, 1}:
                raise ValueError("CPU and fake services require tensor_parallel_size=1")
            self.tensor_parallel_size = 1
        elif self.runtime == ServingRuntime.VLLM:
            if self.runtime_type != RuntimeType.DOCKER:
                raise ValueError("vllm serving runtime currently requires runtime_type='docker'")
            expected_tensor_parallel_size = max(1, self.gpu_count)
            if self.tensor_parallel_size is None:
                self.tensor_parallel_size = expected_tensor_parallel_size
            if self.tensor_parallel_size != expected_tensor_parallel_size:
                raise ValueError(
                    "GPU vllm services require tensor_parallel_size=gpu_count; "
                    "CPU vllm services require tensor_parallel_size=1"
                )
            if self.gpu_count == 0 and (self.gpu_memory_mb or self.gpu_model is not None):
                raise ValueError(
                    "GPU model and memory requirements need gpu_count greater than zero"
                )

        return self._validate_autoscaling_bounds()

    def _validate_autoscaling_bounds(self) -> "ServiceCreate":
        autoscaling = self.autoscaling
        if (
            autoscaling is not None
            and autoscaling.enabled
            and not autoscaling.min_replicas <= self.replicas <= autoscaling.max_replicas
        ):
            raise ValueError("replicas must be within enabled autoscaling min/max bounds")
        return self


class ServiceScale(RequestModel):
    replicas: StrictInt = Field(ge=0, le=1000)


class ServiceResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    registered_model_id: uuid.UUID | None
    name: str
    model: str
    model_revision: str | None
    runtime: ServingRuntime
    runtime_type: RuntimeType
    image: str | None
    cpu_millicores: int = Field(ge=1)
    memory_mb: int = Field(ge=16)
    gpu_count: int = Field(ge=0)
    gpu_memory_mb: int = Field(ge=0)
    gpu_model: str | None
    tensor_parallel_size: int = Field(ge=1)
    dtype: VLLMDType
    gpu_memory_utilization: float = Field(gt=0, le=1)
    max_model_len: int | None = Field(default=None, ge=1)
    desired_replicas: int = Field(ge=0)
    actual_replicas: int = Field(ge=0)
    healthy_replicas: int = Field(ge=0)
    autoscaling: AutoscalingConfig
    last_scaled_at: datetime | None
    generation: int = Field(ge=1)
    status: ServiceStatus
    scheduling_reason: str | None
    scheduling_details: dict[str, object]
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    stopped_at: datetime | None
    version: int = Field(ge=1)


class ServiceListResponse(ResponseModel):
    items: list[ServiceResponse]
    pagination: PaginationMeta


class ServiceReplicaResponse(ResponseModel):
    id: uuid.UUID
    service_id: uuid.UUID
    runtime: ServingRuntime
    generation: int = Field(ge=1)
    ordinal: int = Field(ge=0)
    status: ReplicaStatus
    health: ReplicaHealth
    worker_id: str | None
    execution_id: uuid.UUID | None
    endpoint_url: str | None
    lease_expires_at: datetime | None
    last_health_at: datetime | None
    health_failure_count: int = Field(ge=0)
    active_requests: int = Field(ge=0)
    model_revision: str | None
    image_digest: str | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    container_started_at: datetime | None
    ready_at: datetime | None
    drain_started_at: datetime | None
    drain_deadline: datetime | None
    stopped_at: datetime | None
    version: int = Field(ge=1)


class ServiceReplicaListResponse(ResponseModel):
    service_id: uuid.UUID
    items: list[ServiceReplicaResponse]


class ServiceEndpointResponse(ResponseModel):
    service_id: uuid.UUID
    replica_id: uuid.UUID
    generation: int = Field(ge=1)
    execution_id: uuid.UUID
    endpoint_url: str
