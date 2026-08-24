import re
import uuid
from datetime import datetime

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.enums import RuntimeType
from models.service import (
    ReplicaHealth,
    ReplicaStatus,
    ServiceStatus,
    ServingRuntime,
)

_SERVICE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


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
    model: StrictStr = Field(min_length=1, max_length=512)
    runtime: ServingRuntime = ServingRuntime.VLLM
    runtime_type: RuntimeType = RuntimeType.DOCKER
    image: StrictStr | None = Field(default=None, min_length=1, max_length=512)
    cpu_millicores: StrictInt = Field(default=1000, ge=1, le=1_024_000)
    memory_mb: StrictInt = Field(default=1024, ge=16, le=1_048_576)
    gpu_count: StrictInt = Field(default=0, ge=0, le=64)
    gpu_memory_mb: StrictInt = Field(default=0, ge=0, le=1_048_576)
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

    @field_validator("model", "image")
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

    @model_validator(mode="after")
    def validate_initial_replica_count(self) -> "ServiceCreate":
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
    name: str
    model: str
    runtime: ServingRuntime
    runtime_type: RuntimeType
    image: str | None
    cpu_millicores: int = Field(ge=1)
    memory_mb: int = Field(ge=16)
    gpu_count: int = Field(ge=0)
    gpu_memory_mb: int = Field(ge=0)
    desired_replicas: int = Field(ge=0)
    actual_replicas: int = Field(ge=0)
    healthy_replicas: int = Field(ge=0)
    autoscaling: AutoscalingConfig
    last_scaled_at: datetime | None
    generation: int = Field(ge=1)
    status: ServiceStatus
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
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
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
