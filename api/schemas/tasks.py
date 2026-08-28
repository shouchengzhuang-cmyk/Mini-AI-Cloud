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

from api.schemas.accelerators import (
    AcceleratorRequest,
    reconcile_legacy_gpu_fields,
    require_current_execution_support,
)
from api.schemas.common import PaginationMeta, PaginationQuery, RequestModel, ResponseModel
from api.schemas.task_artifacts import TaskInputArtifact, TaskOutputArtifact
from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    AllocationAuthority,
    ErrorCategory,
    ErrorCode,
    LogStream,
    RetryBackoff,
    RuntimeType,
    TaskStatus,
    WorkloadType,
)

_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LABEL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")


def _validate_labels(labels: dict[str, str]) -> dict[str, str]:
    if len(labels) > 64:
        raise ValueError("labels may contain at most 64 entries")

    validated: dict[str, str] = {}
    for raw_key, raw_value in labels.items():
        key = raw_key.strip()
        value = raw_value.strip()
        if not _LABEL_NAME.fullmatch(key):
            raise ValueError(
                "label names must be 1-63 characters using letters, digits, '.', '_' or '-'"
            )
        if len(value) > 255 or "\x00" in value:
            raise ValueError("label values must be at most 255 characters and contain no NUL")
        validated[key] = value
    return validated


class TaskSecretBindingInput(RequestModel):
    secret_id: uuid.UUID
    version: StrictInt = Field(ge=1)
    env_name: StrictStr = Field(min_length=1, max_length=255)

    @field_validator("env_name")
    @classmethod
    def validate_env_name(cls, value: str) -> str:
        if not _ENVIRONMENT_NAME.fullmatch(value):
            raise ValueError("env_name must be a valid environment variable name")
        return value


class RetryPolicy(RequestModel):
    max_attempts: StrictInt = Field(default=1, ge=1, le=101)
    backoff: RetryBackoff = RetryBackoff.EXPONENTIAL
    base_seconds: StrictFloat = Field(default=1.0, gt=0, le=86_400)
    max_seconds: StrictFloat = Field(default=60.0, gt=0, le=86_400)
    retry_on_exit_codes: list[StrictInt] = Field(default_factory=lambda: [1, 137], max_length=256)

    @field_validator("retry_on_exit_codes")
    @classmethod
    def validate_retry_exit_codes(cls, exit_codes: list[int]) -> list[int]:
        if len(exit_codes) != len(set(exit_codes)):
            raise ValueError("retry_on_exit_codes must be unique")
        if any(code < 0 or code > 255 for code in exit_codes):
            raise ValueError("retry_on_exit_codes must be between 0 and 255")
        return exit_codes

    @model_validator(mode="after")
    def validate_backoff_bounds(self) -> "RetryPolicy":
        if self.max_seconds < self.base_seconds:
            raise ValueError("max_seconds must be greater than or equal to base_seconds")
        return self


class TaskCreate(RequestModel):
    workload_type: WorkloadType = WorkloadType.BATCH_JOB
    runtime_type: RuntimeType = RuntimeType.DOCKER
    image: StrictStr = Field(min_length=1, max_length=512)
    command: list[StrictStr] = Field(min_length=1, max_length=256)
    environment: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    timeout_seconds: StrictInt | None = Field(default=None, ge=1, le=86_400)
    max_retries: StrictInt = Field(default=0, ge=0, le=100)
    retry_policy: RetryPolicy | None = None
    cpu_limit: StrictFloat = Field(default=1.0, gt=0, le=1024)
    memory_limit_mb: StrictInt = Field(default=256, ge=16, le=1_048_576)
    labels: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    network_enabled: StrictBool = False
    accelerator: AcceleratorRequest | None = Field(
        default=None,
        description=("Vendor-neutral NVIDIA/Ascend accelerator request used by A9 admission."),
    )
    gpu_count: StrictInt = Field(
        default=0,
        ge=0,
        le=64,
        json_schema_extra={"deprecated": True},
        description="Deprecated v0.4 NVIDIA GPU count; use accelerator.count.",
    )
    gpu_memory_mb: StrictInt = Field(
        default=0,
        ge=0,
        le=1_048_576,
        json_schema_extra={"deprecated": True},
        description=(
            "Deprecated v0.4 memory per NVIDIA GPU; use accelerator.memory_mb_per_device."
        ),
    )
    gpu_model: StrictStr | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        json_schema_extra={"deprecated": True},
        description="Deprecated v0.4 NVIDIA model; use accelerator.allowed_models.",
    )
    tolerations: list[dict[StrictStr, StrictStr]] = Field(default_factory=list, max_length=64)
    priority: StrictInt = Field(default=50, ge=0, le=100)
    preemptible: StrictBool = False
    secret_bindings: list[TaskSecretBindingInput] = Field(default_factory=list, max_length=64)
    inputs: list[TaskInputArtifact] = Field(default_factory=list, max_length=100)
    artifacts: list[TaskOutputArtifact] = Field(default_factory=list, max_length=100)
    depends_on: list[uuid.UUID] = Field(default_factory=list, max_length=1_000)
    dependency_failure_policy: Literal["block", "cancel"] = "cancel"

    @field_validator("image")
    @classmethod
    def validate_image(cls, image: str) -> str:
        image = image.strip()
        if not image:
            raise ValueError("image must not be blank")
        if any(character.isspace() or ord(character) < 32 for character in image):
            raise ValueError("image must not contain whitespace or control characters")
        return image

    @field_validator("command")
    @classmethod
    def validate_command(cls, command: list[str]) -> list[str]:
        if not command[0].strip():
            raise ValueError("the executable in command[0] must not be blank")
        if any("\x00" in item for item in command):
            raise ValueError("command arguments must not contain NUL")
        if any(len(item) > 8192 for item in command):
            raise ValueError("each command argument must be at most 8192 characters")
        if sum(len(item.encode("utf-8")) for item in command) > 65_536:
            raise ValueError("the encoded command must be at most 65536 bytes")
        return command

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, environment: dict[str, str]) -> dict[str, str]:
        if len(environment) > 256:
            raise ValueError("environment may contain at most 256 variables")
        for name, value in environment.items():
            if len(name) > 255 or not _ENVIRONMENT_NAME.fullmatch(name):
                raise ValueError(f"invalid environment variable name: {name!r}")
            if len(value) > 32_768 or "\x00" in value:
                raise ValueError(
                    "environment values must be at most 32768 characters and contain no NUL"
                )
        return environment

    @field_validator("labels")
    @classmethod
    def validate_labels(cls, labels: dict[str, str]) -> dict[str, str]:
        return _validate_labels(labels)

    @field_validator("gpu_model")
    @classmethod
    def validate_gpu_model(cls, value: str | None) -> str | None:
        if value is None:
            return None
        model = value.strip()
        if not model or any(ord(character) < 32 for character in model):
            raise ValueError("gpu_model must not be blank or contain control characters")
        return model

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, dependencies: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(dependencies) != len(set(dependencies)):
            raise ValueError("depends_on task IDs must be unique")
        return dependencies

    @field_validator("inputs")
    @classmethod
    def validate_input_artifacts(cls, inputs: list[TaskInputArtifact]) -> list[TaskInputArtifact]:
        artifact_ids = [item.artifact_id for item in inputs]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("input artifact IDs must be unique")
        return inputs

    @field_validator("artifacts")
    @classmethod
    def validate_output_artifacts(
        cls, artifacts: list[TaskOutputArtifact]
    ) -> list[TaskOutputArtifact]:
        names = [item.name for item in artifacts]
        paths = [item.path for item in artifacts]
        if len(names) != len(set(names)):
            raise ValueError("output artifact names must be unique")
        if len(paths) != len(set(paths)):
            raise ValueError("output artifact paths must be unique")
        return artifacts

    @model_validator(mode="after")
    def validate_secret_environment(self) -> "TaskCreate":
        gpu_count, gpu_memory_mb, gpu_model = reconcile_legacy_gpu_fields(
            accelerator=self.accelerator,
            gpu_count=self.gpu_count,
            gpu_memory_mb=self.gpu_memory_mb,
            gpu_model=self.gpu_model,
            fields_set=self.model_fields_set,
        )
        object.__setattr__(self, "gpu_count", gpu_count)
        object.__setattr__(self, "gpu_memory_mb", gpu_memory_mb)
        object.__setattr__(self, "gpu_model", gpu_model)
        secret_names = [binding.env_name for binding in self.secret_bindings]
        if len(secret_names) != len(set(secret_names)):
            raise ValueError("secret binding env_name values must be unique")
        collisions = sorted(set(secret_names).intersection(self.environment))
        if collisions:
            raise ValueError(
                "secret binding env_name must not overlap environment: " + ", ".join(collisions)
            )
        if self.retry_policy is not None:
            policy_retries = self.retry_policy.max_attempts - 1
            if self.max_retries not in {0, policy_retries}:
                raise ValueError(
                    "max_retries conflicts with retry_policy.max_attempts; "
                    "max_attempts includes the initial attempt"
                )
        return self

    @property
    def effective_retry_policy(self) -> RetryPolicy:
        if self.retry_policy is not None:
            return self.retry_policy
        return RetryPolicy(max_attempts=self.max_retries + 1)

    @property
    def effective_accelerator(self) -> AcceleratorRequest:
        return self.accelerator or AcceleratorRequest.from_legacy_gpu(
            gpu_count=self.gpu_count,
            gpu_memory_mb=self.gpu_memory_mb,
            gpu_model=self.gpu_model,
        )

    def require_current_accelerator_execution_support(self) -> None:
        require_current_execution_support(self.accelerator)


class TaskCreated(ResponseModel):
    id: uuid.UUID
    status: TaskStatus


class TaskResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    workload_type: WorkloadType
    runtime_type: RuntimeType
    image: str
    command: list[str]
    environment: dict[str, str]
    status: TaskStatus

    created_at: datetime
    queued_at: datetime | None
    assigned_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None

    worker_id: str | None
    execution_id: uuid.UUID | None
    lease_expires_at: datetime | None

    exit_code: int | None
    error_message: str | None
    timeout_seconds: int
    retry_count: int
    max_retries: int
    retry_policy: RetryPolicy
    recovery_count: int
    next_attempt_at: datetime | None

    cpu_limit: float
    cpu_millicores: int
    memory_limit_mb: int
    gpu_count: int
    gpu_memory_mb: int
    gpu_model: str | None
    gpu_device_ids: list[str]
    accelerator_request_json: dict[str, object] | None
    selected_vendor: AcceleratorVendor | None
    selected_kind: AcceleratorKind | None
    selected_model: str | None
    runtime_profile_id: str | None
    runtime_profile_version: str | None
    runtime_profile_digest: str | None
    model_variant_id: uuid.UUID | None
    allocation_authority: AllocationAuthority | None
    network_enabled: bool
    network_mode: str
    labels: dict[str, str]
    tolerations: list[dict[str, str]]
    priority: int
    preemptible: bool
    preemption_count: int
    requeue_on_preempt: bool
    unschedulable_reason: str | None
    failure_category: str | None
    error_category: ErrorCategory | None
    error_code: ErrorCode | None

    cancel_requested: bool
    duration_ms: int | None
    cpu_seconds: float | None
    gpu_seconds: float | None
    wall_time_seconds: float | None
    estimated_cost: float | None

    idempotency_key: str | None
    version: int


class TaskListQuery(PaginationQuery):
    status: TaskStatus | None = None
    worker_id: str | None = Field(default=None, min_length=1, max_length=255)


class TaskListResponse(ResponseModel):
    items: list[TaskResponse]
    pagination: PaginationMeta


class TaskLogResponse(ResponseModel):
    id: int
    task_id: uuid.UUID
    timestamp: datetime
    stream: LogStream
    sequence: int = Field(ge=0)
    content: str


class TaskLogsQuery(PaginationQuery):
    limit: int = Field(default=500, ge=1, le=5000)


class TaskLogsResponse(ResponseModel):
    task_id: uuid.UUID
    logs: list[TaskLogResponse]
    pagination: PaginationMeta


class TaskEventResponse(ResponseModel):
    id: uuid.UUID
    event_type: str
    sequence: int
    from_status: TaskStatus | None
    status: TaskStatus
    execution_id: uuid.UUID | None
    worker_id: str | None
    details: dict[str, object]
    created_at: datetime


class TaskTimelineResponse(ResponseModel):
    task_id: uuid.UUID
    events: list[TaskEventResponse]


class TaskSchedulingResponse(ResponseModel):
    task_id: uuid.UUID
    state: str
    reason: str | None
    considered_workers: int = Field(ge=0)
    attempts_total: int = Field(ge=0)
    rejections: dict[str, int]
    outcomes: dict[str, int]
    latest_attempt_at: datetime | None


TaskCreateRequest = TaskCreate
TaskCreateResponse = TaskCreated
