import re
import uuid
from datetime import datetime

from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr, field_validator

from api.schemas.common import PaginationMeta, PaginationQuery, RequestModel, ResponseModel
from core.enums import LogStream, TaskStatus

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


class TaskCreate(RequestModel):
    image: StrictStr = Field(min_length=1, max_length=512)
    command: list[StrictStr] = Field(min_length=1, max_length=256)
    environment: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    timeout_seconds: StrictInt | None = Field(default=None, ge=1, le=86_400)
    max_retries: StrictInt = Field(default=0, ge=0, le=100)
    cpu_limit: StrictFloat = Field(default=1.0, gt=0, le=1024)
    memory_limit_mb: StrictInt = Field(default=256, ge=16, le=1_048_576)
    labels: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    network_enabled: StrictBool = False
    gpu_count: StrictInt = Field(default=0, ge=0, le=64)

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


class TaskCreated(ResponseModel):
    id: uuid.UUID
    status: TaskStatus


class TaskResponse(ResponseModel):
    id: uuid.UUID
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
    recovery_count: int
    next_attempt_at: datetime | None

    cpu_limit: float
    memory_limit_mb: int
    gpu_count: int
    network_enabled: bool
    labels: dict[str, str]

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


TaskCreateRequest = TaskCreate
TaskCreateResponse = TaskCreated
