import json
import re
import uuid
from datetime import datetime

from pydantic import Field, StrictStr, field_validator, model_validator

from api.schemas.common import PaginationMeta, RequestModel, ResponseModel
from core.enums import TaskStatus
from repositories.dag import (
    DependencyFailurePolicy,
    DependencyState,
    JobGroupStatus,
)

_GROUP_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_. -]{0,254}$")


class TaskDependencyCreate(RequestModel):
    task_id: uuid.UUID
    depends_on_task_id: uuid.UUID
    failure_policy: DependencyFailurePolicy = DependencyFailurePolicy.CANCEL

    @model_validator(mode="after")
    def reject_self_dependency(self) -> "TaskDependencyCreate":
        if self.task_id == self.depends_on_task_id:
            raise ValueError("a task cannot depend on itself")
        return self


class JobGroupCreate(RequestModel):
    name: StrictStr = Field(min_length=1, max_length=255)
    retry_policy: dict[str, object] = Field(default_factory=dict)
    dependencies: list[TaskDependencyCreate] = Field(default_factory=list, max_length=1_000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        name = value.strip()
        if not _GROUP_NAME.fullmatch(name):
            raise ValueError("job group name contains unsupported characters")
        return name

    @field_validator("retry_policy")
    @classmethod
    def validate_retry_policy(cls, value: dict[str, object]) -> dict[str, object]:
        try:
            encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("retry_policy must be JSON serializable") from exc
        if len(encoded.encode("utf-8")) > 16_384:
            raise ValueError("retry_policy must be at most 16384 encoded bytes")
        return value

    @field_validator("dependencies")
    @classmethod
    def reject_duplicate_dependencies(
        cls, value: list[TaskDependencyCreate]
    ) -> list[TaskDependencyCreate]:
        keys = [(item.task_id, item.depends_on_task_id) for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("job group dependencies must be unique")
        return value


class TaskDependencyResponse(ResponseModel):
    task_id: uuid.UUID
    depends_on_task_id: uuid.UUID
    job_group_id: uuid.UUID
    failure_policy: DependencyFailurePolicy


class DependencyStateResponse(ResponseModel):
    task_id: uuid.UUID
    task_status: TaskStatus
    dependency_state: DependencyState
    dependency_ids: list[uuid.UUID]
    waiting_on_task_ids: list[uuid.UUID]
    failed_dependency_ids: list[uuid.UUID]


class JobGroupResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    status: JobGroupStatus
    retry_policy: dict[str, object]
    task_count: int = Field(ge=0)
    ready_tasks: int = Field(ge=0)
    waiting_tasks: int = Field(ge=0)
    blocked_tasks: int = Field(ge=0)
    cancelled_tasks: int = Field(ge=0)
    succeeded_tasks: int = Field(ge=0)
    failed_tasks: int = Field(ge=0)
    created_at: datetime
    finished_at: datetime | None


class JobGroupListResponse(ResponseModel):
    items: list[JobGroupResponse]
    pagination: PaginationMeta


class ReadyTasksResponse(ResponseModel):
    job_group_id: uuid.UUID
    items: list[DependencyStateResponse]
