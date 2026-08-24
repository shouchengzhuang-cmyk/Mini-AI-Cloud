import uuid
from datetime import datetime

from pydantic import Field, StrictStr, field_validator

from api.schemas.common import RequestModel, ResponseModel
from repositories.task_artifacts import validate_artifact_name, validate_output_path


class TaskInputArtifact(RequestModel):
    artifact_id: uuid.UUID


class TaskOutputArtifact(RequestModel):
    path: StrictStr = Field(min_length=1, max_length=1024)
    name: StrictStr = Field(min_length=1, max_length=255)
    required: bool = True

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_output_path(value)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return validate_artifact_name(value)


class TaskArtifactResponse(ResponseModel):
    id: uuid.UUID
    task_id: uuid.UUID
    artifact_id: uuid.UUID | None
    direction: str
    name: str
    mount_path: str
    required: bool
    created_at: datetime
