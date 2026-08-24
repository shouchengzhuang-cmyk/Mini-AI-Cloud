from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class APIModel(BaseModel):
    """Base model for all public API contracts.

    Unknown fields are rejected so a client cannot smuggle arbitrary Docker or
    infrastructure options through a request model.
    """

    model_config = ConfigDict(extra="forbid")


class RequestModel(APIModel):
    """Base class for request payloads and validated query objects."""

    model_config = ConfigDict(extra="forbid", validate_default=True)


class ResponseModel(APIModel):
    """Base class for response objects backed by mappings or ORM instances."""

    model_config = ConfigDict(extra="forbid", from_attributes=True)


class PaginationQuery(RequestModel):
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class PaginationMeta(ResponseModel):
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    next_cursor: str | None = None


class ValidationIssue(ResponseModel):
    location: list[str | int]
    message: str
    type: str


class ErrorDetail(ResponseModel):
    code: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4096)
    request_id: str = Field(min_length=1, max_length=255)
    details: Any | None = None


class ErrorResponse(ResponseModel):
    error: ErrorDetail


class HealthResponse(ResponseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, Literal["ok", "error"]] | None = None


class MessageResponse(ResponseModel):
    message: str = Field(min_length=1, max_length=4096)
