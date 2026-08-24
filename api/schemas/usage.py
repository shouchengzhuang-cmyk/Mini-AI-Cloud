import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from pydantic import Field, StrictInt, model_validator

from api.schemas.common import RequestModel, ResponseModel

_MAX_COST = Decimal("999999999999.99999999")


class ProjectQuotaUpdate(RequestModel):
    max_queued_tasks: StrictInt | None = Field(default=None, ge=0, le=1_000_000)
    max_running_tasks: StrictInt | None = Field(default=None, ge=0, le=1_000_000)
    max_cpu_millicores: StrictInt | None = Field(default=None, ge=0, le=2_147_483_647)
    max_memory_mb: StrictInt | None = Field(default=None, ge=0, le=2_147_483_647)
    max_gpus: StrictInt | None = Field(default=None, ge=0, le=1_000_000)
    max_services: StrictInt | None = Field(default=None, ge=0, le=1_000_000)
    max_service_replicas: StrictInt | None = Field(default=None, ge=0, le=1_000_000)
    max_artifact_bytes: StrictInt | None = Field(default=None, ge=0, le=9_223_372_036_854_775_807)
    daily_cost_limit: Decimal | None = Field(default=None, ge=0, le=_MAX_COST)


class ProjectQuotaLimitsResponse(ResponseModel):
    max_queued_tasks: int | None = Field(ge=0)
    max_running_tasks: int | None = Field(ge=0)
    max_cpu_millicores: int | None = Field(ge=0)
    max_memory_mb: int | None = Field(ge=0)
    max_gpus: int | None = Field(ge=0)
    max_services: int | None = Field(ge=0)
    max_service_replicas: int | None = Field(ge=0)
    max_artifact_bytes: int | None = Field(ge=0)
    daily_cost_limit: Decimal | None = Field(ge=0)
    version: int = Field(ge=1)
    updated_at: datetime


class ProjectQuotaStateResponse(ResponseModel):
    queued_tasks: int = Field(ge=0)
    running_tasks: int = Field(ge=0)
    reserved_cpu_millicores: int = Field(ge=0)
    reserved_memory_mb: int = Field(ge=0)
    reserved_gpus: int = Field(ge=0)
    service_count: int = Field(ge=0)
    service_replicas: int = Field(ge=0)
    service_reserved_cpu_millicores: int = Field(ge=0)
    service_reserved_memory_mb: int = Field(ge=0)
    service_reserved_gpus: int = Field(ge=0)
    artifact_bytes: int = Field(ge=0)
    accounting_date: date
    daily_reserved_cost: Decimal = Field(ge=0)
    daily_settled_cost: Decimal = Field(ge=0)
    version: int = Field(ge=1)
    updated_at: datetime


class ProjectQuotaResponse(ResponseModel):
    project_id: uuid.UUID
    limits: ProjectQuotaLimitsResponse
    state: ProjectQuotaStateResponse


class UsageWindow(RequestModel):
    from_time: datetime
    to_time: datetime

    @model_validator(mode="after")
    def validate_window(self) -> "UsageWindow":
        if (
            self.from_time.tzinfo is None
            or self.from_time.utcoffset() is None
            or self.to_time.tzinfo is None
            or self.to_time.utcoffset() is None
        ):
            raise ValueError("usage window timestamps must include a timezone")
        if self.to_time <= self.from_time:
            raise ValueError("usage window end must be after its start")
        if self.to_time - self.from_time > timedelta(days=366):
            raise ValueError("usage window must not exceed 366 days")
        return self


class CurrencyCostResponse(ResponseModel):
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    cost: Decimal = Field(ge=0)


class GPUUsageResponse(ResponseModel):
    gpu_model: str | None
    gpu_seconds: Decimal = Field(ge=0)


class UsageResponse(ResponseModel):
    project_id: uuid.UUID
    from_time: datetime
    to_time: datetime
    settlement_basis: str = "finished_at"
    execution_count: int = Field(ge=0)
    cpu_seconds: Decimal = Field(ge=0)
    memory_gb_seconds: Decimal = Field(ge=0)
    gpu_seconds: Decimal = Field(ge=0)
    gpu_breakdown: list[GPUUsageResponse]
    costs: list[CurrencyCostResponse]


class CostResponse(ResponseModel):
    project_id: uuid.UUID
    from_time: datetime
    to_time: datetime
    settlement_basis: str = "finished_at"
    execution_count: int = Field(ge=0)
    costs: list[CurrencyCostResponse]
