import uuid
from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, StrictStr, field_validator

from api.schemas.common import RequestModel, ResponseModel
from core.enums import TaskStatus, WorkerStatus


class SchedulerDiagnosticStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STARTING = "starting"
    DISABLED = "disabled"
    NOT_OBSERVABLE = "not_observable"


class SchedulerDiagnosticResponse(ResponseModel):
    mode: str
    status: SchedulerDiagnosticStatus
    source: str
    control_plane_enabled: bool
    heartbeat_observable: bool
    queued_tasks: int = Field(ge=0)
    online_workers: int = Field(ge=0)
    runs: int | None = Field(default=None, ge=0)
    failures: int | None = Field(default=None, ge=0)
    last_started_at: datetime | None = None
    last_succeeded_at: datetime | None = None
    last_error_present: bool = False


class OutboxLagResponse(ResponseModel):
    scope: str
    pending_events: int = Field(ge=0)
    ready_events: int = Field(ge=0)
    retrying_events: int = Field(ge=0)
    oldest_ready_at: datetime | None
    lag_seconds: float = Field(ge=0)


class OfflineWorkerResponse(ResponseModel):
    worker_id: str
    status: WorkerStatus
    last_heartbeat_at: datetime
    stale_for_seconds: float = Field(ge=0)
    running_tasks: int = Field(ge=0)


class StuckTaskResponse(ResponseModel):
    task_id: uuid.UUID
    status: TaskStatus
    reason: str
    worker_id: str | None
    lease_expires_at: datetime | None
    stuck_for_seconds: float = Field(ge=0)
    unschedulable_reason: str | None


class ActiveReservationResponse(ResponseModel):
    reservation_id: uuid.UUID
    task_id: uuid.UUID
    execution_id: uuid.UUID
    worker_id: str
    cpu_millicores: int = Field(ge=1)
    memory_mb: int = Field(ge=1)
    gpu_count: int = Field(ge=0)
    created_at: datetime
    age_seconds: float = Field(ge=0)


class ConsistencyIssueResponse(ResponseModel):
    resource_type: str
    resource_id: str
    task_id: uuid.UUID | None
    reason: str
    repairable: bool = False


class ConsistencyCheckResponse(ResponseModel):
    name: str
    status: Literal["clean", "issues", "not_observable"]
    total: int | None = Field(default=None, ge=0)
    issues: list[ConsistencyIssueResponse]
    reason: str | None = None


class ConsistencyResponse(ResponseModel):
    status: Literal["clean", "issues", "incomplete"]
    complete: bool
    issues_total: int = Field(ge=0)
    checks: list[ConsistencyCheckResponse]


class RepairCapabilityResponse(ResponseModel):
    supported: bool = True
    reason: str


class RepairActionResponse(ResponseModel):
    check: str
    resource_type: str
    resource_id: str
    action: str
    outcome: Literal["repaired", "skipped"]
    reason: str


class AdminRepairResponse(ResponseModel):
    project_id: uuid.UUID | None
    observed_at: datetime
    candidates_total: int = Field(ge=0)
    repaired_total: int = Field(ge=0)
    skipped_total: int = Field(ge=0)
    actions: list[RepairActionResponse]
    message: str


class AdminDiagnosticsResponse(ResponseModel):
    project_id: uuid.UUID | None
    observed_at: datetime
    scheduler: SchedulerDiagnosticResponse
    outbox: OutboxLagResponse
    offline_workers_total: int = Field(ge=0)
    offline_workers: list[OfflineWorkerResponse]
    stuck_tasks_total: int = Field(ge=0)
    stuck_tasks: list[StuckTaskResponse]
    active_reservations_total: int = Field(ge=0)
    active_reservations: list[ActiveReservationResponse]
    consistency: ConsistencyResponse
    repair: RepairCapabilityResponse


class WorkerDrainRequest(RequestModel):
    reason: StrictStr = Field(default="operator maintenance", min_length=1, max_length=512)

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        reason = value.strip()
        if not reason:
            raise ValueError("reason must not be blank")
        return reason
