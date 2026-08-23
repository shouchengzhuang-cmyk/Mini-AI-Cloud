from datetime import datetime

from pydantic import Field, computed_field

from api.schemas.common import PaginationMeta, ResponseModel
from core.enums import WorkerStatus


class WorkerCapacity(ResponseModel):
    concurrency: int = Field(ge=1)
    running_tasks: int = Field(ge=0)
    available_slots: int = Field(ge=0)
    reserved_cpu: float = Field(ge=0)
    reserved_memory_mb: int = Field(ge=0)
    reserved_gpus: int = Field(ge=0)
    cpu_count: int = Field(ge=1)
    memory_total_mb: int = Field(ge=0)
    gpu_count: int = Field(ge=0)
    gpu_memory_mb: int = Field(ge=0)


class WorkerResponse(ResponseModel):
    id: str
    hostname: str
    status: WorkerStatus
    started_at: datetime
    last_heartbeat_at: datetime
    running_tasks: int = Field(ge=0)
    concurrency: int = Field(ge=1)
    reserved_cpu: float = Field(ge=0)
    reserved_memory_mb: int = Field(ge=0)
    reserved_gpus: int = Field(ge=0)
    cpu_count: int = Field(ge=1)
    memory_total_mb: int = Field(ge=0)
    docker_version: str | None
    labels: dict[str, str]
    gpu_count: int = Field(ge=0)
    gpu_model: str | None
    gpu_memory_mb: int = Field(ge=0)
    version: int = Field(ge=1)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def capacity(self) -> WorkerCapacity:
        return WorkerCapacity(
            concurrency=self.concurrency,
            running_tasks=self.running_tasks,
            available_slots=max(0, self.concurrency - self.running_tasks),
            reserved_cpu=self.reserved_cpu,
            reserved_memory_mb=self.reserved_memory_mb,
            reserved_gpus=self.reserved_gpus,
            cpu_count=self.cpu_count,
            memory_total_mb=self.memory_total_mb,
            gpu_count=self.gpu_count,
            gpu_memory_mb=self.gpu_memory_mb,
        )


class WorkerListResponse(ResponseModel):
    items: list[WorkerResponse]
    pagination: PaginationMeta


WorkerRead = WorkerResponse
