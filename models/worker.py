import uuid
from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Enum, Float, Index, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from core.enums import WorkerStatus
from models.base import Base, utcnow


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        Index("ix_workers_status_heartbeat", "status", "last_heartbeat_at"),
        Index("ix_workers_started_id", "started_at", "id"),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    worker_session_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), default=uuid.uuid4, index=True
    )
    hostname: Mapped[str] = mapped_column(String(255))
    node_name: Mapped[str | None] = mapped_column(String(255))
    runtime_types: Mapped[list[str]] = mapped_column(JSON, default=lambda: ["docker"])
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(
            WorkerStatus,
            native_enum=False,
            values_callable=lambda enum_class: [item.value for item in enum_class],
        ),
        default=WorkerStatus.ONLINE,
        index=True,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    running_tasks: Mapped[int] = mapped_column(Integer, default=0)
    concurrency: Mapped[int] = mapped_column(Integer, default=1)
    reserved_cpu: Mapped[float] = mapped_column(Float, default=0.0)
    reserved_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    reserved_gpus: Mapped[int] = mapped_column(Integer, default=0)
    cpu_count: Mapped[int] = mapped_column(Integer)
    cpu_total_millicores: Mapped[int] = mapped_column(Integer, default=1000)
    cpu_allocatable_millicores: Mapped[int] = mapped_column(Integer, default=1000)
    memory_total_mb: Mapped[int] = mapped_column(Integer)
    memory_allocatable_mb: Mapped[int] = mapped_column(Integer, default=0)
    docker_version: Mapped[str | None] = mapped_column(String(128))
    labels: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    taints: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    gpu_model: Mapped[str | None] = mapped_column(String(512))
    gpu_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    inventory_generation: Mapped[int] = mapped_column(Integer, default=1)
    inventory_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    overcommitted: Mapped[bool] = mapped_column(Boolean, default=False)
    drain_reason: Mapped[str | None] = mapped_column(String(512))
    version: Mapped[int] = mapped_column(Integer, default=1)
