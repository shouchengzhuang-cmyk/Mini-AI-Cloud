from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from core.enums import WorkerStatus
from models.base import Base, utcnow


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (Index("ix_workers_status_heartbeat", "status", "last_heartbeat_at"),)

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255))
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
    memory_total_mb: Mapped[int] = mapped_column(Integer)
    docker_version: Mapped[str | None] = mapped_column(String(128))
    labels: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    gpu_model: Mapped[str | None] = mapped_column(String(512))
    gpu_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    version: Mapped[int] = mapped_column(Integer, default=1)
