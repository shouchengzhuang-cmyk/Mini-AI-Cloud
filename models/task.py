import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.enums import LogStream, TaskStatus
from models.base import Base, utcnow


def _enum_values(enum_class: type[TaskStatus] | type[LogStream]) -> list[str]:
    return [item.value for item in enum_class]


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ix_tasks_status_created_at", "status", "created_at"),
        Index("ix_tasks_worker_status", "worker_id", "status"),
        Index("ix_tasks_lease_expires_at", "lease_expires_at"),
        Index("ix_tasks_next_attempt_at", "next_attempt_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    image: Mapped[str] = mapped_column(String(512))
    command: Mapped[list[str]] = mapped_column(JSON)
    environment: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, native_enum=False, values_callable=_enum_values),
        default=TaskStatus.PENDING,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    worker_id: Mapped[str | None] = mapped_column(
        ForeignKey("workers.id", ondelete="SET NULL"), nullable=True, index=True
    )
    execution_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    exit_code: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=60)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=0)
    recovery_count: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    cpu_limit: Mapped[float] = mapped_column(Float, default=1.0)
    memory_limit_mb: Mapped[int] = mapped_column(Integer, default=256)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    network_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    labels: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)

    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    cpu_seconds: Mapped[float | None] = mapped_column(Float)
    gpu_seconds: Mapped[float | None] = mapped_column(Float)
    wall_time_seconds: Mapped[float | None] = mapped_column(Float)
    estimated_cost: Mapped[float | None] = mapped_column(Float)

    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    log_sequence: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class TaskLog(Base):
    __tablename__ = "task_logs"
    __table_args__ = (
        UniqueConstraint("task_id", "sequence", name="uq_task_logs_task_sequence"),
        Index("ix_task_logs_task_sequence", "task_id", "sequence"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    execution_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    stream: Mapped[LogStream] = mapped_column(
        Enum(LogStream, native_enum=False, values_callable=_enum_values)
    )
    sequence: Mapped[int] = mapped_column(Integer)
    content: Mapped[str] = mapped_column(Text)
