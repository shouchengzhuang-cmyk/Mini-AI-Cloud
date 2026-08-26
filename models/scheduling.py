import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class GPUDevice(Base):
    __tablename__ = "gpu_devices"
    __table_args__ = (
        CheckConstraint("memory_total_mb > 0", name="total_memory"),
        CheckConstraint("memory_free_mb >= 0", name="free_memory"),
        UniqueConstraint("worker_id", "device_uuid", name="uq_gpu_devices_worker_uuid"),
        UniqueConstraint("worker_id", "device_index", name="uq_gpu_devices_worker_index"),
        Index("ix_gpu_devices_worker_health", "worker_id", "health"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"), index=True)
    device_uuid: Mapped[str] = mapped_column(String(255))
    device_index: Mapped[int] = mapped_column(Integer)
    vendor: Mapped[str] = mapped_column(String(64), default="nvidia")
    model: Mapped[str] = mapped_column(String(255))
    memory_total_mb: Mapped[int] = mapped_column(Integer)
    memory_free_mb: Mapped[int] = mapped_column(Integer)
    compute_capability: Mapped[str | None] = mapped_column(String(32))
    health: Mapped[str] = mapped_column(String(32), default="healthy")
    fake: Mapped[bool] = mapped_column(Boolean, default=False)
    inventory_generation: Mapped[int] = mapped_column(Integer, default=1)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ResourceReservation(Base):
    __tablename__ = "resource_reservations"
    __table_args__ = (
        CheckConstraint("cpu_millicores > 0", name="cpu"),
        CheckConstraint("memory_mb > 0", name="memory"),
        CheckConstraint("gpu_count >= 0", name="gpu"),
        UniqueConstraint("execution_id", name="uq_resource_reservations_execution"),
        Index("ix_reservations_worker_state", "worker_id", "state"),
        Index("ix_reservations_task_state", "task_id", "state"),
        Index(
            "uq_reservations_active_task",
            "task_id",
            unique=True,
            postgresql_where=text("released_at IS NULL"),
            sqlite_where=text("released_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    execution_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("task_executions.id", ondelete="RESTRICT")
    )
    worker_id: Mapped[str] = mapped_column(
        ForeignKey("workers.id", ondelete="RESTRICT"), index=True
    )
    worker_session_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True))
    cpu_millicores: Mapped[int] = mapped_column(Integer)
    memory_mb: Mapped[int] = mapped_column(Integer)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    state: Mapped[str] = mapped_column(String(32), default="active")
    legacy_unbound: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    release_reason: Mapped[str | None] = mapped_column(String(128))
    version: Mapped[int] = mapped_column(Integer, default=1)


class ReservationGPUDevice(Base):
    __tablename__ = "reservation_gpu_devices"
    __table_args__ = (
        UniqueConstraint("reservation_id", "gpu_device_id", name="uq_reservation_gpu_device"),
        Index(
            "uq_reservation_gpu_active",
            "gpu_device_id",
            unique=True,
            postgresql_where=text("released_at IS NULL"),
            sqlite_where=text("released_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    reservation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("resource_reservations.id", ondelete="CASCADE"), index=True
    )
    gpu_device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("gpu_devices.id", ondelete="RESTRICT"), index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class PlacementAttempt(Base):
    __tablename__ = "placement_attempts"
    __table_args__ = (Index("ix_placement_task_created", "task_id", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    scheduler_id: Mapped[str] = mapped_column(String(255))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    policy: Mapped[str] = mapped_column(String(32))
    outcome: Mapped[str] = mapped_column(String(32))
    reason: Mapped[str | None] = mapped_column(String(128))
    detail: Mapped[str | None] = mapped_column(Text)
    effective_priority: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PreemptionPlan(Base):
    __tablename__ = "preemption_plans"
    __table_args__ = (Index("ix_preemption_incoming_state", "incoming_task_id", "state"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incoming_task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    victim_task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id", ondelete="RESTRICT"))
    state: Mapped[str] = mapped_column(String(32), default="requested")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_reason: Mapped[str | None] = mapped_column(Text)
