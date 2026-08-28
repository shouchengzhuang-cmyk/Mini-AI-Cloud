import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
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
        CheckConstraint("memory_free_mb <= memory_total_mb", name="memory_available"),
        CheckConstraint(
            "(vendor = 'nvidia' AND accelerator_kind = 'gpu') OR "
            "(vendor = 'huawei-ascend' AND accelerator_kind = 'npu')",
            name="vendor_kind",
        ),
        UniqueConstraint("worker_id", "device_uuid", name="uq_gpu_devices_worker_uuid"),
        UniqueConstraint(
            "worker_id", "vendor", "device_index", name="uq_gpu_devices_worker_vendor_index"
        ),
        Index("ix_gpu_devices_worker_health", "worker_id", "health"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"), index=True)
    device_uuid: Mapped[str] = mapped_column(String(255))
    device_index: Mapped[int] = mapped_column(Integer)
    vendor: Mapped[str] = mapped_column(String(64), default="nvidia")
    accelerator_kind: Mapped[str] = mapped_column(String(32), default="gpu")
    model: Mapped[str] = mapped_column(String(255))
    memory_total_mb: Mapped[int] = mapped_column(Integer)
    memory_free_mb: Mapped[int] = mapped_column(Integer)
    compute_capability: Mapped[str | None] = mapped_column(String(32))
    compute_arch: Mapped[str | None] = mapped_column(String(128))
    runtime_profile_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    capabilities_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    kubernetes_resource_name: Mapped[str | None] = mapped_column(String(255))
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
        CheckConstraint(
            "allocation_authority IN ('control_plane_exact_device','kubernetes_device_plugin')",
            name="allocation_authority",
        ),
        CheckConstraint(
            "(gpu_count = 0 AND requested_vendor IS NULL AND requested_kind IS NULL "
            "AND requested_profile_id IS NULL AND requested_profile_version IS NULL "
            "AND requested_profile_digest IS NULL AND model_variant_id IS NULL) OR "
            "(gpu_count > 0 AND requested_vendor IS NOT NULL AND requested_kind IS NOT NULL)",
            name="accelerator_request",
        ),
        CheckConstraint(
            "(requested_profile_id IS NULL AND requested_profile_version IS NULL "
            "AND requested_profile_digest IS NULL) OR "
            "(requested_profile_id IS NOT NULL AND requested_profile_version IS NOT NULL "
            "AND requested_profile_digest IS NOT NULL)",
            name="requested_profile_snapshot",
        ),
        CheckConstraint(
            "requested_profile_digest IS NULL OR "
            "(length(requested_profile_digest) = 71 "
            "AND requested_profile_digest LIKE 'sha256:%')",
            name="requested_profile_digest",
        ),
        CheckConstraint(
            "model_variant_id IS NULL OR requested_profile_id IS NOT NULL",
            name="model_variant_profile",
        ),
        CheckConstraint(
            "gpu_count = 0 OR allocation_authority != 'kubernetes_device_plugin' "
            "OR requested_profile_id IS NOT NULL",
            name="requested_profile_authority",
        ),
        CheckConstraint(
            "requested_vendor IS NULL OR "
            "(requested_vendor = 'nvidia' AND requested_kind = 'gpu') OR "
            "(requested_vendor = 'huawei-ascend' AND requested_kind = 'npu')",
            name="requested_vendor_kind",
        ),
        CheckConstraint(
            "(observed_at IS NULL AND observed_vendor IS NULL "
            "AND observed_device_ids_json IS NULL) OR "
            "(observed_at IS NOT NULL AND observed_vendor IS NOT NULL "
            "AND observed_device_ids_json IS NOT NULL)",
            name="observed_allocation",
        ),
        CheckConstraint(
            "observed_vendor IS NULL OR observed_vendor = requested_vendor",
            name="observed_vendor",
        ),
        CheckConstraint(
            "gpu_count = 0 OR allocation_authority = 'kubernetes_device_plugin' "
            "OR legacy_unbound OR observed_device_ids_json IS NOT NULL",
            name="exact_device_evidence",
        ),
        UniqueConstraint("execution_id", name="uq_resource_reservations_execution"),
        Index("ix_reservations_worker_state", "worker_id", "state"),
        Index("ix_reservations_task_state", "task_id", "state"),
        Index("ix_reservations_variant_state", "model_variant_id", "state"),
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
    allocation_authority: Mapped[str] = mapped_column(
        String(64), default="control_plane_exact_device"
    )
    requested_vendor: Mapped[str | None] = mapped_column(String(64))
    requested_kind: Mapped[str | None] = mapped_column(String(32))
    requested_profile_id: Mapped[str | None] = mapped_column(String(128))
    requested_profile_version: Mapped[str | None] = mapped_column(String(32))
    requested_profile_digest: Mapped[str | None] = mapped_column(String(71))
    model_variant_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_variants.id", ondelete="RESTRICT")
    )
    observed_device_ids_json: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True))
    observed_vendor: Mapped[str | None] = mapped_column(String(64))
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
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
