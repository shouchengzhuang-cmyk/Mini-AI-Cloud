import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
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

from core.enums import LogStream, RetryBackoff, RuntimeType, TaskStatus, WorkloadType
from models.base import Base, utcnow


def _enum_values(enum_class: type[TaskStatus] | type[LogStream]) -> list[str]:
    return [item.value for item in enum_class]


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint("priority BETWEEN 0 AND 100", name="priority"),
        CheckConstraint("cpu_millicores > 0", name="cpu_millicores"),
        CheckConstraint("gpu_memory_mb >= 0", name="gpu_memory"),
        CheckConstraint(
            "(selected_vendor IS NULL AND selected_kind IS NULL "
            "AND selected_model IS NULL AND runtime_profile_id IS NULL "
            "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
            "AND model_variant_id IS NULL AND allocation_authority IS NULL) OR "
            "(selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
            "AND selected_model IS NOT NULL AND allocation_authority IS NOT NULL)",
            name="selected_accelerator_snapshot",
        ),
        CheckConstraint(
            "(runtime_profile_id IS NULL AND runtime_profile_version IS NULL "
            "AND runtime_profile_digest IS NULL) OR "
            "(runtime_profile_id IS NOT NULL AND runtime_profile_version IS NOT NULL "
            "AND runtime_profile_digest IS NOT NULL)",
            name="selected_profile_snapshot",
        ),
        CheckConstraint(
            "model_variant_id IS NULL OR runtime_profile_id IS NOT NULL",
            name="selected_variant_profile",
        ),
        CheckConstraint(
            "allocation_authority IS NULL "
            "OR allocation_authority != 'kubernetes_device_plugin' "
            "OR runtime_profile_id IS NOT NULL",
            name="selected_profile_authority",
        ),
        CheckConstraint(
            "selected_vendor IS NULL OR "
            "(selected_vendor = 'nvidia' AND selected_kind = 'gpu') OR "
            "(selected_vendor = 'huawei-ascend' AND selected_kind = 'npu')",
            name="selected_vendor_kind",
        ),
        CheckConstraint(
            "runtime_profile_digest IS NULL OR "
            "(length(runtime_profile_digest) = 71 "
            "AND runtime_profile_digest LIKE 'sha256:%')",
            name="selected_profile_digest",
        ),
        CheckConstraint(
            "allocation_authority IS NULL OR allocation_authority IN "
            "('control_plane_exact_device','kubernetes_device_plugin')",
            name="selected_allocation_authority",
        ),
        CheckConstraint("retry_base_seconds > 0", name="retry_base_seconds"),
        CheckConstraint("retry_max_seconds >= retry_base_seconds", name="retry_max_seconds"),
        Index("ix_tasks_status_created_at", "status", "created_at"),
        Index("ix_tasks_worker_status", "worker_id", "status"),
        Index("ix_tasks_lease_expires_at", "lease_expires_at"),
        Index("ix_tasks_next_attempt_at", "next_attempt_at"),
        Index("ix_tasks_project_created", "project_id", "created_at", "id"),
        Index("ix_tasks_project_variant_status", "project_id", "model_variant_id", "status"),
        Index("ix_tasks_schedule", "status", "priority", "queue_order"),
        UniqueConstraint("project_id", "idempotency_key", name="uq_tasks_project_idempotency"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"),
        default=lambda: uuid.UUID("00000000-0000-0000-0000-000000000001"),
        index=True,
    )
    submitted_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_by_api_key_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("api_keys.id", ondelete="SET NULL"), nullable=True
    )
    workload_type: Mapped[WorkloadType] = mapped_column(
        Enum(
            WorkloadType,
            native_enum=False,
            length=32,
            values_callable=lambda enum_class: [item.value for item in enum_class],
        ),
        default=WorkloadType.BATCH_JOB,
    )
    runtime_type: Mapped[RuntimeType] = mapped_column(
        Enum(
            RuntimeType,
            native_enum=False,
            length=32,
            values_callable=lambda enum_class: [item.value for item in enum_class],
        ),
        default=RuntimeType.DOCKER,
    )
    image: Mapped[str] = mapped_column(String(512))
    command: Mapped[list[str]] = mapped_column(JSON)
    environment: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, native_enum=False, length=32, values_callable=_enum_values),
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
    retry_backoff: Mapped[str] = mapped_column(String(32), default=RetryBackoff.EXPONENTIAL.value)
    retry_base_seconds: Mapped[float] = mapped_column(Float, default=1.0)
    retry_max_seconds: Mapped[float] = mapped_column(Float, default=60.0)
    retry_on_exit_codes: Mapped[list[int]] = mapped_column(JSON, default=lambda: [1, 137])
    recovery_count: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    cpu_limit: Mapped[float] = mapped_column(Float, default=1.0)
    cpu_millicores: Mapped[int] = mapped_column(Integer, default=1000)
    memory_limit_mb: Mapped[int] = mapped_column(Integer, default=256)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    gpu_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    gpu_model: Mapped[str | None] = mapped_column(String(255))
    gpu_device_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    accelerator_request_json: Mapped[dict[str, object] | None] = mapped_column(
        JSON(none_as_null=True)
    )
    selected_vendor: Mapped[str | None] = mapped_column(String(64))
    selected_kind: Mapped[str | None] = mapped_column(String(32))
    selected_model: Mapped[str | None] = mapped_column(String(255))
    runtime_profile_id: Mapped[str | None] = mapped_column(String(128))
    runtime_profile_version: Mapped[str | None] = mapped_column(String(32))
    runtime_profile_digest: Mapped[str | None] = mapped_column(String(71))
    model_variant_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_variants.id", ondelete="RESTRICT")
    )
    allocation_authority: Mapped[str | None] = mapped_column(String(64))
    network_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    network_mode: Mapped[str] = mapped_column(String(32), default="none")
    labels: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    tolerations: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    priority: Mapped[int] = mapped_column(Integer, default=50)
    queue_order: Mapped[int] = mapped_column(BigInteger, default=0)
    preemptible: Mapped[bool] = mapped_column(Boolean, default=False)
    preemption_count: Mapped[int] = mapped_column(Integer, default=0)
    requeue_on_preempt: Mapped[bool] = mapped_column(Boolean, default=True)
    unschedulable_reason: Mapped[str | None] = mapped_column(String(128))
    failure_category: Mapped[str | None] = mapped_column(String(64))
    error_category: Mapped[str | None] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(64))
    runtime_handle: Mapped[dict[str, object] | None] = mapped_column(JSON)

    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    cpu_seconds: Mapped[float | None] = mapped_column(Float)
    gpu_seconds: Mapped[float | None] = mapped_column(Float)
    wall_time_seconds: Mapped[float | None] = mapped_column(Float)
    estimated_cost: Mapped[float | None] = mapped_column(Float)

    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    request_hash: Mapped[str | None] = mapped_column(String(64))
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    log_sequence: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    @property
    def retry_policy(self) -> dict[str, object]:
        return {
            "max_attempts": self.max_retries + 1,
            "backoff": self.retry_backoff,
            "base_seconds": self.retry_base_seconds,
            "max_seconds": self.retry_max_seconds,
            "retry_on_exit_codes": list(self.retry_on_exit_codes),
        }


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


class TaskEvent(Base):
    __tablename__ = "task_events"
    __table_args__ = (
        UniqueConstraint("task_id", "sequence", name="uq_task_events_task_sequence"),
        Index("ix_task_events_task_created", "task_id", "sequence"),
        Index("ix_task_events_project_created", "project_id", "created_at", "id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    event_type: Mapped[str] = mapped_column(String(64))
    sequence: Mapped[int] = mapped_column(Integer)
    from_status: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    execution_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    details: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
