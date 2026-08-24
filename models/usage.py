import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class ProjectQuota(Base):
    __tablename__ = "project_quotas"
    __table_args__ = (
        CheckConstraint("max_queued_tasks IS NULL OR max_queued_tasks >= 0", name="quota_queued"),
        CheckConstraint(
            "max_running_tasks IS NULL OR max_running_tasks >= 0", name="quota_running"
        ),
        CheckConstraint("max_cpu_millicores IS NULL OR max_cpu_millicores >= 0", name="quota_cpu"),
        CheckConstraint("max_memory_mb IS NULL OR max_memory_mb >= 0", name="quota_memory"),
        CheckConstraint("max_gpus IS NULL OR max_gpus >= 0", name="quota_gpus"),
        CheckConstraint("max_services IS NULL OR max_services >= 0", name="quota_services"),
        CheckConstraint(
            "max_service_replicas IS NULL OR max_service_replicas >= 0",
            name="quota_service_replicas",
        ),
        CheckConstraint(
            "max_artifact_bytes IS NULL OR max_artifact_bytes >= 0",
            name="quota_artifact_bytes",
        ),
        CheckConstraint("daily_cost_limit IS NULL OR daily_cost_limit >= 0", name="quota_cost"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    max_queued_tasks: Mapped[int | None] = mapped_column(Integer)
    max_running_tasks: Mapped[int | None] = mapped_column(Integer)
    max_cpu_millicores: Mapped[int | None] = mapped_column(Integer)
    max_memory_mb: Mapped[int | None] = mapped_column(Integer)
    max_gpus: Mapped[int | None] = mapped_column(Integer)
    max_services: Mapped[int | None] = mapped_column(Integer)
    max_service_replicas: Mapped[int | None] = mapped_column(Integer)
    max_artifact_bytes: Mapped[int | None] = mapped_column(BigInteger)
    daily_cost_limit: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ProjectQuotaState(Base):
    __tablename__ = "project_quota_state"
    __table_args__ = (
        CheckConstraint(
            "queued_tasks >= 0 AND running_tasks >= 0 "
            "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
            "AND reserved_gpus >= 0 AND service_count >= 0 "
            "AND service_replicas >= 0 AND service_reserved_cpu_millicores >= 0 "
            "AND service_reserved_memory_mb >= 0 AND service_reserved_gpus >= 0 "
            "AND artifact_bytes >= 0 "
            "AND daily_reserved_cost >= 0 AND daily_settled_cost >= 0",
            name="nonnegative",
        ),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    queued_tasks: Mapped[int] = mapped_column(Integer, default=0)
    running_tasks: Mapped[int] = mapped_column(Integer, default=0)
    reserved_cpu_millicores: Mapped[int] = mapped_column(Integer, default=0)
    reserved_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    reserved_gpus: Mapped[int] = mapped_column(Integer, default=0)
    service_count: Mapped[int] = mapped_column(Integer, default=0)
    service_replicas: Mapped[int] = mapped_column(Integer, default=0)
    service_reserved_cpu_millicores: Mapped[int] = mapped_column(BigInteger, default=0)
    service_reserved_memory_mb: Mapped[int] = mapped_column(BigInteger, default=0)
    service_reserved_gpus: Mapped[int] = mapped_column(BigInteger, default=0)
    artifact_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    accounting_date: Mapped[date] = mapped_column(Date, default=date.today)
    daily_reserved_cost: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=Decimal("0"))
    daily_settled_cost: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=Decimal("0"))
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class TaskExecution(Base):
    __tablename__ = "task_executions"
    __table_args__ = (
        UniqueConstraint("task_id", "attempt", name="uq_task_executions_task_attempt"),
        Index("ix_task_executions_project_started", "project_id", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), index=True
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    worker_id: Mapped[str | None] = mapped_column(
        ForeignKey("workers.id", ondelete="SET NULL"), index=True
    )
    worker_session_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    attempt: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32))
    cpu_millicores: Mapped[int] = mapped_column(Integer)
    memory_mb: Mapped[int] = mapped_column(Integer)
    gpu_count: Mapped[int] = mapped_column(Integer)
    gpu_model: Mapped[str | None] = mapped_column(String(255))
    cpu_price_per_hour: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    memory_price_per_gb_hour: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    gpu_price_per_hour: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    runtime_type: Mapped[str] = mapped_column(String(32))
    runtime_object_id: Mapped[str | None] = mapped_column(String(512))
    error_category: Mapped[str | None] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(Text)


class UsageLedger(Base):
    __tablename__ = "usage_ledger"
    __table_args__ = (
        CheckConstraint(
            "cpu_seconds >= 0 AND memory_gb_seconds >= 0 AND gpu_seconds >= 0 AND cost >= 0",
            name="nonnegative",
        ),
        CheckConstraint("finished_at >= started_at", name="period"),
        UniqueConstraint("execution_id", name="uq_usage_ledger_execution"),
        Index("ix_usage_project_period", "project_id", "finished_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    # These are immutable billing provenance identifiers rather than live
    # relational references. Operational task/execution rows have a bounded
    # retention period, while settled usage must remain auditable indefinitely.
    # The normal write path validates both identifiers against TaskExecution
    # before inserting the ledger row.
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), index=True)
    execution_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    cpu_seconds: Mapped[Decimal] = mapped_column(Numeric(24, 6))
    memory_gb_seconds: Mapped[Decimal] = mapped_column(Numeric(24, 6))
    gpu_seconds: Mapped[Decimal] = mapped_column(Numeric(24, 6))
    gpu_model: Mapped[str | None] = mapped_column(String(255))
    cost: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    pricing_source: Mapped[str] = mapped_column(String(32), default="rate_snapshot")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ServingRequestUsage(Base):
    """Immutable, backend-reported accounting for one gateway request.

    Token fields are deliberately nullable. A missing or malformed upstream
    ``usage`` object must remain unavailable rather than being estimated by the
    control plane.
    """

    __tablename__ = "serving_request_usage"
    __table_args__ = (
        CheckConstraint("finished_at >= started_at", name="period"),
        CheckConstraint("request_duration_seconds >= 0", name="duration_nonnegative"),
        CheckConstraint(
            "time_to_first_token_seconds IS NULL OR time_to_first_token_seconds >= 0",
            name="ttft_nonnegative",
        ),
        CheckConstraint(
            "allocated_gpu_seconds IS NULL OR allocated_gpu_seconds >= 0",
            name="allocated_gpu_seconds_nonnegative",
        ),
        CheckConstraint(
            "(prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL) "
            "OR (prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL "
            "AND total_tokens IS NOT NULL AND prompt_tokens >= 0 AND completion_tokens >= 0 "
            "AND total_tokens = prompt_tokens + completion_tokens)",
            name="tokens_consistent",
        ),
        CheckConstraint(
            "path IN ('/v1/chat/completions','/v1/completions')",
            name="gateway_path",
        ),
        UniqueConstraint("request_id", name="uq_serving_request_usage_request"),
        Index("ix_serving_usage_project_finished", "project_id", "finished_at"),
        Index("ix_serving_usage_service_finished", "service_id", "finished_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    request_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True))
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("model_services.id", ondelete="RESTRICT"), index=True
    )
    # Replica rows are operational and may eventually have a shorter retention
    # period than usage. Preserve the UUID as provenance without a live FK.
    replica_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), index=True)
    path: Mapped[str] = mapped_column(String(64))
    outcome: Mapped[str] = mapped_column(String(64))
    error_code: Mapped[str | None] = mapped_column(String(64))
    streamed: Mapped[bool] = mapped_column(Boolean, default=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    request_duration_seconds: Mapped[Decimal] = mapped_column(Numeric(24, 6))
    time_to_first_token_seconds: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    allocated_gpu_seconds: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    prompt_tokens: Mapped[int | None] = mapped_column(BigInteger)
    completion_tokens: Mapped[int | None] = mapped_column(BigInteger)
    total_tokens: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class BillingRate(Base):
    __tablename__ = "billing_rates"
    __table_args__ = (
        CheckConstraint("unit_price >= 0", name="unit_price_nonnegative"),
        UniqueConstraint(
            "resource_type", "sku", "effective_from", name="uq_billing_rate_effective"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_type: Mapped[str] = mapped_column(String(32))
    sku: Mapped[str] = mapped_column(String(255))
    unit: Mapped[str] = mapped_column(String(64))
    unit_price: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (Index("ix_audit_project_occurred", "project_id", "occurred_at", "id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    actor_type: Mapped[str] = mapped_column(String(32))
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    api_key_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("api_keys.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(128))
    resource_type: Mapped[str] = mapped_column(String(64))
    resource_id: Mapped[str | None] = mapped_column(String(255))
    outcome: Mapped[str] = mapped_column(String(32))
    request_id: Mapped[str | None] = mapped_column(String(255))
    source_ip: Mapped[str | None] = mapped_column(String(64))
    details: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
