import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.enums import RuntimeType
from models.base import Base, utcnow


class ServiceStatus(StrEnum):
    PENDING = "pending"
    DEPLOYING = "deploying"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class ReplicaStatus(StrEnum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    LOST = "lost"


class ReplicaHealth(StrEnum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class ServingRuntime(StrEnum):
    VLLM = "vllm"
    FAKE = "fake"


def _enum_values(enum_class: type[StrEnum]) -> list[str]:
    return [item.value for item in enum_class]


class ModelService(Base):
    __tablename__ = "model_services"
    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_model_services_project_name"),
        CheckConstraint("runtime IN ('vllm','fake')", name="serving_runtime"),
        CheckConstraint(
            "runtime_type IN ('docker','kubernetes','fake')",
            name="service_runtime_type",
        ),
        CheckConstraint(
            "status IN ('pending','deploying','running','degraded','stopping','stopped','failed')",
            name="model_service_status",
        ),
        CheckConstraint(
            "desired_replicas >= 0 AND desired_replicas <= 1000",
            name="desired_replicas",
        ),
        CheckConstraint("generation >= 1", name="generation"),
        CheckConstraint("cpu_millicores >= 1", name="cpu_millicores"),
        CheckConstraint("memory_mb >= 16", name="memory_mb"),
        CheckConstraint("gpu_count >= 0", name="gpu_count"),
        CheckConstraint("gpu_memory_mb >= 0", name="gpu_memory_mb"),
        CheckConstraint(
            "autoscaling_min_replicas >= 0 AND autoscaling_min_replicas <= 1000",
            name="autoscaling_min_replicas",
        ),
        CheckConstraint(
            "autoscaling_max_replicas >= 1 "
            "AND autoscaling_max_replicas >= autoscaling_min_replicas "
            "AND autoscaling_max_replicas <= 1000",
            name="autoscaling_max_replicas",
        ),
        CheckConstraint(
            "autoscaling_target_concurrency >= 1 AND autoscaling_target_concurrency <= 1000000",
            name="autoscaling_target_concurrency",
        ),
        CheckConstraint(
            "autoscaling_cooldown_seconds >= 0 AND autoscaling_cooldown_seconds <= 86400",
            name="autoscaling_cooldown_seconds",
        ),
        Index("ix_model_services_project_status", "project_id", "status", "created_at"),
        Index("ix_model_services_reconcile", "status", "updated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    name: Mapped[str] = mapped_column(String(128))
    model: Mapped[str] = mapped_column(String(512))
    runtime: Mapped[ServingRuntime] = mapped_column(
        Enum(
            ServingRuntime,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="serving_runtime",
        )
    )
    runtime_type: Mapped[RuntimeType] = mapped_column(
        Enum(
            RuntimeType,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="service_runtime_type",
        ),
        default=RuntimeType.DOCKER,
    )
    image: Mapped[str | None] = mapped_column(String(512))
    cpu_millicores: Mapped[int] = mapped_column(Integer, default=1000)
    memory_mb: Mapped[int] = mapped_column(Integer, default=1024)
    gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    gpu_memory_mb: Mapped[int] = mapped_column(Integer, default=0)
    desired_replicas: Mapped[int] = mapped_column(Integer, default=1)
    autoscaling_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    autoscaling_min_replicas: Mapped[int] = mapped_column(Integer, default=1)
    autoscaling_max_replicas: Mapped[int] = mapped_column(Integer, default=4)
    autoscaling_target_concurrency: Mapped[int] = mapped_column(Integer, default=8)
    autoscaling_cooldown_seconds: Mapped[int] = mapped_column(Integer, default=60)
    last_scaled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_autoscale_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    generation: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[ServiceStatus] = mapped_column(
        Enum(
            ServiceStatus,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="model_service_status",
        ),
        default=ServiceStatus.PENDING,
    )
    round_robin_cursor: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)


class ServiceReplica(Base):
    __tablename__ = "service_replicas"
    __table_args__ = (
        UniqueConstraint(
            "service_id",
            "generation",
            "ordinal",
            name="uq_service_replicas_generation_ordinal",
        ),
        UniqueConstraint("execution_id", name="uq_service_replicas_execution_id"),
        CheckConstraint(
            "status IN ('pending','starting','running','stopping','stopped','failed','lost')",
            name="service_replica_status",
        ),
        CheckConstraint(
            "health IN ('unknown','healthy','unhealthy')",
            name="service_replica_health",
        ),
        CheckConstraint("generation >= 1", name="generation"),
        CheckConstraint("ordinal >= 0", name="ordinal"),
        CheckConstraint(
            "health_failure_count >= 0",
            name="health_failure_count",
        ),
        Index(
            "ix_service_replicas_service_generation_status",
            "service_id",
            "generation",
            "status",
        ),
        Index(
            "ix_service_replicas_ready",
            "service_id",
            "generation",
            "status",
            "health",
        ),
        Index("ix_service_replicas_worker_status", "worker_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("model_services.id", ondelete="CASCADE"), index=True
    )
    generation: Mapped[int] = mapped_column(Integer)
    ordinal: Mapped[int] = mapped_column(Integer)
    status: Mapped[ReplicaStatus] = mapped_column(
        Enum(
            ReplicaStatus,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="service_replica_status",
        ),
        default=ReplicaStatus.PENDING,
    )
    health: Mapped[ReplicaHealth] = mapped_column(
        Enum(
            ReplicaHealth,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="service_replica_health",
        ),
        default=ReplicaHealth.UNKNOWN,
    )
    worker_id: Mapped[str | None] = mapped_column(
        ForeignKey("workers.id", ondelete="SET NULL"), nullable=True
    )
    execution_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True
    )
    endpoint_url: Mapped[str | None] = mapped_column(String(2048))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_health_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    health_probe_token: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    health_probe_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), index=True
    )
    health_failure_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)
