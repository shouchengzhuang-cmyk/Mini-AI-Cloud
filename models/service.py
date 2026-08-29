import uuid
from datetime import datetime
from enum import StrEnum

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
    LOADING = "loading"
    RUNNING = "running"
    DRAINING = "draining"
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
            "(logical_model_id IS NULL AND model_variant_id IS NULL "
            "AND selected_vendor IS NULL AND selected_kind IS NULL "
            "AND selected_model IS NULL AND runtime_profile_id IS NULL "
            "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
            "AND allocation_authority IS NULL AND accelerator_resource_name IS NULL "
            "AND selection_policy IS NULL AND eligible_node_names IS NULL) OR "
            "(logical_model_id IS NOT NULL AND model_variant_id IS NOT NULL "
            "AND selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
            "AND selected_model IS NOT NULL AND runtime_profile_id IS NOT NULL "
            "AND runtime_profile_version IS NOT NULL AND runtime_profile_digest IS NOT NULL "
            "AND allocation_authority IS NOT NULL AND selection_policy IS NOT NULL "
            "AND eligible_node_names IS NOT NULL)",
            name="admission_snapshot",
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
            name="runtime_profile_digest",
        ),
        CheckConstraint(
            "allocation_authority IS NULL OR allocation_authority IN "
            "('control_plane_exact_device','kubernetes_device_plugin')",
            name="allocation_authority",
        ),
        CheckConstraint(
            "allocation_authority IS NULL "
            "OR allocation_authority = 'control_plane_exact_device' "
            "OR accelerator_resource_name IS NOT NULL",
            name="resource_authority",
        ),
        CheckConstraint(
            "selection_policy IS NULL OR selection_policy IN "
            "('any','nvidia-only','ascend-only','prefer-nvidia','prefer-ascend')",
            name="selection_policy",
        ),
        CheckConstraint(
            "selected_vendor IS NULL OR selection_policy NOT IN ('nvidia-only','ascend-only') "
            "OR (selection_policy = 'nvidia-only' AND selected_vendor = 'nvidia') "
            "OR (selection_policy = 'ascend-only' "
            "AND selected_vendor = 'huawei-ascend')",
            name="policy_vendor",
        ),
        CheckConstraint("tensor_parallel_size >= 1", name="tensor_parallel_size"),
        CheckConstraint(
            "gpu_memory_utilization > 0 AND gpu_memory_utilization <= 1",
            name="gpu_memory_utilization",
        ),
        CheckConstraint(
            "max_model_len IS NULL OR max_model_len >= 1",
            name="max_model_len",
        ),
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
        Index(
            "ix_model_services_project_variant_status",
            "project_id",
            "model_variant_id",
            "status",
        ),
        Index("ix_model_services_reconcile", "status", "updated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="RESTRICT"), index=True
    )
    registered_model_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("registered_models.id", ondelete="SET NULL"), nullable=True, index=True
    )
    name: Mapped[str] = mapped_column(String(128))
    model: Mapped[str] = mapped_column(String(512))
    model_revision: Mapped[str | None] = mapped_column(String(255))
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
    gpu_model: Mapped[str | None] = mapped_column(String(255))
    logical_model_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("logical_models.id", ondelete="RESTRICT")
    )
    model_variant_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_variants.id", ondelete="RESTRICT")
    )
    selected_vendor: Mapped[str | None] = mapped_column(String(64))
    selected_kind: Mapped[str | None] = mapped_column(String(32))
    selected_model: Mapped[str | None] = mapped_column(String(255))
    runtime_profile_id: Mapped[str | None] = mapped_column(String(128))
    runtime_profile_version: Mapped[str | None] = mapped_column(String(32))
    runtime_profile_digest: Mapped[str | None] = mapped_column(String(71))
    allocation_authority: Mapped[str | None] = mapped_column(String(64))
    accelerator_resource_name: Mapped[str | None] = mapped_column(String(255))
    selection_policy: Mapped[str | None] = mapped_column(String(32))
    eligible_node_names: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True))
    tensor_parallel_size: Mapped[int] = mapped_column(Integer, default=1)
    dtype: Mapped[str] = mapped_column(String(32), default="auto")
    gpu_memory_utilization: Mapped[float] = mapped_column(Float, default=0.9)
    max_model_len: Mapped[int | None] = mapped_column(Integer)
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
    scheduling_reason: Mapped[str | None] = mapped_column(String(128))
    scheduling_details: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
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
            "status IN ('pending','starting','loading','running','draining','stopping',"
            "'stopped','failed','lost')",
            name="service_replica_status",
        ),
        CheckConstraint(
            "health IN ('unknown','healthy','unhealthy')",
            name="service_replica_health",
        ),
        CheckConstraint("runtime IN ('vllm','fake')", name="service_replica_runtime"),
        CheckConstraint("generation >= 1", name="generation"),
        CheckConstraint("ordinal >= 0", name="ordinal"),
        CheckConstraint(
            "health_failure_count >= 0",
            name="health_failure_count",
        ),
        CheckConstraint("active_requests >= 0", name="active_requests"),
        CheckConstraint(
            "(logical_model_id IS NULL AND model_variant_id IS NULL "
            "AND selected_vendor IS NULL AND selected_kind IS NULL "
            "AND selected_model IS NULL AND runtime_profile_id IS NULL "
            "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
            "AND allocation_authority IS NULL AND accelerator_resource_name IS NULL "
            "AND selection_policy IS NULL AND eligible_node_names IS NULL) OR "
            "(logical_model_id IS NOT NULL AND model_variant_id IS NOT NULL "
            "AND selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
            "AND selected_model IS NOT NULL AND runtime_profile_id IS NOT NULL "
            "AND runtime_profile_version IS NOT NULL AND runtime_profile_digest IS NOT NULL "
            "AND allocation_authority IS NOT NULL AND selection_policy IS NOT NULL "
            "AND eligible_node_names IS NOT NULL)",
            name="admission_snapshot",
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
            name="runtime_profile_digest",
        ),
        CheckConstraint(
            "allocation_authority IS NULL OR allocation_authority IN "
            "('control_plane_exact_device','kubernetes_device_plugin')",
            name="allocation_authority",
        ),
        CheckConstraint(
            "allocation_authority IS NULL "
            "OR allocation_authority = 'control_plane_exact_device' "
            "OR accelerator_resource_name IS NOT NULL",
            name="resource_authority",
        ),
        CheckConstraint(
            "selection_policy IS NULL OR selection_policy IN "
            "('any','nvidia-only','ascend-only','prefer-nvidia','prefer-ascend')",
            name="selection_policy",
        ),
        CheckConstraint(
            "selected_vendor IS NULL OR selection_policy NOT IN ('nvidia-only','ascend-only') "
            "OR (selection_policy = 'nvidia-only' AND selected_vendor = 'nvidia') "
            "OR (selection_policy = 'ascend-only' "
            "AND selected_vendor = 'huawei-ascend')",
            name="policy_vendor",
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
        Index("ix_service_replicas_variant_status", "model_variant_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("model_services.id", ondelete="CASCADE"), index=True
    )
    runtime: Mapped[ServingRuntime] = mapped_column(
        Enum(
            ServingRuntime,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_enum_values,
            name="service_replica_runtime",
        )
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
    active_requests: Mapped[int] = mapped_column(Integer, default=0)
    logical_model_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("logical_models.id", ondelete="RESTRICT")
    )
    model_variant_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_variants.id", ondelete="RESTRICT")
    )
    selected_vendor: Mapped[str | None] = mapped_column(String(64))
    selected_kind: Mapped[str | None] = mapped_column(String(32))
    selected_model: Mapped[str | None] = mapped_column(String(255))
    runtime_profile_id: Mapped[str | None] = mapped_column(String(128))
    runtime_profile_version: Mapped[str | None] = mapped_column(String(32))
    runtime_profile_digest: Mapped[str | None] = mapped_column(String(71))
    allocation_authority: Mapped[str | None] = mapped_column(String(64))
    accelerator_resource_name: Mapped[str | None] = mapped_column(String(255))
    selection_policy: Mapped[str | None] = mapped_column(String(32))
    eligible_node_names: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True))
    assigned_node_name: Mapped[str | None] = mapped_column(String(253))
    model_revision: Mapped[str | None] = mapped_column(String(255))
    image_digest: Mapped[str | None] = mapped_column(String(255))
    error_code: Mapped[str | None] = mapped_column(String(128))
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    container_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ready_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    drain_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    drain_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)
