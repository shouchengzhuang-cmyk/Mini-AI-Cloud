import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.enums import AcceleratorKind, AcceleratorVendor, ModelAvailabilityStatus
from models.base import Base, utcnow


def _enum_values(enum_class: type[ModelAvailabilityStatus]) -> list[str]:
    return [item.value for item in enum_class]


def _vendor_values(enum_class: type[AcceleratorVendor]) -> list[str]:
    return [item.value for item in enum_class]


def _kind_values(enum_class: type[AcceleratorKind]) -> list[str]:
    return [item.value for item in enum_class]


class LogicalModel(Base):
    __tablename__ = "logical_models"
    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_logical_models_project_name"),
        CheckConstraint(
            "status IN ('ready','degraded','disabled')",
            name="logical_model_status",
        ),
        Index("ix_logical_models_project_status_created", "project_id", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(128))
    public_name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[ModelAvailabilityStatus] = mapped_column(
        Enum(
            ModelAvailabilityStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_enum_values,
            name="model_availability_status",
        ),
        default=ModelAvailabilityStatus.DISABLED,
    )
    metadata_json: Mapped[dict[str, object]] = mapped_column("metadata", JSON, default=dict)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    version: Mapped[int] = mapped_column(Integer, default=1)


class ModelVariant(Base):
    __tablename__ = "model_variants"
    __table_args__ = (
        UniqueConstraint(
            "logical_model_id",
            "name",
            name="uq_model_variants_logical_model_name",
        ),
        CheckConstraint(
            "(vendor = 'nvidia' AND accelerator_kind = 'gpu') OR "
            "(vendor = 'huawei-ascend' AND accelerator_kind = 'npu')",
            name="model_variant_vendor_kind",
        ),
        CheckConstraint(
            "status IN ('ready','degraded','disabled')",
            name="model_variant_status",
        ),
        CheckConstraint(
            "length(runtime_profile_digest) = 71 AND runtime_profile_digest LIKE 'sha256:%'",
            name="model_variant_profile_digest",
        ),
        CheckConstraint(
            "length(artifact_digest) = 71 AND artifact_digest LIKE 'sha256:%'",
            name="model_variant_artifact_digest",
        ),
        CheckConstraint("length(artifact_revision) > 0", name="model_variant_revision"),
        CheckConstraint("length(artifact_source) > 0", name="model_variant_source"),
        CheckConstraint("length(dtype) > 0", name="model_variant_dtype"),
        Index(
            "ix_model_variants_runtime_profile",
            "runtime_profile_id",
            "runtime_profile_version",
        ),
        Index(
            "ix_model_variants_logical_status",
            "logical_model_id",
            "status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    logical_model_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("logical_models.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(128))
    vendor: Mapped[AcceleratorVendor] = mapped_column(
        Enum(
            AcceleratorVendor,
            native_enum=False,
            length=64,
            create_constraint=False,
            values_callable=_vendor_values,
            name="accelerator_vendor",
        )
    )
    kind: Mapped[AcceleratorKind] = mapped_column(
        "accelerator_kind",
        Enum(
            AcceleratorKind,
            native_enum=False,
            length=32,
            create_constraint=False,
            values_callable=_kind_values,
            name="accelerator_kind",
        ),
    )
    runtime_profile_id: Mapped[str] = mapped_column(String(128))
    runtime_profile_version: Mapped[str] = mapped_column(String(32))
    runtime_profile_digest: Mapped[str] = mapped_column(String(71))
    artifact_source: Mapped[str] = mapped_column(String(1024))
    artifact_revision: Mapped[str] = mapped_column(String(255))
    artifact_digest: Mapped[str] = mapped_column(String(71))
    architecture: Mapped[str] = mapped_column(String(255))
    dtype: Mapped[str] = mapped_column(String(64))
    quantization: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[ModelAvailabilityStatus] = mapped_column(
        Enum(
            ModelAvailabilityStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_enum_values,
            name="model_availability_status",
        ),
        default=ModelAvailabilityStatus.DISABLED,
    )
    status_reason: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, object]] = mapped_column("metadata", JSON, default=dict)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
    version: Mapped[int] = mapped_column(Integer, default=1)


class LogicalModelStatusEvent(Base):
    __tablename__ = "logical_model_status_events"
    __table_args__ = (
        UniqueConstraint(
            "logical_model_id",
            "model_version",
            name="uq_logical_model_status_events_model_version",
        ),
        CheckConstraint(
            "from_status IS NULL OR from_status IN ('ready','degraded','disabled')",
            name="logical_model_event_from_status",
        ),
        CheckConstraint(
            "to_status IN ('ready','degraded','disabled')",
            name="logical_model_event_to_status",
        ),
        CheckConstraint("length(reason) > 0", name="logical_model_event_reason"),
        Index("ix_logical_model_status_events_model_version", "logical_model_id", "model_version"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    logical_model_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("logical_models.id", ondelete="CASCADE"), index=True
    )
    from_status: Mapped[ModelAvailabilityStatus | None] = mapped_column(
        Enum(
            ModelAvailabilityStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_enum_values,
            name="model_availability_status",
        )
    )
    to_status: Mapped[ModelAvailabilityStatus] = mapped_column(
        Enum(
            ModelAvailabilityStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_enum_values,
            name="model_availability_status",
        )
    )
    reason: Mapped[str] = mapped_column(Text)
    model_version: Mapped[int] = mapped_column(Integer)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
