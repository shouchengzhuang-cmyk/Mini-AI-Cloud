import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base, utcnow


class RegisteredModel(Base):
    __tablename__ = "registered_models"
    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_registered_models_project_name"),
        CheckConstraint("runtime IN ('vllm','fake')", name="registered_model_runtime"),
        CheckConstraint(
            "default_gpu_count >= 0 AND default_gpu_count <= 64",
            name="registered_model_default_gpu_count",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255))
    provider: Mapped[str] = mapped_column(String(64))
    source: Mapped[str] = mapped_column(String(1024))
    revision: Mapped[str | None] = mapped_column(String(255))
    runtime: Mapped[str] = mapped_column(String(32), default="vllm")
    default_gpu_count: Mapped[int] = mapped_column(Integer, default=0)
    runtime_defaults: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    gpu_memory_mb: Mapped[int | None] = mapped_column(Integer)
    architecture: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[dict[str, object]] = mapped_column("metadata", JSON, default=dict)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Secret(Base):
    __tablename__ = "secrets"
    __table_args__ = (UniqueConstraint("project_id", "name", name="uq_secrets_project_name"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    current_version: Mapped[int] = mapped_column(Integer, default=1)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class SecretVersion(Base):
    __tablename__ = "secret_versions"

    secret_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("secrets.id", ondelete="CASCADE"), primary_key=True
    )
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    nonce: Mapped[bytes] = mapped_column(LargeBinary)
    key_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class TaskSecretBinding(Base):
    __tablename__ = "task_secret_bindings"
    __table_args__ = (
        ForeignKeyConstraint(
            ["secret_id", "secret_version"],
            ["secret_versions.secret_id", "secret_versions.version"],
            ondelete="RESTRICT",
            name="fk_task_secret_binding_version",
        ),
        Index("ix_task_secret_bindings_secret_id", "secret_id"),
    )

    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"), primary_key=True
    )
    env_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    secret_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True))
    secret_version: Mapped[int] = mapped_column(Integer)


class ImagePolicy(Base):
    __tablename__ = "image_policies"
    __table_args__ = (CheckConstraint("default_action IN ('allow','deny')", name="action"),)

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    default_action: Mapped[str] = mapped_column(String(16), default="deny")
    require_digest: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ImagePolicyRule(Base):
    __tablename__ = "image_policy_rules"
    __table_args__ = (
        CheckConstraint("action IN ('allow','deny')", name="action"),
        Index("ix_image_rules_project_priority", "project_id", "priority"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), index=True
    )
    action: Mapped[str] = mapped_column(String(16))
    registry_host: Mapped[str | None] = mapped_column("registry", String(255))
    repository_glob: Mapped[str] = mapped_column(String(512))
    tag_glob: Mapped[str | None] = mapped_column(String(255))
    digest: Mapped[str | None] = mapped_column(String(255))
    priority: Mapped[int] = mapped_column(Integer, default=100)
