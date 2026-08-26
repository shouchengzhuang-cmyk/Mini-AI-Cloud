import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from core.rbac import MembershipStatus, ProjectRole, ProjectStatus, UserStatus
from models.base import Base, utcnow


def _enum_values(enum_class: type[UserStatus]) -> list[str]:
    return [item.value for item in enum_class]


def _project_status_values(enum_class: type[ProjectStatus]) -> list[str]:
    return [item.value for item in enum_class]


def _membership_status_values(enum_class: type[MembershipStatus]) -> list[str]:
    return [item.value for item in enum_class]


def _project_role_values(enum_class: type[ProjectRole]) -> list[str]:
    return [item.value for item in enum_class]


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("status IN ('active','disabled')", name="user_status"),
        UniqueConstraint("username_normalized", name="uq_users_username_normalized"),
        UniqueConstraint("email_normalized", name="uq_users_email_normalized"),
        Index("ix_users_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    username: Mapped[str] = mapped_column(String(64))
    username_normalized: Mapped[str] = mapped_column(String(64))
    email: Mapped[str] = mapped_column(String(320))
    email_normalized: Mapped[str] = mapped_column(String(320))
    password_hash: Mapped[str] = mapped_column(String(512))
    status: Mapped[UserStatus] = mapped_column(
        Enum(
            UserStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_enum_values,
            name="user_status",
        ),
        default=UserStatus.ACTIVE,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
    )
    password_changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    version: Mapped[int] = mapped_column(Integer, default=1)


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (
        CheckConstraint("status IN ('active','suspended','deleted')", name="project_status"),
        UniqueConstraint("slug", name="uq_projects_slug"),
        Index("ix_projects_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128))
    slug: Mapped[str] = mapped_column(String(63))
    status: Mapped[ProjectStatus] = mapped_column(
        Enum(
            ProjectStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_project_status_values,
            name="project_status",
        ),
        default=ProjectStatus.ACTIVE,
        index=True,
    )
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
    )
    version: Mapped[int] = mapped_column(Integer, default=1)


class ProjectMembership(Base):
    __tablename__ = "project_memberships"
    __table_args__ = (
        CheckConstraint("role IN ('owner','admin','member','viewer')", name="project_role"),
        CheckConstraint("status IN ('active','removed')", name="membership_status"),
        Index("ix_project_memberships_user_status", "user_id", "status"),
        Index("ix_project_memberships_project_role", "project_id", "role"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    role: Mapped[ProjectRole] = mapped_column(
        Enum(
            ProjectRole,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_project_role_values,
            name="project_role",
        )
    )
    status: Mapped[MembershipStatus] = mapped_column(
        Enum(
            MembershipStatus,
            native_enum=False,
            length=16,
            create_constraint=False,
            values_callable=_membership_status_values,
            name="membership_status",
        ),
        default=MembershipStatus.ACTIVE,
    )
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
    )
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)


class ApiKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (
        ForeignKeyConstraint(
            ("project_id", "user_id"),
            ("project_memberships.project_id", "project_memberships.user_id"),
            ondelete="RESTRICT",
        ),
        UniqueConstraint("key_prefix", name="uq_api_keys_key_prefix"),
        UniqueConstraint("secret_hash", name="uq_api_keys_secret_hash"),
        Index("ix_api_keys_project_created_at", "project_id", "created_at"),
        Index("ix_api_keys_user_revoked", "user_id", "revoked_at"),
        Index("ix_api_keys_expires_at", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column()
    user_id: Mapped[uuid.UUID] = mapped_column()
    name: Mapped[str] = mapped_column(String(128))
    key_prefix: Mapped[str] = mapped_column(String(20))
    secret_hash: Mapped[bytes] = mapped_column(LargeBinary(32))
    hash_key_id: Mapped[str] = mapped_column(String(64))
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)
