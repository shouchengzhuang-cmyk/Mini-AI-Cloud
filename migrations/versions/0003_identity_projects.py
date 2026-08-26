"""Add users, projects, memberships and hashed API keys.

Revision ID: 0003_identity_projects
Revises: 0002_worker_reservations
Create Date: 2026-08-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_identity_projects"
down_revision: str | Sequence[str] | None = "0002_worker_reservations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LEGACY_PROJECT_ID = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("username", sa.String(64), nullable=False),
        sa.Column("username_normalized", sa.String(64), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("email_normalized", sa.String(320), nullable=False),
        sa.Column("password_hash", sa.String(512), nullable=False),
        sa.Column("status", sa.String(16), server_default="active", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "password_changed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("status IN ('active','disabled')", name="user_status"),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("username_normalized", name="uq_users_username_normalized"),
        sa.UniqueConstraint("email_normalized", name="uq_users_email_normalized"),
    )
    op.create_index("ix_users_status", "users", ["status"])
    op.create_index("ix_users_status_created_at", "users", ["status", "created_at"])

    op.create_table(
        "projects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("slug", sa.String(63), nullable=False),
        sa.Column("status", sa.String(16), server_default="active", nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("status IN ('active','suspended','deleted')", name="project_status"),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"], ondelete="SET NULL", name="fk_projects_creator"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_projects"),
        sa.UniqueConstraint("slug", name="uq_projects_slug"),
    )
    op.create_index("ix_projects_status", "projects", ["status"])
    op.create_index("ix_projects_status_created_at", "projects", ["status", "created_at"])

    op.execute(
        sa.text(
            "INSERT INTO projects (id,name,slug,status,version) "
            f"VALUES ('{LEGACY_PROJECT_ID}'::uuid,'Legacy local project',"
            "'legacy-local','active',1)"
        )
    )

    op.create_table(
        "project_memberships",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), server_default="active", nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("role IN ('owner','admin','member','viewer')", name="project_role"),
        sa.CheckConstraint("status IN ('active','removed')", name="membership_status"),
        sa.ForeignKeyConstraint(
            ["project_id"], ["projects.id"], ondelete="CASCADE", name="fk_memberships_project"
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], ondelete="RESTRICT", name="fk_memberships_user"
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            ondelete="SET NULL",
            name="fk_memberships_creator",
        ),
        sa.PrimaryKeyConstraint("project_id", "user_id", name="pk_project_memberships"),
    )
    op.create_index(
        "ix_project_memberships_user_status", "project_memberships", ["user_id", "status"]
    )
    op.create_index(
        "ix_project_memberships_project_role", "project_memberships", ["project_id", "role"]
    )

    op.create_table(
        "api_keys",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("key_prefix", sa.String(20), nullable=False),
        sa.Column("secret_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("hash_key_id", sa.String(64), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.ForeignKeyConstraint(
            ["project_id", "user_id"],
            ["project_memberships.project_id", "project_memberships.user_id"],
            ondelete="RESTRICT",
            name="fk_api_keys_membership",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"], ondelete="SET NULL", name="fk_api_keys_creator"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_api_keys"),
        sa.UniqueConstraint("key_prefix", name="uq_api_keys_key_prefix"),
        sa.UniqueConstraint("secret_hash", name="uq_api_keys_secret_hash"),
    )
    op.create_index("ix_api_keys_project_created_at", "api_keys", ["project_id", "created_at"])
    op.create_index("ix_api_keys_user_revoked", "api_keys", ["user_id", "revoked_at"])
    op.create_index("ix_api_keys_expires_at", "api_keys", ["expires_at"])
    op.create_index("ix_api_keys_revoked_at", "api_keys", ["revoked_at"])


def downgrade() -> None:
    op.drop_table("api_keys")
    op.drop_table("project_memberships")
    op.drop_table("projects")
    op.drop_table("users")
