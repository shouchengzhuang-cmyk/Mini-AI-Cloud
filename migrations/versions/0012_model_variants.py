"""Add logical models, vendor variants and status audit history.

Revision ID: 0012_model_variants
Revises: 0011_accelerator_persistence
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_model_variants"
down_revision: str | Sequence[str] | None = "0011_accelerator_persistence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "logical_models",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("public_name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("status", sa.String(16), server_default="disabled", nullable=False),
        sa.Column("metadata", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "status IN ('ready','degraded','disabled')",
            name=op.f("ck_logical_models_logical_model_status"),
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name=op.f("fk_logical_models_created_by_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_logical_models_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_logical_models")),
        sa.UniqueConstraint(
            "project_id",
            "name",
            name="uq_logical_models_project_name",
        ),
    )
    op.create_index(
        op.f("ix_logical_models_project_id"),
        "logical_models",
        ["project_id"],
        unique=False,
    )
    op.create_index(
        "ix_logical_models_project_status_created",
        "logical_models",
        ["project_id", "status", "created_at"],
        unique=False,
    )

    op.create_table(
        "model_variants",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("logical_model_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("vendor", sa.String(64), nullable=False),
        sa.Column("accelerator_kind", sa.String(32), nullable=False),
        sa.Column("runtime_profile_id", sa.String(128), nullable=False),
        sa.Column("runtime_profile_version", sa.String(32), nullable=False),
        sa.Column("runtime_profile_digest", sa.String(71), nullable=False),
        sa.Column("artifact_source", sa.String(1024), nullable=False),
        sa.Column("artifact_revision", sa.String(255), nullable=False),
        sa.Column("artifact_digest", sa.String(71), nullable=False),
        sa.Column("architecture", sa.String(255), nullable=False),
        sa.Column("dtype", sa.String(64), nullable=False),
        sa.Column("quantization", sa.String(128)),
        sa.Column("status", sa.String(16), server_default="disabled", nullable=False),
        sa.Column("status_reason", sa.Text()),
        sa.Column("metadata", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "(vendor = 'nvidia' AND accelerator_kind = 'gpu') OR "
            "(vendor = 'huawei-ascend' AND accelerator_kind = 'npu')",
            name=op.f("ck_model_variants_model_variant_vendor_kind"),
        ),
        sa.CheckConstraint(
            "status IN ('ready','degraded','disabled')",
            name=op.f("ck_model_variants_model_variant_status"),
        ),
        sa.CheckConstraint(
            "length(runtime_profile_digest) = 71 AND runtime_profile_digest LIKE 'sha256:%'",
            name=op.f("ck_model_variants_model_variant_profile_digest"),
        ),
        sa.CheckConstraint(
            "length(artifact_digest) = 71 AND artifact_digest LIKE 'sha256:%'",
            name=op.f("ck_model_variants_model_variant_artifact_digest"),
        ),
        sa.CheckConstraint(
            "length(artifact_revision) > 0",
            name=op.f("ck_model_variants_model_variant_revision"),
        ),
        sa.CheckConstraint(
            "length(artifact_source) > 0",
            name=op.f("ck_model_variants_model_variant_source"),
        ),
        sa.CheckConstraint(
            "length(dtype) > 0",
            name=op.f("ck_model_variants_model_variant_dtype"),
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name=op.f("fk_model_variants_created_by_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["logical_model_id"],
            ["logical_models.id"],
            name=op.f("fk_model_variants_logical_model_id_logical_models"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_variants")),
        sa.UniqueConstraint(
            "logical_model_id",
            "name",
            name="uq_model_variants_logical_model_name",
        ),
    )
    op.create_index(
        op.f("ix_model_variants_logical_model_id"),
        "model_variants",
        ["logical_model_id"],
        unique=False,
    )
    op.create_index(
        "ix_model_variants_runtime_profile",
        "model_variants",
        ["runtime_profile_id", "runtime_profile_version"],
        unique=False,
    )
    op.create_index(
        "ix_model_variants_logical_status",
        "model_variants",
        ["logical_model_id", "status"],
        unique=False,
    )

    op.create_table(
        "logical_model_status_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("logical_model_id", sa.Uuid(), nullable=False),
        sa.Column("from_status", sa.String(16)),
        sa.Column("to_status", sa.String(16), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("model_version", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "from_status IS NULL OR from_status IN ('ready','degraded','disabled')",
            name=op.f("ck_logical_model_status_events_logical_model_event_from_status"),
        ),
        sa.CheckConstraint(
            "to_status IN ('ready','degraded','disabled')",
            name=op.f("ck_logical_model_status_events_logical_model_event_to_status"),
        ),
        sa.CheckConstraint(
            "length(reason) > 0",
            name=op.f("ck_logical_model_status_events_logical_model_event_reason"),
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name=op.f("fk_logical_model_status_events_created_by_user_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["logical_model_id"],
            ["logical_models.id"],
            name=op.f("fk_logical_model_status_events_logical_model_id_logical_models"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_logical_model_status_events")),
        sa.UniqueConstraint(
            "logical_model_id",
            "model_version",
            name="uq_logical_model_status_events_model_version",
        ),
    )
    op.create_index(
        op.f("ix_logical_model_status_events_logical_model_id"),
        "logical_model_status_events",
        ["logical_model_id"],
        unique=False,
    )
    op.create_index(
        "ix_logical_model_status_events_model_version",
        "logical_model_status_events",
        ["logical_model_id", "model_version"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_logical_model_status_events_model_version",
        table_name="logical_model_status_events",
    )
    op.drop_index(
        op.f("ix_logical_model_status_events_logical_model_id"),
        table_name="logical_model_status_events",
    )
    op.drop_table("logical_model_status_events")
    op.drop_index("ix_model_variants_logical_status", table_name="model_variants")
    op.drop_index("ix_model_variants_runtime_profile", table_name="model_variants")
    op.drop_index(op.f("ix_model_variants_logical_model_id"), table_name="model_variants")
    op.drop_table("model_variants")
    op.drop_index(
        "ix_logical_models_project_status_created",
        table_name="logical_models",
    )
    op.drop_index(op.f("ix_logical_models_project_id"), table_name="logical_models")
    op.drop_table("logical_models")
