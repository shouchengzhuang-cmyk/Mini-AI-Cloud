"""Persist per-vendor circuits and physical serving usage provenance.

Revision ID: 0014_vendor_routing
Revises: 0013_vendor_aware_admission
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_vendor_routing"
down_revision: str | Sequence[str] | None = "0013_vendor_aware_admission"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "vendor_circuit_states",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("logical_model_id", sa.Uuid(), nullable=False),
        sa.Column("vendor", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("opened_until", sa.DateTime(timezone=True)),
        sa.Column("last_error_code", sa.String(64)),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "vendor IN ('nvidia','huawei-ascend')", name=op.f("ck_vendor_circuit_states_vendor")
        ),
        sa.CheckConstraint(
            "state IN ('closed','open')", name=op.f("ck_vendor_circuit_states_state")
        ),
        sa.CheckConstraint(
            "failure_count >= 0", name=op.f("ck_vendor_circuit_states_failure_count")
        ),
        sa.CheckConstraint(
            "(state = 'closed' AND opened_until IS NULL) OR "
            "(state = 'open' AND opened_until IS NOT NULL)",
            name=op.f("ck_vendor_circuit_states_opened_until"),
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["logical_model_id"], ["logical_models.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "project_id",
            "logical_model_id",
            "vendor",
            name="uq_vendor_circuit_project_model_vendor",
        ),
    )
    op.create_index(
        op.f("ix_vendor_circuit_states_project_id"), "vendor_circuit_states", ["project_id"]
    )
    op.create_index(
        op.f("ix_vendor_circuit_states_logical_model_id"),
        "vendor_circuit_states",
        ["logical_model_id"],
    )
    op.create_index("ix_vendor_circuit_open", "vendor_circuit_states", ["state", "opened_until"])

    with op.batch_alter_table("serving_request_usage") as batch:
        batch.add_column(sa.Column("logical_model_id", sa.Uuid()))
        batch.add_column(sa.Column("model_variant_id", sa.Uuid()))
        batch.add_column(sa.Column("selected_vendor", sa.String(64)))
        batch.create_index(op.f("ix_serving_request_usage_logical_model_id"), ["logical_model_id"])
        batch.create_index(op.f("ix_serving_request_usage_model_variant_id"), ["model_variant_id"])


def downgrade() -> None:
    with op.batch_alter_table("serving_request_usage") as batch:
        batch.drop_index(op.f("ix_serving_request_usage_model_variant_id"))
        batch.drop_index(op.f("ix_serving_request_usage_logical_model_id"))
        batch.drop_column("selected_vendor")
        batch.drop_column("model_variant_id")
        batch.drop_column("logical_model_id")
    op.drop_index("ix_vendor_circuit_open", table_name="vendor_circuit_states")
    op.drop_index(
        op.f("ix_vendor_circuit_states_logical_model_id"), table_name="vendor_circuit_states"
    )
    op.drop_index(op.f("ix_vendor_circuit_states_project_id"), table_name="vendor_circuit_states")
    op.drop_table("vendor_circuit_states")
