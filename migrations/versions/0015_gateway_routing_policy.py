"""Add explicit logical-model gateway routing policy and cursor.

Revision ID: 0015_gateway_routing_policy
Revises: 0014_vendor_routing
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_gateway_routing_policy"
down_revision: str | Sequence[str] | None = "0014_vendor_routing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("logical_models") as batch:
        batch.add_column(
            sa.Column(
                "routing_policy",
                sa.String(32),
                server_default="balanced",
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column(
                "routing_cursor",
                sa.BigInteger(),
                server_default="0",
                nullable=False,
            )
        )
        batch.create_check_constraint(
            op.f("ck_logical_models_logical_model_routing_policy"),
            "routing_policy IN "
            "('strict-nvidia','strict-ascend','prefer-nvidia','prefer-ascend','balanced')",
        )
        batch.create_check_constraint(
            op.f("ck_logical_models_logical_model_routing_cursor"),
            "routing_cursor >= 0",
        )
        batch.create_unique_constraint(
            "uq_logical_models_project_public_name",
            ["project_id", "public_name"],
        )


def downgrade() -> None:
    with op.batch_alter_table("logical_models") as batch:
        batch.drop_constraint("uq_logical_models_project_public_name", type_="unique")
        batch.drop_constraint(
            op.f("ck_logical_models_logical_model_routing_cursor"),
            type_="check",
        )
        batch.drop_constraint(
            op.f("ck_logical_models_logical_model_routing_policy"),
            type_="check",
        )
        batch.drop_column("routing_cursor")
        batch.drop_column("routing_policy")
