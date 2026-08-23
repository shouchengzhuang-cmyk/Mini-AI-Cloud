"""Add aggregate Worker resource reservations.

Revision ID: 0002_worker_reservations
Revises: 0001_initial
Create Date: 2026-08-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_worker_reservations"
down_revision: str | Sequence[str] | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "workers",
        sa.Column("reserved_cpu", sa.Float(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "workers",
        sa.Column(
            "reserved_memory_mb", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
    )
    op.add_column(
        "workers",
        sa.Column("reserved_gpus", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("workers", "reserved_gpus")
    op.drop_column("workers", "reserved_memory_mb")
    op.drop_column("workers", "reserved_cpu")
