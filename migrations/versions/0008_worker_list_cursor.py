"""Index the Worker keyset-pagination order.

Revision ID: 0008_worker_list_cursor
Revises: 0007_detach_usage_ledger
Create Date: 2026-08-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008_worker_list_cursor"
down_revision: str | Sequence[str] | None = "0007_detach_usage_ledger"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_workers_started_id",
        "workers",
        ["started_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_workers_started_id", table_name="workers")
