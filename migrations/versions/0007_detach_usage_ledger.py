"""Detach immutable usage provenance from operational task retention.

Revision ID: 0007_detach_usage_ledger
Revises: 0006_service_quota_resources
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_detach_usage_ledger"
down_revision: str | Sequence[str] | None = "0006_service_quota_resources"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Keep source UUIDs while allowing operational rows to expire."""

    op.drop_constraint(
        "fk_usage_ledger_task_id_tasks",
        "usage_ledger",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_usage_ledger_execution_id_task_executions",
        "usage_ledger",
        type_="foreignkey",
    )


def downgrade() -> None:
    """Restore strict FKs only while all retained provenance rows still resolve."""

    connection = op.get_bind()
    orphan_count = connection.execute(
        sa.text(
            "SELECT COUNT(*) FROM usage_ledger AS usage "
            "LEFT JOIN tasks AS task ON task.id = usage.task_id "
            "LEFT JOIN task_executions AS execution ON execution.id = usage.execution_id "
            "WHERE task.id IS NULL OR execution.id IS NULL"
        )
    ).scalar_one()
    if orphan_count:
        raise RuntimeError(
            "cannot downgrade 0007_detach_usage_ledger: "
            f"{orphan_count} retained usage rows refer to expired tasks or executions"
        )

    op.create_foreign_key(
        "fk_usage_ledger_execution_id_task_executions",
        "usage_ledger",
        "task_executions",
        ["execution_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_usage_ledger_task_id_tasks",
        "usage_ledger",
        "tasks",
        ["task_id"],
        ["id"],
        ondelete="RESTRICT",
    )
