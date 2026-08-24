"""index durable event cursor polling

Revision ID: 0009_outbox_event_cursor
Revises: 0008_worker_list_cursor
Create Date: 2026-08-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009_outbox_event_cursor"
down_revision: str | None = "0008_worker_list_cursor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_outbox_events_created_id",
        "outbox_events",
        ["created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_created_id", table_name="outbox_events")
