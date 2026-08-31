"""Persist the Kubernetes node selected for batch accelerator admission.

Revision ID: 0018_k8s_batch_node_placement
Revises: 0017_k8s_job_observation
Create Date: 2026-08-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_k8s_batch_node_placement"
down_revision: str | Sequence[str] | None = "0017_k8s_job_observation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("gpu_devices") as batch:
        batch.add_column(sa.Column("kubernetes_node_name", sa.String(253), nullable=True))
    with op.batch_alter_table("tasks") as batch:
        batch.add_column(sa.Column("kubernetes_node_name", sa.String(253), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_column("kubernetes_node_name")
    with op.batch_alter_table("gpu_devices") as batch:
        batch.drop_column("kubernetes_node_name")
