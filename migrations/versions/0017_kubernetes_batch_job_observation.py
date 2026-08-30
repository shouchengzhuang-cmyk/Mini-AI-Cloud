"""Persist fenced Kubernetes batch Job and controlled Pod identity.

Revision ID: 0017_k8s_job_observation
Revises: 0016_service_node_placement
Create Date: 2026-08-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_k8s_job_observation"
down_revision: str | Sequence[str] | None = "0016_service_node_placement"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("task_executions") as batch:
        batch.add_column(sa.Column("runtime_namespace", sa.String(253), nullable=True))
        batch.add_column(sa.Column("runtime_resource_kind", sa.String(32), nullable=True))
        batch.add_column(sa.Column("runtime_resource_name", sa.String(253), nullable=True))
        batch.add_column(sa.Column("runtime_resource_uid", sa.String(128), nullable=True))
        batch.add_column(sa.Column("runtime_worker_session_id", sa.Uuid(), nullable=True))
        batch.add_column(sa.Column("observed_pod_name", sa.String(253), nullable=True))
        batch.add_column(sa.Column("observed_pod_uid", sa.String(128), nullable=True))
        batch.add_column(sa.Column("runtime_spec_hash", sa.String(64), nullable=True))
        batch.create_check_constraint(
            op.f("ck_task_executions_runtime_resource_observation"),
            "(runtime_namespace IS NULL AND runtime_resource_kind IS NULL "
            "AND runtime_resource_name IS NULL AND runtime_resource_uid IS NULL "
            "AND runtime_spec_hash IS NULL AND runtime_worker_session_id IS NULL) OR "
            "(runtime_namespace IS NOT NULL AND runtime_resource_kind IS NOT NULL "
            "AND runtime_resource_name IS NOT NULL AND runtime_resource_uid IS NOT NULL "
            "AND runtime_spec_hash IS NOT NULL AND runtime_worker_session_id IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f("ck_task_executions_observed_pod_identity"),
            "(observed_pod_name IS NULL AND observed_pod_uid IS NULL) OR "
            "(observed_pod_name IS NOT NULL AND observed_pod_uid IS NOT NULL)",
        )


def downgrade() -> None:
    with op.batch_alter_table("task_executions") as batch:
        batch.drop_constraint(
            op.f("ck_task_executions_observed_pod_identity"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_task_executions_runtime_resource_observation"), type_="check"
        )
        batch.drop_column("runtime_spec_hash")
        batch.drop_column("observed_pod_uid")
        batch.drop_column("observed_pod_name")
        batch.drop_column("runtime_resource_uid")
        batch.drop_column("runtime_worker_session_id")
        batch.drop_column("runtime_resource_name")
        batch.drop_column("runtime_resource_kind")
        batch.drop_column("runtime_namespace")
