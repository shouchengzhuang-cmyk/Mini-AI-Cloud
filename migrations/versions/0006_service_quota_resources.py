"""Account service desired resources in project quotas.

Revision ID: 0006_service_quota_resources
Revises: 0005_platform_resources
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_service_quota_resources"
down_revision: str | Sequence[str] | None = "0005_platform_resources"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "project_quota_state",
        sa.Column(
            "service_reserved_cpu_millicores",
            sa.BigInteger(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "project_quota_state",
        sa.Column(
            "service_reserved_memory_mb",
            sa.BigInteger(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "project_quota_state",
        sa.Column(
            "service_reserved_gpus",
            sa.BigInteger(),
            server_default="0",
            nullable=False,
        ),
    )
    op.drop_constraint(
        op.f("ck_project_quota_state_nonnegative"),
        "project_quota_state",
        type_="check",
    )
    op.create_check_constraint(
        "nonnegative",
        "project_quota_state",
        "queued_tasks >= 0 AND running_tasks >= 0 "
        "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
        "AND reserved_gpus >= 0 AND service_count >= 0 "
        "AND service_replicas >= 0 AND service_reserved_cpu_millicores >= 0 "
        "AND service_reserved_memory_mb >= 0 AND service_reserved_gpus >= 0 "
        "AND artifact_bytes >= 0 AND daily_reserved_cost >= 0 "
        "AND daily_settled_cost >= 0",
    )
    op.execute(
        sa.text(
            "UPDATE project_quota_state AS quota_state SET "
            "service_count = commitments.service_count, "
            "service_replicas = commitments.service_replicas, "
            "service_reserved_cpu_millicores = commitments.cpu_millicores, "
            "service_reserved_memory_mb = commitments.memory_mb, "
            "service_reserved_gpus = commitments.gpus "
            "FROM ("
            "SELECT project_id, "
            "COUNT(*) FILTER (WHERE desired_replicas > 0) AS service_count, "
            "COALESCE(SUM(desired_replicas), 0) AS service_replicas, "
            "COALESCE(SUM(cpu_millicores::bigint * desired_replicas), 0) "
            "AS cpu_millicores, "
            "COALESCE(SUM(memory_mb::bigint * desired_replicas), 0) AS memory_mb, "
            "COALESCE(SUM(gpu_count::bigint * desired_replicas), 0) AS gpus "
            "FROM model_services GROUP BY project_id"
            ") AS commitments "
            "WHERE quota_state.project_id = commitments.project_id"
        )
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_project_quota_state_nonnegative"),
        "project_quota_state",
        type_="check",
    )
    op.create_check_constraint(
        "nonnegative",
        "project_quota_state",
        "queued_tasks >= 0 AND running_tasks >= 0 "
        "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
        "AND reserved_gpus >= 0 AND service_count >= 0 "
        "AND service_replicas >= 0 AND artifact_bytes >= 0 "
        "AND daily_reserved_cost >= 0 AND daily_settled_cost >= 0",
    )
    op.drop_column("project_quota_state", "service_reserved_gpus")
    op.drop_column("project_quota_state", "service_reserved_memory_mb")
    op.drop_column("project_quota_state", "service_reserved_cpu_millicores")
