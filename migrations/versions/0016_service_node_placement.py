"""Persist Kubernetes service node compatibility and actual placement.

Revision ID: 0016_service_node_placement
Revises: 0015_gateway_routing_policy
Create Date: 2026-08-29
"""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import sqlalchemy as sa
from alembic import op

revision: str = "0016_service_node_placement"
down_revision: str | Sequence[str] | None = "0015_gateway_routing_policy"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE_NAMES = ("model_services", "service_replicas")


@contextmanager
def _preserve_sqlite_referencing_rows() -> Iterator[None]:
    """Prevent SQLite parent-table batch rebuilds from cascading child rows."""

    if op.get_bind().dialect.name != "sqlite":
        yield
        return
    context = op.get_context()
    with context.autocommit_block():
        op.execute(sa.text("PRAGMA foreign_keys=OFF"))
        try:
            yield
        finally:
            op.execute(sa.text("PRAGMA foreign_keys=ON"))


def _snapshot_sql(*, include_eligible_nodes: bool) -> str:
    empty_nodes = " AND eligible_node_names IS NULL" if include_eligible_nodes else ""
    present_nodes = " AND eligible_node_names IS NOT NULL" if include_eligible_nodes else ""
    return (
        "(logical_model_id IS NULL AND model_variant_id IS NULL "
        "AND selected_vendor IS NULL AND selected_kind IS NULL "
        "AND selected_model IS NULL AND runtime_profile_id IS NULL "
        "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
        "AND allocation_authority IS NULL AND accelerator_resource_name IS NULL "
        f"AND selection_policy IS NULL{empty_nodes}) OR "
        "(logical_model_id IS NOT NULL AND model_variant_id IS NOT NULL "
        "AND selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
        "AND selected_model IS NOT NULL AND runtime_profile_id IS NOT NULL "
        "AND runtime_profile_version IS NOT NULL AND runtime_profile_digest IS NOT NULL "
        f"AND allocation_authority IS NOT NULL AND selection_policy IS NOT NULL{present_nodes})"
    )


def upgrade() -> None:
    with _preserve_sqlite_referencing_rows():
        for table_name in _TABLE_NAMES:
            with op.batch_alter_table(table_name) as batch:
                batch.add_column(
                    sa.Column("eligible_node_names", sa.JSON(none_as_null=True), nullable=True)
                )

            json_empty = (
                "'[]'::json" if op.get_bind().dialect.name == "postgresql" else "'[]'"
            )
            op.execute(
                sa.text(
                    f"UPDATE {table_name} SET eligible_node_names = {json_empty} "
                    "WHERE logical_model_id IS NOT NULL"
                )
            )

            with op.batch_alter_table(table_name) as batch:
                batch.drop_constraint(
                    op.f(f"ck_{table_name}_admission_snapshot"), type_="check"
                )
                batch.create_check_constraint(
                    op.f(f"ck_{table_name}_admission_snapshot"),
                    _snapshot_sql(include_eligible_nodes=True),
                )

        with op.batch_alter_table("service_replicas") as batch:
            batch.add_column(sa.Column("assigned_node_name", sa.String(253), nullable=True))


def downgrade() -> None:
    with _preserve_sqlite_referencing_rows():
        with op.batch_alter_table("service_replicas") as batch:
            batch.drop_column("assigned_node_name")

        for table_name in reversed(_TABLE_NAMES):
            with op.batch_alter_table(table_name) as batch:
                batch.drop_constraint(
                    op.f(f"ck_{table_name}_admission_snapshot"), type_="check"
                )
                batch.create_check_constraint(
                    op.f(f"ck_{table_name}_admission_snapshot"),
                    _snapshot_sql(include_eligible_nodes=False),
                )
                batch.drop_column("eligible_node_names")
