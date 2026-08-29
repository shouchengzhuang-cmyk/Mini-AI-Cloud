"""Add explicit logical-model gateway routing policy and cursor.

Revision ID: 0015_gateway_routing_policy
Revises: 0014_vendor_routing
Create Date: 2026-08-28
"""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import sqlalchemy as sa
from alembic import op

revision: str = "0015_gateway_routing_policy"
down_revision: str | Sequence[str] | None = "0014_vendor_routing"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


@contextmanager
def _preserve_sqlite_referencing_rows() -> Iterator[None]:
    """Prevent SQLite logical-model rebuilds from mutating child tables."""

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


def _reject_duplicate_public_names() -> None:
    """Fail before DDL when the new gateway identity would be ambiguous."""

    context = op.get_context()
    if context.as_sql:
        if context.dialect.name != "postgresql":
            raise RuntimeError("0015 offline SQL preflight is supported only for PostgreSQL")
        op.execute(
            sa.text(
                "DO $$ BEGIN "
                "IF EXISTS (SELECT 1 FROM logical_models "
                "GROUP BY project_id, public_name HAVING count(*) > 1) THEN "
                "RAISE EXCEPTION '0015 cannot add the logical-model public-name "
                "constraint while duplicate gateway identities exist'; "
                "END IF; END $$"
            )
        )
        return
    logical_models = sa.table(
        "logical_models",
        sa.column("project_id", sa.Uuid()),
        sa.column("public_name", sa.String(255)),
    )
    duplicate = (
        op.get_bind()
        .execute(
            sa.select(logical_models.c.project_id, logical_models.c.public_name)
            .group_by(logical_models.c.project_id, logical_models.c.public_name)
            .having(sa.func.count() > 1)
            .limit(1)
        )
        .first()
    )
    if duplicate is not None:
        raise RuntimeError(
            "0015 cannot add the logical-model public-name constraint while duplicate "
            "project/public_name rows exist; resolve the duplicate gateway identities "
            "before upgrading"
        )


def upgrade() -> None:
    _reject_duplicate_public_names()
    with _preserve_sqlite_referencing_rows():
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
    with _preserve_sqlite_referencing_rows():
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
