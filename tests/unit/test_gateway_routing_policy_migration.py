import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


def _load_migration() -> Any:
    path = Path(__file__).parents[2] / "migrations" / "versions" / "0015_gateway_routing_policy.py"
    spec = importlib.util.spec_from_file_location("migration_0015", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_gateway_routing_policy_migration_backfills_enforces_and_downgrades(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'gateway-routing-policy.sqlite3'}")
    metadata = sa.MetaData()
    projects = sa.Table("projects", metadata, sa.Column("id", sa.Uuid(), primary_key=True))
    logical_models = sa.Table(
        "logical_models",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("public_name", sa.String(255), nullable=False),
        sa.UniqueConstraint("project_id", "name", name="uq_logical_models_project_name"),
    )
    metadata.create_all(engine)
    first_project_id = uuid.uuid4()
    second_project_id = uuid.uuid4()
    existing_model_id = uuid.uuid4()

    migration = _load_migration()
    with engine.begin() as connection:
        connection.execute(sa.text("PRAGMA foreign_keys=ON"))
        connection.execute(
            projects.insert(),
            [{"id": first_project_id}, {"id": second_project_id}],
        )
        connection.execute(
            logical_models.insert().values(
                id=existing_model_id,
                project_id=first_project_id,
                name="existing",
                public_name="Existing Public",
            )
        )

        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

        inspector = sa.inspect(connection)
        columns = {item["name"]: item for item in inspector.get_columns("logical_models")}
        assert columns["routing_policy"]["nullable"] is False
        assert columns["routing_cursor"]["nullable"] is False
        checks = {item["name"] for item in inspector.get_check_constraints("logical_models")}
        assert {
            "ck_logical_models_logical_model_routing_policy",
            "ck_logical_models_logical_model_routing_cursor",
        } <= checks
        uniques = {
            (item["name"], tuple(item["column_names"]))
            for item in inspector.get_unique_constraints("logical_models")
        }
        assert (
            "uq_logical_models_project_public_name",
            ("project_id", "public_name"),
        ) in uniques

        upgraded = sa.Table("logical_models", sa.MetaData(), autoload_with=connection)
        existing = (
            connection.execute(sa.select(upgraded).where(upgraded.c.id == existing_model_id))
            .mappings()
            .one()
        )
        assert existing["routing_policy"] == "balanced"
        assert existing["routing_cursor"] == 0

        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded.insert().values(
                    id=uuid.uuid4().hex,
                    project_id=first_project_id.hex,
                    name="bad-policy",
                    public_name="Bad Policy",
                    routing_policy="any",
                    routing_cursor=0,
                )
            )
        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded.insert().values(
                    id=uuid.uuid4().hex,
                    project_id=first_project_id.hex,
                    name="bad-cursor",
                    public_name="Bad Cursor",
                    routing_policy="balanced",
                    routing_cursor=-1,
                )
            )
        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded.insert().values(
                    id=uuid.uuid4().hex,
                    project_id=first_project_id.hex,
                    name="duplicate-public",
                    public_name="Existing Public",
                    routing_policy="balanced",
                    routing_cursor=0,
                )
            )
        connection.execute(
            upgraded.insert().values(
                id=uuid.uuid4().hex,
                project_id=second_project_id.hex,
                name="same-public-other-project",
                public_name="Existing Public",
                routing_policy="prefer-ascend",
                routing_cursor=0,
            )
        )

        migration.downgrade()
        downgraded_columns = {
            item["name"] for item in sa.inspect(connection).get_columns("logical_models")
        }
        assert "routing_policy" not in downgraded_columns
        assert "routing_cursor" not in downgraded_columns
    engine.dispose()
