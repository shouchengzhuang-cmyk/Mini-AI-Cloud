import importlib.util
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration() -> Any:
    path = Path(__file__).parents[2] / "migrations" / "versions" / "0014_vendor_routing.py"
    spec = importlib.util.spec_from_file_location("migration_0014", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vendor_routing_migration_upgrades_and_downgrades(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'routing.sqlite3'}")
    metadata = sa.MetaData()
    sa.Table("projects", metadata, sa.Column("id", sa.Uuid(), primary_key=True))
    sa.Table(
        "logical_models",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
    )
    sa.Table(
        "serving_request_usage",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("request_id", sa.Uuid(), nullable=False),
    )
    metadata.create_all(engine)

    migration = _load_migration()
    with engine.begin() as connection:
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
        inspector = sa.inspect(connection)
        assert "vendor_circuit_states" in inspector.get_table_names()
        usage_columns = {item["name"] for item in inspector.get_columns("serving_request_usage")}
        assert {"logical_model_id", "model_variant_id", "selected_vendor"} <= usage_columns
        circuit_checks = {
            item["name"] for item in inspector.get_check_constraints("vendor_circuit_states")
        }
        assert {
            "ck_vendor_circuit_states_vendor",
            "ck_vendor_circuit_states_state",
            "ck_vendor_circuit_states_failure_count",
            "ck_vendor_circuit_states_opened_until",
        } <= circuit_checks

        migration.downgrade()
        inspector = sa.inspect(connection)
        assert "vendor_circuit_states" not in inspector.get_table_names()
        usage_columns = {item["name"] for item in inspector.get_columns("serving_request_usage")}
        assert "logical_model_id" not in usage_columns
    engine.dispose()
