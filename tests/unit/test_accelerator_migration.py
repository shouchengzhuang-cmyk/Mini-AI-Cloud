import importlib.util
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


def _load_migration() -> Any:
    path = Path(__file__).parents[2] / "migrations" / "versions" / "0011_accelerator_persistence.py"
    spec = importlib.util.spec_from_file_location("migration_0011", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _legacy_metadata() -> sa.MetaData:
    metadata = sa.MetaData()
    sa.Table(
        "gpu_devices",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("device_uuid", sa.String(255), nullable=False),
        sa.Column("device_index", sa.Integer(), nullable=False),
        sa.Column("vendor", sa.String(64), nullable=False),
        sa.Column("compute_capability", sa.String(32)),
        sa.Column("memory_total_mb", sa.Integer(), nullable=False),
        sa.Column("memory_free_mb", sa.Integer(), nullable=False),
        sa.Column("fake", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("worker_id", "device_index", name="uq_gpu_devices_worker_index"),
    )
    sa.Table(
        "task_executions",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("gpu_count", sa.Integer(), nullable=False),
    )
    sa.Table(
        "resource_reservations",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("gpu_count", sa.Integer(), nullable=False),
        sa.Column("legacy_unbound", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    sa.Table(
        "reservation_gpu_devices",
        metadata,
        sa.Column("reservation_id", sa.Uuid(), nullable=False),
        sa.Column("gpu_device_id", sa.Uuid(), nullable=False),
    )
    return metadata


def test_sqlite_upgrade_backfills_legacy_rows_and_downgrades(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'migration.sqlite3'}")
    metadata = _legacy_metadata()
    metadata.create_all(engine)
    device_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    reservation_id = uuid.uuid4()
    unbound_execution_id = uuid.uuid4()
    unbound_reservation_id = uuid.uuid4()
    now = datetime.now(UTC)

    with engine.begin() as connection:
        tables = metadata.tables
        connection.execute(
            tables["gpu_devices"]
            .insert()
            .values(
                id=device_id,
                worker_id="legacy-worker",
                device_uuid="FAKE-GPU-0",
                device_index=0,
                vendor="fake",
                compute_capability="8.0",
                memory_total_mb=40_960,
                memory_free_mb=40_960,
                fake=True,
            )
        )
        connection.execute(
            tables["task_executions"].insert(),
            [
                {"id": execution_id, "gpu_count": 1},
                {"id": unbound_execution_id, "gpu_count": 1},
            ],
        )
        connection.execute(
            tables["resource_reservations"].insert(),
            [
                {
                    "id": reservation_id,
                    "execution_id": execution_id,
                    "gpu_count": 1,
                    "legacy_unbound": False,
                    "created_at": now,
                },
                {
                    "id": unbound_reservation_id,
                    "execution_id": unbound_execution_id,
                    "gpu_count": 1,
                    "legacy_unbound": False,
                    "created_at": now,
                },
            ],
        )
        connection.execute(
            tables["reservation_gpu_devices"]
            .insert()
            .values(
                reservation_id=reservation_id,
                gpu_device_id=device_id,
            )
        )

        migration = _load_migration()
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

        device = connection.execute(sa.text("SELECT * FROM gpu_devices")).mappings().one()
        bound = (
            connection.execute(
                sa.text("SELECT * FROM resource_reservations WHERE id = :id"),
                {"id": reservation_id.hex},
            )
            .mappings()
            .one()
        )
        unbound = (
            connection.execute(
                sa.text("SELECT * FROM resource_reservations WHERE id = :id"),
                {"id": unbound_reservation_id.hex},
            )
            .mappings()
            .one()
        )
        execution = (
            connection.execute(
                sa.text("SELECT * FROM task_executions WHERE id = :id"),
                {"id": execution_id.hex},
            )
            .mappings()
            .one()
        )

        assert device["vendor"] == "nvidia"
        assert device["accelerator_kind"] == "gpu"
        assert device["compute_arch"] == "8.0"
        unique_constraints = {
            constraint["name"]: tuple(constraint["column_names"])
            for constraint in sa.inspect(connection).get_unique_constraints("gpu_devices")
        }
        assert unique_constraints["uq_gpu_devices_worker_vendor_index"] == (
            "worker_id",
            "vendor",
            "device_index",
        )
        assert bound["allocation_authority"] == "control_plane_exact_device"
        assert bound["requested_vendor"] == "nvidia"
        assert bound["requested_kind"] == "gpu"
        assert bound["observed_vendor"] == "nvidia"
        assert "FAKE-GPU-0" in bound["observed_device_ids_json"]
        assert execution["observed_vendor"] == "nvidia"
        assert unbound["legacy_unbound"] == 1
        with pytest.raises(IntegrityError, match="observed_vendor"):
            connection.execute(
                sa.text(
                    "UPDATE resource_reservations SET observed_vendor = 'huawei-ascend' "
                    "WHERE id = :id"
                ),
                {"id": reservation_id.hex},
            )

        migration.downgrade()
        columns = {column["name"] for column in sa.inspect(connection).get_columns("gpu_devices")}
        assert "accelerator_kind" not in columns
        downgraded_unique_constraints = {
            constraint["name"]: tuple(constraint["column_names"])
            for constraint in sa.inspect(connection).get_unique_constraints("gpu_devices")
        }
        assert downgraded_unique_constraints["uq_gpu_devices_worker_index"] == (
            "worker_id",
            "device_index",
        )
        execution_columns = {
            column["name"] for column in sa.inspect(connection).get_columns("task_executions")
        }
        assert "allocation_authority" not in execution_columns

    engine.dispose()
