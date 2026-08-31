import importlib.util
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


def _load_migration() -> Any:
    path = (
        Path(__file__).parents[2]
        / "migrations"
        / "versions"
        / "0018_kubernetes_batch_node_placement.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_0018_kubernetes_batch_node_placement",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kubernetes_batch_node_placement_migration_round_trip(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'kubernetes-batch-node.sqlite3'}")
    metadata = sa.MetaData()
    devices = sa.Table(
        "gpu_devices",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("device_uuid", sa.String(255), nullable=False),
    )
    tasks = sa.Table(
        "tasks",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("image", sa.String(512), nullable=False),
    )
    metadata.create_all(engine)

    migration = _load_migration()
    with engine.connect() as connection:
        connection.execute(devices.insert().values(id="device-a", device_uuid="GPU-a"))
        connection.execute(tasks.insert().values(id="task-a", image="example/task:1"))
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)
        with context.begin_transaction(_per_migration=True):
            migration.upgrade()

        upgraded_devices = sa.Table("gpu_devices", sa.MetaData(), autoload_with=connection)
        upgraded_tasks = sa.Table("tasks", sa.MetaData(), autoload_with=connection)
        assert "kubernetes_node_name" in upgraded_devices.c
        assert "kubernetes_node_name" in upgraded_tasks.c
        connection.execute(
            upgraded_devices.update()
            .where(upgraded_devices.c.id == "device-a")
            .values(kubernetes_node_name="gpu-node-a")
        )
        connection.execute(
            upgraded_tasks.update()
            .where(upgraded_tasks.c.id == "task-a")
            .values(kubernetes_node_name="gpu-node-a")
        )
        connection.commit()

        with context.begin_transaction(_per_migration=True):
            migration.downgrade()
        downgraded_devices = sa.Table("gpu_devices", sa.MetaData(), autoload_with=connection)
        downgraded_tasks = sa.Table("tasks", sa.MetaData(), autoload_with=connection)
        assert "kubernetes_node_name" not in downgraded_devices.c
        assert "kubernetes_node_name" not in downgraded_tasks.c
        assert (
            connection.execute(sa.select(downgraded_devices.c.device_uuid)).scalar_one() == "GPU-a"
        )
        assert (
            connection.execute(sa.select(downgraded_tasks.c.image)).scalar_one() == "example/task:1"
        )

    engine.dispose()
