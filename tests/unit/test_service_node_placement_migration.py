import importlib.util
import uuid
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


def _load_migration() -> Any:
    path = Path(__file__).parents[2] / "migrations" / "versions" / "0016_service_node_placement.py"
    spec = importlib.util.spec_from_file_location("migration_0016_service_node_placement", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot_sql() -> str:
    return (
        "(logical_model_id IS NULL AND model_variant_id IS NULL "
        "AND selected_vendor IS NULL AND selected_kind IS NULL "
        "AND selected_model IS NULL AND runtime_profile_id IS NULL "
        "AND runtime_profile_version IS NULL AND runtime_profile_digest IS NULL "
        "AND allocation_authority IS NULL AND accelerator_resource_name IS NULL "
        "AND selection_policy IS NULL) OR "
        "(logical_model_id IS NOT NULL AND model_variant_id IS NOT NULL "
        "AND selected_vendor IS NOT NULL AND selected_kind IS NOT NULL "
        "AND selected_model IS NOT NULL AND runtime_profile_id IS NOT NULL "
        "AND runtime_profile_version IS NOT NULL AND runtime_profile_digest IS NOT NULL "
        "AND allocation_authority IS NOT NULL AND selection_policy IS NOT NULL)"
    )


def _table(metadata: sa.MetaData, name: str) -> sa.Table:
    columns: list[sa.Column[Any] | sa.CheckConstraint] = [
        sa.Column("id", sa.Uuid(), primary_key=True)
    ]
    if name == "model_services":
        columns.extend(
            [
                sa.Column("runtime_type", sa.String(32), nullable=False),
                sa.Column("status", sa.String(32), nullable=False),
                sa.Column("desired_replicas", sa.Integer(), nullable=False),
            ]
        )
    if name == "service_replicas":
        columns.extend(
            [
                sa.Column(
                    "service_id",
                    sa.Uuid(),
                    sa.ForeignKey("model_services.id", ondelete="CASCADE"),
                    nullable=False,
                ),
                sa.Column("status", sa.String(32), nullable=False),
            ]
        )
    columns.extend(
        [
            sa.Column("logical_model_id", sa.Uuid()),
            sa.Column("model_variant_id", sa.Uuid()),
            sa.Column("selected_vendor", sa.String(64)),
            sa.Column("selected_kind", sa.String(32)),
            sa.Column("selected_model", sa.String(255)),
            sa.Column("runtime_profile_id", sa.String(128)),
            sa.Column("runtime_profile_version", sa.String(32)),
            sa.Column("runtime_profile_digest", sa.String(71)),
            sa.Column("allocation_authority", sa.String(64)),
            sa.Column("accelerator_resource_name", sa.String(255)),
            sa.Column("selection_policy", sa.String(32)),
            sa.CheckConstraint(
                _snapshot_sql(),
                name=f"ck_{name}_admission_snapshot",
            ),
        ]
    )
    return sa.Table(name, metadata, *columns)


def _logical_snapshot(
    row_id: uuid.UUID,
    *,
    service: bool = False,
    **overrides: object,
) -> dict[str, object]:
    values: dict[str, object] = {
        "id": row_id,
        "logical_model_id": uuid.uuid4(),
        "model_variant_id": uuid.uuid4(),
        "selected_vendor": "nvidia",
        "selected_kind": "gpu",
        "selected_model": "NVIDIA A100",
        "runtime_profile_id": "nvidia-vllm-k8s",
        "runtime_profile_version": "2.0.0",
        "runtime_profile_digest": "sha256:" + "a" * 64,
        "allocation_authority": "kubernetes_device_plugin",
        "accelerator_resource_name": "nvidia.com/gpu",
        "selection_policy": "nvidia-only",
    }
    if service:
        values.update(runtime_type="kubernetes", status="running", desired_replicas=1)
    else:
        values.update(status="stopped")
    values.update(overrides)
    return values


def _reflected_logical_snapshot(**overrides: object) -> dict[str, object]:
    values = _logical_snapshot(uuid.uuid4(), service=True, **overrides)
    for key in ("id", "service_id", "logical_model_id", "model_variant_id"):
        if isinstance(values.get(key), uuid.UUID):
            values[key] = str(values[key])
    return values


def test_service_node_placement_migration_backfills_and_downgrades(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'service-node-placement.sqlite3'}")
    metadata = sa.MetaData()
    services = _table(metadata, "model_services")
    replicas = _table(metadata, "service_replicas")
    metadata.create_all(engine)
    service_id = uuid.uuid4()
    replica_id = uuid.uuid4()

    migration = _load_migration()
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.execute(
            services.insert().values(
                _logical_snapshot(service_id, service=True, desired_replicas=0)
            )
        )
        connection.execute(
            replicas.insert().values(_logical_snapshot(replica_id, service_id=service_id))
        )
        connection.commit()
        migration_context = MigrationContext.configure(connection)
        migration.op = Operations(migration_context)
        with migration_context.begin_transaction(_per_migration=True):
            migration.upgrade()

        upgraded_services = sa.Table("model_services", sa.MetaData(), autoload_with=connection)
        upgraded_replicas = sa.Table("service_replicas", sa.MetaData(), autoload_with=connection)
        assert (
            connection.execute(sa.select(upgraded_services.c.eligible_node_names)).scalar_one()
            == []
        )
        assert (
            connection.execute(sa.select(upgraded_replicas.c.eligible_node_names)).scalar_one()
            == []
        )
        assert (
            connection.execute(
                sa.select(sa.func.count()).select_from(upgraded_replicas)
            ).scalar_one()
            == 1
        )
        assert (
            str(connection.execute(sa.select(upgraded_replicas.c.id)).scalar_one()).replace("-", "")
            == replica_id.hex
        )
        replica_row = connection.execute(sa.select(upgraded_replicas)).mappings().one()
        assert str(replica_row["service_id"]).replace("-", "") == service_id.hex
        assert replica_row["selected_model"] == "NVIDIA A100"
        assert replica_row["assigned_node_name"] is None

        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded_services.insert().values(
                    _reflected_logical_snapshot(eligible_node_names=sa.null())
                )
            )
        connection.execute(
            upgraded_services.insert().values(
                _reflected_logical_snapshot(
                    eligible_node_names=["gpu-node-a", "gpu-node-b"],
                )
            )
        )
        connection.commit()

        with migration_context.begin_transaction(_per_migration=True):
            migration.downgrade()
        downgraded_replicas = sa.Table("service_replicas", sa.MetaData(), autoload_with=connection)
        assert (
            connection.execute(
                sa.select(sa.func.count()).select_from(downgraded_replicas)
            ).scalar_one()
            == 1
        )
        assert (
            str(connection.execute(sa.select(downgraded_replicas.c.id)).scalar_one()).replace(
                "-", ""
            )
            == replica_id.hex
        )
        downgraded_row = connection.execute(sa.select(downgraded_replicas)).mappings().one()
        assert str(downgraded_row["service_id"]).replace("-", "") == service_id.hex
        assert downgraded_row["selected_model"] == "NVIDIA A100"
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        for table_name in ("model_services", "service_replicas"):
            assert "eligible_node_names" not in {
                column["name"] for column in sa.inspect(connection).get_columns(table_name)
            }
    engine.dispose()


@pytest.mark.parametrize(
    ("status", "desired_replicas"),
    [
        ("pending", 1),
        ("deploying", 1),
        ("running", 1),
        ("degraded", 1),
        ("failed", 1),
    ],
)
def test_service_node_placement_migration_rejects_unrecoverable_service_before_ddl(
    tmp_path: Path,
    status: str,
    desired_replicas: int,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'inflight-service.sqlite3'}")
    metadata = sa.MetaData()
    services = _table(metadata, "model_services")
    replicas = _table(metadata, "service_replicas")
    metadata.create_all(engine)
    service_id = uuid.uuid4()
    replica_id = uuid.uuid4()

    migration = _load_migration()
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.execute(
            services.insert().values(
                _logical_snapshot(
                    service_id,
                    service=True,
                    status=status,
                    desired_replicas=desired_replicas,
                )
            )
        )
        connection.execute(
            replicas.insert().values(_logical_snapshot(replica_id, service_id=service_id))
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with pytest.raises(
            RuntimeError,
            match="cannot reconstruct the full immutable eligible-node set",
        ):
            with context.begin_transaction(_per_migration=True):
                migration.upgrade()

        assert "eligible_node_names" not in {
            column["name"] for column in sa.inspect(connection).get_columns("model_services")
        }
        assert (
            connection.execute(sa.select(sa.func.count()).select_from(services)).scalar_one() == 1
        )
        assert (
            connection.execute(sa.select(sa.func.count()).select_from(replicas)).scalar_one() == 1
        )
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    engine.dispose()


def test_service_node_placement_migration_rejects_nonterminal_replica_at_zero_scale(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'draining-service.sqlite3'}")
    metadata = sa.MetaData()
    services = _table(metadata, "model_services")
    replicas = _table(metadata, "service_replicas")
    metadata.create_all(engine)
    service_id = uuid.uuid4()

    migration = _load_migration()
    with engine.connect() as connection:
        connection.execute(
            services.insert().values(
                _logical_snapshot(service_id, service=True, desired_replicas=0)
            )
        )
        connection.execute(
            replicas.insert().values(
                _logical_snapshot(
                    uuid.uuid4(),
                    service_id=service_id,
                    status="draining",
                )
            )
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with pytest.raises(RuntimeError, match="wait for every replica"):
            with context.begin_transaction(_per_migration=True):
                migration.upgrade()

        assert "eligible_node_names" not in {
            column["name"] for column in sa.inspect(connection).get_columns("model_services")
        }
    engine.dispose()


@pytest.mark.parametrize("status", ["degraded", "failed"])
def test_service_node_placement_migration_allows_unhealthy_service_scaled_to_zero(
    tmp_path: Path,
    status: str,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'failed-stopped-service.sqlite3'}")
    metadata = sa.MetaData()
    services = _table(metadata, "model_services")
    replicas = _table(metadata, "service_replicas")
    metadata.create_all(engine)
    service_id = uuid.uuid4()

    migration = _load_migration()
    with engine.connect() as connection:
        connection.execute(
            services.insert().values(
                _logical_snapshot(
                    service_id,
                    service=True,
                    status=status,
                    desired_replicas=0,
                )
            )
        )
        connection.execute(
            replicas.insert().values(_logical_snapshot(uuid.uuid4(), service_id=service_id))
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with context.begin_transaction(_per_migration=True):
            migration.upgrade()

        upgraded = sa.Table("model_services", sa.MetaData(), autoload_with=connection)
        row = connection.execute(
            sa.select(
                upgraded.c.status, upgraded.c.desired_replicas, upgraded.c.eligible_node_names
            )
        ).one()
        assert row == (status, 0, [])
    engine.dispose()


def test_service_node_placement_migration_compiles_postgresql_offline_sql() -> None:
    output = StringIO()
    context = MigrationContext.configure(
        url="postgresql://",
        opts={"as_sql": True, "literal_binds": True, "output_buffer": output},
    )
    migration = _load_migration()
    migration.op = Operations(context)

    migration.upgrade()

    rendered = output.getvalue()
    assert "'[]'::json" in rendered
    assert "assigned_node_name" in rendered
    assert "service.desired_replicas > 0" in rendered
    assert "replica.status IN ('pending', 'starting', 'loading', 'running'" in rendered


def test_sqlite_foreign_keys_are_restored_when_migration_body_fails() -> None:
    engine = sa.create_engine("sqlite://")
    migration = _load_migration()
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with pytest.raises(RuntimeError, match="injected migration failure"):
            with context.begin_transaction(_per_migration=True):
                with migration._preserve_sqlite_referencing_rows():
                    assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 0
                    raise RuntimeError("injected migration failure")

        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    engine.dispose()
