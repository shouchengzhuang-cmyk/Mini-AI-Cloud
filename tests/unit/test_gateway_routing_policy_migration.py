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
    status_events = sa.Table(
        "logical_model_status_events",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "logical_model_id",
            sa.Uuid(),
            sa.ForeignKey("logical_models.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )
    variants = sa.Table(
        "model_variants",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "logical_model_id",
            sa.Uuid(),
            sa.ForeignKey("logical_models.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )
    circuits = sa.Table(
        "vendor_circuit_states",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "logical_model_id",
            sa.Uuid(),
            sa.ForeignKey("logical_models.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )
    services = sa.Table(
        "model_services",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "logical_model_id",
            sa.Uuid(),
            sa.ForeignKey("logical_models.id", ondelete="RESTRICT"),
            nullable=False,
        ),
    )
    metadata.create_all(engine)
    first_project_id = uuid.uuid4()
    second_project_id = uuid.uuid4()
    existing_model_id = uuid.uuid4()
    child_ids = {
        "status": uuid.uuid4(),
        "variant": uuid.uuid4(),
        "circuit": uuid.uuid4(),
        "service": uuid.uuid4(),
    }

    migration = _load_migration()
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
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
        connection.execute(
            status_events.insert().values(
                id=child_ids["status"], logical_model_id=existing_model_id
            )
        )
        connection.execute(
            variants.insert().values(id=child_ids["variant"], logical_model_id=existing_model_id)
        )
        connection.execute(
            circuits.insert().values(id=child_ids["circuit"], logical_model_id=existing_model_id)
        )
        connection.execute(
            services.insert().values(
                id=child_ids["service"],
                project_id=first_project_id,
                name="Existing Public",
                logical_model_id=existing_model_id,
            )
        )
        connection.commit()

        migration_context = MigrationContext.configure(connection)
        migration.op = Operations(migration_context)
        with migration_context.begin_transaction(_per_migration=True):
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
        for child_table, key in (
            (status_events, "status"),
            (variants, "variant"),
            (circuits, "circuit"),
            (services, "service"),
        ):
            assert connection.execute(sa.select(child_table.c.id)).scalar_one() == child_ids[key]
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []

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

        connection.commit()
        with migration_context.begin_transaction(_per_migration=True):
            migration.downgrade()
        downgraded_columns = {
            item["name"] for item in sa.inspect(connection).get_columns("logical_models")
        }
        assert "routing_policy" not in downgraded_columns
        assert "routing_cursor" not in downgraded_columns
        for child_table, key in (
            (status_events, "status"),
            (variants, "variant"),
            (circuits, "circuit"),
            (services, "service"),
        ):
            assert connection.execute(sa.select(child_table.c.id)).scalar_one() == child_ids[key]
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    engine.dispose()


def test_gateway_routing_policy_migration_rejects_duplicate_public_names_before_ddl(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'duplicate-public-names.sqlite3'}")
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
    sa.Table(
        "model_services",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("logical_model_id", sa.Uuid(), nullable=True),
    )
    metadata.create_all(engine)
    project_id = uuid.uuid4()
    migration = _load_migration()

    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.execute(projects.insert().values(id=project_id))
        connection.execute(
            logical_models.insert(),
            [
                {
                    "id": uuid.uuid4(),
                    "project_id": project_id,
                    "name": "first",
                    "public_name": "Shared Public",
                },
                {
                    "id": uuid.uuid4(),
                    "project_id": project_id,
                    "name": "second",
                    "public_name": "Shared Public",
                },
            ],
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with pytest.raises(RuntimeError, match="duplicate gateway identities"):
            with context.begin_transaction(_per_migration=True):
                migration.upgrade()

        columns = {item["name"] for item in sa.inspect(connection).get_columns("logical_models")}
        assert "routing_policy" not in columns
        assert "routing_cursor" not in columns
        assert (
            connection.execute(sa.select(sa.func.count()).select_from(logical_models)).scalar_one()
            == 2
        )
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    engine.dispose()


def test_gateway_routing_policy_migration_rejects_cross_namespace_name_collision(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'cross-namespace-name.sqlite3'}")
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
    services = sa.Table(
        "model_services",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("logical_model_id", sa.Uuid(), nullable=True),
    )
    metadata.create_all(engine)
    project_id = uuid.uuid4()
    migration = _load_migration()

    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.execute(projects.insert().values(id=project_id))
        connection.execute(
            logical_models.insert().values(
                id=uuid.uuid4(),
                project_id=project_id,
                name="logical-chat",
                public_name="Public Chat",
            )
        )
        connection.execute(
            services.insert().values(
                id=uuid.uuid4(),
                project_id=project_id,
                name="Public Chat",
                logical_model_id=None,
            )
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)

        with pytest.raises(RuntimeError, match="cross-namespace gateway identity"):
            with context.begin_transaction(_per_migration=True):
                migration.upgrade()

        columns = {item["name"] for item in sa.inspect(connection).get_columns("logical_models")}
        assert "routing_policy" not in columns
        assert "routing_cursor" not in columns
        assert (
            connection.execute(sa.select(sa.func.count()).select_from(services)).scalar_one() == 1
        )
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    engine.dispose()


def test_sqlite_foreign_keys_are_restored_when_gateway_migration_body_fails() -> None:
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


def test_gateway_routing_policy_migration_compiles_postgresql_offline_guard() -> None:
    output = StringIO()
    context = MigrationContext.configure(
        url="postgresql://",
        opts={"as_sql": True, "literal_binds": True, "output_buffer": output},
    )
    migration = _load_migration()
    migration.op = Operations(context)

    migration.upgrade()

    rendered = output.getvalue()
    assert "DO $$ BEGIN" in rendered
    assert "duplicate gateway identities" in rendered
    assert "cross-namespace gateway identities" in rendered
    assert "JOIN model_services AS service" in rendered
    assert "service.logical_model_id IS NULL" in rendered
    assert "uq_logical_models_project_public_name" in rendered
