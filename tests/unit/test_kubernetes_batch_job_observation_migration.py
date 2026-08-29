import importlib.util
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError


def _load_migration() -> Any:
    path = (
        Path(__file__).parents[2]
        / "migrations"
        / "versions"
        / "0017_kubernetes_batch_job_observation.py"
    )
    spec = importlib.util.spec_from_file_location(
        "migration_0017_kubernetes_batch_job_observation",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kubernetes_batch_job_observation_migration_round_trip_and_constraints(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'kubernetes-job-observation.sqlite3'}")
    metadata = sa.MetaData()
    executions = sa.Table(
        "task_executions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("runtime_type", sa.String(32), nullable=False),
        sa.Column("runtime_object_id", sa.String(512)),
    )
    metadata.create_all(engine)

    migration = _load_migration()
    with engine.connect() as connection:
        connection.execute(
            executions.insert().values(
                id="execution-a",
                runtime_type="kubernetes",
                runtime_object_id="legacy-pod-name",
            )
        )
        connection.commit()
        context = MigrationContext.configure(connection)
        migration.op = Operations(context)
        with context.begin_transaction(_per_migration=True):
            migration.upgrade()

        expected_columns = {
            "runtime_namespace",
            "runtime_resource_kind",
            "runtime_resource_name",
            "runtime_resource_uid",
            "runtime_worker_session_id",
            "observed_pod_name",
            "observed_pod_uid",
            "runtime_spec_hash",
        }
        upgraded = sa.Table("task_executions", sa.MetaData(), autoload_with=connection)
        assert expected_columns <= set(upgraded.c.keys())
        row = connection.execute(sa.select(upgraded)).mappings().one()
        assert row["runtime_object_id"] == "legacy-pod-name"
        assert all(row[column] is None for column in expected_columns)

        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded.update()
                .where(upgraded.c.id == "execution-a")
                .values(runtime_namespace="mini-ai-runtime")
            )

        complete_job_identity = {
            "runtime_namespace": "mini-ai-runtime",
            "runtime_resource_kind": "job",
            "runtime_resource_name": "mini-ai-job-a",
            "runtime_resource_uid": "job-uid-a",
            "runtime_worker_session_id": "44444444444444444444444444444444",
            "runtime_spec_hash": "a" * 32,
        }
        with connection.begin_nested(), pytest.raises(IntegrityError):
            connection.execute(
                upgraded.update()
                .where(upgraded.c.id == "execution-a")
                .values(**complete_job_identity, observed_pod_name="controlled-pod-a")
            )

        connection.execute(
            upgraded.update()
            .where(upgraded.c.id == "execution-a")
            .values(
                **complete_job_identity,
                observed_pod_name="controlled-pod-a",
                observed_pod_uid="pod-uid-a",
            )
        )
        connection.commit()

        with context.begin_transaction(_per_migration=True):
            migration.downgrade()
        downgraded = sa.Table("task_executions", sa.MetaData(), autoload_with=connection)
        assert expected_columns.isdisjoint(downgraded.c.keys())
        downgraded_row = connection.execute(sa.select(downgraded)).mappings().one()
        assert downgraded_row["id"] == "execution-a"
        assert downgraded_row["runtime_object_id"] == "legacy-pod-name"

    engine.dispose()
