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
    path = Path(__file__).parents[2] / "migrations" / "versions" / "0012_model_variants.py"
    spec = importlib.util.spec_from_file_location("migration_0012", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_model_variant_migration_enforces_database_contract_and_downgrades(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'model-variants.sqlite3'}")
    metadata = sa.MetaData()
    sa.Table("users", metadata, sa.Column("id", sa.Uuid(), primary_key=True))
    projects = sa.Table("projects", metadata, sa.Column("id", sa.Uuid(), primary_key=True))
    metadata.create_all(engine)
    project_id = uuid.uuid4()

    with engine.begin() as connection:
        connection.execute(sa.text("PRAGMA foreign_keys=ON"))
        connection.execute(projects.insert().values(id=project_id))
        migration = _load_migration()
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

        inspector = sa.inspect(connection)
        assert {
            "logical_models",
            "model_variants",
            "logical_model_status_events",
        } <= set(inspector.get_table_names())
        logical_models = sa.Table("logical_models", sa.MetaData(), autoload_with=connection)
        model_variants = sa.Table("model_variants", sa.MetaData(), autoload_with=connection)
        logical_model_id = uuid.uuid4()
        connection.execute(
            logical_models.insert().values(
                id=logical_model_id.hex,
                project_id=project_id.hex,
                name="qwen-small",
                public_name="Qwen Small",
                status="disabled",
                metadata={},
                version=1,
            )
        )
        valid = {
            "id": uuid.uuid4().hex,
            "logical_model_id": logical_model_id.hex,
            "name": "qwen-small-nvidia",
            "vendor": "nvidia",
            "accelerator_kind": "gpu",
            "runtime_profile_id": "nvidia-vllm-k8s",
            "runtime_profile_version": "1.0.0",
            "runtime_profile_digest": "sha256:" + "a" * 64,
            "artifact_source": "modelscope/qwen-small",
            "artifact_revision": "revision-1",
            "artifact_digest": "sha256:" + "b" * 64,
            "architecture": "qwen2",
            "dtype": "bfloat16",
            "status": "ready",
            "metadata": {},
            "version": 1,
        }
        connection.execute(model_variants.insert().values(**valid))

        with connection.begin_nested(), pytest.raises(IntegrityError, match="vendor_kind"):
            invalid = dict(valid)
            invalid.update(
                id=uuid.uuid4().hex,
                name="invalid-pair",
                accelerator_kind="npu",
            )
            connection.execute(model_variants.insert().values(**invalid))
        with connection.begin_nested(), pytest.raises(IntegrityError, match="artifact_digest"):
            invalid = dict(valid)
            invalid.update(
                id=uuid.uuid4().hex,
                name="invalid-digest",
                artifact_digest="",
            )
            connection.execute(model_variants.insert().values(**invalid))

        migration.downgrade()
        assert "logical_models" not in sa.inspect(connection).get_table_names()

    engine.dispose()
