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
    path = (
        Path(__file__).parents[2]
        / "migrations"
        / "versions"
        / "0013_vendor_aware_admission.py"
    )
    spec = importlib.util.spec_from_file_location("migration_0013", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _create_legacy_schema(connection: sa.Connection) -> dict[str, sa.Table]:
    metadata = sa.MetaData()
    projects = sa.Table("projects", metadata, sa.Column("id", sa.Uuid(), primary_key=True))
    logical_models = sa.Table(
        "logical_models",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
    )
    model_variants = sa.Table(
        "model_variants",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("logical_model_id", sa.Uuid(), nullable=False),
    )
    project_quotas = sa.Table(
        "project_quotas",
        metadata,
        sa.Column("project_id", sa.Uuid(), primary_key=True),
        sa.Column("max_gpus", sa.Integer()),
    )
    project_quota_state = sa.Table(
        "project_quota_state",
        metadata,
        sa.Column("project_id", sa.Uuid(), primary_key=True),
        sa.Column("queued_tasks", sa.Integer(), nullable=False),
        sa.Column("running_tasks", sa.Integer(), nullable=False),
        sa.Column("reserved_cpu_millicores", sa.Integer(), nullable=False),
        sa.Column("reserved_memory_mb", sa.Integer(), nullable=False),
        sa.Column("reserved_gpus", sa.Integer(), nullable=False),
        sa.Column("service_count", sa.Integer(), nullable=False),
        sa.Column("service_replicas", sa.Integer(), nullable=False),
        sa.Column("service_reserved_cpu_millicores", sa.BigInteger(), nullable=False),
        sa.Column("service_reserved_memory_mb", sa.BigInteger(), nullable=False),
        sa.Column("service_reserved_gpus", sa.BigInteger(), nullable=False),
        sa.Column("artifact_bytes", sa.BigInteger(), nullable=False),
        sa.Column("daily_reserved_cost", sa.Numeric(20, 8), nullable=False),
        sa.Column("daily_settled_cost", sa.Numeric(20, 8), nullable=False),
        sa.CheckConstraint(
            "queued_tasks >= 0 AND running_tasks >= 0 "
            "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
            "AND reserved_gpus >= 0 AND service_count >= 0 "
            "AND service_replicas >= 0 AND service_reserved_cpu_millicores >= 0 "
            "AND service_reserved_memory_mb >= 0 AND service_reserved_gpus >= 0 "
            "AND artifact_bytes >= 0 AND daily_reserved_cost >= 0 "
            "AND daily_settled_cost >= 0",
            name="ck_project_quota_state_nonnegative",
        ),
    )
    tasks = sa.Table(
        "tasks",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("gpu_count", sa.Integer(), nullable=False),
        sa.Column("gpu_memory_mb", sa.Integer(), nullable=False),
        sa.Column("gpu_model", sa.String(255)),
    )
    model_services = sa.Table(
        "model_services",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("gpu_count", sa.Integer(), nullable=False),
    )
    service_replicas = sa.Table(
        "service_replicas",
        metadata,
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
    )
    for table_name, state_column in (
        ("task_executions", "status"),
        ("resource_reservations", "state"),
    ):
        sa.Table(
            table_name,
            metadata,
            sa.Column("id", sa.Uuid(), primary_key=True),
            sa.Column("gpu_count", sa.Integer(), nullable=False),
            sa.Column("allocation_authority", sa.String(64), nullable=False),
            sa.Column("requested_vendor", sa.String(64)),
            sa.Column("requested_kind", sa.String(32)),
            sa.Column("requested_profile_id", sa.String(128)),
            sa.Column(state_column, sa.String(32), nullable=False),
            sa.CheckConstraint(
                "(gpu_count = 0 AND requested_vendor IS NULL "
                "AND requested_kind IS NULL AND requested_profile_id IS NULL) OR "
                "(gpu_count > 0 AND requested_vendor IS NOT NULL "
                "AND requested_kind IS NOT NULL)",
                name=f"ck_{table_name}_accelerator_request",
            ),
        )
    metadata.create_all(connection)
    return {
        "projects": projects,
        "logical_models": logical_models,
        "model_variants": model_variants,
        "project_quotas": project_quotas,
        "project_quota_state": project_quota_state,
        "tasks": tasks,
        "model_services": model_services,
        "service_replicas": service_replicas,
    }


def test_vendor_admission_migration_backfills_enforces_and_downgrades(
    tmp_path: Path,
) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'vendor-admission.sqlite3'}")
    project_id = uuid.uuid4()
    logical_model_id = uuid.uuid4()
    variant_id = uuid.uuid4()
    task_id = uuid.uuid4()

    with engine.begin() as connection:
        connection.execute(sa.text("PRAGMA foreign_keys=ON"))
        legacy = _create_legacy_schema(connection)
        connection.execute(legacy["projects"].insert().values(id=project_id))
        connection.execute(
            legacy["logical_models"].insert().values(
                id=logical_model_id,
                project_id=project_id,
            )
        )
        connection.execute(
            legacy["model_variants"].insert().values(
                id=variant_id,
                logical_model_id=logical_model_id,
            )
        )
        connection.execute(
            legacy["project_quotas"].insert().values(
                project_id=project_id,
                max_gpus=8,
            )
        )
        connection.execute(
            legacy["project_quota_state"].insert().values(
                project_id=project_id,
                queued_tasks=0,
                running_tasks=1,
                reserved_cpu_millicores=1000,
                reserved_memory_mb=2048,
                reserved_gpus=3,
                service_count=1,
                service_replicas=1,
                service_reserved_cpu_millicores=1000,
                service_reserved_memory_mb=4096,
                service_reserved_gpus=2,
                artifact_bytes=0,
                daily_reserved_cost=0,
                daily_settled_cost=0,
            )
        )
        connection.execute(
            legacy["tasks"].insert().values(
                id=task_id,
                project_id=project_id,
                status="running",
                gpu_count=2,
                gpu_memory_mb=24_000,
                gpu_model="A10",
            )
        )

        migration = _load_migration()
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

        inspector = sa.inspect(connection)
        assert "admission_events" in inspector.get_table_names()
        assert "model_variant_id" in {
            column["name"] for column in inspector.get_columns("model_services")
        }
        assert {
            "requested_profile_version",
            "requested_profile_digest",
            "model_variant_id",
        } <= {
            column["name"] for column in inspector.get_columns("task_executions")
        }

        quotas = sa.Table("project_quotas", sa.MetaData(), autoload_with=connection)
        quota_state = sa.Table(
            "project_quota_state", sa.MetaData(), autoload_with=connection
        )
        tasks = sa.Table("tasks", sa.MetaData(), autoload_with=connection)
        services = sa.Table("model_services", sa.MetaData(), autoload_with=connection)
        events = sa.Table("admission_events", sa.MetaData(), autoload_with=connection)

        quota = connection.execute(sa.select(quotas)).mappings().one()
        assert quota["max_nvidia_gpus"] == 8
        assert quota["max_ascend_npus"] == 0
        state = connection.execute(sa.select(quota_state)).mappings().one()
        assert state["reserved_nvidia_gpus"] == 3
        assert state["reserved_ascend_npus"] == 0
        assert state["service_reserved_nvidia_gpus"] == 2
        assert state["service_reserved_ascend_npus"] == 0

        task = connection.execute(sa.select(tasks)).mappings().one()
        request = task["accelerator_request_json"]
        assert request["allowed_vendors"] == ["nvidia"]
        assert request["allowed_kinds"] == ["gpu"]
        assert request["selection_policy"] == "nvidia-only"
        assert request["count"] == 2

        # Docker exact-device tasks remain valid without a Kubernetes profile.
        connection.execute(
            tasks.update()
            .where(tasks.c.id == task["id"])
            .values(
                selected_vendor="nvidia",
                selected_kind="gpu",
                selected_model="A10",
                allocation_authority="control_plane_exact_device",
            )
        )
        with connection.begin_nested(), pytest.raises(IntegrityError, match="profile_authority"):
            connection.execute(
                tasks.update()
                .where(tasks.c.id == task["id"])
                .values(allocation_authority="kubernetes_device_plugin")
            )

        valid_service = {
            "id": uuid.uuid4().hex,
            "project_id": project_id.hex,
            "status": "running",
            "gpu_count": 1,
            "logical_model_id": logical_model_id.hex,
            "model_variant_id": variant_id.hex,
            "selected_vendor": "huawei-ascend",
            "selected_kind": "npu",
            "selected_model": "Ascend 910B",
            "runtime_profile_id": "ascend-vllm-k8s",
            "runtime_profile_version": "1.0.0",
            "runtime_profile_digest": "sha256:" + "a" * 64,
            "allocation_authority": "kubernetes_device_plugin",
            "accelerator_resource_name": "huawei.com/Ascend910",
            "selection_policy": "prefer-ascend",
        }
        connection.execute(services.insert().values(**valid_service))
        with connection.begin_nested(), pytest.raises(IntegrityError, match="vendor_kind"):
            invalid_service = dict(valid_service)
            invalid_service.update(id=uuid.uuid4().hex, selected_kind="gpu")
            connection.execute(services.insert().values(**invalid_service))

        with connection.begin_nested(), pytest.raises(
            IntegrityError, match="task_accelerator_totals"
        ):
            connection.execute(
                quota_state.update().values(reserved_ascend_npus=1)
            )

        event = {
            "id": uuid.uuid4().hex,
            "project_id": project_id.hex,
            "workload_type": "batch_job",
            "workload_id": task_id.hex,
            "policy": "nvidia-only",
            "outcome": "admitted",
            "reason": "SELECTED",
            "selected_vendor": "nvidia",
            "selected_kind": "gpu",
            "selected_model": "A10",
            "allocation_authority": "control_plane_exact_device",
            "candidate_summary": [{"vendor": "nvidia", "healthy": True}],
        }
        connection.execute(events.insert().values(**event))
        with connection.begin_nested(), pytest.raises(
            IntegrityError, match="candidate_summary_size"
        ):
            oversized = dict(event)
            oversized.update(
                id=uuid.uuid4().hex,
                candidate_summary=[{"reason": "x" * 17_000}],
            )
            connection.execute(events.insert().values(**oversized))
        with connection.begin_nested(), pytest.raises(
            IntegrityError, match="candidate_summary_devices"
        ):
            leaking = dict(event)
            leaking.update(
                id=uuid.uuid4().hex,
                candidate_summary=[{"concrete_device_ids": ["GPU-secret"]}],
            )
            connection.execute(events.insert().values(**leaking))

        migration.downgrade()
        inspector = sa.inspect(connection)
        assert "admission_events" not in inspector.get_table_names()
        assert "max_nvidia_gpus" not in {
            column["name"] for column in inspector.get_columns("project_quotas")
        }
        assert "accelerator_request_json" not in {
            column["name"] for column in inspector.get_columns("tasks")
        }
        assert "model_variant_id" not in {
            column["name"] for column in inspector.get_columns("model_services")
        }

    engine.dispose()
