"""Persist vendor-aware admission decisions and immutable selection snapshots.

Revision ID: 0013_vendor_aware_admission
Revises: 0012_model_variants
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013_vendor_aware_admission"
down_revision: str | Sequence[str] | None = "0012_model_variants"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

POLICIES = "'any','nvidia-only','ascend-only','prefer-nvidia','prefer-ascend'"
AUTHORITIES = "'control_plane_exact_device','kubernetes_device_plugin'"


def upgrade() -> None:
    _add_quota_columns()
    _add_task_columns()
    _add_service_columns("model_services", include_logical_model=True)
    _add_service_columns("service_replicas", include_logical_model=True)
    _add_execution_columns("task_executions")
    _add_execution_columns("resource_reservations")
    _backfill_legacy_data()
    _add_quota_constraints()
    _add_task_constraints()
    _add_service_constraints("model_services")
    _add_service_constraints("service_replicas")
    _add_execution_constraints("task_executions")
    _add_execution_constraints("resource_reservations")
    _create_admission_events()


def downgrade() -> None:
    op.drop_index("ix_admission_events_workload_occurred", table_name="admission_events")
    op.drop_index("ix_admission_events_project_occurred", table_name="admission_events")
    op.drop_index(op.f("ix_admission_events_model_variant_id"), table_name="admission_events")
    op.drop_index(op.f("ix_admission_events_project_id"), table_name="admission_events")
    op.drop_table("admission_events")
    _drop_execution_columns("resource_reservations")
    _drop_execution_columns("task_executions")
    _drop_service_columns("service_replicas")
    _drop_service_columns("model_services")
    _drop_task_columns()
    _drop_quota_columns()


def _add_quota_columns() -> None:
    with op.batch_alter_table("project_quotas") as batch:
        batch.add_column(sa.Column("max_nvidia_gpus", sa.Integer()))
        batch.add_column(
            sa.Column("max_ascend_npus", sa.Integer(), server_default="0")
        )
    with op.batch_alter_table("project_quota_state") as batch:
        batch.add_column(
            sa.Column(
                "reserved_nvidia_gpus",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column(
                "reserved_ascend_npus",
                sa.Integer(),
                server_default="0",
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column(
                "service_reserved_nvidia_gpus",
                sa.BigInteger(),
                server_default="0",
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column(
                "service_reserved_ascend_npus",
                sa.BigInteger(),
                server_default="0",
                nullable=False,
            )
        )


def _add_task_columns() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.add_column(sa.Column("accelerator_request_json", sa.JSON(none_as_null=True)))
        batch.add_column(sa.Column("selected_vendor", sa.String(64)))
        batch.add_column(sa.Column("selected_kind", sa.String(32)))
        batch.add_column(sa.Column("selected_model", sa.String(255)))
        batch.add_column(sa.Column("runtime_profile_id", sa.String(128)))
        batch.add_column(sa.Column("runtime_profile_version", sa.String(32)))
        batch.add_column(sa.Column("runtime_profile_digest", sa.String(71)))
        batch.add_column(sa.Column("model_variant_id", sa.Uuid()))
        batch.add_column(sa.Column("allocation_authority", sa.String(64)))


def _add_service_columns(table_name: str, *, include_logical_model: bool) -> None:
    with op.batch_alter_table(table_name) as batch:
        if include_logical_model:
            batch.add_column(sa.Column("logical_model_id", sa.Uuid()))
        batch.add_column(sa.Column("model_variant_id", sa.Uuid()))
        batch.add_column(sa.Column("selected_vendor", sa.String(64)))
        batch.add_column(sa.Column("selected_kind", sa.String(32)))
        batch.add_column(sa.Column("selected_model", sa.String(255)))
        batch.add_column(sa.Column("runtime_profile_id", sa.String(128)))
        batch.add_column(sa.Column("runtime_profile_version", sa.String(32)))
        batch.add_column(sa.Column("runtime_profile_digest", sa.String(71)))
        batch.add_column(sa.Column("allocation_authority", sa.String(64)))
        batch.add_column(sa.Column("accelerator_resource_name", sa.String(255)))
        batch.add_column(sa.Column("selection_policy", sa.String(32)))


def _add_execution_columns(table_name: str) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.add_column(sa.Column("requested_profile_version", sa.String(32)))
        batch.add_column(sa.Column("requested_profile_digest", sa.String(71)))
        batch.add_column(sa.Column("model_variant_id", sa.Uuid()))


def _backfill_legacy_data() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text(
            "UPDATE project_quotas SET max_nvidia_gpus = max_gpus, "
            "max_ascend_npus = 0"
        )
    )
    bind.execute(
        sa.text(
            "UPDATE project_quota_state SET "
            "reserved_nvidia_gpus = reserved_gpus, reserved_ascend_npus = 0, "
            "service_reserved_nvidia_gpus = service_reserved_gpus, "
            "service_reserved_ascend_npus = 0"
        )
    )

    tasks = sa.table(
        "tasks",
        sa.column("id", sa.Uuid()),
        sa.column("gpu_count", sa.Integer()),
        sa.column("gpu_memory_mb", sa.Integer()),
        sa.column("gpu_model", sa.String()),
        sa.column("accelerator_request_json", sa.JSON(none_as_null=True)),
    )
    rows = bind.execute(
        sa.select(
            tasks.c.id,
            tasks.c.gpu_count,
            tasks.c.gpu_memory_mb,
            tasks.c.gpu_model,
        )
    ).mappings()
    for row in rows:
        gpu_count = row["gpu_count"]
        gpu_model = row["gpu_model"]
        request = {
            "count": gpu_count,
            "memory_mb_per_device": row["gpu_memory_mb"],
            "allowed_vendors": ["nvidia"] if gpu_count > 0 else [],
            "allowed_kinds": ["gpu"] if gpu_count > 0 else [],
            "allowed_models": [gpu_model] if gpu_model is not None else [],
            "required_capabilities": [],
            "runtime_profile": None,
            "selection_policy": "nvidia-only" if gpu_count > 0 else "any",
        }
        bind.execute(
            sa.update(tasks)
            .where(tasks.c.id == row["id"])
            .values(accelerator_request_json=request)
        )


def _add_quota_constraints() -> None:
    with op.batch_alter_table("project_quotas") as batch:
        batch.create_check_constraint(
            op.f("ck_project_quotas_quota_nvidia_gpus"),
            "max_nvidia_gpus IS NULL OR max_nvidia_gpus >= 0",
        )
        batch.create_check_constraint(
            op.f("ck_project_quotas_quota_ascend_npus"),
            "max_ascend_npus IS NULL OR max_ascend_npus >= 0",
        )
    with op.batch_alter_table("project_quota_state") as batch:
        batch.drop_constraint(
            op.f("ck_project_quota_state_nonnegative"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_project_quota_state_nonnegative"),
            "queued_tasks >= 0 AND running_tasks >= 0 "
            "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
            "AND reserved_gpus >= 0 AND reserved_nvidia_gpus >= 0 "
            "AND reserved_ascend_npus >= 0 AND service_count >= 0 "
            "AND service_replicas >= 0 AND service_reserved_cpu_millicores >= 0 "
            "AND service_reserved_memory_mb >= 0 AND service_reserved_gpus >= 0 "
            "AND service_reserved_nvidia_gpus >= 0 "
            "AND service_reserved_ascend_npus >= 0 AND artifact_bytes >= 0 "
            "AND daily_reserved_cost >= 0 AND daily_settled_cost >= 0",
        )
        batch.create_check_constraint(
            op.f("ck_project_quota_state_task_accelerator_totals"),
            "reserved_gpus = reserved_nvidia_gpus + reserved_ascend_npus",
        )
        batch.create_check_constraint(
            op.f("ck_project_quota_state_service_accelerator_totals"),
            "service_reserved_gpus = service_reserved_nvidia_gpus "
            "+ service_reserved_ascend_npus",
        )


def _add_task_constraints() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.create_foreign_key(
            op.f("fk_tasks_model_variant_id_model_variants"),
            "model_variants",
            ["model_variant_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_accelerator_snapshot"),
            _selected_snapshot_sql(
                include_logical_model=False, require_model_variant=False
            ),
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_profile_snapshot"), _profile_snapshot_sql()
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_variant_profile"),
            "model_variant_id IS NULL OR runtime_profile_id IS NOT NULL",
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_profile_authority"),
            "allocation_authority IS NULL "
            "OR allocation_authority != 'kubernetes_device_plugin' "
            "OR runtime_profile_id IS NOT NULL",
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_vendor_kind"), _selected_vendor_kind_sql()
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_profile_digest"), _profile_digest_sql()
        )
        batch.create_check_constraint(
            op.f("ck_tasks_selected_allocation_authority"),
            f"allocation_authority IS NULL OR allocation_authority IN ({AUTHORITIES})",
        )
        batch.create_index(
            "ix_tasks_project_variant_status",
            ["project_id", "model_variant_id", "status"],
        )


def _add_service_constraints(table_name: str) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.create_foreign_key(
            op.f(f"fk_{table_name}_logical_model_id_logical_models"),
            "logical_models",
            ["logical_model_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        batch.create_foreign_key(
            op.f(f"fk_{table_name}_model_variant_id_model_variants"),
            "model_variants",
            ["model_variant_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_admission_snapshot"),
            _selected_snapshot_sql(
                include_logical_model=True, require_model_variant=True
            ),
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_selected_vendor_kind"),
            _selected_vendor_kind_sql(),
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_runtime_profile_digest"),
            _profile_digest_sql(),
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_allocation_authority"),
            f"allocation_authority IS NULL OR allocation_authority IN ({AUTHORITIES})",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_resource_authority"),
            "allocation_authority IS NULL "
            "OR allocation_authority = 'control_plane_exact_device' "
            "OR accelerator_resource_name IS NOT NULL",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_selection_policy"),
            f"selection_policy IS NULL OR selection_policy IN ({POLICIES})",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_policy_vendor"), _policy_vendor_sql()
        )
        if table_name == "model_services":
            batch.create_index(
                "ix_model_services_project_variant_status",
                ["project_id", "model_variant_id", "status"],
            )
        else:
            batch.create_index(
                "ix_service_replicas_variant_status",
                ["model_variant_id", "status"],
            )


def _add_execution_constraints(table_name: str) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.drop_constraint(
            op.f(f"ck_{table_name}_accelerator_request"), type_="check"
        )
        batch.create_foreign_key(
            op.f(f"fk_{table_name}_model_variant_id_model_variants"),
            "model_variants",
            ["model_variant_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_accelerator_request"),
            "(gpu_count = 0 AND requested_vendor IS NULL AND requested_kind IS NULL "
            "AND requested_profile_id IS NULL AND requested_profile_version IS NULL "
            "AND requested_profile_digest IS NULL AND model_variant_id IS NULL) OR "
            "(gpu_count > 0 AND requested_vendor IS NOT NULL "
            "AND requested_kind IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_requested_profile_snapshot"),
            "(requested_profile_id IS NULL AND requested_profile_version IS NULL "
            "AND requested_profile_digest IS NULL) OR "
            "(requested_profile_id IS NOT NULL AND requested_profile_version IS NOT NULL "
            "AND requested_profile_digest IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_requested_profile_digest"),
            "requested_profile_digest IS NULL OR "
            "(length(requested_profile_digest) = 71 "
            "AND requested_profile_digest LIKE 'sha256:%')",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_model_variant_profile"),
            "model_variant_id IS NULL OR requested_profile_id IS NOT NULL",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_requested_profile_authority"),
            "gpu_count = 0 OR allocation_authority != 'kubernetes_device_plugin' "
            "OR requested_profile_id IS NOT NULL",
        )
        index_name = (
            "ix_task_executions_variant_status"
            if table_name == "task_executions"
            else "ix_reservations_variant_state"
        )
        state_column = "status" if table_name == "task_executions" else "state"
        batch.create_index(index_name, ["model_variant_id", state_column])


def _create_admission_events() -> None:
    op.create_table(
        "admission_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("workload_type", sa.String(32), nullable=False),
        sa.Column("workload_id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid()),
        sa.Column("policy", sa.String(32), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("selected_vendor", sa.String(64)),
        sa.Column("selected_kind", sa.String(32)),
        sa.Column("selected_model", sa.String(255)),
        sa.Column("runtime_profile_id", sa.String(128)),
        sa.Column("runtime_profile_version", sa.String(32)),
        sa.Column("runtime_profile_digest", sa.String(71)),
        sa.Column("model_variant_id", sa.Uuid()),
        sa.Column("allocation_authority", sa.String(64)),
        sa.Column(
            "candidate_summary",
            sa.JSON(),
            server_default=sa.text("'[]'"),
            nullable=False,
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "workload_type IN ('batch_job','model_service')",
            name=op.f("ck_admission_events_workload_type"),
        ),
        sa.CheckConstraint(
            f"policy IN ({POLICIES})",
            name=op.f("ck_admission_events_policy"),
        ),
        sa.CheckConstraint(
            "length(outcome) > 0", name=op.f("ck_admission_events_outcome")
        ),
        sa.CheckConstraint(
            "length(reason) > 0", name=op.f("ck_admission_events_reason")
        ),
        sa.CheckConstraint(
            _selected_snapshot_sql(
                include_logical_model=False, require_model_variant=False
            ),
            name=op.f("ck_admission_events_selected_snapshot"),
        ),
        sa.CheckConstraint(
            _profile_snapshot_sql(),
            name=op.f("ck_admission_events_selected_profile_snapshot"),
        ),
        sa.CheckConstraint(
            "model_variant_id IS NULL OR runtime_profile_id IS NOT NULL",
            name=op.f("ck_admission_events_selected_variant_profile"),
        ),
        sa.CheckConstraint(
            "allocation_authority IS NULL "
            "OR allocation_authority != 'kubernetes_device_plugin' "
            "OR runtime_profile_id IS NOT NULL",
            name=op.f("ck_admission_events_selected_profile_authority"),
        ),
        sa.CheckConstraint(
            _selected_vendor_kind_sql(),
            name=op.f("ck_admission_events_selected_vendor_kind"),
        ),
        sa.CheckConstraint(
            _profile_digest_sql(),
            name=op.f("ck_admission_events_profile_digest"),
        ),
        sa.CheckConstraint(
            f"allocation_authority IS NULL OR allocation_authority IN ({AUTHORITIES})",
            name=op.f("ck_admission_events_allocation_authority"),
        ),
        sa.CheckConstraint(
            _policy_vendor_sql(policy_column="policy"),
            name=op.f("ck_admission_events_policy_vendor"),
        ),
        sa.CheckConstraint(
            "length(CAST(candidate_summary AS TEXT)) <= 16384",
            name=op.f("ck_admission_events_candidate_summary_size"),
        ),
        sa.CheckConstraint(
            "CAST(candidate_summary AS TEXT) NOT LIKE '%\"concrete_device_ids\"%' "
            "AND CAST(candidate_summary AS TEXT) NOT LIKE '%\"device_ids\"%' "
            "AND CAST(candidate_summary AS TEXT) NOT LIKE '%\"device_uuid\"%'",
            name=op.f("ck_admission_events_candidate_summary_devices"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_admission_events_project_id_projects"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["model_variant_id"],
            ["model_variants.id"],
            name=op.f("fk_admission_events_model_variant_id_model_variants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_admission_events")),
    )
    op.create_index(
        op.f("ix_admission_events_project_id"),
        "admission_events",
        ["project_id"],
    )
    op.create_index(
        op.f("ix_admission_events_model_variant_id"),
        "admission_events",
        ["model_variant_id"],
    )
    op.create_index(
        "ix_admission_events_project_occurred",
        "admission_events",
        ["project_id", "occurred_at", "id"],
    )
    op.create_index(
        "ix_admission_events_workload_occurred",
        "admission_events",
        ["workload_type", "workload_id", "occurred_at"],
    )


def _drop_execution_columns(table_name: str) -> None:
    index_name = (
        "ix_task_executions_variant_status"
        if table_name == "task_executions"
        else "ix_reservations_variant_state"
    )
    with op.batch_alter_table(table_name) as batch:
        batch.drop_index(index_name)
        for suffix in (
            "requested_profile_authority",
            "model_variant_profile",
            "requested_profile_digest",
            "requested_profile_snapshot",
            "accelerator_request",
        ):
            batch.drop_constraint(op.f(f"ck_{table_name}_{suffix}"), type_="check")
        batch.drop_constraint(
            op.f(f"fk_{table_name}_model_variant_id_model_variants"), type_="foreignkey"
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_accelerator_request"),
            "(gpu_count = 0 AND requested_vendor IS NULL AND requested_kind IS NULL "
            "AND requested_profile_id IS NULL) OR "
            "(gpu_count > 0 AND requested_vendor IS NOT NULL "
            "AND requested_kind IS NOT NULL)",
        )
        batch.drop_column("model_variant_id")
        batch.drop_column("requested_profile_digest")
        batch.drop_column("requested_profile_version")


def _drop_service_columns(table_name: str) -> None:
    index_name = (
        "ix_model_services_project_variant_status"
        if table_name == "model_services"
        else "ix_service_replicas_variant_status"
    )
    with op.batch_alter_table(table_name) as batch:
        batch.drop_index(index_name)
        for suffix in (
            "policy_vendor",
            "selection_policy",
            "resource_authority",
            "allocation_authority",
            "runtime_profile_digest",
            "selected_vendor_kind",
            "admission_snapshot",
        ):
            batch.drop_constraint(op.f(f"ck_{table_name}_{suffix}"), type_="check")
        batch.drop_constraint(
            op.f(f"fk_{table_name}_model_variant_id_model_variants"), type_="foreignkey"
        )
        batch.drop_constraint(
            op.f(f"fk_{table_name}_logical_model_id_logical_models"), type_="foreignkey"
        )
        for column in (
            "selection_policy",
            "accelerator_resource_name",
            "allocation_authority",
            "runtime_profile_digest",
            "runtime_profile_version",
            "runtime_profile_id",
            "selected_model",
            "selected_kind",
            "selected_vendor",
            "model_variant_id",
            "logical_model_id",
        ):
            batch.drop_column(column)


def _drop_task_columns() -> None:
    with op.batch_alter_table("tasks") as batch:
        batch.drop_index("ix_tasks_project_variant_status")
        for suffix in (
            "selected_allocation_authority",
            "selected_profile_authority",
            "selected_variant_profile",
            "selected_profile_snapshot",
            "selected_profile_digest",
            "selected_vendor_kind",
            "selected_accelerator_snapshot",
        ):
            batch.drop_constraint(op.f(f"ck_tasks_{suffix}"), type_="check")
        batch.drop_constraint(
            op.f("fk_tasks_model_variant_id_model_variants"), type_="foreignkey"
        )
        for column in (
            "allocation_authority",
            "model_variant_id",
            "runtime_profile_digest",
            "runtime_profile_version",
            "runtime_profile_id",
            "selected_model",
            "selected_kind",
            "selected_vendor",
            "accelerator_request_json",
        ):
            batch.drop_column(column)


def _drop_quota_columns() -> None:
    with op.batch_alter_table("project_quota_state") as batch:
        batch.drop_constraint(
            op.f("ck_project_quota_state_service_accelerator_totals"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_project_quota_state_task_accelerator_totals"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_project_quota_state_nonnegative"), type_="check"
        )
        batch.create_check_constraint(
            op.f("ck_project_quota_state_nonnegative"),
            "queued_tasks >= 0 AND running_tasks >= 0 "
            "AND reserved_cpu_millicores >= 0 AND reserved_memory_mb >= 0 "
            "AND reserved_gpus >= 0 AND service_count >= 0 "
            "AND service_replicas >= 0 AND service_reserved_cpu_millicores >= 0 "
            "AND service_reserved_memory_mb >= 0 AND service_reserved_gpus >= 0 "
            "AND artifact_bytes >= 0 AND daily_reserved_cost >= 0 "
            "AND daily_settled_cost >= 0",
        )
        batch.drop_column("service_reserved_ascend_npus")
        batch.drop_column("service_reserved_nvidia_gpus")
        batch.drop_column("reserved_ascend_npus")
        batch.drop_column("reserved_nvidia_gpus")
    with op.batch_alter_table("project_quotas") as batch:
        batch.drop_constraint(
            op.f("ck_project_quotas_quota_ascend_npus"), type_="check"
        )
        batch.drop_constraint(
            op.f("ck_project_quotas_quota_nvidia_gpus"), type_="check"
        )
        batch.drop_column("max_ascend_npus")
        batch.drop_column("max_nvidia_gpus")


def _selected_snapshot_sql(
    *, include_logical_model: bool, require_model_variant: bool
) -> str:
    nullable_columns = [
        "model_variant_id",
        "selected_vendor",
        "selected_kind",
        "selected_model",
        "runtime_profile_id",
        "runtime_profile_version",
        "runtime_profile_digest",
        "allocation_authority",
    ]
    required_columns = list(nullable_columns)
    if not require_model_variant:
        for column in (
            "model_variant_id",
            "runtime_profile_id",
            "runtime_profile_version",
            "runtime_profile_digest",
        ):
            required_columns.remove(column)
    if include_logical_model:
        nullable_columns.insert(0, "logical_model_id")
        required_columns.insert(0, "logical_model_id")
        nullable_columns.extend(["accelerator_resource_name", "selection_policy"])
        required_columns.append("selection_policy")
    null_side = " AND ".join(f"{column} IS NULL" for column in nullable_columns)
    required_side = " AND ".join(
        f"{column} IS NOT NULL" for column in required_columns
    )
    return f"({null_side}) OR ({required_side})"


def _selected_vendor_kind_sql() -> str:
    return (
        "selected_vendor IS NULL OR "
        "(selected_vendor = 'nvidia' AND selected_kind = 'gpu') OR "
        "(selected_vendor = 'huawei-ascend' AND selected_kind = 'npu')"
    )


def _profile_digest_sql() -> str:
    return (
        "runtime_profile_digest IS NULL OR "
        "(length(runtime_profile_digest) = 71 "
        "AND runtime_profile_digest LIKE 'sha256:%')"
    )


def _profile_snapshot_sql() -> str:
    return (
        "(runtime_profile_id IS NULL AND runtime_profile_version IS NULL "
        "AND runtime_profile_digest IS NULL) OR "
        "(runtime_profile_id IS NOT NULL AND runtime_profile_version IS NOT NULL "
        "AND runtime_profile_digest IS NOT NULL)"
    )


def _policy_vendor_sql(*, policy_column: str = "selection_policy") -> str:
    return (
        f"selected_vendor IS NULL OR {policy_column} NOT IN "
        "('nvidia-only','ascend-only') "
        f"OR ({policy_column} = 'nvidia-only' AND selected_vendor = 'nvidia') "
        f"OR ({policy_column} = 'ascend-only' "
        "AND selected_vendor = 'huawei-ascend')"
    )
