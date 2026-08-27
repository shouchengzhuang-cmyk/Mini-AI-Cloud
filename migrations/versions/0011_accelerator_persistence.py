"""Persist accelerator profiles and allocation authority.

Revision ID: 0011_accelerator_persistence
Revises: 0010_ai_serving_infrastructure
Create Date: 2026-08-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011_accelerator_persistence"
down_revision: str | Sequence[str] | None = "0010_ai_serving_infrastructure"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EXACT_DEVICE = "control_plane_exact_device"


def upgrade() -> None:
    _add_device_columns()
    _add_allocation_columns("resource_reservations")
    _add_allocation_columns("task_executions")
    _backfill_legacy_rows()
    _add_device_constraints()
    _add_allocation_constraints("resource_reservations", include_legacy_exception=True)
    _add_allocation_constraints("task_executions", include_legacy_exception=False)


def downgrade() -> None:
    _drop_allocation_constraints("task_executions", include_legacy_exception=False)
    _drop_allocation_constraints("resource_reservations", include_legacy_exception=True)
    _drop_device_constraints()
    _drop_allocation_columns("task_executions")
    _drop_allocation_columns("resource_reservations")
    with op.batch_alter_table("gpu_devices") as batch:
        for column in (
            "kubernetes_resource_name",
            "capabilities_json",
            "runtime_profile_ids",
            "compute_arch",
            "accelerator_kind",
        ):
            batch.drop_column(column)


def _add_device_columns() -> None:
    with op.batch_alter_table("gpu_devices") as batch:
        batch.add_column(
            sa.Column("accelerator_kind", sa.String(32), server_default="gpu", nullable=False)
        )
        batch.add_column(sa.Column("compute_arch", sa.String(128)))
        batch.add_column(
            sa.Column(
                "runtime_profile_ids",
                sa.JSON(),
                server_default=sa.text("'[]'"),
                nullable=False,
            )
        )
        batch.add_column(
            sa.Column(
                "capabilities_json",
                sa.JSON(),
                server_default=sa.text("'[]'"),
                nullable=False,
            )
        )
        batch.add_column(sa.Column("kubernetes_resource_name", sa.String(255)))


def _add_allocation_columns(table_name: str) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.add_column(
            sa.Column(
                "allocation_authority",
                sa.String(64),
                server_default=EXACT_DEVICE,
                nullable=False,
            )
        )
        batch.add_column(sa.Column("requested_vendor", sa.String(64)))
        batch.add_column(sa.Column("requested_kind", sa.String(32)))
        batch.add_column(sa.Column("requested_profile_id", sa.String(128)))
        batch.add_column(sa.Column("observed_device_ids_json", sa.JSON(none_as_null=True)))
        batch.add_column(sa.Column("observed_vendor", sa.String(64)))
        batch.add_column(sa.Column("observed_at", sa.DateTime(timezone=True)))


def _backfill_legacy_rows() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("UPDATE gpu_devices SET vendor = 'nvidia' WHERE vendor = 'fake' AND fake"))
    bind.execute(
        sa.text(
            "UPDATE gpu_devices SET accelerator_kind = "
            "CASE WHEN vendor = 'huawei-ascend' THEN 'npu' ELSE 'gpu' END, "
            "compute_arch = compute_capability"
        )
    )
    for table_name in ("resource_reservations", "task_executions"):
        bind.execute(
            sa.text(
                f"UPDATE {table_name} SET requested_vendor = 'nvidia', "
                "requested_kind = 'gpu' WHERE gpu_count > 0"
            )
        )

    reservations = sa.table(
        "resource_reservations",
        sa.column("id", sa.Uuid()),
        sa.column("execution_id", sa.Uuid()),
        sa.column("gpu_count", sa.Integer()),
        sa.column("legacy_unbound", sa.Boolean()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("observed_device_ids_json", sa.JSON(none_as_null=True)),
        sa.column("observed_vendor", sa.String()),
        sa.column("observed_at", sa.DateTime(timezone=True)),
    )
    links = sa.table(
        "reservation_gpu_devices",
        sa.column("reservation_id", sa.Uuid()),
        sa.column("gpu_device_id", sa.Uuid()),
    )
    devices = sa.table(
        "gpu_devices",
        sa.column("id", sa.Uuid()),
        sa.column("device_uuid", sa.String()),
        sa.column("vendor", sa.String()),
    )
    executions = sa.table(
        "task_executions",
        sa.column("id", sa.Uuid()),
        sa.column("observed_device_ids_json", sa.JSON(none_as_null=True)),
        sa.column("observed_vendor", sa.String()),
        sa.column("observed_at", sa.DateTime(timezone=True)),
    )
    legacy_rows = bind.execute(
        sa.select(
            reservations.c.id,
            reservations.c.execution_id,
            reservations.c.gpu_count,
            reservations.c.legacy_unbound,
            reservations.c.created_at,
        ).where(reservations.c.gpu_count > 0)
    ).mappings()
    for row in legacy_rows:
        bound_devices = list(
            bind.execute(
                sa.select(devices.c.device_uuid, devices.c.vendor)
                .select_from(links.join(devices, devices.c.id == links.c.gpu_device_id))
                .where(links.c.reservation_id == row["id"])
                .order_by(devices.c.device_uuid)
            ).mappings()
        )
        vendors = {device["vendor"] for device in bound_devices}
        if len(bound_devices) != row["gpu_count"] or len(vendors) != 1:
            if not row["legacy_unbound"]:
                bind.execute(
                    sa.update(reservations)
                    .where(reservations.c.id == row["id"])
                    .values(legacy_unbound=True)
                )
            continue
        observed_device_ids = [device["device_uuid"] for device in bound_devices]
        observed_vendor = vendors.pop()
        observation = {
            "observed_device_ids_json": observed_device_ids,
            "observed_vendor": observed_vendor,
            "observed_at": row["created_at"],
        }
        bind.execute(
            sa.update(reservations).where(reservations.c.id == row["id"]).values(**observation)
        )
        bind.execute(
            sa.update(executions)
            .where(executions.c.id == row["execution_id"])
            .values(**observation)
        )


def _add_device_constraints() -> None:
    with op.batch_alter_table("gpu_devices") as batch:
        batch.create_check_constraint(
            op.f("ck_gpu_devices_memory_available"),
            "memory_free_mb <= memory_total_mb",
        )
        batch.create_check_constraint(
            op.f("ck_gpu_devices_vendor_kind"),
            "(vendor = 'nvidia' AND accelerator_kind = 'gpu') OR "
            "(vendor = 'huawei-ascend' AND accelerator_kind = 'npu')",
        )


def _drop_device_constraints() -> None:
    with op.batch_alter_table("gpu_devices") as batch:
        batch.drop_constraint(op.f("ck_gpu_devices_vendor_kind"), type_="check")
        batch.drop_constraint(op.f("ck_gpu_devices_memory_available"), type_="check")


def _add_allocation_constraints(table_name: str, *, include_legacy_exception: bool) -> None:
    with op.batch_alter_table(table_name) as batch:
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_allocation_authority"),
            "allocation_authority IN ('control_plane_exact_device','kubernetes_device_plugin')",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_accelerator_request"),
            "(gpu_count = 0 AND requested_vendor IS NULL AND requested_kind IS NULL "
            "AND requested_profile_id IS NULL) OR "
            "(gpu_count > 0 AND requested_vendor IS NOT NULL AND requested_kind IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_requested_vendor_kind"),
            "requested_vendor IS NULL OR "
            "(requested_vendor = 'nvidia' AND requested_kind = 'gpu') OR "
            "(requested_vendor = 'huawei-ascend' AND requested_kind = 'npu')",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_observed_allocation"),
            "(observed_at IS NULL AND observed_vendor IS NULL "
            "AND observed_device_ids_json IS NULL) OR "
            "(observed_at IS NOT NULL AND observed_vendor IS NOT NULL "
            "AND observed_device_ids_json IS NOT NULL)",
        )
        batch.create_check_constraint(
            op.f(f"ck_{table_name}_observed_vendor"),
            "observed_vendor IS NULL OR observed_vendor = requested_vendor",
        )
        if include_legacy_exception:
            batch.create_check_constraint(
                op.f("ck_resource_reservations_exact_device_evidence"),
                "gpu_count = 0 OR allocation_authority = 'kubernetes_device_plugin' "
                "OR legacy_unbound OR observed_device_ids_json IS NOT NULL",
            )


def _drop_allocation_constraints(table_name: str, *, include_legacy_exception: bool) -> None:
    with op.batch_alter_table(table_name) as batch:
        if include_legacy_exception:
            batch.drop_constraint(
                op.f("ck_resource_reservations_exact_device_evidence"), type_="check"
            )
        for suffix in (
            "observed_vendor",
            "observed_allocation",
            "requested_vendor_kind",
            "accelerator_request",
            "allocation_authority",
        ):
            batch.drop_constraint(op.f(f"ck_{table_name}_{suffix}"), type_="check")


def _drop_allocation_columns(table_name: str) -> None:
    with op.batch_alter_table(table_name) as batch:
        for column in (
            "observed_at",
            "observed_vendor",
            "observed_device_ids_json",
            "requested_profile_id",
            "requested_kind",
            "requested_vendor",
            "allocation_authority",
        ):
            batch.drop_column(column)
