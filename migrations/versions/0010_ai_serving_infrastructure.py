"""add AI serving lifecycle, runtime spec, registry defaults and usage

Revision ID: 0010_ai_serving_infrastructure
Revises: 0009_outbox_event_cursor
Create Date: 2026-08-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_ai_serving_infrastructure"
down_revision: str | Sequence[str] | None = "0009_outbox_event_cursor"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    _add_registry_runtime_defaults()
    _add_model_service_runtime_spec()
    _add_replica_lifecycle()
    _create_serving_usage()


def downgrade() -> None:
    op.drop_table("serving_request_usage")

    op.drop_constraint(
        op.f("ck_service_replicas_service_replica_status"),
        "service_replicas",
        type_="check",
    )
    op.execute("UPDATE service_replicas SET status = 'starting' WHERE status = 'loading'")
    op.execute("UPDATE service_replicas SET status = 'stopping' WHERE status = 'draining'")
    op.create_check_constraint(
        "service_replica_status",
        "service_replicas",
        "status IN ('pending','starting','running','stopping','stopped','failed','lost')",
    )
    for column in (
        "drain_deadline",
        "drain_started_at",
        "ready_at",
        "container_started_at",
        "error_code",
        "image_digest",
        "model_revision",
        "active_requests",
        "runtime",
    ):
        op.drop_column("service_replicas", column)

    for column in (
        "scheduling_details",
        "scheduling_reason",
        "max_model_len",
        "gpu_memory_utilization",
        "dtype",
        "tensor_parallel_size",
        "gpu_model",
        "model_revision",
    ):
        op.drop_column("model_services", column)
    op.drop_index("ix_model_services_registered_model_id", table_name="model_services")
    op.drop_constraint(
        "fk_model_services_registered_model_id_registered_models",
        "model_services",
        type_="foreignkey",
    )
    op.drop_column("model_services", "registered_model_id")

    op.drop_column("registered_models", "runtime_defaults")
    op.drop_column("registered_models", "default_gpu_count")
    op.drop_column("registered_models", "runtime")


def _add_registry_runtime_defaults() -> None:
    op.add_column(
        "registered_models",
        sa.Column("runtime", sa.String(length=32), server_default="vllm", nullable=False),
    )
    op.add_column(
        "registered_models",
        sa.Column("default_gpu_count", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "registered_models",
        sa.Column("runtime_defaults", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
    )
    op.create_check_constraint(
        "registered_model_runtime",
        "registered_models",
        "runtime IN ('vllm','fake')",
    )
    op.create_check_constraint(
        "registered_model_default_gpu_count",
        "registered_models",
        "default_gpu_count >= 0 AND default_gpu_count <= 64",
    )


def _add_model_service_runtime_spec() -> None:
    op.add_column("model_services", sa.Column("registered_model_id", sa.Uuid()))
    op.create_foreign_key(
        "fk_model_services_registered_model_id_registered_models",
        "model_services",
        "registered_models",
        ["registered_model_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_model_services_registered_model_id",
        "model_services",
        ["registered_model_id"],
    )
    op.add_column("model_services", sa.Column("model_revision", sa.String(255)))
    op.add_column("model_services", sa.Column("gpu_model", sa.String(255)))
    op.add_column(
        "model_services",
        sa.Column("tensor_parallel_size", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "model_services",
        sa.Column("dtype", sa.String(32), server_default="auto", nullable=False),
    )
    op.add_column(
        "model_services",
        sa.Column(
            "gpu_memory_utilization",
            sa.Float(),
            server_default="0.9",
            nullable=False,
        ),
    )
    op.add_column("model_services", sa.Column("max_model_len", sa.Integer()))
    op.add_column("model_services", sa.Column("scheduling_reason", sa.String(128)))
    op.add_column(
        "model_services",
        sa.Column("scheduling_details", sa.JSON(), server_default=sa.text("'{}'"), nullable=False),
    )
    op.execute(
        "UPDATE model_services SET tensor_parallel_size = "
        "CASE WHEN gpu_count > 0 THEN gpu_count ELSE 1 END"
    )
    op.create_check_constraint(
        "tensor_parallel_size",
        "model_services",
        "tensor_parallel_size >= 1",
    )
    op.create_check_constraint(
        "gpu_memory_utilization",
        "model_services",
        "gpu_memory_utilization > 0 AND gpu_memory_utilization <= 1",
    )
    op.create_check_constraint(
        "max_model_len",
        "model_services",
        "max_model_len IS NULL OR max_model_len >= 1",
    )


def _add_replica_lifecycle() -> None:
    op.drop_constraint(
        op.f("ck_service_replicas_service_replica_status"),
        "service_replicas",
        type_="check",
    )
    op.create_check_constraint(
        "service_replica_status",
        "service_replicas",
        "status IN ('pending','starting','loading','running','draining','stopping',"
        "'stopped','failed','lost')",
    )
    op.add_column("service_replicas", sa.Column("runtime", sa.String(32)))
    op.execute(
        "UPDATE service_replicas AS replica SET runtime = service.runtime "
        "FROM model_services AS service WHERE service.id = replica.service_id"
    )
    op.alter_column("service_replicas", "runtime", nullable=False)
    op.create_check_constraint(
        "service_replica_runtime",
        "service_replicas",
        "runtime IN ('vllm','fake')",
    )
    op.add_column(
        "service_replicas",
        sa.Column("active_requests", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column("service_replicas", sa.Column("model_revision", sa.String(255)))
    op.add_column("service_replicas", sa.Column("image_digest", sa.String(255)))
    op.add_column("service_replicas", sa.Column("error_code", sa.String(128)))
    op.add_column(
        "service_replicas",
        sa.Column("container_started_at", sa.DateTime(timezone=True)),
    )
    op.add_column("service_replicas", sa.Column("ready_at", sa.DateTime(timezone=True)))
    op.add_column(
        "service_replicas",
        sa.Column("drain_started_at", sa.DateTime(timezone=True)),
    )
    op.add_column(
        "service_replicas",
        sa.Column("drain_deadline", sa.DateTime(timezone=True)),
    )
    op.create_check_constraint(
        "active_requests",
        "service_replicas",
        "active_requests >= 0",
    )
    op.execute(
        "UPDATE service_replicas AS replica SET model_revision = service.model_revision "
        "FROM model_services AS service WHERE service.id = replica.service_id"
    )
    op.execute(
        "UPDATE service_replicas AS replica SET image_digest = "
        "substring(service.image from '@(sha256:[0-9a-f]{64})$') "
        "FROM model_services AS service WHERE service.id = replica.service_id"
    )


def _create_serving_usage() -> None:
    op.create_table(
        "serving_request_usage",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("replica_id", sa.Uuid()),
        sa.Column("path", sa.String(64), nullable=False),
        sa.Column("outcome", sa.String(64), nullable=False),
        sa.Column("error_code", sa.String(64)),
        sa.Column("streamed", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("request_duration_seconds", sa.Numeric(24, 6), nullable=False),
        sa.Column("time_to_first_token_seconds", sa.Numeric(24, 6)),
        sa.Column("allocated_gpu_seconds", sa.Numeric(24, 6)),
        sa.Column("prompt_tokens", sa.BigInteger()),
        sa.Column("completion_tokens", sa.BigInteger()),
        sa.Column("total_tokens", sa.BigInteger()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("finished_at >= started_at", name="period"),
        sa.CheckConstraint("request_duration_seconds >= 0", name="duration_nonnegative"),
        sa.CheckConstraint(
            "time_to_first_token_seconds IS NULL OR time_to_first_token_seconds >= 0",
            name="ttft_nonnegative",
        ),
        sa.CheckConstraint(
            "allocated_gpu_seconds IS NULL OR allocated_gpu_seconds >= 0",
            name="allocated_gpu_seconds_nonnegative",
        ),
        sa.CheckConstraint(
            "(prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL) "
            "OR (prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL "
            "AND total_tokens IS NOT NULL AND prompt_tokens >= 0 AND completion_tokens >= 0 "
            "AND total_tokens = prompt_tokens + completion_tokens)",
            name="tokens_consistent",
        ),
        sa.CheckConstraint(
            "path IN ('/v1/chat/completions','/v1/completions')",
            name="gateway_path",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["service_id"], ["model_services.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_serving_request_usage"),
        sa.UniqueConstraint("request_id", name="uq_serving_request_usage_request"),
    )
    op.create_index(
        "ix_serving_request_usage_project_id",
        "serving_request_usage",
        ["project_id"],
    )
    op.create_index(
        "ix_serving_request_usage_service_id",
        "serving_request_usage",
        ["service_id"],
    )
    op.create_index(
        "ix_serving_request_usage_replica_id",
        "serving_request_usage",
        ["replica_id"],
    )
    op.create_index(
        "ix_serving_usage_project_finished",
        "serving_request_usage",
        ["project_id", "finished_at"],
    )
    op.create_index(
        "ix_serving_usage_service_finished",
        "serving_request_usage",
        ["service_id", "finished_at"],
    )
