"""Create the initial task platform schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-08-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "0001_initial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    worker_status = sa.Enum(
        "online",
        "offline",
        "draining",
        name="workerstatus",
        native_enum=False,
    )
    task_status = sa.Enum(
        "pending",
        "queued",
        "assigned",
        "pulling",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "timed_out",
        "retrying",
        name="taskstatus",
        native_enum=False,
    )
    log_stream = sa.Enum(
        "stdout",
        "stderr",
        "system",
        name="logstream",
        native_enum=False,
    )

    op.create_table(
        "workers",
        sa.Column("id", sa.String(length=255), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column(
            "status",
            worker_status,
            server_default=sa.text("'online'"),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "last_heartbeat_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("running_tasks", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("concurrency", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("cpu_count", sa.Integer(), nullable=False),
        sa.Column("memory_total_mb", sa.Integer(), nullable=False),
        sa.Column("docker_version", sa.String(length=128), nullable=True),
        sa.Column(
            "labels",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column("gpu_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("gpu_model", sa.String(length=512), nullable=True),
        sa.Column("gpu_memory_mb", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_workers"),
    )
    op.create_index("ix_workers_status", "workers", ["status"], unique=False)
    op.create_index(
        "ix_workers_status_heartbeat",
        "workers",
        ["status", "last_heartbeat_at"],
        unique=False,
    )

    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("image", sa.String(length=512), nullable=False),
        sa.Column("command", sa.JSON(), nullable=False),
        sa.Column(
            "environment",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column(
            "status",
            task_status,
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", sa.String(length=255), nullable=True),
        sa.Column("execution_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("exit_code", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), server_default=sa.text("60"), nullable=False),
        sa.Column("retry_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_retries", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("recovery_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cpu_limit", sa.Float(), server_default=sa.text("1.0"), nullable=False),
        sa.Column("memory_limit_mb", sa.Integer(), server_default=sa.text("256"), nullable=False),
        sa.Column("gpu_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "network_enabled",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "labels",
            sa.JSON(),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column(
            "cancel_requested",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("cpu_seconds", sa.Float(), nullable=True),
        sa.Column("gpu_seconds", sa.Float(), nullable=True),
        sa.Column("wall_time_seconds", sa.Float(), nullable=True),
        sa.Column("estimated_cost", sa.Float(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("request_hash", sa.String(length=64), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("log_sequence", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.ForeignKeyConstraint(
            ["worker_id"],
            ["workers.id"],
            name="fk_tasks_worker_id_workers",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_tasks"),
        sa.UniqueConstraint("idempotency_key", name="uq_tasks_idempotency_key"),
    )
    op.create_index("ix_tasks_lease_expires_at", "tasks", ["lease_expires_at"], unique=False)
    op.create_index("ix_tasks_next_attempt_at", "tasks", ["next_attempt_at"], unique=False)
    op.create_index("ix_tasks_status", "tasks", ["status"], unique=False)
    op.create_index(
        "ix_tasks_status_created_at", "tasks", ["status", "created_at"], unique=False
    )
    op.create_index("ix_tasks_worker_id", "tasks", ["worker_id"], unique=False)
    op.create_index(
        "ix_tasks_worker_status", "tasks", ["worker_id", "status"], unique=False
    )

    op.create_table(
        "task_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=True),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("stream", log_stream, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name="fk_task_logs_task_id_tasks",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_logs"),
        sa.UniqueConstraint("task_id", "sequence", name="uq_task_logs_task_sequence"),
    )
    op.create_index("ix_task_logs_task_id", "task_logs", ["task_id"], unique=False)
    op.create_index(
        "ix_task_logs_execution_id", "task_logs", ["execution_id"], unique=False
    )
    op.create_index(
        "ix_task_logs_task_sequence",
        "task_logs",
        ["task_id", "sequence"],
        unique=False,
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.Uuid(), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_events"),
    )
    op.create_index(
        "ix_outbox_events_aggregate_id",
        "outbox_events",
        ["aggregate_id"],
        unique=False,
    )
    op.create_index(
        "ix_outbox_unprocessed_available",
        "outbox_events",
        ["processed_at", "available_at", "locked_until"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unprocessed_available", table_name="outbox_events")
    op.drop_index("ix_outbox_events_aggregate_id", table_name="outbox_events")
    op.drop_table("outbox_events")

    op.drop_index("ix_task_logs_task_sequence", table_name="task_logs")
    op.drop_index("ix_task_logs_execution_id", table_name="task_logs")
    op.drop_index("ix_task_logs_task_id", table_name="task_logs")
    op.drop_table("task_logs")

    op.drop_index("ix_tasks_worker_status", table_name="tasks")
    op.drop_index("ix_tasks_worker_id", table_name="tasks")
    op.drop_index("ix_tasks_status_created_at", table_name="tasks")
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_index("ix_tasks_next_attempt_at", table_name="tasks")
    op.drop_index("ix_tasks_lease_expires_at", table_name="tasks")
    op.drop_table("tasks")

    op.drop_index("ix_workers_status_heartbeat", table_name="workers")
    op.drop_index("ix_workers_status", table_name="workers")
    op.drop_table("workers")
