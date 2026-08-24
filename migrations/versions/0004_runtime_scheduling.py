"""Add runtime abstraction, schedulable resources and fenced reservations.

Revision ID: 0004_runtime_scheduling
Revises: 0003_identity_projects
Create Date: 2026-08-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_runtime_scheduling"
down_revision: str | Sequence[str] | None = "0003_identity_projects"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LEGACY_PROJECT_ID = "00000000-0000-0000-0000-000000000001"


def upgrade() -> None:
    # Phase I's non-native Enum was emitted as VARCHAR(9). New values such as
    # ``scheduling`` and ``preempting`` require a wider representation.
    op.alter_column(
        "tasks",
        "status",
        existing_type=sa.String(length=9),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
    op.add_column("tasks", sa.Column("project_id", sa.Uuid(), nullable=True))
    op.add_column("tasks", sa.Column("submitted_by_user_id", sa.Uuid(), nullable=True))
    op.add_column("tasks", sa.Column("created_by_api_key_id", sa.Uuid(), nullable=True))
    op.add_column(
        "tasks",
        sa.Column("workload_type", sa.String(32), server_default="batch_job", nullable=False),
    )
    op.add_column(
        "tasks", sa.Column("runtime_type", sa.String(32), server_default="docker", nullable=False)
    )
    op.add_column("tasks", sa.Column("cpu_millicores", sa.Integer(), nullable=True))
    op.add_column(
        "tasks", sa.Column("gpu_memory_mb", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column("tasks", sa.Column("gpu_model", sa.String(255), nullable=True))
    op.add_column(
        "tasks",
        sa.Column(
            "gpu_device_ids", sa.JSON(), server_default=sa.text("'[]'::json"), nullable=False
        ),
    )
    op.add_column(
        "tasks", sa.Column("network_mode", sa.String(32), server_default="none", nullable=False)
    )
    op.add_column(
        "tasks",
        sa.Column("tolerations", sa.JSON(), server_default=sa.text("'[]'::json"), nullable=False),
    )
    op.add_column("tasks", sa.Column("priority", sa.Integer(), server_default="50", nullable=False))
    op.add_column("tasks", sa.Column("queue_order", sa.BigInteger(), nullable=True))
    op.add_column(
        "tasks", sa.Column("preemptible", sa.Boolean(), server_default=sa.false(), nullable=False)
    )
    op.add_column(
        "tasks", sa.Column("preemption_count", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "tasks",
        sa.Column("requeue_on_preempt", sa.Boolean(), server_default=sa.true(), nullable=False),
    )
    op.add_column("tasks", sa.Column("unschedulable_reason", sa.String(128), nullable=True))
    op.add_column("tasks", sa.Column("failure_category", sa.String(64), nullable=True))
    op.add_column("tasks", sa.Column("error_category", sa.String(64), nullable=True))
    op.add_column("tasks", sa.Column("error_code", sa.String(64), nullable=True))
    op.add_column(
        "tasks",
        sa.Column("retry_backoff", sa.String(32), server_default="exponential", nullable=False),
    )
    op.add_column(
        "tasks", sa.Column("retry_base_seconds", sa.Float(), server_default="1", nullable=False)
    )
    op.add_column(
        "tasks", sa.Column("retry_max_seconds", sa.Float(), server_default="60", nullable=False)
    )
    op.add_column(
        "tasks",
        sa.Column(
            "retry_on_exit_codes",
            sa.JSON(),
            server_default=sa.text("'[1,137]'::json"),
            nullable=False,
        ),
    )
    op.add_column("tasks", sa.Column("runtime_handle", sa.JSON(), nullable=True))

    op.execute(sa.text(f"UPDATE tasks SET project_id = '{LEGACY_PROJECT_ID}'::uuid"))
    op.execute(sa.text("UPDATE tasks SET cpu_millicores = CEIL(cpu_limit * 1000)::integer"))
    op.execute(
        sa.text(
            "UPDATE tasks SET network_mode = CASE WHEN network_enabled "
            "THEN 'internet' ELSE 'none' END"
        )
    )
    op.execute(sa.text("CREATE SEQUENCE task_queue_order_seq"))
    op.execute(
        sa.text(
            "UPDATE tasks AS t SET queue_order = ranked.rn "
            "FROM (SELECT id, row_number() OVER (ORDER BY created_at,id) AS rn FROM tasks) ranked "
            "WHERE ranked.id = t.id"
        )
    )
    op.execute(
        sa.text(
            "SELECT setval('task_queue_order_seq', "
            "GREATEST(COALESCE((SELECT MAX(queue_order) FROM tasks),1),1))"
        )
    )
    op.alter_column("tasks", "project_id", nullable=False)
    op.alter_column("tasks", "cpu_millicores", nullable=False)
    op.alter_column(
        "tasks",
        "queue_order",
        nullable=False,
        server_default=sa.text("nextval('task_queue_order_seq')"),
    )
    op.create_foreign_key(
        "fk_tasks_project_id_projects",
        "tasks",
        "projects",
        ["project_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_tasks_submitted_by_user_id_users",
        "tasks",
        "users",
        ["submitted_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_tasks_created_by_api_key_id_api_keys",
        "tasks",
        "api_keys",
        ["created_by_api_key_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.drop_constraint("uq_tasks_idempotency_key", "tasks", type_="unique")
    op.create_unique_constraint(
        "uq_tasks_project_idempotency", "tasks", ["project_id", "idempotency_key"]
    )
    op.create_check_constraint("priority", "tasks", "priority BETWEEN 0 AND 100")
    op.create_check_constraint("cpu_millicores", "tasks", "cpu_millicores > 0")
    op.create_check_constraint("gpu_memory", "tasks", "gpu_memory_mb >= 0")
    op.create_check_constraint("retry_base_seconds", "tasks", "retry_base_seconds > 0")
    op.create_check_constraint(
        "retry_max_seconds", "tasks", "retry_max_seconds >= retry_base_seconds"
    )
    op.create_index("ix_tasks_project_id", "tasks", ["project_id"])
    op.create_index("ix_tasks_project_created", "tasks", ["project_id", "created_at", "id"])
    op.create_index("ix_tasks_schedule", "tasks", ["status", "priority", "queue_order"])

    op.add_column("workers", sa.Column("worker_session_id", sa.Uuid(), nullable=True))
    op.add_column("workers", sa.Column("node_name", sa.String(255), nullable=True))
    op.add_column(
        "workers",
        sa.Column(
            "runtime_types",
            sa.JSON(),
            server_default=sa.text("'[\"docker\"]'::json"),
            nullable=False,
        ),
    )
    op.add_column("workers", sa.Column("cpu_total_millicores", sa.Integer(), nullable=True))
    op.add_column("workers", sa.Column("cpu_allocatable_millicores", sa.Integer(), nullable=True))
    op.add_column("workers", sa.Column("memory_allocatable_mb", sa.Integer(), nullable=True))
    op.add_column(
        "workers",
        sa.Column("taints", sa.JSON(), server_default=sa.text("'[]'::json"), nullable=False),
    )
    op.add_column(
        "workers",
        sa.Column("inventory_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "workers",
        sa.Column(
            "inventory_updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.add_column(
        "workers",
        sa.Column("overcommitted", sa.Boolean(), server_default=sa.false(), nullable=False),
    )
    op.add_column("workers", sa.Column("drain_reason", sa.String(512), nullable=True))
    op.execute(
        sa.text(
            "UPDATE workers SET worker_session_id=gen_random_uuid(), "
            "node_name=hostname, cpu_total_millicores=cpu_count*1000, "
            "cpu_allocatable_millicores=cpu_count*1000, memory_allocatable_mb=memory_total_mb"
        )
    )
    op.alter_column("workers", "worker_session_id", nullable=False)
    op.alter_column("workers", "cpu_total_millicores", nullable=False)
    op.alter_column("workers", "cpu_allocatable_millicores", nullable=False)
    op.alter_column("workers", "memory_allocatable_mb", nullable=False)
    op.create_index("ix_workers_worker_session_id", "workers", ["worker_session_id"])

    op.create_table(
        "gpu_devices",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("device_uuid", sa.String(255), nullable=False),
        sa.Column("device_index", sa.Integer(), nullable=False),
        sa.Column("vendor", sa.String(64), server_default="nvidia", nullable=False),
        sa.Column("model", sa.String(255), nullable=False),
        sa.Column("memory_total_mb", sa.Integer(), nullable=False),
        sa.Column("memory_free_mb", sa.Integer(), nullable=False),
        sa.Column("compute_capability", sa.String(32), nullable=True),
        sa.Column("health", sa.String(32), server_default="healthy", nullable=False),
        sa.Column("fake", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("inventory_generation", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "last_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("memory_total_mb > 0", name="total_memory"),
        sa.CheckConstraint("memory_free_mb >= 0", name="free_memory"),
        sa.ForeignKeyConstraint(
            ["worker_id"], ["workers.id"], ondelete="CASCADE", name="fk_gpu_devices_worker"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_gpu_devices"),
        sa.UniqueConstraint("worker_id", "device_uuid", name="uq_gpu_devices_worker_uuid"),
        sa.UniqueConstraint("worker_id", "device_index", name="uq_gpu_devices_worker_index"),
    )
    op.create_index("ix_gpu_devices_worker_id", "gpu_devices", ["worker_id"])
    op.create_index("ix_gpu_devices_worker_health", "gpu_devices", ["worker_id", "health"])

    op.create_table(
        "task_executions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("worker_session_id", sa.Uuid(), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("cpu_millicores", sa.Integer(), nullable=False),
        sa.Column("memory_mb", sa.Integer(), nullable=False),
        sa.Column("gpu_count", sa.Integer(), nullable=False),
        sa.Column("gpu_model", sa.String(255), nullable=True),
        sa.Column("cpu_price_per_hour", sa.Numeric(20, 8), nullable=False),
        sa.Column("memory_price_per_gb_hour", sa.Numeric(20, 8), nullable=False),
        sa.Column("gpu_price_per_hour", sa.Numeric(20, 8), nullable=False),
        sa.Column(
            "assigned_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("runtime_type", sa.String(32), nullable=False),
        sa.Column("runtime_object_id", sa.String(512), nullable=True),
        sa.Column("error_category", sa.String(64), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], ondelete="CASCADE", name="fk_executions_task"
        ),
        sa.ForeignKeyConstraint(
            ["project_id"], ["projects.id"], ondelete="RESTRICT", name="fk_executions_project"
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"], ["workers.id"], ondelete="SET NULL", name="fk_executions_worker"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_task_executions"),
        sa.UniqueConstraint("task_id", "attempt", name="uq_task_executions_task_attempt"),
    )
    op.create_index("ix_task_executions_task_id", "task_executions", ["task_id"])
    op.create_index("ix_task_executions_project_id", "task_executions", ["project_id"])
    op.create_index("ix_task_executions_worker_id", "task_executions", ["worker_id"])
    op.create_index(
        "ix_task_executions_project_started", "task_executions", ["project_id", "started_at"]
    )

    op.create_table(
        "resource_reservations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("worker_session_id", sa.Uuid(), nullable=False),
        sa.Column("cpu_millicores", sa.Integer(), nullable=False),
        sa.Column("memory_mb", sa.Integer(), nullable=False),
        sa.Column("gpu_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("state", sa.String(32), server_default="active", nullable=False),
        sa.Column("legacy_unbound", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("release_reason", sa.String(128), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("cpu_millicores > 0", name="cpu"),
        sa.CheckConstraint("memory_mb > 0", name="memory"),
        sa.CheckConstraint("gpu_count >= 0", name="gpu"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["execution_id"], ["task_executions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_resource_reservations"),
        sa.UniqueConstraint("execution_id", name="uq_resource_reservations_execution"),
    )
    op.create_index("ix_reservations_worker_state", "resource_reservations", ["worker_id", "state"])
    op.create_index("ix_reservations_task_state", "resource_reservations", ["task_id", "state"])
    op.create_index("ix_resource_reservations_project_id", "resource_reservations", ["project_id"])
    op.create_index("ix_resource_reservations_task_id", "resource_reservations", ["task_id"])
    op.create_index("ix_resource_reservations_worker_id", "resource_reservations", ["worker_id"])
    op.create_index(
        "uq_reservations_active_task",
        "resource_reservations",
        ["task_id"],
        unique=True,
        postgresql_where=sa.text("released_at IS NULL"),
    )
    # Preserve Phase I executions that were live while the upgrade ran. Their
    # existing execution_id remains the fencing token and their aggregate Worker
    # capacity becomes an explicit reservation before the Phase II app starts.
    op.execute(
        sa.text(
            "INSERT INTO task_executions "
            "(id,task_id,project_id,worker_id,worker_session_id,attempt,status,"
            "cpu_millicores,memory_mb,gpu_count,gpu_model,cpu_price_per_hour,"
            "memory_price_per_gb_hour,gpu_price_per_hour,assigned_at,started_at,"
            "runtime_type) "
            "SELECT t.execution_id,t.id,t.project_id,t.worker_id,w.worker_session_id,"
            "t.retry_count+t.recovery_count+1,t.status,t.cpu_millicores,"
            "t.memory_limit_mb,t.gpu_count,t.gpu_model,0.05,0.005,1.0,"
            "COALESCE(t.assigned_at,t.created_at),t.started_at,t.runtime_type "
            "FROM tasks t JOIN workers w ON w.id=t.worker_id "
            "WHERE t.execution_id IS NOT NULL "
            "AND t.status IN ('assigned','pulling','running')"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO resource_reservations "
            "(id,project_id,task_id,execution_id,worker_id,worker_session_id,"
            "cpu_millicores,memory_mb,gpu_count,state,legacy_unbound,created_at,version) "
            "SELECT gen_random_uuid(),t.project_id,t.id,t.execution_id,t.worker_id,"
            "w.worker_session_id,t.cpu_millicores,t.memory_limit_mb,t.gpu_count,"
            "'active',(t.gpu_count > 0),COALESCE(t.assigned_at,t.created_at),1 "
            "FROM tasks t JOIN workers w ON w.id=t.worker_id "
            "WHERE t.execution_id IS NOT NULL "
            "AND t.status IN ('assigned','pulling','running')"
        )
    )

    op.create_table(
        "reservation_gpu_devices",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("reservation_id", sa.Uuid(), nullable=False),
        sa.Column("gpu_device_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["reservation_id"], ["resource_reservations.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["gpu_device_id"], ["gpu_devices.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_reservation_gpu_devices"),
        sa.UniqueConstraint("reservation_id", "gpu_device_id", name="uq_reservation_gpu_device"),
    )
    op.create_index(
        "ix_reservation_gpu_devices_reservation_id", "reservation_gpu_devices", ["reservation_id"]
    )
    op.create_index(
        "ix_reservation_gpu_devices_gpu_device_id", "reservation_gpu_devices", ["gpu_device_id"]
    )
    op.create_index(
        "uq_reservation_gpu_active",
        "reservation_gpu_devices",
        ["gpu_device_id"],
        unique=True,
        postgresql_where=sa.text("released_at IS NULL"),
    )

    op.create_table(
        "placement_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("scheduler_id", sa.String(255), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("policy", sa.String(32), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("reason", sa.String(128), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("effective_priority", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_placement_attempts"),
    )
    op.create_index("ix_placement_attempts_task_id", "placement_attempts", ["task_id"])
    op.create_index("ix_placement_task_created", "placement_attempts", ["task_id", "created_at"])

    op.create_table(
        "preemption_plans",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("incoming_task_id", sa.Uuid(), nullable=False),
        sa.Column("victim_task_id", sa.Uuid(), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), server_default="requested", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["incoming_task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["victim_task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_preemption_plans"),
    )
    op.create_index(
        "ix_preemption_incoming_state", "preemption_plans", ["incoming_task_id", "state"]
    )
    op.create_index(
        "ix_preemption_plans_incoming_task_id", "preemption_plans", ["incoming_task_id"]
    )
    op.create_index("ix_preemption_plans_victim_task_id", "preemption_plans", ["victim_task_id"])

    op.create_table(
        "task_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("from_status", sa.String(32), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=True),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("details", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_task_events"),
        sa.UniqueConstraint("task_id", "sequence", name="uq_task_events_task_sequence"),
    )
    op.create_index("ix_task_events_project_id", "task_events", ["project_id"])
    op.create_index("ix_task_events_task_id", "task_events", ["task_id"])
    op.create_index("ix_task_events_task_created", "task_events", ["task_id", "sequence"])
    op.create_index(
        "ix_task_events_project_created",
        "task_events",
        ["project_id", "created_at", "id"],
    )
    op.execute(
        sa.text(
            "INSERT INTO task_events "
            "(id,project_id,task_id,event_type,sequence,status,execution_id,"
            "worker_id,details,created_at) "
            "SELECT gen_random_uuid(),project_id,id,'migration.snapshot',0,status,"
            "execution_id,worker_id,'{}'::json,COALESCE(assigned_at,queued_at,created_at) "
            "FROM tasks"
        )
    )

    op.add_column(
        "outbox_events",
        sa.Column("aggregate_type", sa.String(64), server_default="task", nullable=False),
    )
    op.add_column(
        "outbox_events",
        sa.Column("event_version", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column("outbox_events", sa.Column("correlation_id", sa.String(255), nullable=True))
    op.add_column("outbox_events", sa.Column("trace_id", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("outbox_events", "trace_id")
    op.drop_column("outbox_events", "correlation_id")
    op.drop_column("outbox_events", "event_version")
    op.drop_column("outbox_events", "aggregate_type")
    # Development snapshots may already carry this revision marker from before
    # the timeline table was added during the unreleased Phase II iteration.
    op.drop_table("task_events", if_exists=True)
    op.drop_table("preemption_plans")
    op.drop_table("placement_attempts")
    op.drop_table("reservation_gpu_devices")
    op.drop_table("resource_reservations")
    op.drop_table("task_executions")
    op.drop_table("gpu_devices")

    compatibility_columns = {
        "retry_on_exit_codes",
        "retry_max_seconds",
        "retry_base_seconds",
        "retry_backoff",
        "error_code",
        "error_category",
    }
    for column in (
        "drain_reason",
        "overcommitted",
        "inventory_updated_at",
        "inventory_generation",
        "taints",
        "memory_allocatable_mb",
        "cpu_allocatable_millicores",
        "cpu_total_millicores",
        "runtime_types",
        "node_name",
        "worker_session_id",
    ):
        op.drop_column("workers", column)

    for constraint_name in (
        "ck_tasks_gpu_memory",
        "ck_tasks_ck_tasks_gpu_memory",
        "ck_tasks_cpu_millicores",
        "ck_tasks_ck_tasks_cpu_millicores",
        "ck_tasks_priority",
        "ck_tasks_ck_tasks_priority",
        "ck_tasks_retry_base_seconds",
        "ck_tasks_ck_tasks_retry_base_seconds",
        "ck_tasks_retry_max_seconds",
        "ck_tasks_ck_tasks_retry_max_seconds",
    ):
        op.execute(sa.text(f'ALTER TABLE tasks DROP CONSTRAINT IF EXISTS "{constraint_name}"'))
    op.drop_constraint("uq_tasks_project_idempotency", "tasks", type_="unique")
    op.create_unique_constraint("uq_tasks_idempotency_key", "tasks", ["idempotency_key"])
    op.drop_constraint("fk_tasks_created_by_api_key_id_api_keys", "tasks", type_="foreignkey")
    op.drop_constraint("fk_tasks_submitted_by_user_id_users", "tasks", type_="foreignkey")
    op.drop_constraint("fk_tasks_project_id_projects", "tasks", type_="foreignkey")
    op.drop_index("ix_tasks_schedule", table_name="tasks")
    op.drop_index("ix_tasks_project_created", table_name="tasks")
    op.drop_index("ix_tasks_project_id", table_name="tasks")
    for column in (
        "runtime_handle",
        "retry_on_exit_codes",
        "retry_max_seconds",
        "retry_base_seconds",
        "retry_backoff",
        "error_code",
        "error_category",
        "failure_category",
        "unschedulable_reason",
        "preemptible",
        "preemption_count",
        "requeue_on_preempt",
        "queue_order",
        "priority",
        "tolerations",
        "network_mode",
        "gpu_device_ids",
        "gpu_model",
        "gpu_memory_mb",
        "cpu_millicores",
        "runtime_type",
        "workload_type",
        "created_by_api_key_id",
        "submitted_by_user_id",
        "project_id",
    ):
        op.drop_column("tasks", column, if_exists=column in compatibility_columns)
    op.alter_column(
        "tasks",
        "status",
        existing_type=sa.String(length=32),
        type_=sa.String(length=9),
        existing_nullable=False,
    )
    op.execute(sa.text("DROP SEQUENCE task_queue_order_seq"))
