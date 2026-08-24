"""Add quotas, usage, registry, services, artifacts and DAG resources.

Revision ID: 0005_platform_resources
Revises: 0004_runtime_scheduling
Create Date: 2026-08-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_platform_resources"
down_revision: str | Sequence[str] | None = "0004_runtime_scheduling"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_quotas",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("max_queued_tasks", sa.Integer(), nullable=True),
        sa.Column("max_running_tasks", sa.Integer(), nullable=True),
        sa.Column("max_cpu_millicores", sa.Integer(), nullable=True),
        sa.Column("max_memory_mb", sa.Integer(), nullable=True),
        sa.Column("max_gpus", sa.Integer(), nullable=True),
        sa.Column("max_services", sa.Integer(), nullable=True),
        sa.Column("max_service_replicas", sa.Integer(), nullable=True),
        sa.Column("max_artifact_bytes", sa.BigInteger(), nullable=True),
        sa.Column("daily_cost_limit", sa.Numeric(20, 8), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "max_queued_tasks IS NULL OR max_queued_tasks >= 0", name="quota_queued"
        ),
        sa.CheckConstraint(
            "max_running_tasks IS NULL OR max_running_tasks >= 0", name="quota_running"
        ),
        sa.CheckConstraint(
            "max_cpu_millicores IS NULL OR max_cpu_millicores >= 0", name="quota_cpu"
        ),
        sa.CheckConstraint("max_memory_mb IS NULL OR max_memory_mb >= 0", name="quota_memory"),
        sa.CheckConstraint("max_gpus IS NULL OR max_gpus >= 0", name="quota_gpus"),
        sa.CheckConstraint("max_services IS NULL OR max_services >= 0", name="quota_services"),
        sa.CheckConstraint(
            "max_service_replicas IS NULL OR max_service_replicas >= 0",
            name="quota_service_replicas",
        ),
        sa.CheckConstraint(
            "max_artifact_bytes IS NULL OR max_artifact_bytes >= 0",
            name="quota_artifact_bytes",
        ),
        sa.CheckConstraint("daily_cost_limit IS NULL OR daily_cost_limit >= 0", name="quota_cost"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("project_id", name="pk_project_quotas"),
    )
    op.create_table(
        "project_quota_state",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("queued_tasks", sa.Integer(), server_default="0", nullable=False),
        sa.Column("running_tasks", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved_cpu_millicores", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved_memory_mb", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved_gpus", sa.Integer(), server_default="0", nullable=False),
        sa.Column("service_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("service_replicas", sa.Integer(), server_default="0", nullable=False),
        sa.Column("artifact_bytes", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column(
            "accounting_date", sa.Date(), server_default=sa.func.current_date(), nullable=False
        ),
        sa.Column("daily_reserved_cost", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("daily_settled_cost", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "queued_tasks >= 0 AND running_tasks >= 0 AND reserved_cpu_millicores >= 0 "
            "AND reserved_memory_mb >= 0 AND reserved_gpus >= 0 AND service_count >= 0 "
            "AND service_replicas >= 0 AND artifact_bytes >= 0 "
            "AND daily_reserved_cost >= 0 AND daily_settled_cost >= 0",
            name="nonnegative",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("project_id", name="pk_project_quota_state"),
    )
    op.execute(
        sa.text(
            "INSERT INTO project_quotas (project_id) SELECT id FROM projects "
            "ON CONFLICT (project_id) DO NOTHING"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO project_quota_state "
            "(project_id,queued_tasks,running_tasks,reserved_cpu_millicores,"
            "reserved_memory_mb,reserved_gpus) "
            "SELECT p.id, "
            "COUNT(t.id) FILTER (WHERE t.status IN ('pending','queued','retrying')), "
            "COUNT(t.id) FILTER (WHERE t.status IN "
            "('assigned','preparing','pulling','starting','running','preempting','stopping')), "
            "COALESCE(SUM(t.cpu_millicores) FILTER (WHERE t.status IN "
            "('assigned','preparing','pulling','starting','running','preempting','stopping')),0), "
            "COALESCE(SUM(t.memory_limit_mb) FILTER (WHERE t.status IN "
            "('assigned','preparing','pulling','starting','running','preempting','stopping')),0), "
            "COALESCE(SUM(t.gpu_count) FILTER (WHERE t.status IN "
            "('assigned','preparing','pulling','starting','running','preempting','stopping')),0) "
            "FROM projects p LEFT JOIN tasks t ON t.project_id=p.id GROUP BY p.id"
        )
    )

    op.create_table(
        "usage_ledger",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cpu_seconds", sa.Numeric(24, 6), nullable=False),
        sa.Column("memory_gb_seconds", sa.Numeric(24, 6), nullable=False),
        sa.Column("gpu_seconds", sa.Numeric(24, 6), nullable=False),
        sa.Column("gpu_model", sa.String(255), nullable=True),
        sa.Column("cost", sa.Numeric(20, 8), nullable=False),
        sa.Column("currency", sa.String(3), server_default="USD", nullable=False),
        sa.Column("pricing_source", sa.String(32), server_default="rate_snapshot", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "cpu_seconds >= 0 AND memory_gb_seconds >= 0 AND gpu_seconds >= 0 AND cost >= 0",
            name="nonnegative",
        ),
        sa.CheckConstraint("finished_at >= started_at", name="period"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["execution_id"], ["task_executions.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_usage_ledger"),
        sa.UniqueConstraint("execution_id", name="uq_usage_ledger_execution"),
    )
    op.create_index("ix_usage_ledger_project_id", "usage_ledger", ["project_id"])
    op.create_index("ix_usage_ledger_task_id", "usage_ledger", ["task_id"])
    op.create_index("ix_usage_project_period", "usage_ledger", ["project_id", "finished_at"])

    op.create_table(
        "billing_rates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("resource_type", sa.String(32), nullable=False),
        sa.Column("sku", sa.String(255), nullable=False),
        sa.Column("unit", sa.String(64), nullable=False),
        sa.Column("unit_price", sa.Numeric(20, 8), nullable=False),
        sa.Column("currency", sa.String(3), server_default="USD", nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("unit_price >= 0", name="unit_price_nonnegative"),
        sa.PrimaryKeyConstraint("id", name="pk_billing_rates"),
        sa.UniqueConstraint(
            "resource_type", "sku", "effective_from", name="uq_billing_rate_effective"
        ),
    )

    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=True),
        sa.Column("actor_type", sa.String(32), nullable=False),
        sa.Column("actor_user_id", sa.Uuid(), nullable=True),
        sa.Column("api_key_id", sa.Uuid(), nullable=True),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.String(255), nullable=True),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("request_id", sa.String(255), nullable=True),
        sa.Column("source_ip", sa.String(64), nullable=True),
        sa.Column("details", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["api_key_id"], ["api_keys.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_audit_events"),
    )
    op.create_index(
        "ix_audit_project_occurred", "audit_events", ["project_id", "occurred_at", "id"]
    )
    op.create_index("ix_audit_events_project_id", "audit_events", ["project_id"])

    _create_registry_tables()
    _create_artifact_dag_tables()
    _create_service_tables()


def _create_registry_tables() -> None:
    op.create_table(
        "registered_models",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("source", sa.String(1024), nullable=False),
        sa.Column("revision", sa.String(255), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("gpu_memory_mb", sa.Integer(), nullable=True),
        sa.Column("architecture", sa.String(255), nullable=True),
        sa.Column("metadata", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_registered_models"),
        sa.UniqueConstraint("project_id", "name", name="uq_registered_models_project_name"),
    )
    op.create_index("ix_registered_models_project_id", "registered_models", ["project_id"])
    op.create_table(
        "secrets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("current_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_secrets"),
        sa.UniqueConstraint("project_id", "name", name="uq_secrets_project_name"),
    )
    op.create_index("ix_secrets_project_id", "secrets", ["project_id"])
    op.create_table(
        "secret_versions",
        sa.Column("secret_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("key_id", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["secret_id"], ["secrets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("secret_id", "version", name="pk_secret_versions"),
    )
    op.create_table(
        "task_secret_bindings",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("env_name", sa.String(255), nullable=False),
        sa.Column("secret_id", sa.Uuid(), nullable=False),
        sa.Column("secret_version", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["secret_id", "secret_version"],
            ["secret_versions.secret_id", "secret_versions.version"],
            ondelete="RESTRICT",
            name="fk_task_secret_binding_version",
        ),
        sa.PrimaryKeyConstraint("task_id", "env_name", name="pk_task_secret_bindings"),
    )
    op.create_index("ix_task_secret_bindings_secret_id", "task_secret_bindings", ["secret_id"])
    op.create_table(
        "image_policies",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("default_action", sa.String(16), server_default="deny", nullable=False),
        sa.Column("require_digest", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("default_action IN ('allow','deny')", name="action"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("project_id", name="pk_image_policies"),
    )
    op.create_table(
        "image_policy_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("registry", sa.String(255), nullable=True),
        sa.Column("repository_glob", sa.String(512), nullable=False),
        sa.Column("tag_glob", sa.String(255), nullable=True),
        sa.Column("digest", sa.String(255), nullable=True),
        sa.Column("priority", sa.Integer(), server_default="100", nullable=False),
        sa.CheckConstraint("action IN ('allow','deny')", name="action"),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_image_policy_rules"),
    )
    op.create_index(
        "ix_image_rules_project_priority", "image_policy_rules", ["project_id", "priority"]
    )
    op.create_index("ix_image_policy_rules_project_id", "image_policy_rules", ["project_id"])


def _create_artifact_dag_tables() -> None:
    op.create_table(
        "artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), server_default="pending", nullable=False),
        sa.Column("backend", sa.String(32), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("content_type", sa.String(255), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("sha256", sa.String(64), nullable=True),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_artifacts"),
        sa.UniqueConstraint("project_id", "object_key", name="uq_artifacts_project_object_key"),
    )
    op.create_index("ix_artifacts_project_created", "artifacts", ["project_id", "created_at", "id"])
    op.create_index("ix_artifacts_project_id", "artifacts", ["project_id"])
    op.create_table(
        "task_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.Uuid(), nullable=True),
        sa.Column("direction", sa.String(16), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("mount_path", sa.String(1024), nullable=False),
        sa.Column("required", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "direction IN ('input','output')",
            name=op.f("ck_task_artifacts_task_artifact_direction"),
        ),
        sa.CheckConstraint(
            "direction <> 'input' OR artifact_id IS NOT NULL",
            name=op.f("ck_task_artifacts_task_artifact_input_bound"),
        ),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_task_artifacts"),
        sa.UniqueConstraint(
            "task_id",
            "direction",
            "name",
            name="uq_task_artifacts_task_direction_name",
        ),
        sa.UniqueConstraint(
            "task_id",
            "direction",
            "mount_path",
            name="uq_task_artifacts_task_direction_path",
        ),
    )
    op.create_index("ix_task_artifacts_artifact_id", "task_artifacts", ["artifact_id"])
    op.create_index("ix_task_artifacts_task_id", "task_artifacts", ["task_id"])
    op.create_table(
        "datasets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("current_version", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_datasets"),
        sa.UniqueConstraint("project_id", "name", name="uq_datasets_project_name"),
    )
    op.create_table(
        "dataset_versions",
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("metadata", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["dataset_id"], ["datasets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("dataset_id", "version", name="pk_dataset_versions"),
    )
    op.create_index("ix_datasets_project_id", "datasets", ["project_id"])
    op.create_table(
        "job_groups",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(32), server_default="pending", nullable=False),
        sa.Column("retry_policy", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_job_groups"),
    )
    op.create_index("ix_job_groups_project_created", "job_groups", ["project_id", "created_at"])
    op.create_index("ix_job_groups_project_id", "job_groups", ["project_id"])
    op.create_table(
        "task_dependencies",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("depends_on_task_id", sa.Uuid(), nullable=False),
        sa.Column("job_group_id", sa.Uuid(), nullable=True),
        sa.Column("failure_policy", sa.String(32), server_default="cancel", nullable=False),
        sa.CheckConstraint("task_id <> depends_on_task_id", name="task_dependencies_not_self"),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["depends_on_task_id"], ["tasks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_group_id"], ["job_groups.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_id", "depends_on_task_id", name="pk_task_dependencies"),
    )
    op.create_index("ix_task_dependencies_job_group_id", "task_dependencies", ["job_group_id"])
    op.create_index(
        "ix_task_dependencies_depends_on",
        "task_dependencies",
        ["depends_on_task_id"],
    )


def _create_service_tables() -> None:
    op.create_table(
        "model_services",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("model", sa.String(512), nullable=False),
        sa.Column("runtime", sa.String(32), nullable=False),
        sa.Column("runtime_type", sa.String(32), server_default="docker", nullable=False),
        sa.Column("image", sa.String(512), nullable=True),
        sa.Column("cpu_millicores", sa.Integer(), server_default="1000", nullable=False),
        sa.Column("memory_mb", sa.Integer(), server_default="1024", nullable=False),
        sa.Column("gpu_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("gpu_memory_mb", sa.Integer(), server_default="0", nullable=False),
        sa.Column("desired_replicas", sa.Integer(), server_default="1", nullable=False),
        sa.Column("autoscaling_enabled", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("autoscaling_min_replicas", sa.Integer(), server_default="1", nullable=False),
        sa.Column("autoscaling_max_replicas", sa.Integer(), server_default="4", nullable=False),
        sa.Column(
            "autoscaling_target_concurrency", sa.Integer(), server_default="8", nullable=False
        ),
        sa.Column(
            "autoscaling_cooldown_seconds", sa.Integer(), server_default="60", nullable=False
        ),
        sa.Column("last_scaled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_autoscale_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("generation", sa.Integer(), server_default="1", nullable=False),
        sa.Column("status", sa.String(32), server_default="pending", nullable=False),
        sa.Column("round_robin_cursor", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "desired_replicas >= 0 AND desired_replicas <= 1000",
            name="desired_replicas",
        ),
        sa.CheckConstraint("generation >= 1", name="generation"),
        sa.CheckConstraint("cpu_millicores >= 1", name="cpu_millicores"),
        sa.CheckConstraint("memory_mb >= 16", name="memory_mb"),
        sa.CheckConstraint("gpu_count >= 0", name="gpu_count"),
        sa.CheckConstraint("gpu_memory_mb >= 0", name="gpu_memory_mb"),
        sa.CheckConstraint("runtime IN ('vllm','fake')", name="serving_runtime"),
        sa.CheckConstraint(
            "runtime_type IN ('docker','kubernetes','fake')",
            name="service_runtime_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending','deploying','running','degraded','stopping','stopped','failed')",
            name="model_service_status",
        ),
        sa.CheckConstraint(
            "autoscaling_min_replicas >= 0 AND autoscaling_min_replicas <= 1000",
            name="autoscaling_min_replicas",
        ),
        sa.CheckConstraint(
            "autoscaling_max_replicas >= 1 "
            "AND autoscaling_max_replicas >= autoscaling_min_replicas "
            "AND autoscaling_max_replicas <= 1000",
            name="autoscaling_max_replicas",
        ),
        sa.CheckConstraint(
            "autoscaling_target_concurrency >= 1 AND autoscaling_target_concurrency <= 1000000",
            name="autoscaling_target_concurrency",
        ),
        sa.CheckConstraint(
            "autoscaling_cooldown_seconds >= 0 AND autoscaling_cooldown_seconds <= 86400",
            name="autoscaling_cooldown_seconds",
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_model_services"),
        sa.UniqueConstraint("project_id", "name", name="uq_model_services_project_name"),
    )
    op.create_index("ix_model_services_project_id", "model_services", ["project_id"])
    op.create_index(
        "ix_model_services_project_status", "model_services", ["project_id", "status", "created_at"]
    )
    op.create_index("ix_model_services_reconcile", "model_services", ["status", "updated_at"])
    op.create_index(
        "ix_model_services_last_autoscale_checked_at",
        "model_services",
        ["last_autoscale_checked_at"],
    )
    op.create_table(
        "service_replicas",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("service_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), server_default="pending", nullable=False),
        sa.Column("health", sa.String(32), server_default="unknown", nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("execution_id", sa.Uuid(), nullable=True),
        sa.Column("endpoint_url", sa.String(2048), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_health_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health_probe_token", sa.Uuid(), nullable=True),
        sa.Column("health_probe_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health_failure_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint("generation >= 1", name="generation"),
        sa.CheckConstraint("ordinal >= 0", name="ordinal"),
        sa.CheckConstraint("health_failure_count >= 0", name="health_failure_count"),
        sa.CheckConstraint(
            "status IN ('pending','starting','running','stopping','stopped','failed','lost')",
            name="service_replica_status",
        ),
        sa.CheckConstraint(
            "health IN ('unknown','healthy','unhealthy')",
            name="service_replica_health",
        ),
        sa.ForeignKeyConstraint(["service_id"], ["model_services.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["worker_id"], ["workers.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_service_replicas"),
        sa.UniqueConstraint(
            "service_id", "generation", "ordinal", name="uq_service_replicas_generation_ordinal"
        ),
        sa.UniqueConstraint("execution_id", name="uq_service_replicas_execution_id"),
    )
    op.create_index("ix_service_replicas_service_id", "service_replicas", ["service_id"])
    op.create_index("ix_service_replicas_execution_id", "service_replicas", ["execution_id"])
    op.create_index(
        "ix_service_replicas_service_generation_status",
        "service_replicas",
        ["service_id", "generation", "status"],
    )
    op.create_index(
        "ix_service_replicas_ready",
        "service_replicas",
        ["service_id", "generation", "status", "health"],
    )
    op.create_index(
        "ix_service_replicas_worker_status", "service_replicas", ["worker_id", "status"]
    )
    op.create_index(
        "ix_service_replicas_lease_expires_at", "service_replicas", ["lease_expires_at"]
    )
    op.create_index(
        "ix_service_replicas_health_probe_expires_at",
        "service_replicas",
        ["health_probe_expires_at"],
    )


def downgrade() -> None:
    op.drop_table("service_replicas")
    op.drop_table("model_services")
    op.drop_table("task_dependencies")
    op.drop_table("job_groups")
    op.drop_table("dataset_versions")
    op.drop_table("datasets")
    op.drop_table("task_artifacts")
    op.drop_table("artifacts")
    op.drop_table("image_policy_rules")
    op.drop_table("image_policies")
    op.drop_table("task_secret_bindings")
    op.drop_table("secret_versions")
    op.drop_table("secrets")
    op.drop_table("registered_models")
    op.drop_table("audit_events")
    op.drop_table("billing_rates")
    op.drop_table("usage_ledger")
    op.drop_table("project_quota_state")
    op.drop_table("project_quotas")
