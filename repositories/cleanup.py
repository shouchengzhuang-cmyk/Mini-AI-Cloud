import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import FINAL_TASK_STATUSES
from models.artifact import Artifact, DatasetVersion, TaskArtifact, TaskDependency
from models.identity import ApiKey
from models.outbox import OutboxEvent
from models.scheduling import ResourceReservation
from models.task import Task, TaskLog
from models.usage import AuditEvent, TaskExecution
from repositories.clock import database_utcnow


@dataclass(frozen=True, slots=True)
class DatabaseCleanupResult:
    expired_api_keys: int
    task_logs_deleted: int
    task_runtime_handles_cleared: int
    execution_runtime_handles_cleared: int
    tasks_deleted: int
    audit_events_deleted: int
    outbox_events_deleted: int
    log_task_ids: tuple[uuid.UUID, ...]


class CleanupRepository:
    @staticmethod
    async def run_database_cleanup(
        session: AsyncSession,
        *,
        task_retention_days: int,
        log_retention_days: int,
        audit_retention_days: int,
        limit: int,
    ) -> DatabaseCleanupResult:
        now = await database_utcnow(session)
        task_cutoff = now - timedelta(days=task_retention_days)
        log_cutoff = now - timedelta(days=log_retention_days)
        audit_cutoff = now - timedelta(days=audit_retention_days)

        expired_key_ids = (
            select(ApiKey.id)
            .where(
                ApiKey.revoked_at.is_(None),
                ApiKey.expires_at.is_not(None),
                ApiKey.expires_at <= now,
            )
            .order_by(ApiKey.expires_at, ApiKey.id)
            .limit(limit)
        )
        expired_keys = await session.execute(
            update(ApiKey)
            .where(ApiKey.id.in_(expired_key_ids))
            .values(revoked_at=now, version=ApiKey.version + 1)
            .returning(ApiKey.id)
        )

        old_log_rows = list(
            (
                await session.execute(
                    select(TaskLog.id, TaskLog.task_id)
                    .where(TaskLog.timestamp < log_cutoff)
                    .order_by(TaskLog.timestamp, TaskLog.id)
                    .limit(limit)
                )
            )
            .tuples()
            .all()
        )
        logs_deleted = 0
        deleted_log_task_ids: set[uuid.UUID] = set()
        if old_log_rows:
            result = await session.execute(
                delete(TaskLog)
                .where(TaskLog.id.in_([log_id for log_id, _task_id in old_log_rows]))
                .returning(TaskLog.id)
            )
            logs_deleted = len(result.scalars().all())
            deleted_log_task_ids = {task_id for _log_id, task_id in old_log_rows}

        stream_task_ids: tuple[uuid.UUID, ...] = ()
        if deleted_log_task_ids:
            stream_task_ids = tuple(
                await session.scalars(
                    select(Task.id)
                    .where(
                        Task.id.in_(deleted_log_task_ids),
                        Task.status.in_(FINAL_TASK_STATUSES),
                        Task.finished_at.is_not(None),
                        Task.finished_at < log_cutoff,
                        ~exists(select(TaskLog.id).where(TaskLog.task_id == Task.id)),
                    )
                    .order_by(Task.finished_at, Task.id)
                )
            )

        retained_task_ids = tuple(
            await session.scalars(
                select(Task.id)
                .where(
                    Task.status.in_(FINAL_TASK_STATUSES),
                    Task.finished_at.is_not(None),
                    Task.finished_at < task_cutoff,
                    ~exists(
                        select(TaskDependency.task_id).where(
                            TaskDependency.depends_on_task_id == Task.id
                        )
                    ),
                    ~exists(
                        select(ResourceReservation.id).where(
                            ResourceReservation.task_id == Task.id,
                            ResourceReservation.released_at.is_(None),
                        )
                    ),
                )
                .order_by(Task.finished_at, Task.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        task_handles_cleared: tuple[uuid.UUID, ...] = ()
        execution_handles_cleared: tuple[uuid.UUID, ...] = ()
        tasks_deleted = 0
        if retained_task_ids:
            task_handles = await session.execute(
                update(Task)
                .where(Task.id.in_(retained_task_ids), Task.runtime_handle.is_not(None))
                .values(runtime_handle=None, version=Task.version + 1)
                .returning(Task.id)
            )
            task_handles_cleared = tuple(task_handles.scalars().all())
            execution_handles = await session.execute(
                update(TaskExecution)
                .where(
                    TaskExecution.task_id.in_(retained_task_ids),
                    TaskExecution.runtime_object_id.is_not(None),
                )
                .values(
                    runtime_object_id=None,
                    runtime_namespace=None,
                    runtime_resource_kind=None,
                    runtime_resource_name=None,
                    runtime_resource_uid=None,
                    runtime_worker_session_id=None,
                    observed_pod_name=None,
                    observed_pod_uid=None,
                    runtime_spec_hash=None,
                )
                .returning(TaskExecution.id)
            )
            execution_handles_cleared = tuple(execution_handles.scalars().all())
            result = await session.execute(
                delete(Task).where(Task.id.in_(retained_task_ids)).returning(Task.id)
            )
            tasks_deleted = len(result.scalars().all())

        audit_ids = (
            select(AuditEvent.id)
            .where(AuditEvent.occurred_at < audit_cutoff)
            .order_by(AuditEvent.occurred_at, AuditEvent.id)
            .limit(limit)
        )
        audit_deleted = await session.execute(
            delete(AuditEvent).where(AuditEvent.id.in_(audit_ids)).returning(AuditEvent.id)
        )
        outbox_ids = (
            select(OutboxEvent.id)
            .where(
                OutboxEvent.processed_at.is_not(None),
                OutboxEvent.processed_at < task_cutoff,
            )
            .order_by(OutboxEvent.processed_at, OutboxEvent.id)
            .limit(limit)
        )
        outbox_deleted = await session.execute(
            delete(OutboxEvent).where(OutboxEvent.id.in_(outbox_ids)).returning(OutboxEvent.id)
        )
        return DatabaseCleanupResult(
            expired_api_keys=len(expired_keys.scalars().all()),
            task_logs_deleted=logs_deleted,
            task_runtime_handles_cleared=len(task_handles_cleared),
            execution_runtime_handles_cleared=len(execution_handles_cleared),
            tasks_deleted=tasks_deleted,
            audit_events_deleted=len(audit_deleted.scalars().all()),
            outbox_events_deleted=len(outbox_deleted.scalars().all()),
            log_task_ids=tuple(set(stream_task_ids) | set(retained_task_ids)),
        )

    @staticmethod
    async def artifact_candidates(
        session: AsyncSession,
        *,
        retention_days: int,
        backend: str,
        limit: int,
    ) -> list[tuple[uuid.UUID, uuid.UUID]]:
        now = await database_utcnow(session)
        cutoff = now - timedelta(days=retention_days)
        result = await session.execute(
            select(Artifact.project_id, Artifact.id)
            .where(
                func.coalesce(Artifact.verified_at, Artifact.created_at) < cutoff,
                Artifact.deleted_at.is_(None),
                Artifact.backend == backend,
                Artifact.state.in_({"pending", "ready", "failed", "deleting"}),
                ~exists(
                    select(TaskArtifact.task_id).where(TaskArtifact.artifact_id == Artifact.id)
                ),
                ~exists(
                    select(DatasetVersion.dataset_id).where(
                        DatasetVersion.artifact_id == Artifact.id
                    )
                ),
            )
            .order_by(Artifact.created_at, Artifact.id)
            .limit(limit)
        )
        return list(result.tuples().all())
