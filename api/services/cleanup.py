import uuid
from dataclasses import asdict, dataclass

from redis.exceptions import RedisError

from core.artifacts import ArtifactStore, ArtifactStoreError, build_artifact_store
from core.config import Settings
from core.database import Database
from core.logging import get_logger
from core.redis import RedisQueue
from repositories.artifacts import (
    ArtifactRecordNotFoundError,
    ArtifactReferencedError,
    ArtifactRepository,
)
from repositories.cleanup import CleanupRepository


@dataclass(frozen=True, slots=True)
class CleanupResult:
    expired_api_keys: int = 0
    task_logs_deleted: int = 0
    tasks_deleted: int = 0
    audit_events_deleted: int = 0
    outbox_events_deleted: int = 0
    redis_streams_deleted: int = 0
    artifacts_deleted: int = 0
    artifact_failures: int = 0


class CleanupController:
    """Bound retained data and retry object deletion without breaking references."""

    def __init__(
        self,
        database: Database,
        queue: RedisQueue,
        settings: Settings,
        *,
        store: ArtifactStore | None = None,
    ) -> None:
        self.database = database
        self.queue = queue
        self.settings = settings
        self._store = store
        self.logger = get_logger("cleanup")

    async def run_once(self) -> CleanupResult:
        async with self.database.session() as session, session.begin():
            database_result = await CleanupRepository.run_database_cleanup(
                session,
                task_retention_days=self.settings.task_retention_days,
                log_retention_days=self.settings.log_retention_days,
                audit_retention_days=self.settings.audit_retention_days,
                limit=self.settings.batch_size,
            )
            artifact_candidates = await CleanupRepository.artifact_candidates(
                session,
                retention_days=self.settings.artifact_retention_days,
                backend=self.store.backend,
                limit=self.settings.batch_size,
            )

        redis_deleted = 0
        for task_id in database_result.log_task_ids:
            try:
                await self.queue.delete_log_stream(task_id)
                redis_deleted += 1
            except RedisError as exc:
                self.logger.warning(
                    "failed to delete retained Redis log stream",
                    task_id=str(task_id),
                    error_type=type(exc).__name__,
                )

        artifacts_deleted = 0
        artifact_failures = 0
        for project_id, artifact_id in artifact_candidates:
            try:
                deleted = await self._delete_retained_artifact(project_id, artifact_id)
                artifacts_deleted += int(deleted)
            except (ArtifactStoreError, OSError) as exc:
                artifact_failures += 1
                self.logger.warning(
                    "retained artifact cleanup failed",
                    artifact_id=str(artifact_id),
                    error_type=type(exc).__name__,
                )

        result = CleanupResult(
            expired_api_keys=database_result.expired_api_keys,
            task_logs_deleted=database_result.task_logs_deleted,
            tasks_deleted=database_result.tasks_deleted,
            audit_events_deleted=database_result.audit_events_deleted,
            outbox_events_deleted=database_result.outbox_events_deleted,
            redis_streams_deleted=redis_deleted,
            artifacts_deleted=artifacts_deleted,
            artifact_failures=artifact_failures,
        )
        fields = asdict(result)
        if any(fields.values()):
            self.logger.info("retention cleanup completed", **fields)
        return result

    async def _delete_retained_artifact(
        self, project_id: uuid.UUID, artifact_id: uuid.UUID
    ) -> bool:
        async with self.database.session() as session, session.begin():
            try:
                artifact = await ArtifactRepository.begin_delete(
                    session,
                    project_id=project_id,
                    artifact_id=artifact_id,
                )
            except (ArtifactRecordNotFoundError, ArtifactReferencedError):
                return False
            # Candidates are backend-scoped before this state transition. Keep
            # this guard as defense in depth against an unexpected store swap.
            if artifact.backend != self.store.backend:
                raise ArtifactStoreError("artifact backend changed during retention cleanup")
            keys = {
                artifact.object_key,
                f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/staging",
                f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/content",
            }

        for key in keys:
            await self.store.delete(key)
        async with self.database.session() as session, session.begin():
            await ArtifactRepository.mark_deleted(
                session,
                project_id=project_id,
                artifact_id=artifact_id,
            )
        return True

    @property
    def store(self) -> ArtifactStore:
        if self._store is None:
            self._store = build_artifact_store(self.settings)
        return self._store
