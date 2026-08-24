import hashlib
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from api.services.cleanup import CleanupController
from core.artifacts import ArtifactObjectNotFoundError, ArtifactState, LocalArtifactStore
from core.config import Settings
from core.database import Database
from core.enums import LogStream, TaskStatus
from core.rbac import MembershipStatus, ProjectRole
from core.redis import RedisQueue
from models.artifact import Artifact
from models.identity import ApiKey, ProjectMembership, User
from models.outbox import OutboxEvent
from models.task import Task, TaskLog
from models.usage import AuditEvent, ProjectQuota, ProjectQuotaState

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def _content(body: bytes) -> AsyncIterator[bytes]:
    yield body


async def test_cleanup_controller_bounds_retained_state_without_deleting_references(
    database: Database,
    redis_queue: RedisQueue,
    tmp_path: Path,
) -> None:
    root = tmp_path / "cleanup-artifacts"
    store = LocalArtifactStore(root, max_bytes=1024)
    settings = Settings(
        _env_file=None,
        artifact_local_root=str(root),
        task_retention_days=1,
        log_retention_days=1,
        audit_retention_days=1,
        artifact_retention_days=1,
        batch_size=100,
    )
    old = datetime.now(UTC) - timedelta(days=5)
    user_id = uuid.uuid4()
    api_key_id = uuid.uuid4()
    task_id = uuid.uuid4()
    artifact_id = uuid.uuid4()
    audit_event_id = uuid.uuid4()
    outbox_event_id = uuid.uuid4()
    object_key = f"projects/{PROJECT_ID.hex}/artifacts/{artifact_id.hex}/content"
    body = b"unreferenced-ready-artifact"
    checksum = hashlib.sha256(body).hexdigest()
    await store.put(
        object_key,
        _content(body),
        content_type="application/octet-stream",
        expected_size_bytes=len(body),
        expected_sha256=checksum,
    )

    async with database.session() as session, session.begin():
        session.add(
            User(
                id=user_id,
                username="cleanup-user",
                username_normalized="cleanup-user",
                email="cleanup@example.test",
                email_normalized="cleanup@example.test",
                password_hash="not-a-real-password-hash",
            )
        )
        await session.flush()
        session.add(
            ProjectMembership(
                project_id=PROJECT_ID,
                user_id=user_id,
                role=ProjectRole.OWNER,
                status=MembershipStatus.ACTIVE,
            )
        )
        await session.flush()
        session.add(
            ApiKey(
                id=api_key_id,
                project_id=PROJECT_ID,
                user_id=user_id,
                name="expired",
                key_prefix="mdc_cleanup",
                secret_hash=b"x" * 32,
                hash_key_id="v1",
                expires_at=old,
            )
        )
        session.add(ProjectQuota(project_id=PROJECT_ID, max_artifact_bytes=1024))
        session.add(ProjectQuotaState(project_id=PROJECT_ID, artifact_bytes=len(body)))
        session.add(
            Task(
                id=task_id,
                project_id=PROJECT_ID,
                image="example/old@sha256:" + "a" * 64,
                command=["true"],
                status=TaskStatus.SUCCEEDED,
                created_at=old,
                finished_at=old,
                runtime_handle={"runtime_type": "docker", "object_id": "old-container"},
            )
        )
        await session.flush()
        session.add(
            TaskLog(
                task_id=task_id,
                timestamp=old,
                stream=LogStream.STDOUT,
                sequence=1,
                content="old log",
            )
        )
        session.add(
            AuditEvent(
                id=audit_event_id,
                project_id=PROJECT_ID,
                actor_type="system",
                action="old.action",
                resource_type="task",
                resource_id=str(task_id),
                outcome="success",
                occurred_at=old,
            )
        )
        session.add(
            OutboxEvent(
                id=outbox_event_id,
                aggregate_id=task_id,
                event_type="task.old",
                payload={},
                created_at=old,
                available_at=old,
                processed_at=old,
            )
        )
        session.add(
            Artifact(
                id=artifact_id,
                project_id=PROJECT_ID,
                name="expired-output.bin",
                state=ArtifactState.READY.value,
                backend="local",
                object_key=object_key,
                size_bytes=len(body),
                sha256=checksum,
                created_at=old,
                verified_at=old,
            )
        )

    await redis_queue.publish_log(task_id=task_id, sequence=1)
    result = await CleanupController(
        database,
        redis_queue,
        settings,
        store=store,
    ).run_once()

    assert result.expired_api_keys == 1
    assert result.tasks_deleted == 1
    assert result.task_logs_deleted == 1
    assert result.audit_events_deleted == 1
    assert result.outbox_events_deleted == 1
    assert result.redis_streams_deleted == 1
    assert result.artifacts_deleted == 1

    async with database.session() as session:
        key = await session.get(ApiKey, api_key_id)
        artifact = await session.get(Artifact, artifact_id)
        quota = await session.get(ProjectQuotaState, PROJECT_ID)
        assert key is not None and key.revoked_at is not None
        assert await session.get(Task, task_id) is None
        assert await session.get(AuditEvent, audit_event_id) is None
        assert await session.get(OutboxEvent, outbox_event_id) is None
        assert artifact is not None and artifact.state == ArtifactState.DELETED.value
        assert quota is not None and quota.artifact_bytes == 0
    assert await redis_queue.client.exists(redis_queue.log_stream_key(task_id)) == 0
    with pytest.raises(ArtifactObjectNotFoundError):
        await store.inspect(object_key)
