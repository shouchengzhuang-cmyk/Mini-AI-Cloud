import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select

from core.artifacts import ArtifactObjectInfo, LocalArtifactStore
from core.config import Settings
from core.database import Database
from core.enums import LogStream, TaskStatus, WorkerStatus
from models.artifact import Artifact
from models.scheduling import ResourceReservation
from models.task import TaskLog
from models.usage import ProjectQuotaState
from repositories.secrets import SecretResolutionError, TaskSecretBindingRepository
from repositories.task_artifacts import (
    OutputArtifactSpec,
    TaskArtifactConflictError,
    TaskArtifactRepository,
)
from repositories.tasks import ClaimRejected, StaleExecutionError, TaskRepository
from repositories.workers import WorkerRepository
from scheduler import Scheduler
from worker.artifact_workspace import ArtifactWorkspaceManager

pytestmark = pytest.mark.integration


async def _register(
    database: Database,
    *,
    worker_id: str,
    worker_session_id: uuid.UUID,
) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            hostname=f"{worker_id}.test",
            concurrency=4,
            cpu_count=4,
            memory_total_mb=4096,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def _create_task(database: Database) -> uuid.UUID:
    async with database.session() as session, session.begin():
        task = await TaskRepository.create_queued(
            session,
            image="python:3.12-slim",
            command=["python", "-c", "print('ok')"],
            environment={},
            timeout_seconds=30,
            max_retries=0,
            cpu_limit=1.0,
            memory_limit_mb=128,
            labels={"runtime": "docker"},
            network_enabled=False,
            gpu_count=0,
            idempotency_key=None,
            request_hash=None,
        )
        return task.id


async def _claim(
    database: Database,
    *,
    task_id: uuid.UUID,
    worker_id: str,
    worker_session_id: uuid.UUID,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        _task, execution_id = await TaskRepository.claim(
            session,
            task_id=task_id,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            lease_seconds=30,
        )
        return execution_id


async def test_reregistration_fences_every_old_worker_write(database: Database) -> None:
    worker_id = "worker-fixed-session"
    session_a = uuid.uuid4()
    session_b = uuid.uuid4()
    await _register(database, worker_id=worker_id, worker_session_id=session_a)
    running_task_id = await _create_task(database)
    assigned_task_id = await _create_task(database)
    queued_task_id = await _create_task(database)

    running_execution_id = await _claim(
        database,
        task_id=running_task_id,
        worker_id=worker_id,
        worker_session_id=session_a,
    )
    assigned_execution_id = await _claim(
        database,
        task_id=assigned_task_id,
        worker_id=worker_id,
        worker_session_id=session_a,
    )
    async with database.session() as session, session.begin():
        await TaskRepository.mark_pulling(
            session,
            task_id=running_task_id,
            worker_id=worker_id,
            execution_id=running_execution_id,
            worker_session_id=session_a,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=running_task_id,
            worker_id=worker_id,
            execution_id=running_execution_id,
            worker_session_id=session_a,
            lease_seconds=30,
        )

    await _register(database, worker_id=worker_id, worker_session_id=session_b)

    async with database.session() as session, session.begin():
        assert (
            await WorkerRepository.heartbeat(
                session,
                worker_id,
                running_tasks=2,
                worker_session_id=session_a,
            )
            is None
        )
        assert (
            await WorkerRepository.set_status(
                session,
                worker_id,
                WorkerStatus.OFFLINE,
                worker_session_id=session_a,
            )
            is None
        )

    async with database.session() as session, session.begin():
        assert not await TaskRepository.renew_lease(
            session,
            task_id=running_task_id,
            worker_id=worker_id,
            execution_id=running_execution_id,
            worker_session_id=session_a,
            lease_seconds=30,
        )
        assert not await TaskRepository.renew_lease(
            session,
            task_id=running_task_id,
            worker_id=worker_id,
            execution_id=running_execution_id,
            worker_session_id=session_b,
            lease_seconds=30,
        )

    async with database.session() as session, session.begin():
        with pytest.raises(StaleExecutionError, match="worker session is stale"):
            await TaskRepository.mark_pulling(
                session,
                task_id=assigned_task_id,
                worker_id=worker_id,
                execution_id=assigned_execution_id,
                worker_session_id=session_a,
                lease_seconds=30,
            )
        with pytest.raises(StaleExecutionError, match="worker session is stale"):
            await TaskRepository.cancellation_requested(
                session,
                task_id=running_task_id,
                worker_id=worker_id,
                execution_id=running_execution_id,
                worker_session_id=session_a,
            )
        with pytest.raises(StaleExecutionError, match="worker session is stale"):
            await TaskRepository.append_log(
                session,
                task_id=running_task_id,
                execution_id=running_execution_id,
                worker_id=worker_id,
                worker_session_id=session_a,
                stream=LogStream.SYSTEM,
                content="stale process log",
            )

    async with database.session() as session, session.begin():
        with pytest.raises(SecretResolutionError, match="ownership is no longer valid"):
            await TaskSecretBindingRepository.resolve_for_execution(
                session,
                task_id=running_task_id,
                project_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
                worker_id=worker_id,
                execution_id=running_execution_id,
                worker_session_id=session_a,
                cipher=None,
            )

    for worker_session_id in (session_a, session_b):
        async with database.session() as session, session.begin():
            result = await TaskRepository.finish_execution(
                session,
                task_id=running_task_id,
                worker_id=worker_id,
                execution_id=running_execution_id,
                worker_session_id=worker_session_id,
                target=TaskStatus.SUCCEEDED,
                exit_code=0,
                error_message=None,
                retry_max_backoff_seconds=60,
                cpu_price_per_hour=0.05,
                gpu_price_per_hour=1.0,
            )
        assert result.accepted is False
        assert result.status is None

    async with database.session() as session, session.begin():
        assert (
            await TaskRepository.take_global_assignment(
                session,
                worker_id=worker_id,
                worker_session_id=session_a,
                lease_seconds=30,
            )
            is None
        )
        with pytest.raises(ClaimRejected, match="worker session is stale"):
            await TaskRepository.claim(
                session,
                task_id=queued_task_id,
                worker_id=worker_id,
                worker_session_id=session_a,
                lease_seconds=30,
            )

    stale_scheduler = Scheduler(
        database.session,
        lease_seconds=30,
        mode="pull",
        worker_session_id=session_a,
    )
    assert await stale_scheduler.claim_for_worker(worker_id=worker_id) is None

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
        running_task = await TaskRepository.get(session, running_task_id)
        assigned_task = await TaskRepository.get(session, assigned_task_id)
        queued_task = await TaskRepository.get(session, queued_task_id)
        active_reservations = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.released_at.is_(None)
                )
            )
            or 0
        )
        log_count = int(await session.scalar(select(func.count(TaskLog.id))) or 0)

    assert worker is not None
    assert worker.worker_session_id == session_b
    assert worker.status == WorkerStatus.ONLINE
    assert worker.running_tasks == 2
    assert worker.reserved_cpu == 2.0
    assert running_task is not None and running_task.status == TaskStatus.RUNNING
    assert assigned_task is not None and assigned_task.status == TaskStatus.ASSIGNED
    assert queued_task is not None and queued_task.status == TaskStatus.QUEUED
    assert active_reservations == 2
    assert log_count == 0

    current_scheduler = Scheduler(
        database.session,
        lease_seconds=30,
        mode="pull",
        worker_session_id=session_b,
    )
    assignment = await current_scheduler.claim_for_worker(worker_id=worker_id)
    assert assignment is not None
    assert assignment.task_id == queued_task_id

    async with database.session() as session:
        worker = await WorkerRepository.get(session, worker_id)
        active_reservations = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.released_at.is_(None)
                )
            )
            or 0
        )
    assert worker is not None
    assert worker.running_tasks == 3
    assert worker.reserved_cpu == 3.0
    assert active_reservations == 3


async def test_reregistration_fences_artifact_materialize_and_publish(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id = "worker-artifact-session"
    session_a = uuid.uuid4()
    session_b = uuid.uuid4()
    await _register(database, worker_id=worker_id, worker_session_id=session_a)
    task_id = await _create_task(database)
    async with database.session() as session, session.begin():
        task = await TaskRepository.get(session, task_id, for_update=True)
        assert task is not None
        await TaskArtifactRepository.create_bindings(
            session,
            task=task,
            project_id=task.project_id,
            input_artifact_ids=[],
            outputs=[OutputArtifactSpec(name="model", path="/output/model.bin")],
        )
    execution_id = await _claim(
        database,
        task_id=task_id,
        worker_id=worker_id,
        worker_session_id=session_a,
    )
    async with database.session() as session, session.begin():
        await TaskRepository.mark_pulling(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=session_a,
            lease_seconds=30,
        )
        await TaskRepository.mark_running(
            session,
            task_id=task_id,
            worker_id=worker_id,
            execution_id=execution_id,
            worker_session_id=session_a,
            lease_seconds=30,
        )

    store = LocalArtifactStore(tmp_path / "objects", max_bytes=4096)
    manager = ArtifactWorkspaceManager(
        database,
        Settings(
            _env_file=None,
            artifact_local_root=str(tmp_path / "objects"),
            artifact_workspace_root=str(tmp_path / "workspaces"),
            artifact_max_bytes=4096,
        ),
        store,
    )
    workspace = await manager.prepare(
        task_id=task_id,
        project_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        worker_id=worker_id,
        execution_id=execution_id,
        worker_session_id=session_a,
    )
    output_mount = next(mount for mount in workspace.mounts if not mount.read_only)
    output_mount.host_path.write_bytes(b"model output")

    original_finalize = store.finalize

    async def finalize_and_reregister(
        staging_key: str,
        final_key: str,
        *,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo:
        info = await original_finalize(
            staging_key,
            final_key,
            expected_size_bytes=expected_size_bytes,
            expected_sha256=expected_sha256,
        )
        await _register(database, worker_id=worker_id, worker_session_id=session_b)
        return info

    monkeypatch.setattr(store, "finalize", finalize_and_reregister)

    with pytest.raises(TaskArtifactConflictError, match="ownership is stale"):
        await manager.publish_outputs(workspace)

    for worker_session_id in (session_a, session_b):
        with pytest.raises(TaskArtifactConflictError, match="ownership is stale"):
            await manager.prepare(
                task_id=task_id,
                project_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
                worker_id=worker_id,
                execution_id=execution_id,
                worker_session_id=worker_session_id,
            )
    async with database.session() as session:
        bindings = await TaskArtifactRepository.list_for_task(
            session,
            task_id=task_id,
            project_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        )
        quota = await session.get(
            ProjectQuotaState,
            uuid.UUID("00000000-0000-0000-0000-000000000001"),
        )
        artifacts = list(await session.scalars(select(Artifact)))
    output = next(binding for binding in bindings if binding.direction == "output")
    assert output.artifact_id is None
    assert quota is not None and quota.artifact_bytes == 0
    assert len(artifacts) == 1
    assert artifacts[0].state == "failed"
    assert [path for path in (tmp_path / "objects").rglob("*") if path.is_file()] == []
    await manager.cleanup(workspace)
