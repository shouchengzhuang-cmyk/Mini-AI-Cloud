import asyncio
import hashlib
import stat
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import select

from core.artifacts import ArtifactState, LocalArtifactStore
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus
from models.artifact import Artifact
from models.task import Task
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.task_artifacts import (
    OutputArtifactSpec,
    TaskArtifactRepository,
    validate_output_path,
)
from repositories.workers import WorkerRepository
from worker.artifact_workspace import (
    ArtifactOutputNotProducedError,
    ArtifactWorkspaceManager,
)

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def _chunks(content: bytes) -> AsyncIterator[bytes]:
    yield content


async def test_worker_materializes_inputs_and_publishes_fenced_output(
    database: Database,
    tmp_path: Path,
) -> None:
    store = LocalArtifactStore(tmp_path / "objects", max_bytes=4096)
    settings = Settings(
        _env_file=None,
        artifact_local_root=str(tmp_path / "objects"),
        artifact_workspace_root=str(tmp_path / "workspaces"),
        docker_artifact_workspace_volume="test-task-workspaces",
        artifact_max_bytes=4096,
    )
    input_content = b"training input"
    input_id = uuid.uuid4()
    input_key = f"projects/{PROJECT_ID.hex}/artifacts/{input_id.hex}/content"
    input_sha = hashlib.sha256(input_content).hexdigest()
    await store.put(
        input_key,
        _chunks(input_content),
        content_type="application/octet-stream",
        expected_size_bytes=len(input_content),
        expected_sha256=input_sha,
    )
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    worker_id = "artifact-worker"

    async with database.session() as session, session.begin():
        session.add(ProjectQuota(project_id=PROJECT_ID, max_artifact_bytes=4096))
        session.add(ProjectQuotaState(project_id=PROJECT_ID, artifact_bytes=len(input_content)))
        session.add(
            Artifact(
                id=input_id,
                project_id=PROJECT_ID,
                name="input data.bin",
                state=ArtifactState.READY.value,
                backend="local",
                object_key=input_key,
                content_type="application/octet-stream",
                size_bytes=len(input_content),
                sha256=input_sha,
            )
        )
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname="artifact-worker.test",
            concurrency=1,
            cpu_count=2,
            memory_total_mb=2048,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )
        await session.flush()
        task = Task(
            id=task_id,
            project_id=PROJECT_ID,
            image="example/task@sha256:" + "a" * 64,
            command=["true"],
            status=TaskStatus.RUNNING,
            worker_id=worker_id,
            execution_id=execution_id,
        )
        session.add(task)
        await session.flush()
        await TaskArtifactRepository.create_bindings(
            session,
            task=task,
            project_id=PROJECT_ID,
            input_artifact_ids=[input_id],
            outputs=[
                OutputArtifactSpec(name="model", path="/output/model.bin"),
                OutputArtifactSpec(name="empty", path="/output/empty.bin"),
                OutputArtifactSpec(
                    name="optional-metrics",
                    path="/output/metrics.json",
                    required=False,
                ),
            ],
        )

    manager = ArtifactWorkspaceManager(database, settings, store)
    workspace = await manager.prepare(
        task_id=task_id,
        project_id=PROJECT_ID,
        worker_id=worker_id,
        execution_id=execution_id,
    )
    assert workspace.root is not None
    assert len(workspace.mounts) == 4
    input_mount = next(item for item in workspace.mounts if item.read_only)
    output_mounts = {item.container_path: item for item in workspace.mounts if not item.read_only}
    model_mount = output_mounts["/output/model.bin"]
    empty_mount = output_mounts["/output/empty.bin"]
    optional_mount = output_mounts["/output/metrics.json"]
    assert await asyncio.to_thread(input_mount.host_path.read_bytes) == input_content
    assert stat.S_IMODE(input_mount.host_path.stat().st_mode) == 0o444
    assert input_mount.container_path.startswith("/workspace/inputs/")
    assert stat.S_IMODE(model_mount.host_path.stat().st_mode) == 0o666
    assert input_mount.volume_name == "test-task-workspaces"
    assert input_mount.volume_subpath is not None
    assert input_mount.volume_subpath.endswith(f"inputs/{input_mount.binding_id.hex}")
    for mount in output_mounts.values():
        assert mount.output_placeholder_size_bytes is not None
        assert mount.output_placeholder_sha256 is not None
        assert mount.host_path.stat().st_size == mount.output_placeholder_size_bytes
    output_content = b"trained model"
    await asyncio.to_thread(model_mount.host_path.write_bytes, output_content)
    # An empty file is a valid workload output. It differs from the non-empty
    # private placeholder even if the filesystem timestamp resolution is low.
    await asyncio.to_thread(empty_mount.host_path.write_bytes, b"")

    published = await manager.publish_outputs(workspace)

    assert {artifact.name for artifact in published} == {"model", "empty"}
    published_by_name = {artifact.name: artifact for artifact in published}
    assert published_by_name["model"].sha256 == hashlib.sha256(output_content).hexdigest()
    assert published_by_name["empty"].size_bytes == 0
    assert published_by_name["empty"].sha256 == hashlib.sha256(b"").hexdigest()
    async with database.session() as session:
        bindings = await TaskArtifactRepository.list_for_task(
            session,
            task_id=task_id,
            project_id=PROJECT_ID,
        )
        outputs = {item.name: item for item in bindings if item.direction == "output"}
        quota = await session.get(ProjectQuotaState, PROJECT_ID)
        assert outputs["model"].artifact_id == published_by_name["model"].id
        assert outputs["empty"].artifact_id == published_by_name["empty"].id
        assert outputs["optional-metrics"].artifact_id is None
        assert quota is not None
        assert quota.artifact_bytes == len(input_content) + len(output_content)
        assert len(list(await session.scalars(select(Artifact)))) == 3
    stored = b"".join([chunk async for chunk in store.read(published_by_name["model"].object_key)])
    assert stored == output_content
    stored_empty = b"".join(
        [chunk async for chunk in store.read(published_by_name["empty"].object_key)]
    )
    assert stored_empty == b""
    assert optional_mount.host_path.stat().st_size > 0

    await manager.cleanup(workspace)
    assert not await asyncio.to_thread(workspace.root.exists)


async def test_required_output_left_as_placeholder_is_rejected(
    database: Database,
    tmp_path: Path,
) -> None:
    store = LocalArtifactStore(tmp_path / "objects", max_bytes=4096)
    settings = Settings(
        _env_file=None,
        artifact_local_root=str(tmp_path / "objects"),
        artifact_workspace_root=str(tmp_path / "workspaces"),
        artifact_max_bytes=4096,
    )
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    worker_id = "missing-output-worker"

    async with database.session() as session, session.begin():
        session.add(ProjectQuota(project_id=PROJECT_ID, max_artifact_bytes=4096))
        session.add(ProjectQuotaState(project_id=PROJECT_ID, artifact_bytes=0))
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname="missing-output-worker.test",
            concurrency=1,
            cpu_count=2,
            memory_total_mb=2048,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )
        await session.flush()
        task = Task(
            id=task_id,
            project_id=PROJECT_ID,
            image="example/task@sha256:" + "b" * 64,
            command=["true"],
            status=TaskStatus.RUNNING,
            worker_id=worker_id,
            execution_id=execution_id,
        )
        session.add(task)
        await session.flush()
        await TaskArtifactRepository.create_bindings(
            session,
            task=task,
            project_id=PROJECT_ID,
            input_artifact_ids=[],
            outputs=[OutputArtifactSpec(name="required-model", path="/output/model.bin")],
        )

    manager = ArtifactWorkspaceManager(database, settings, store)
    workspace = await manager.prepare(
        task_id=task_id,
        project_id=PROJECT_ID,
        worker_id=worker_id,
        execution_id=execution_id,
    )
    try:
        with pytest.raises(
            ArtifactOutputNotProducedError,
            match="declared output artifact 'required-model' was not produced",
        ):
            await manager.publish_outputs(workspace)

        async with database.session() as session:
            bindings = await TaskArtifactRepository.list_for_task(
                session,
                task_id=task_id,
                project_id=PROJECT_ID,
            )
            output = next(item for item in bindings if item.direction == "output")
            quota = await session.get(ProjectQuotaState, PROJECT_ID)
            assert output.artifact_id is None
            assert quota is not None
            assert quota.artifact_bytes == 0
            assert list(await session.scalars(select(Artifact))) == []
    finally:
        await manager.cleanup(workspace)


@pytest.mark.parametrize(
    "path",
    [
        "../../etc/passwd",
        "/etc/passwd",
        "/output/../etc/passwd",
        "/output/",
        "/output/model.bin/",
        "C:\\Windows\\System32\\config",
    ],
)
def test_output_artifact_path_rejects_traversal_and_host_targets(path: str) -> None:
    with pytest.raises(ValueError):
        validate_output_path(path)
