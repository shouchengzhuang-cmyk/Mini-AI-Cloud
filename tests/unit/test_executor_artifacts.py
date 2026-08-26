import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast

import pytest

from core.config import Settings
from core.database import Database
from core.enums import RuntimeType, TaskStatus
from core.redis import RedisQueue
from models.artifact import Artifact
from models.task import Task
from repositories.secrets import ResolvedTaskSecrets
from repositories.tasks import ExecutionResult
from worker.artifact_workspace import (
    ArtifactOutputNotProducedError,
    ArtifactWorkspaceError,
    ArtifactWorkspaceManager,
    PreparedArtifactMount,
    PreparedArtifactWorkspace,
    _create_output_placeholder,
    _inspect_output_file,
)
from worker.executor import TaskExecutor, WaitOutcome
from worker.heartbeat import ActiveExecution
from worker.runtime import ComputeRuntime, ExecutionSpec, RuntimeHandle, RuntimeLog


class _Runtime:
    runtime_type = "docker"

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.spec: ExecutionSpec | None = None
        self.handle = RuntimeHandle(
            runtime_type=self.runtime_type,
            resource_kind="container",
            object_id="container-id",
            display_id="container",
        )

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        self.events.append("runtime.prepare")
        self.spec = spec
        return self.handle

    async def start(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("runtime.start")

    async def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        assert handle is self.handle
        if ready is not None:
            ready.set()
        if False:  # pragma: no cover - keeps this an async generator
            yield RuntimeLog("stdout", b"")

    async def wait(self, handle: RuntimeHandle) -> int:
        assert handle is self.handle
        return 0

    async def stop(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("runtime.stop")

    async def cleanup(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("runtime.cleanup")


class _ArtifactWorkspace:
    def __init__(
        self,
        prepared: PreparedArtifactWorkspace,
        events: list[str],
        *,
        publish_error: Exception | None = None,
    ) -> None:
        self.prepared = prepared
        self.events = events
        self.publish_error = publish_error

    async def prepare(
        self,
        *,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID | None = None,
    ) -> PreparedArtifactWorkspace:
        assert (task_id, project_id, worker_id, execution_id) == (
            self.prepared.task_id,
            self.prepared.project_id,
            self.prepared.worker_id,
            self.prepared.execution_id,
        )
        assert worker_session_id == self.prepared.worker_session_id
        self.events.append("artifacts.prepare")
        return self.prepared

    async def publish_outputs(self, workspace: PreparedArtifactWorkspace) -> list[Artifact]:
        assert workspace is self.prepared
        self.events.append("artifacts.publish")
        if self.publish_error is not None:
            raise self.publish_error
        return []

    async def cleanup(self, workspace: PreparedArtifactWorkspace) -> None:
        assert workspace is self.prepared
        self.events.append("artifacts.cleanup")


def test_output_placeholder_distinguishes_intentional_empty_file(tmp_path: Path) -> None:
    output = tmp_path / "result.bin"

    placeholder_size, placeholder_sha256 = _create_output_placeholder(output)

    assert placeholder_size > 0
    assert _inspect_output_file(output) == (placeholder_size, placeholder_sha256)
    output.write_bytes(b"")
    assert _inspect_output_file(output) == (0, hashlib.sha256(b"").hexdigest())
    assert _inspect_output_file(output) != (placeholder_size, placeholder_sha256)


def test_output_inspection_rejects_oversized_file_before_hashing(tmp_path: Path) -> None:
    output = tmp_path / "oversized.bin"
    output.write_bytes(b"12345")

    with pytest.raises(ArtifactWorkspaceError, match="exceeds size limit"):
        _inspect_output_file(output, maximum_bytes=4)


@pytest.mark.parametrize("missing_required_output", [False, True])
@pytest.mark.asyncio
async def test_executor_mounts_then_publishes_before_terminal_transition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing_required_output: bool,
) -> None:
    events: list[str] = []
    task_id = uuid.uuid4()
    project_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    worker_session_id = uuid.uuid4()
    worker_id = "artifact-worker"
    output = tmp_path / "output.bin"
    output.touch()
    prepared = PreparedArtifactWorkspace(
        task_id=task_id,
        project_id=project_id,
        worker_id=worker_id,
        execution_id=execution_id,
        root=tmp_path,
        mounts=(
            PreparedArtifactMount(
                binding_id=uuid.uuid4(),
                host_path=output,
                container_path="/output/model.bin",
                read_only=False,
            ),
        ),
        outputs=(),
        worker_session_id=worker_session_id,
    )
    runtime = _Runtime(events)
    missing_error = "declared output artifact 'model' was not produced"
    workspace = _ArtifactWorkspace(
        prepared,
        events,
        publish_error=(
            ArtifactOutputNotProducedError(missing_error) if missing_required_output else None
        ),
    )
    executor = TaskExecutor(
        cast(Database, object()),
        cast(RedisQueue, object()),
        cast(ComputeRuntime, runtime),
        worker_id=worker_id,
        settings=Settings(_env_file=None, control_plane_enabled=False),
        artifact_workspace=cast(ArtifactWorkspaceManager, workspace),
        worker_session_id=worker_session_id,
    )
    task = Task(
        id=task_id,
        project_id=project_id,
        image="example/task@sha256:" + "a" * 64,
        command=["true"],
        environment={},
        timeout_seconds=60,
        cpu_limit=1,
        memory_limit_mb=128,
        gpu_count=0,
        gpu_device_ids=[],
        network_enabled=False,
        labels={},
        runtime_type=RuntimeType.DOCKER,
        status=TaskStatus.PULLING,
        worker_id=worker_id,
        execution_id=execution_id,
    )
    active = ActiveExecution(task_id=task_id, execution_id=execution_id)

    async def mark_pulling(execution: ActiveExecution) -> Task:
        assert execution is active
        return task

    async def resolve_secrets(
        resolved_task: Task, execution: ActiveExecution
    ) -> ResolvedTaskSecrets:
        assert resolved_task is task
        assert execution is active
        return ResolvedTaskSecrets({})

    async def no_system_log(execution: ActiveExecution, content: str) -> None:
        del execution, content

    async def no_stop_reason(execution: ActiveExecution) -> str | None:
        assert execution is active
        return None

    async def mark_state(execution: ActiveExecution) -> None:
        assert execution is active

    async def wait_for_outcome(
        handle: RuntimeHandle,
        execution: ActiveExecution,
        deadline: float,
    ) -> WaitOutcome:
        assert handle is runtime.handle
        assert execution is active
        assert deadline > 0
        events.append("runtime.wait")
        return WaitOutcome(reason="exited", exit_code=0)

    async def finish(
        execution: ActiveExecution,
        *,
        target: TaskStatus,
        exit_code: int | None,
        error_message: str | None,
        **_: object,
    ) -> ExecutionResult:
        assert execution is active
        expected = (
            (TaskStatus.FAILED, None, missing_error)
            if missing_required_output
            else (TaskStatus.SUCCEEDED, 0, None)
        )
        assert (target, exit_code, error_message) == expected
        events.append("task.finish")
        return ExecutionResult(accepted=True, status=target)

    monkeypatch.setattr(executor, "_mark_pulling", mark_pulling)
    monkeypatch.setattr(executor, "_resolve_secrets", resolve_secrets)
    monkeypatch.setattr(executor, "_system_log", no_system_log)
    monkeypatch.setattr(executor, "_pre_start_stop_reason", no_stop_reason)
    monkeypatch.setattr(executor, "_mark_starting", mark_state)
    monkeypatch.setattr(executor, "_mark_running", mark_state)
    monkeypatch.setattr(executor, "_wait_for_outcome", wait_for_outcome)
    monkeypatch.setattr(executor, "_finish", finish)

    result = await executor.execute(active)

    assert result.status == (TaskStatus.FAILED if missing_required_output else TaskStatus.SUCCEEDED)
    assert runtime.spec is not None
    assert [
        (item.host_path, item.container_path, item.read_only) for item in runtime.spec.mounts
    ] == [(str(output), "/output/model.bin", False)]
    expected_events = [
        "artifacts.prepare",
        "runtime.prepare",
        "runtime.start",
        "runtime.wait",
        "artifacts.publish",
    ]
    if missing_required_output:
        expected_events.append("runtime.stop")
    expected_events.extend(["task.finish", "runtime.cleanup", "artifacts.cleanup"])
    assert events == expected_events
