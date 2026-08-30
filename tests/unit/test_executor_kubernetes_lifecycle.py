from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from core.config import Settings
from core.database import Database
from core.enums import RuntimeType, TaskStatus, WorkerStatus
from core.redis import RedisQueue
from models.task import Task
from repositories.secrets import ResolvedTaskSecrets
from repositories.tasks import ExecutionResult
from worker.executor import TaskExecutor, WaitOutcome
from worker.heartbeat import ActiveExecution
from worker.main import WorkerService
from worker.runtime import ComputeRuntime, ExecutionSpec, RuntimeHandle, RuntimeLog

pytestmark = pytest.mark.asyncio


class _KubernetesRuntime:
    runtime_type = "kubernetes"

    def __init__(self, worker_session_id: uuid.UUID) -> None:
        self.events: list[str] = []
        self.handle = RuntimeHandle(
            runtime_type="kubernetes",
            resource_kind="job",
            object_id="task-job",
            display_id="task-job",
            namespace="runtime-tests",
            resource_uid="job-uid",
            resource_version="1",
            controller_session_id=worker_session_id,
            spec_hash="a" * 64,
            labels={"mini-ai-cloud/worker-session-id": str(worker_session_id)},
        )

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        del spec
        self.events.append("prepare")
        return self.handle

    async def start(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("start")

    async def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        assert handle is self.handle
        if ready is not None:
            ready.set()
        if False:  # pragma: no cover - define an async generator without output
            yield RuntimeLog("stdout", b"")

    async def wait(self, handle: RuntimeHandle) -> int:
        assert handle is self.handle
        return 0

    async def stop(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("stop")

    async def cleanup(self, handle: RuntimeHandle) -> None:
        assert handle is self.handle
        self.events.append("cleanup")


def _task(
    *,
    task_id: uuid.UUID,
    execution_id: uuid.UUID,
    worker_id: str,
) -> Task:
    return Task(
        id=task_id,
        project_id=uuid.uuid4(),
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
        runtime_type=RuntimeType.KUBERNETES,
        status=TaskStatus.PULLING,
        worker_id=worker_id,
        execution_id=execution_id,
    )


def _executor(
    runtime: _KubernetesRuntime,
    *,
    worker_id: str,
    worker_session_id: uuid.UUID,
) -> TaskExecutor:
    return TaskExecutor(
        cast(Database, object()),
        cast(RedisQueue, object()),
        cast(ComputeRuntime, runtime),
        worker_id=worker_id,
        settings=Settings(_env_file=None, control_plane_enabled=False),
        worker_session_id=worker_session_id,
    )


async def _stub_execution_dependencies(
    executor: TaskExecutor,
    monkeypatch: pytest.MonkeyPatch,
    *,
    active: ActiveExecution,
    task: Task,
) -> None:
    async def mark_pulling(execution: ActiveExecution) -> Task:
        assert execution is active
        return task

    async def resolve_secrets(
        resolved_task: Task,
        execution: ActiveExecution,
    ) -> ResolvedTaskSecrets:
        assert resolved_task is task
        assert execution is active
        return ResolvedTaskSecrets({})

    async def no_system_log(execution: ActiveExecution, content: str) -> None:
        del execution, content

    async def finish(
        execution: ActiveExecution,
        *,
        target: TaskStatus,
        **_: object,
    ) -> ExecutionResult:
        assert execution is active
        return ExecutionResult(accepted=True, status=target)

    monkeypatch.setattr(executor, "_mark_pulling", mark_pulling)
    monkeypatch.setattr(executor, "_resolve_secrets", resolve_secrets)
    monkeypatch.setattr(executor, "_system_log", no_system_log)
    monkeypatch.setattr(executor, "_finish", finish)


async def test_handle_persistence_failure_compensates_suspended_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id = "worker-k8s"
    worker_session_id = uuid.uuid4()
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    runtime = _KubernetesRuntime(worker_session_id)
    executor = _executor(runtime, worker_id=worker_id, worker_session_id=worker_session_id)
    active = ActiveExecution(
        task_id=task_id,
        execution_id=execution_id,
        runtime_type="kubernetes",
    )
    task = _task(task_id=task_id, execution_id=execution_id, worker_id=worker_id)
    await _stub_execution_dependencies(
        executor,
        monkeypatch,
        active=active,
        task=task,
    )

    async def fail_record(execution: ActiveExecution, handle: RuntimeHandle) -> None:
        del execution, handle
        raise RuntimeError("database write failed")

    async def not_owned(handle: RuntimeHandle, execution: ActiveExecution) -> bool:
        del handle, execution
        return False

    monkeypatch.setattr(executor, "_record_runtime_handle", fail_record)
    monkeypatch.setattr(executor, "_runtime_cleanup_owned", not_owned)

    result = await executor.execute(active)

    assert result.status == TaskStatus.FAILED
    assert runtime.events == ["prepare", "cleanup"]
    assert active.runtime_handle_durable.is_set() is False


async def test_relinquish_preserves_durable_running_job_for_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_id = "worker-k8s"
    worker_session_id = uuid.uuid4()
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    runtime = _KubernetesRuntime(worker_session_id)
    executor = _executor(runtime, worker_id=worker_id, worker_session_id=worker_session_id)
    active = ActiveExecution(
        task_id=task_id,
        execution_id=execution_id,
        runtime_type="kubernetes",
    )
    task = _task(task_id=task_id, execution_id=execution_id, worker_id=worker_id)
    await _stub_execution_dependencies(
        executor,
        monkeypatch,
        active=active,
        task=task,
    )

    async def record(execution: ActiveExecution, handle: RuntimeHandle) -> None:
        assert execution is active
        assert handle is runtime.handle

    async def no_stop_reason(execution: ActiveExecution) -> str | None:
        assert execution is active
        return None

    async def mark_state(execution: ActiveExecution) -> None:
        assert execution is active

    preserved: list[tuple[ActiveExecution, Task, RuntimeHandle]] = []

    async def preserve_handoff(
        execution: ActiveExecution,
        task: Task,
        handle: RuntimeHandle,
    ) -> None:
        preserved.append((execution, task, handle))

    async def relinquish(
        handle: RuntimeHandle,
        execution: ActiveExecution,
        deadline: float,
    ) -> WaitOutcome:
        assert handle is runtime.handle
        assert execution is active
        assert deadline > 0
        execution.relinquish_requested.set()
        return WaitOutcome(reason="relinquish", exit_code=None)

    monkeypatch.setattr(executor, "_record_runtime_handle", record)
    monkeypatch.setattr(executor, "_pre_start_stop_reason", no_stop_reason)
    monkeypatch.setattr(executor, "_mark_starting", mark_state)
    monkeypatch.setattr(executor, "_mark_running", mark_state)
    monkeypatch.setattr(executor, "_preserve_kubernetes_handoff_lease", preserve_handoff)
    monkeypatch.setattr(executor, "_wait_for_outcome", relinquish)

    result = await executor.execute(active)

    assert result.accepted is False
    assert runtime.events == ["prepare", "start"]
    assert active.runtime_handle_durable.is_set()
    assert preserved == [(active, task, runtime.handle)]


@pytest.mark.parametrize(
    ("durable", "replacement_expected", "expected_relinquish"),
    [(True, True, True), (True, False, False), (False, True, False)],
)
async def test_shutdown_relinquishes_only_durable_kubernetes_jobs(
    durable: bool,
    replacement_expected: bool,
    expected_relinquish: bool,
) -> None:
    service = cast(Any, object.__new__(WorkerService))
    service.worker_id = "worker-k8s"
    service.logger = Mock()
    service.settings = SimpleNamespace(worker_shutdown_timeout=0, docker_stop_timeout=0)
    service.heartbeat_stop = asyncio.Event()
    execution = ActiveExecution(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        runtime_type="kubernetes",
    )
    if durable:
        execution.runtime_handle_durable.set()
    service.active = {execution.task_id: execution}

    async def finish_when_signalled() -> None:
        relinquish_wait = asyncio.create_task(execution.relinquish_requested.wait())
        ownership_wait = asyncio.create_task(execution.ownership_lost.wait())
        done, pending = await asyncio.wait(
            {relinquish_wait, ownership_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert done
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    assignment = asyncio.create_task(finish_when_signalled())
    service.inflight = {assignment}
    service.runtime = SimpleNamespace(close=AsyncMock())
    service.kubernetes_runtime = SimpleNamespace(
        replacement_worker_expected=AsyncMock(return_value=replacement_expected)
    )
    service.queue = SimpleNamespace(close=AsyncMock())
    service.database = SimpleNamespace(dispose=AsyncMock())
    service._best_effort_worker_status = AsyncMock()
    service._preserve_kubernetes_handoff_leases = AsyncMock(
        return_value={execution.task_id} if expected_relinquish else set()
    )

    async def heartbeat() -> None:
        await service.heartbeat_stop.wait()

    await service._shutdown(asyncio.create_task(heartbeat()))

    assert execution.relinquish_requested.is_set() is expected_relinquish
    assert execution.ownership_lost.is_set() is (not expected_relinquish)
    assert assignment.done()
    if durable:
        service.kubernetes_runtime.replacement_worker_expected.assert_awaited_once_with(
            worker_id="worker-k8s"
        )
    else:
        service.kubernetes_runtime.replacement_worker_expected.assert_not_awaited()
    if durable and replacement_expected:
        service._preserve_kubernetes_handoff_leases.assert_awaited_once_with()
    else:
        service._preserve_kubernetes_handoff_leases.assert_not_awaited()
    service._best_effort_worker_status.assert_any_await(WorkerStatus.DRAINING)
    service._best_effort_worker_status.assert_any_await(WorkerStatus.OFFLINE)


async def test_shutdown_replacement_probe_failure_is_fail_closed() -> None:
    service = cast(Any, object.__new__(WorkerService))
    service.worker_id = "mini-ai-worker-0"
    service.logger = Mock()
    execution = ActiveExecution(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        runtime_type="kubernetes",
    )
    execution.runtime_handle_durable.set()
    service.active = {execution.task_id: execution}
    service.kubernetes_runtime = SimpleNamespace(
        replacement_worker_expected=AsyncMock(side_effect=RuntimeError("API unavailable"))
    )

    assert await service._replacement_worker_expected() is False
    service.logger.error.assert_called_once()
