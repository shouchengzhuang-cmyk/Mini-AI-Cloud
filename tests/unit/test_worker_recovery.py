import uuid
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest

from core.config import Settings
from core.database import Database
from repositories.tasks import RecoverableRuntimeExecution, TaskRepository
from worker.kubernetes_runtime import (
    EXECUTION_ID_LABEL,
    TASK_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    KubernetesRuntime,
)
from worker.main import WorkerService
from worker.runtime import RuntimeHandle


class _Transaction:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        return None


class _Session:
    def begin(self) -> _Transaction:
        return _Transaction()

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _Database:
    def session(self) -> _Session:
        return _Session()


def _handle(execution_id: uuid.UUID, controller_session_id: uuid.UUID) -> RuntimeHandle:
    return RuntimeHandle(
        runtime_type="kubernetes",
        resource_kind="job",
        object_id=f"task-{execution_id.hex[:8]}",
        display_id="job",
        namespace="workloads",
        resource_uid=f"uid-{execution_id.hex}",
        resource_version="1",
        controller_session_id=controller_session_id,
        spec_hash="a" * 64,
        labels=MappingProxyType(
            {
                TASK_ID_LABEL: str(uuid.uuid4()),
                EXECUTION_ID_LABEL: str(execution_id),
                WORKER_SESSION_ID_LABEL: str(controller_session_id),
            }
        ),
    )


@pytest.mark.asyncio
async def test_recovery_cleans_controller_transferred_job_without_database_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_session = uuid.uuid4()
    new_session = uuid.uuid4()
    matched_execution_id = uuid.uuid4()
    unmatched_execution_id = uuid.uuid4()
    matched_handle = _handle(matched_execution_id, old_session)
    unmatched_handle = _handle(unmatched_execution_id, old_session)
    runtime = SimpleNamespace(
        recovery_conflicts=[],
        transfer_controller=AsyncMock(
            side_effect=lambda handle, **_kwargs: replace(handle, controller_session_id=new_session)
        ),
        cleanup=AsyncMock(),
    )
    service = object.__new__(WorkerService)
    service.kubernetes_runtime = cast(KubernetesRuntime, runtime)
    service.recovery_kubernetes_handles = [matched_handle, unmatched_handle]
    service.recovered_kubernetes_executions = []
    service.worker_id = "worker-a"
    service.worker_session_id = new_session
    service.database = cast(Database, _Database())
    service.settings = cast(
        Settings,
        SimpleNamespace(
            task_lease_seconds=30,
            kubernetes_cleanup_grace_seconds=5,
        ),
    )
    service.logger = Mock()
    monkeypatch.setattr(
        TaskRepository,
        "transfer_recoverable_kubernetes_executions",
        AsyncMock(
            return_value=[
                RecoverableRuntimeExecution(
                    task_id=uuid.UUID(matched_handle.labels[TASK_ID_LABEL]),
                    execution_id=matched_execution_id,
                    worker_session_id=old_session,
                    runtime_log_cursor_bytes=0,
                )
            ]
        ),
    )

    await service._transfer_recoverable_kubernetes_executions()

    runtime.cleanup.assert_awaited_once()
    assert runtime.cleanup.await_args.args[0].object_id == unmatched_handle.object_id
    assert [item.execution_id for item, _handle in service.recovered_kubernetes_executions] == [
        matched_execution_id
    ]
