from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import Table, event
from sqlalchemy.dialects import postgresql

from api.services.vllm_replica_runtime import VLLMReplicaRuntimeController
from core.database import Database
from core.enums import RuntimeType, WorkerStatus
from models.base import Base
from models.identity import Project, User
from models.model_variant import LogicalModel, ModelVariant
from models.outbox import OutboxEvent
from models.registry import RegisteredModel
from models.scheduling import GPUDevice as PersistedGPUDevice
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServingRuntime,
)
from models.usage import ProjectQuota, ProjectQuotaState
from models.worker import Worker
from repositories.services import ServiceRepository
from worker.gpu_inventory import GPUDevice
from worker.vllm_runtime import (
    WORKER_SESSION_ID_LABEL,
    VLLMContainerHandle,
    VLLMContainerState,
    VLLMLaunchSpec,
)

PROJECT_ID = uuid.UUID("d0000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def vllm_runtime_database(tmp_path: Path) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'vllm-runtime.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                cast(Table, User.__table__),
                cast(Table, Project.__table__),
                cast(Table, ProjectQuota.__table__),
                cast(Table, ProjectQuotaState.__table__),
                cast(Table, Worker.__table__),
                cast(Table, PersistedGPUDevice.__table__),
                cast(Table, RegisteredModel.__table__),
                cast(Table, LogicalModel.__table__),
                cast(Table, ModelVariant.__table__),
                cast(Table, ModelService.__table__),
                cast(Table, ServiceReplica.__table__),
                cast(Table, OutboxEvent.__table__),
            ],
        )
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_ID, name="vLLM Runtime Tests", slug="vllm-runtime-tests"))
    try:
        yield database
    finally:
        await database.dispose()


class _Inventory:
    def __init__(self, devices: tuple[GPUDevice, ...]) -> None:
        self.devices = devices

    def list_devices(self) -> tuple[GPUDevice, ...]:
        return self.devices


class _Runtime:
    def __init__(self) -> None:
        self.prepared: list[tuple[VLLMLaunchSpec, dict[str, object]]] = []
        self.handles: dict[str, VLLMContainerHandle] = {}
        self.states: dict[str, VLLMContainerState] = {}
        self.stopped: list[str] = []
        self.cleaned: list[str] = []
        self.closed = False

    async def version(self) -> str:
        return "mock-docker"

    async def prepare(
        self,
        spec: VLLMLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
    ) -> VLLMContainerHandle:
        options: dict[str, object] = {
            "worker_id": worker_id,
            "worker_session_id": worker_session_id,
            "cpu_millicores": cpu_millicores,
            "memory_mb": memory_mb,
        }
        self.prepared.append((spec, options))
        object_id = f"container-{len(self.prepared)}"
        handle = VLLMContainerHandle(
            object_id=object_id,
            display_id=object_id,
            labels=MappingProxyType(
                {**dict(spec.labels), WORKER_SESSION_ID_LABEL: str(worker_session_id)}
            ),
        )
        self.handles[object_id] = handle
        self.states[object_id] = VLLMContainerState(
            status="created", running=False, exit_code=None, oom_killed=False
        )
        return handle

    async def start(self, handle: VLLMContainerHandle) -> VLLMContainerHandle:
        running = VLLMContainerHandle(
            object_id=handle.object_id,
            display_id=handle.display_id,
            endpoint_url=f"http://127.0.0.1:{31000 + len(self.prepared)}",
            labels=handle.labels,
        )
        self.handles[handle.object_id] = running
        self.states[handle.object_id] = VLLMContainerState(
            status="running", running=True, exit_code=None, oom_killed=False
        )
        return running

    async def inspect(self, handle: VLLMContainerHandle) -> VLLMContainerState:
        return self.states.get(
            handle.object_id,
            VLLMContainerState(status="missing", running=False, exit_code=None, oom_killed=False),
        )

    async def stop(self, handle: VLLMContainerHandle) -> None:
        self.stopped.append(handle.object_id)
        self.states[handle.object_id] = VLLMContainerState(
            status="exited", running=False, exit_code=0, oom_killed=False
        )

    async def cleanup(self, handle: VLLMContainerHandle) -> None:
        self.cleaned.append(handle.object_id)
        self.handles.pop(handle.object_id, None)
        self.states.pop(handle.object_id, None)

    async def list_managed(self, *, worker_id: str) -> Sequence[VLLMContainerHandle]:
        del worker_id
        return tuple(self.handles.values())

    async def close(self) -> None:
        self.closed = True


def _gpu(device_uuid: str = "GPU-real-1", *, index: int = 0) -> GPUDevice:
    return GPUDevice(
        uuid=device_uuid,
        index=index,
        vendor="nvidia",
        model="NVIDIA A100",
        memory_total_mb=40_960,
        memory_free_mb=39_000,
        compute_capability="8.0",
    )


async def _create_service(
    database: Database,
    *,
    gpu_count: int = 1,
    gpu_model: str | None = None,
    tensor_parallel_size: int | None = None,
    desired_replicas: int = 1,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name=f"vllm-{uuid.uuid4().hex[:8]}",
            model="Qwen/test-model",
            model_revision="revision-1",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="vllm/vllm-openai@sha256:test",
            cpu_millicores=1000,
            memory_mb=2048,
            gpu_count=gpu_count,
            gpu_memory_mb=4096,
            gpu_model=gpu_model,
            tensor_parallel_size=tensor_parallel_size,
            dtype="bfloat16",
            gpu_memory_utilization=0.8,
            max_model_len=8192,
            desired_replicas=desired_replicas,
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id


async def _replicas(database: Database, service_id: uuid.UUID) -> list[ServiceReplica]:
    async with database.session() as session:
        return await ServiceRepository.list_replicas(session, service_id)


def _healthy_response(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"status": "ok"})


def _unhealthy_response(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, json={"status": "starting"})


def test_vllm_runtime_claims_only_vllm_docker_with_skip_locked() -> None:
    compiled = str(
        VLLMReplicaRuntimeController.claim_candidates_query(10).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "model_services.runtime" in compiled
    assert "model_services.runtime_type" in compiled


async def test_vllm_runtime_starts_exact_gpu_replica_and_stops_scale_down(
    vllm_runtime_database: Database,
) -> None:
    service_id = await _create_service(vllm_runtime_database)
    runtime = _Runtime()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_healthy_response)) as client:
        controller = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory((_gpu(),)),
            worker_id="vllm-worker",
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        try:
            result = await controller.run_once()
            replicas = await _replicas(vllm_runtime_database, service_id)

            assert (result.claimed, result.launched, result.ready) == (1, 1, 1)
            assert controller.active_container_count == 1
            assert len(replicas) == 1
            replica = replicas[0]
            assert replica.status == ReplicaStatus.RUNNING
            assert replica.health == ReplicaHealth.HEALTHY
            assert replica.worker_id == controller.worker_id
            assert replica.endpoint_url == "http://127.0.0.1:31001"
            assert runtime.prepared[0][0].gpu_device_ids == ("GPU-real-1",)
            assert runtime.prepared[0][0].environment["NVIDIA_VISIBLE_DEVICES"] == "GPU-real-1"
            assert runtime.prepared[0][1]["worker_session_id"] == controller.worker_session_id

            async with vllm_runtime_database.session() as session:
                worker = await session.get(Worker, controller.worker_id)
            assert worker is not None
            assert worker.status == WorkerStatus.DRAINING
            assert worker.worker_session_id == controller.worker_session_id

            async with vllm_runtime_database.session() as session, session.begin():
                service = await ServiceRepository.set_desired_replicas(
                    session,
                    service_id=service_id,
                    project_id=PROJECT_ID,
                    desired_replicas=0,
                )
                assert service is not None
                await ServiceRepository.reconcile_locked(session, service)

            stopped = await controller.run_once()
            replica = (await _replicas(vllm_runtime_database, service_id))[0]
            assert stopped.stopped == 1
            assert replica.status == ReplicaStatus.STOPPED
            assert replica.endpoint_url is None
            assert controller.active_container_count == 0
            assert runtime.stopped == ["container-1"]
            assert runtime.cleaned == ["container-1"]
        finally:
            await controller.close()


async def test_vllm_runtime_launches_four_gpu_tensor_parallel_gang_on_one_worker(
    vllm_runtime_database: Database,
) -> None:
    service_id = await _create_service(
        vllm_runtime_database,
        gpu_count=4,
        gpu_model="NVIDIA A100",
        tensor_parallel_size=4,
    )
    runtime = _Runtime()
    devices = tuple(_gpu(f"GPU-real-{index}", index=index) for index in range(4))
    async with httpx.AsyncClient(transport=httpx.MockTransport(_healthy_response)) as client:
        controller = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory(devices),
            worker_id="tensor-worker",
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        try:
            result = await controller.run_once()
            assert (result.claimed, result.ready, result.waiting_capacity) == (1, 1, 0)

            spec = runtime.prepared[0][0]
            assert spec.gpu_device_ids == tuple(f"GPU-real-{index}" for index in range(4))
            assert spec.argv[spec.argv.index("--tensor-parallel-size") + 1] == "4"
            assert spec.argv[spec.argv.index("--revision") + 1] == "revision-1"
            assert spec.argv[spec.argv.index("--dtype") + 1] == "bfloat16"
            assert spec.argv[spec.argv.index("--gpu-memory-utilization") + 1] == "0.8"
            assert spec.argv[spec.argv.index("--max-model-len") + 1] == "8192"

            replica = (await _replicas(vllm_runtime_database, service_id))[0]
            assert replica.status == ReplicaStatus.RUNNING
            assert replica.model_revision == "revision-1"
            async with vllm_runtime_database.session() as session:
                service = await ServiceRepository.get(session, service_id)
            assert service is not None
            assert service.scheduling_reason is None
            assert service.scheduling_details == {}
        finally:
            await controller.close()


async def test_vllm_runtime_leaves_gpu_service_pending_without_concrete_capacity(
    vllm_runtime_database: Database,
) -> None:
    service_id = await _create_service(vllm_runtime_database, gpu_count=2)
    runtime = _Runtime()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_healthy_response)) as client:
        controller = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory((_gpu(),)),
            worker_id="capacity-worker",
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        try:
            result = await controller.run_once()
            replica = (await _replicas(vllm_runtime_database, service_id))[0]

            assert result.claimed == 0
            assert result.waiting_capacity == 1
            assert runtime.prepared == []
            assert replica.status == ReplicaStatus.PENDING
            assert replica.execution_id is None
            async with vllm_runtime_database.session() as session:
                service = await ServiceRepository.get(session, service_id)
            assert service is not None
            assert service.scheduling_reason == "INSUFFICIENT_CONTIGUOUS_GPUS"
            assert service.scheduling_details["requested_gpu_count"] == 2
            assert service.scheduling_details["largest_available_worker_gpu_count"] == 1
        finally:
            await controller.close()


async def test_vllm_runtime_fails_and_cleans_replica_after_readiness_timeout(
    vllm_runtime_database: Database,
) -> None:
    service_id = await _create_service(vllm_runtime_database)
    runtime = _Runtime()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_unhealthy_response)) as client:
        controller = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory((_gpu(),)),
            worker_id="startup-timeout-worker",
            ready_timeout_seconds=0.001,
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        try:
            launched = await controller.run_once()
            assert (launched.claimed, launched.launched, launched.ready) == (1, 1, 0)

            await asyncio.sleep(0.01)
            timed_out = await controller.run_once()
            replica = (await _replicas(vllm_runtime_database, service_id))[0]

            assert timed_out.failed == 1
            assert replica.status == ReplicaStatus.FAILED
            assert replica.error_code == "MODEL_LOAD_TIMEOUT"
            assert replica.endpoint_url is None
            assert controller.active_container_count == 0
            assert runtime.stopped == ["container-1"]
            assert runtime.cleaned == ["container-1"]
        finally:
            await controller.close()


async def test_new_worker_session_cleans_orphan_and_fences_old_controller(
    vllm_runtime_database: Database,
) -> None:
    service_id = await _create_service(vllm_runtime_database)
    runtime = _Runtime()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_healthy_response)) as client:
        old = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory((_gpu(),)),
            worker_id="stable-vllm-worker",
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        first = await old.run_once()
        assert first.ready == 1
        old_replica = (await _replicas(vllm_runtime_database, service_id))[0]
        assert old_replica.execution_id is not None
        old_execution_id = old_replica.execution_id

        replacement = VLLMReplicaRuntimeController(
            vllm_runtime_database,
            runtime,
            http_client=client,
            inventory_provider=_Inventory((_gpu(),)),
            worker_id="stable-vllm-worker",
            probe_timeout_seconds=1,
            lease_seconds=30,
        )
        try:
            recovered = await replacement.run_once()
            old_replica = (await _replicas(vllm_runtime_database, service_id))[0]
            assert recovered.recovered == 1
            assert old_replica.status == ReplicaStatus.LOST
            assert runtime.cleaned == ["container-1"]

            with pytest.raises(RuntimeError, match="session is stale"):
                await old.run_once()

            async with vllm_runtime_database.session() as session, session.begin():
                service = await ServiceRepository.get(session, service_id, for_update=True)
                assert service is not None
                await ServiceRepository.reconcile_locked(session, service)

            restarted = await replacement.run_once()
            replicas = await _replicas(vllm_runtime_database, service_id)
            new_replica = replicas[-1]
            assert restarted.ready == 1
            assert new_replica.status == ReplicaStatus.RUNNING
            assert new_replica.execution_id is not None
            assert new_replica.execution_id != old_execution_id
            assert new_replica.worker_id == replacement.worker_id
        finally:
            await replacement.close()
            await old.close()


async def test_real_vllm_controller_rejects_fake_gpu_inventory(
    vllm_runtime_database: Database,
) -> None:
    fake = GPUDevice(
        uuid="FAKE-device",
        index=0,
        vendor="fake",
        model="FAKE-A100",
        memory_total_mb=40960,
        memory_free_mb=40960,
        compute_capability="0.0",
        fake=True,
    )
    runtime = _Runtime()
    controller = VLLMReplicaRuntimeController(
        vllm_runtime_database,
        runtime,
        inventory_provider=_Inventory((fake,)),
        worker_id="fake-inventory-worker",
        probe_timeout_seconds=1,
        lease_seconds=30,
    )
    try:
        with pytest.raises(ValueError, match="fake GPU inventory"):
            await controller.run_once()
    finally:
        await controller.close()
