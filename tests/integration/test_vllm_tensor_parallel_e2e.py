from __future__ import annotations

import uuid
from collections.abc import Sequence
from types import MappingProxyType

import httpx
import pytest
from sqlalchemy import select

from api.services.vllm_replica_runtime import VLLMReplicaRuntimeController
from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, RuntimeType, WorkerStatus
from models.identity import Project
from models.scheduling import GPUDevice as PersistedGPUDevice
from models.service import ReplicaStatus, ServingRuntime
from models.worker import Worker
from repositories.services import ServiceRepository
from worker.gpu_inventory import GPUDevice
from worker.vllm_runtime import (
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    VLLMContainerHandle,
    VLLMContainerState,
    VLLMLaunchSpec,
)

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("e0000000-0000-0000-0000-000000000001")
IMAGE_DIGEST = "sha256:" + "d" * 64


class _FakeInventory:
    def __init__(self, worker_name: str, gpu_count: int) -> None:
        self.devices = tuple(
            GPUDevice(
                device_id=f"{worker_name}-gpu-{index}",
                device_index=index,
                vendor=AcceleratorVendor.NVIDIA,
                kind=AcceleratorKind.GPU,
                model="FAKE-A100",
                memory_total_mb=40_960,
                memory_free_mb=39_000,
                compute_arch="8.0",
                fake=True,
            )
            for index in range(gpu_count)
        )

    def list_devices(self) -> tuple[GPUDevice, ...]:
        return self.devices


class _FakeVLLMRuntime:
    def __init__(self) -> None:
        self.prepared: list[tuple[VLLMLaunchSpec, str]] = []
        self.handles: dict[str, VLLMContainerHandle] = {}
        self.closed = False

    async def version(self) -> str:
        return "fake-vllm-runtime"

    async def prepare(
        self,
        spec: VLLMLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
    ) -> VLLMContainerHandle:
        assert cpu_millicores == 1_000
        assert memory_mb == 2_048
        self.prepared.append((spec, worker_id))
        object_id = f"fake-container-{len(self.prepared)}"
        handle = VLLMContainerHandle(
            object_id=object_id,
            display_id=object_id,
            labels=MappingProxyType(
                {
                    **dict(spec.labels),
                    WORKER_ID_LABEL: worker_id,
                    WORKER_SESSION_ID_LABEL: str(worker_session_id),
                }
            ),
        )
        self.handles[object_id] = handle
        return handle

    async def start(self, handle: VLLMContainerHandle) -> VLLMContainerHandle:
        running = VLLMContainerHandle(
            object_id=handle.object_id,
            display_id=handle.display_id,
            endpoint_url="http://127.0.0.1:38000",
            image_digest=IMAGE_DIGEST,
            labels=handle.labels,
        )
        self.handles[handle.object_id] = running
        return running

    async def inspect(self, handle: VLLMContainerHandle) -> VLLMContainerState:
        return VLLMContainerState(
            status="running" if handle.object_id in self.handles else "missing",
            running=handle.object_id in self.handles,
            exit_code=None,
            oom_killed=False,
        )

    async def stop(self, handle: VLLMContainerHandle) -> None:
        del handle

    async def cleanup(self, handle: VLLMContainerHandle) -> None:
        self.handles.pop(handle.object_id, None)

    async def list_managed(self, *, worker_id: str) -> Sequence[VLLMContainerHandle]:
        return tuple(
            handle
            for handle in self.handles.values()
            if handle.labels.get(WORKER_ID_LABEL) == worker_id
        )

    async def close(self) -> None:
        self.closed = True


def _healthy_response(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"status": "ok"})


async def _create_tensor_parallel_service(database: Database) -> uuid.UUID:
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_ID, name="Demo H", slug="demo-h"))
        await session.flush()
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="demo-h-tensor-parallel",
            model="Qwen/demo-h",
            model_revision="demo-h-revision",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image=f"docker.io/vllm/vllm-openai@{IMAGE_DIGEST}",
            cpu_millicores=1_000,
            memory_mb=2_048,
            gpu_count=4,
            gpu_memory_mb=20_000,
            gpu_model="FAKE-A100",
            tensor_parallel_size=4,
            dtype="float16",
            gpu_memory_utilization=0.8,
            max_model_len=8_192,
            desired_replicas=1,
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id


async def test_demo_h_tensor_parallel_claims_all_four_gpus_from_worker_a(
    database: Database,
) -> None:
    service_id = await _create_tensor_parallel_service(database)
    worker_b_runtime = _FakeVLLMRuntime()
    worker_a_runtime = _FakeVLLMRuntime()

    async with httpx.AsyncClient(transport=httpx.MockTransport(_healthy_response)) as client:
        worker_b = VLLMReplicaRuntimeController(
            database,
            worker_b_runtime,
            http_client=client,
            inventory_provider=_FakeInventory("worker-b", 2),
            worker_id="worker-b",
            probe_timeout_seconds=1,
            lease_seconds=30,
            allow_fake_gpu_inventory=True,
        )
        worker_a = VLLMReplicaRuntimeController(
            database,
            worker_a_runtime,
            http_client=client,
            inventory_provider=_FakeInventory("worker-a", 4),
            worker_id="worker-a",
            probe_timeout_seconds=1,
            lease_seconds=30,
            allow_fake_gpu_inventory=True,
        )
        try:
            insufficient = await worker_b.run_once()
            async with database.session() as session:
                service = await ServiceRepository.get(session, service_id)
                replicas = await ServiceRepository.list_replicas(session, service_id)
            assert service is not None
            assert (insufficient.claimed, insufficient.waiting_capacity) == (0, 1)
            assert worker_b_runtime.prepared == []
            assert len(replicas) == 1
            assert replicas[0].status == ReplicaStatus.PENDING
            assert replicas[0].worker_id is None
            assert replicas[0].execution_id is None
            assert service.scheduling_reason == "INSUFFICIENT_CONTIGUOUS_GPUS"
            assert service.scheduling_details["requested_gpu_count"] == 4
            assert service.scheduling_details["largest_available_worker_gpu_count"] == 2

            placed = await worker_a.run_once()
            async with database.session() as session:
                service = await ServiceRepository.get(session, service_id)
                replicas = await ServiceRepository.list_replicas(session, service_id)
                persisted_worker_a = await session.get(Worker, "worker-a")
                persisted_worker_b = await session.get(Worker, "worker-b")
                gpu_devices = list(
                    await session.scalars(
                        select(PersistedGPUDevice).order_by(
                            PersistedGPUDevice.worker_id,
                            PersistedGPUDevice.device_index,
                        )
                    )
                )

            assert service is not None
            assert (placed.claimed, placed.ready, placed.waiting_capacity) == (1, 1, 0)
            assert service.scheduling_reason is None
            assert service.scheduling_details == {}
            assert len(replicas) == 1
            assert replicas[0].status == ReplicaStatus.RUNNING
            assert replicas[0].worker_id == "worker-a"
            assert replicas[0].execution_id is not None
            assert persisted_worker_a is not None
            assert persisted_worker_b is not None
            assert persisted_worker_a.status == WorkerStatus.DRAINING
            assert persisted_worker_b.status == WorkerStatus.DRAINING

            assert len(worker_a_runtime.prepared) == 1
            launch_spec, selected_worker = worker_a_runtime.prepared[0]
            assert selected_worker == "worker-a"
            assert launch_spec.gpu_device_ids == tuple(
                f"worker-a-gpu-{index}" for index in range(4)
            )
            tensor_parallel_index = launch_spec.argv.index("--tensor-parallel-size")
            assert launch_spec.argv[tensor_parallel_index + 1] == "4"
            assert all(device.fake for device in gpu_devices)
            assert [device.worker_id for device in gpu_devices] == [
                "worker-a",
                "worker-a",
                "worker-a",
                "worker-a",
                "worker-b",
                "worker-b",
            ]
        finally:
            await worker_a.close()
            await worker_b.close()
