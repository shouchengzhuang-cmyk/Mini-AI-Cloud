import inspect
from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio

from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, RuntimeType
from models.base import Base
from repositories.admission import AdmissionRepository
from repositories.workers import WorkerRepository


@pytest_asyncio.fixture
async def inventory_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(
        f"sqlite+aiosqlite:///{(tmp_path / 'admission-inventory.sqlite3').as_posix()}"
    )
    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield database
    finally:
        await database.dispose()


async def test_kubernetes_capacity_slots_are_admissible_but_unknown_cli_health_is_not(
    inventory_database: Database,
) -> None:
    async with inventory_database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id="dual-stack-node",
            hostname="dual-stack-node",
            concurrency=2,
            cpu_count=8,
            memory_total_mb=16_384,
            docker_version=None,
            labels={},
            gpu_count=2,
            gpu_model="mixed",
            gpu_memory_mb=131_072,
            runtime_types=["kubernetes"],
        )
        await WorkerRepository.replace_gpu_inventory(
            session,
            worker_id=worker.id,
            worker_session_id=worker.worker_session_id,
            devices=[
                {
                    "uuid": "k8s-capacity:node:nvidia.com/gpu:0",
                    "index": 0,
                    "vendor": "nvidia",
                    "accelerator_kind": "gpu",
                    "model": "NVIDIA A100",
                    "memory_total_mb": 40_960,
                    "memory_free_mb": 40_960,
                    "health": "inventory-only",
                    "runtime_profile_ids": ["nvidia-vllm-k8s"],
                    "capabilities": ["bfloat16", "streaming"],
                    "kubernetes_resource_name": "nvidia.com/gpu",
                },
                {
                    "uuid": "ASCEND-0-0",
                    "index": 0,
                    "vendor": "huawei-ascend",
                    "accelerator_kind": "npu",
                    "model": "Ascend 910B1",
                    "memory_total_mb": 65_536,
                    "memory_free_mb": 65_536,
                    "health": "unknown",
                    "runtime_profile_ids": ["ascend-vllm-k8s-a2"],
                    "capabilities": ["bfloat16", "streaming"],
                    "kubernetes_resource_name": "huawei.com/Ascend910",
                },
            ],
        )

    async with inventory_database.session() as session:
        devices = await AdmissionRepository.list_healthy_inventory_devices(
            session,
            vendors=(AcceleratorVendor.NVIDIA, AcceleratorVendor.HUAWEI_ASCEND),
            kinds=(AcceleratorKind.GPU, AcceleratorKind.NPU),
            runtime_type=RuntimeType.KUBERNETES,
        )

    assert [(device.vendor, device.device_uuid) for device in devices] == [
        (AcceleratorVendor.NVIDIA, "k8s-capacity:node:nvidia.com/gpu:0")
    ]


def test_inventory_snapshot_cannot_lock_and_canonical_fence_is_physically_split() -> None:
    """Keep Worker/GPU fences out of joined ``FOR UPDATE`` statements."""

    snapshot_signature = inspect.signature(AdmissionRepository.list_healthy_inventory_devices)
    snapshot_source = inspect.getsource(AdmissionRepository.list_healthy_inventory_devices)
    fence_source = inspect.getsource(AdmissionRepository.lock_inventory_canonical)

    assert "for_update" not in snapshot_signature.parameters
    assert "with_for_update" not in snapshot_source
    assert fence_source.count(".with_for_update()") == 2
    assert fence_source.index("select(Worker)") < fence_source.index("select(GPUDevice)")
    assert fence_source.index("select(Worker)") < fence_source.index(".with_for_update()")
    assert fence_source.index("select(GPUDevice)") < fence_source.rindex(".with_for_update()")
    assert ".join(" not in fence_source
