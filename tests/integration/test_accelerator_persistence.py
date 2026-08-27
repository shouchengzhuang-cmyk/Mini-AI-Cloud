import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    AllocationAuthority,
    RuntimeType,
    TaskStatus,
)
from models.scheduling import ReservationGPUDevice, ResourceReservation
from models.task import Task
from models.usage import TaskExecution
from repositories.diagnostics import DiagnosticsRepository
from repositories.reservations import (
    AllocationObservationConflict,
    ReservationRepository,
)
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration

LEGACY_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def _create_allocation(
    database: Database,
    *,
    worker_id: str,
    authority: AllocationAuthority,
    vendor: AcceleratorVendor,
    kind: AcceleratorKind,
    observed_device_ids: list[str] | None = None,
) -> tuple[uuid.UUID, uuid.UUID]:
    now = datetime.now(UTC)
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.test",
            concurrency=1,
            cpu_count=4,
            memory_total_mb=8192,
            docker_version="test",
            labels={},
            gpu_count=2,
            gpu_model="test-accelerator",
            gpu_memory_mb=32_768,
        )
        await session.flush()
        task = Task(
            id=task_id,
            project_id=LEGACY_PROJECT_ID,
            image="example.invalid/test@sha256:" + "0" * 64,
            command=["python", "-V"],
            status=TaskStatus.ASSIGNED,
            runtime_type=RuntimeType.KUBERNETES,
            worker_id=worker_id,
            execution_id=execution_id,
            gpu_count=2,
            gpu_memory_mb=16_384,
            gpu_model="test-accelerator",
        )
        session.add(task)
        await session.flush()
        observation = {
            "observed_device_ids_json": observed_device_ids,
            "observed_vendor": vendor.value if observed_device_ids is not None else None,
            "observed_at": now if observed_device_ids is not None else None,
        }
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=task_id,
                project_id=LEGACY_PROJECT_ID,
                worker_id=worker_id,
                worker_session_id=worker.worker_session_id,
                attempt=1,
                status=TaskStatus.ASSIGNED.value,
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=2,
                gpu_model="test-accelerator",
                allocation_authority=authority.value,
                requested_vendor=vendor.value,
                requested_kind=kind.value,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1"),
                assigned_at=now,
                runtime_type=RuntimeType.KUBERNETES.value,
                **observation,
            )
        )
        await session.flush()
        reservation = ResourceReservation(
            project_id=LEGACY_PROJECT_ID,
            task_id=task_id,
            execution_id=execution_id,
            worker_id=worker_id,
            worker_session_id=worker.worker_session_id,
            cpu_millicores=1000,
            memory_mb=256,
            gpu_count=2,
            allocation_authority=authority.value,
            requested_vendor=vendor.value,
            requested_kind=kind.value,
            legacy_unbound=False,
            created_at=now,
            **observation,
        )
        session.add(reservation)
        await session.flush()
        return execution_id, reservation.id


async def test_kubernetes_observation_is_deferred_atomic_and_idempotent(
    database: Database,
) -> None:
    execution_id, reservation_id = await _create_allocation(
        database,
        worker_id="ascend-plugin-worker",
        authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
        vendor=AcceleratorVendor.HUAWEI_ASCEND,
        kind=AcceleratorKind.NPU,
    )
    observed_at = datetime.now(UTC)
    device_ids = ("ASCEND-0", "ASCEND-1")

    async with database.session() as session, session.begin():
        with pytest.raises(AllocationObservationConflict, match="does not match requested"):
            await ReservationRepository.record_observed_allocation(
                session,
                execution_id=execution_id,
                vendor=AcceleratorVendor.NVIDIA,
                device_ids=device_ids,
                observed_at=observed_at,
            )
        assert await ReservationRepository.record_observed_allocation(
            session,
            execution_id=execution_id,
            vendor=AcceleratorVendor.HUAWEI_ASCEND,
            device_ids=device_ids,
            observed_at=observed_at,
        )
        assert not await ReservationRepository.record_observed_allocation(
            session,
            execution_id=execution_id,
            vendor=AcceleratorVendor.HUAWEI_ASCEND,
            device_ids=device_ids,
            observed_at=observed_at,
        )

    async with database.session() as session:
        reservation = await session.get(ResourceReservation, reservation_id)
        execution = await session.get(TaskExecution, execution_id)
        link_count = int(
            await session.scalar(
                select(func.count(ReservationGPUDevice.id)).where(
                    ReservationGPUDevice.reservation_id == reservation_id
                )
            )
            or 0
        )
    assert reservation is not None and execution is not None
    assert reservation.observed_device_ids_json == list(device_ids)
    assert reservation.observed_vendor == AcceleratorVendor.HUAWEI_ASCEND
    assert reservation.observed_at is not None
    assert execution.observed_device_ids_json == list(device_ids)
    assert execution.observed_vendor == AcceleratorVendor.HUAWEI_ASCEND
    assert link_count == 0


async def test_inventory_persists_ascend_profile_and_resource_metadata(
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id="ascend-inventory-worker",
            hostname="ascend-inventory-worker.test",
            concurrency=1,
            cpu_count=8,
            memory_total_mb=65_536,
            docker_version=None,
            labels={},
            gpu_count=1,
            gpu_model="Atlas A2",
            gpu_memory_mb=64_000,
        )
        await session.flush()
        devices = await WorkerRepository.replace_gpu_inventory(
            session,
            worker_id=worker.id,
            worker_session_id=worker.worker_session_id,
            devices=[
                {
                    "uuid": "ASCEND-910B-0",
                    "index": 0,
                    "vendor": "huawei-ascend",
                    "accelerator_kind": "npu",
                    "model": "Atlas A2",
                    "memory_total_mb": 64_000,
                    "memory_free_mb": 63_000,
                    "compute_arch": "Ascend910B",
                    "runtime_profile_ids": ["ascend-vllm-k8s-a2"],
                    "capabilities": ["bf16", "tensor-parallel"],
                    "kubernetes_resource_name": "huawei.com/Ascend910",
                }
            ],
        )

    assert len(devices) == 1
    device = devices[0]
    assert device.vendor == AcceleratorVendor.HUAWEI_ASCEND
    assert device.accelerator_kind == AcceleratorKind.NPU
    assert device.compute_arch == "Ascend910B"
    assert device.runtime_profile_ids == ["ascend-vllm-k8s-a2"]
    assert device.capabilities_json == ["bf16", "tensor-parallel"]
    assert device.kubernetes_resource_name == "huawei.com/Ascend910"


async def test_diagnostics_report_orphan_exact_device_allocation(database: Database) -> None:
    _execution_id, reservation_id = await _create_allocation(
        database,
        worker_id="orphan-exact-worker",
        authority=AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE,
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        observed_device_ids=["GPU-MISSING-0", "GPU-MISSING-1"],
    )

    async with database.session() as session:
        snapshot = await DiagnosticsRepository.snapshot(
            session,
            project_id=LEGACY_PROJECT_ID,
            worker_offline_timeout_seconds=60,
            stuck_after_seconds=60,
        )

    checks = {check.name: check for check in snapshot.consistency.checks}
    orphan = checks["orphan_accelerator_allocation"]
    assert orphan.total == 1
    assert orphan.issues[0].resource_id == str(reservation_id)
    assert "0 active exact-device bindings; expected 2" in orphan.issues[0].reason
