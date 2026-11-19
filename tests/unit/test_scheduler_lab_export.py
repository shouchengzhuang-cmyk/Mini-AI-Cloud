from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast

import pytest

from core.enums import TaskStatus, WorkerStatus, WorkloadType
from core.scheduler_lab_export import (
    CONTRACT_VERSION,
    SchedulerLabExportError,
    build_v2_export,
)
from models.scheduling import GPUDevice
from models.task import Task
from models.worker import Worker


def _worker(worker_id: str = "worker-a") -> Worker:
    return Worker(
        id=worker_id,
        hostname=f"{worker_id}.example.invalid",
        status=WorkerStatus.ONLINE,
        running_tasks=0,
        concurrency=4,
        cpu_count=8,
        memory_total_mb=65_536,
        docker_version=None,
        labels={"zone": "zone-1"},
        gpu_count=1,
        gpu_model="A100-80GB",
        gpu_memory_mb=81_920,
        overcommitted=False,
        version=1,
    )


def _device(
    *, worker_id: str = "worker-a", device_uuid: str = "GPU-a0", fake: bool = False
) -> GPUDevice:
    return GPUDevice(
        id=uuid.uuid4(),
        worker_id=worker_id,
        device_uuid=device_uuid,
        device_index=0,
        vendor="nvidia",
        accelerator_kind="gpu",
        model="A100-80GB",
        memory_total_mb=81_920,
        memory_free_mb=81_920,
        runtime_profile_ids=["nvidia-vllm-k8s"],
        capabilities_json=["bf16", "tensor-parallel"],
        health="healthy",
        fake=fake,
    )


def _task(*, typed: bool = True, policy: str = "any", memory_mb: int = 40_960) -> Task:
    accelerator: dict[str, object] | None = None
    if typed:
        accelerator = {
            "count": 1,
            "memory_mb_per_device": memory_mb,
            "allowed_vendors": ["nvidia"],
            "allowed_kinds": ["gpu"],
            "allowed_models": ["A100-80GB"],
            "required_capabilities": ["bf16"],
            "runtime_profile": "nvidia-vllm-k8s",
            "selection_policy": policy,
        }
    return Task(
        id=uuid.UUID("00000000-0000-0000-0000-000000000010"),
        project_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        image="example.invalid/inference@sha256:" + "a" * 64,
        command=["serve"],
        environment={},
        status=TaskStatus.QUEUED,
        workload_type=WorkloadType.MODEL_SERVICE,
        created_at=datetime(2026, 9, 12, tzinfo=UTC),
        queued_at=datetime(2026, 9, 12, tzinfo=UTC),
        timeout_seconds=300,
        retry_count=0,
        max_retries=0,
        cpu_limit=1.0,
        memory_limit_mb=1024,
        gpu_count=1,
        gpu_memory_mb=memory_mb,
        accelerator_request_json=accelerator,
        network_enabled=False,
        labels={"purpose": "x0"},
        priority=80,
        cancel_requested=False,
        version=1,
        log_sequence=0,
    )


def test_build_v2_export_is_typed_and_deterministic() -> None:
    payload = build_v2_export(
        workers=[_worker()],
        devices=[_device()],
        tasks=[_task()],
        producer="mini-ai-cloud/9ef5dbf34e0e49b2a117d15a541d9cac919dd051",
    )

    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["workers"] == [
        {
            "id": "worker-a",
            "schedulable": True,
            "labels": {"zone": "zone-1"},
            "gpu_devices": [
                {
                    "device_uuid": "GPU-a0",
                    "vendor": "nvidia",
                    "kind": "gpu",
                    "model": "A100-80GB",
                    "memory_total_mb": 81_920,
                    "health": "healthy",
                    "runtime_profiles": ["nvidia-vllm-k8s"],
                    "capabilities": ["bf16", "tensor-parallel"],
                }
            ],
        }
    ]
    tasks = cast(list[dict[str, object]], payload["tasks"])
    task = tasks[0]
    assert task["gpu_count"] == 1
    assert task["gpu_memory_mb"] == 40_960
    assert task["allowed_vendors"] == ["nvidia"]
    assert task["allowed_kinds"] == ["gpu"]
    assert task["selection_policy"] == "any"


def test_build_v2_export_rejects_zero_memory_for_positive_accelerator_count() -> None:
    task = _task(memory_mb=0)
    with pytest.raises(SchedulerLabExportError, match=rf"task {task.id}.*memory_mb_per_device=0"):
        build_v2_export(workers=[], devices=[], tasks=[task], producer="mini-ai-cloud/test")


@pytest.mark.parametrize("typed", [False, True])
def test_build_v2_export_preserves_zero_accelerator_tasks(typed: bool) -> None:
    task = _task(typed=False, memory_mb=0)
    task.gpu_count = 0
    if typed:
        task.accelerator_request_json = {"count": 0, "memory_mb_per_device": 0}

    payload = build_v2_export(workers=[], devices=[], tasks=[task], producer="mini-ai-cloud/test")

    exported_task = cast(list[dict[str, object]], payload["tasks"])[0]
    assert exported_task["gpu_count"] == 0
    if typed:
        assert exported_task["gpu_memory_mb"] == 0
    else:
        assert "gpu_memory_mb" not in exported_task


def test_build_v2_export_rejects_persisted_memory_mismatch() -> None:
    task = _task()
    task.gpu_memory_mb = 20_480
    with pytest.raises(
        SchedulerLabExportError, match=rf"task {task.id}.*does not match persisted gpu_memory_mb"
    ):
        build_v2_export(workers=[], devices=[], tasks=[task], producer="mini-ai-cloud/test")


@pytest.mark.parametrize("other_worker_id", ["worker-a", "worker-b"])
def test_build_v2_export_rejects_duplicate_device_uuids(other_worker_id: str) -> None:
    workers = [_worker("worker-b"), _worker()]
    devices = [_device(worker_id=other_worker_id), _device()]
    for ordered_devices in (devices, list(reversed(devices))):
        with pytest.raises(
            SchedulerLabExportError, match=rf"'GPU-a0'.*'worker-a'.*'{other_worker_id}'"
        ):
            build_v2_export(
                workers=workers, devices=ordered_devices, tasks=[], producer="mini-ai-cloud/test"
            )


def test_build_v2_export_preserves_device_uuid_case_and_worker_order() -> None:
    workers = [_worker("worker-b"), _worker()]
    devices = [_device(worker_id="worker-b", device_uuid="GPU-A0"), _device()]
    payload = build_v2_export(
        workers=workers, devices=devices, tasks=[], producer="mini-ai-cloud/test"
    )

    assert payload == build_v2_export(
        workers=reversed(workers),
        devices=reversed(devices),
        tasks=[],
        producer="mini-ai-cloud/test",
    )
    exported_workers = cast(list[dict[str, object]], payload["workers"])
    assert [worker["id"] for worker in exported_workers] == ["worker-a", "worker-b"]
    assert [
        cast(list[dict[str, object]], worker["gpu_devices"])[0]["device_uuid"]
        for worker in exported_workers
    ] == ["GPU-a0", "GPU-A0"]


def test_build_v2_export_rejects_invalid_device_uuid_text() -> None:
    with pytest.raises(SchedulerLabExportError, match=r"gpu device\.device_uuid.*canonical text"):
        build_v2_export(
            workers=[_worker()],
            devices=[_device(device_uuid=" GPU-a0 "), _device(device_uuid=" GPU-a0 ")],
            tasks=[],
            producer="mini-ai-cloud/test",
        )


def test_build_v2_export_rejects_orphan_device() -> None:
    with pytest.raises(SchedulerLabExportError, match="devices for missing workers: worker-b"):
        build_v2_export(
            workers=[_worker()],
            devices=[_device(worker_id="worker-b")],
            tasks=[],
            producer="mini-ai-cloud/test",
        )


def test_build_v2_export_rejects_legacy_gpu_task() -> None:
    with pytest.raises(SchedulerLabExportError, match="v1 fallback is forbidden"):
        build_v2_export(
            workers=[_worker()],
            devices=[_device()],
            tasks=[_task(typed=False)],
            producer="mini-ai-cloud/test",
        )


def test_build_v2_export_rejects_unsupported_selection_policy() -> None:
    with pytest.raises(SchedulerLabExportError, match="accepts only 'any'"):
        build_v2_export(
            workers=[_worker()],
            devices=[_device()],
            tasks=[_task(policy="nvidia-only")],
            producer="mini-ai-cloud/test",
        )


def test_build_v2_export_rejects_fake_inventory() -> None:
    with pytest.raises(SchedulerLabExportError, match="marked fake"):
        build_v2_export(
            workers=[_worker()],
            devices=[_device(fake=True)],
            tasks=[_task()],
            producer="mini-ai-cloud/test",
        )
