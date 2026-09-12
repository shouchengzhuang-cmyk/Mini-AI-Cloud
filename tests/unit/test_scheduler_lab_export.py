from __future__ import annotations

import uuid
from datetime import UTC, datetime

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


def _device(*, worker_id: str = "worker-a", fake: bool = False) -> GPUDevice:
    return GPUDevice(
        id=uuid.uuid4(),
        worker_id=worker_id,
        device_uuid="GPU-a0",
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


def _task(*, typed: bool = True, policy: str = "any") -> Task:
    accelerator: dict[str, object] | None = None
    if typed:
        accelerator = {
            "count": 1,
            "memory_mb_per_device": 40_960,
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
        gpu_memory_mb=40_960,
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
    task = payload["tasks"][0]
    assert task["allowed_vendors"] == ["nvidia"]
    assert task["allowed_kinds"] == ["gpu"]
    assert task["selection_policy"] == "any"


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
