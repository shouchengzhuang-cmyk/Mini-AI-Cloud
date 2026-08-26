import uuid

import pytest

from core.enums import TaskStatus, WorkerStatus
from models.task import Task
from models.worker import Worker
from scheduler.policies import (
    GPUDeviceSnapshot,
    RejectionReason,
    TaskSnapshot,
    WorkerSnapshot,
    choose_placement,
    evaluate,
    labels_match,
    worker_accepts_new_tasks,
)


def _task(**overrides: object) -> Task:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "image": "alpine:3.21",
        "command": ["true"],
        "environment": {},
        "status": TaskStatus.QUEUED,
        "timeout_seconds": 60,
        "retry_count": 0,
        "max_retries": 0,
        "cpu_limit": 2.0,
        "memory_limit_mb": 512,
        "gpu_count": 1,
        "network_enabled": False,
        "labels": {"region": "local", "runtime": "docker"},
        "cancel_requested": False,
        "version": 1,
        "log_sequence": 0,
    }
    values.update(overrides)
    return Task(**values)


def _worker(**overrides: object) -> Worker:
    values: dict[str, object] = {
        "id": "worker-1",
        "hostname": "worker-host",
        "status": WorkerStatus.ONLINE,
        "running_tasks": 0,
        "concurrency": 4,
        "cpu_count": 8,
        "memory_total_mb": 16_384,
        "docker_version": "27.0",
        "labels": {"region": "local", "runtime": "docker", "zone": "test"},
        "gpu_count": 2,
        "gpu_model": "Test GPU",
        "gpu_memory_mb": 16_384,
        "version": 1,
    }
    values.update(overrides)
    return Worker(**values)


def test_scheduler_accepts_worker_with_capacity_resources_and_required_labels() -> None:
    decision = evaluate(_worker(), _task())

    assert decision.allowed is True
    assert decision.reason is None


@pytest.mark.parametrize(
    ("worker_overrides", "task_overrides", "reason"),
    [
        ({"status": WorkerStatus.OFFLINE}, {}, RejectionReason.WORKER_NOT_ONLINE),
        ({"running_tasks": 4}, {}, RejectionReason.WORKER_AT_CAPACITY),
        ({"cpu_count": 1}, {}, RejectionReason.INSUFFICIENT_CPU),
        ({"memory_total_mb": 256}, {}, RejectionReason.INSUFFICIENT_MEMORY),
        ({"gpu_count": 0}, {}, RejectionReason.INSUFFICIENT_GPU),
        (
            {"labels": {"region": "remote", "runtime": "docker"}},
            {},
            RejectionReason.LABEL_MISMATCH,
        ),
        ({}, {"status": TaskStatus.RUNNING}, RejectionReason.TASK_NOT_QUEUED),
        ({}, {"cancel_requested": True}, RejectionReason.TASK_CANCELLED),
    ],
)
def test_scheduler_returns_stable_rejection_reason(
    worker_overrides: dict[str, object],
    task_overrides: dict[str, object],
    reason: RejectionReason,
) -> None:
    decision = evaluate(_worker(**worker_overrides), _task(**task_overrides))

    assert decision.allowed is False
    assert decision.reason is reason


def test_required_labels_are_a_subset_of_worker_labels() -> None:
    assert labels_match({"region": "local"}, {"region": "local", "zone": "a"}) is True
    assert labels_match({"region": "local"}, {"region": "remote", "zone": "a"}) is False


def test_draining_worker_does_not_accept_new_tasks() -> None:
    assert worker_accepts_new_tasks(_worker(status=WorkerStatus.DRAINING)) is False


def test_global_scheduler_uses_free_not_total_gpu_memory() -> None:
    worker = WorkerSnapshot(
        id="gpu-worker",
        status=WorkerStatus.ONLINE,
        runtime_types=frozenset({"docker"}),
        running_tasks=0,
        concurrency=1,
        cpu_allocatable_millicores=4_000,
        reserved_cpu_millicores=0,
        memory_allocatable_mb=16_384,
        reserved_memory_mb=0,
        gpu_devices=(
            GPUDeviceSnapshot(
                id="device-1",
                uuid="GPU-1",
                model="A100",
                memory_total_mb=40_960,
                memory_free_mb=2_048,
            ),
        ),
    )
    task = TaskSnapshot(
        id="task-1",
        project_id="project-1",
        status=TaskStatus.QUEUED,
        runtime_type="docker",
        cpu_millicores=1_000,
        memory_mb=256,
        gpu_count=1,
        gpu_memory_mb=8_192,
    )

    placement, rejected = choose_placement(task, [worker])

    assert placement is None
    assert rejected == {"gpu-worker": RejectionReason.INSUFFICIENT_GPU_MEMORY}


def test_global_scheduler_gpu_best_fit_orders_by_free_memory() -> None:
    worker = WorkerSnapshot(
        id="gpu-worker",
        status=WorkerStatus.ONLINE,
        runtime_types=frozenset({"docker"}),
        running_tasks=0,
        concurrency=1,
        cpu_allocatable_millicores=4_000,
        reserved_cpu_millicores=0,
        memory_allocatable_mb=16_384,
        reserved_memory_mb=0,
        gpu_devices=(
            GPUDeviceSnapshot(
                id="smaller-total",
                uuid="GPU-1",
                model="A100",
                memory_total_mb=40_960,
                memory_free_mb=20_000,
            ),
            GPUDeviceSnapshot(
                id="smaller-free",
                uuid="GPU-2",
                model="A100",
                memory_total_mb=81_920,
                memory_free_mb=10_000,
            ),
        ),
    )
    task = TaskSnapshot(
        id="task-1",
        project_id="project-1",
        status=TaskStatus.QUEUED,
        runtime_type="docker",
        cpu_millicores=1_000,
        memory_mb=256,
        gpu_count=1,
        gpu_memory_mb=8_000,
    )

    placement, rejected = choose_placement(task, [worker])

    assert rejected == {}
    assert placement is not None
    assert placement.gpu_device_ids == ("smaller-free",)


def test_global_scheduler_routes_gpu_model_and_count_to_matching_workers() -> None:
    workers = [
        WorkerSnapshot(
            id="worker-a100",
            status=WorkerStatus.ONLINE,
            runtime_types=frozenset({"docker"}),
            running_tasks=0,
            concurrency=4,
            cpu_allocatable_millicores=32_000,
            reserved_cpu_millicores=0,
            memory_allocatable_mb=262_144,
            reserved_memory_mb=0,
            gpu_devices=tuple(
                GPUDeviceSnapshot(
                    id=f"a100-{index}",
                    uuid=f"GPU-A100-{index}",
                    model="A100",
                    memory_total_mb=81_920,
                )
                for index in range(4)
            ),
        ),
        WorkerSnapshot(
            id="worker-rtx4090",
            status=WorkerStatus.ONLINE,
            runtime_types=frozenset({"docker"}),
            running_tasks=0,
            concurrency=1,
            cpu_allocatable_millicores=16_000,
            reserved_cpu_millicores=0,
            memory_allocatable_mb=65_536,
            reserved_memory_mb=0,
            gpu_devices=(
                GPUDeviceSnapshot(
                    id="rtx4090-0",
                    uuid="GPU-RTX4090-0",
                    model="RTX4090",
                    memory_total_mb=24_576,
                ),
            ),
        ),
    ]

    a100_task = TaskSnapshot(
        id="task-a100",
        project_id="project-1",
        status=TaskStatus.QUEUED,
        runtime_type="docker",
        cpu_millicores=4_000,
        memory_mb=16_384,
        gpu_count=2,
        gpu_model="A100",
    )
    rtx4090_task = TaskSnapshot(
        id="task-rtx4090",
        project_id="project-1",
        status=TaskStatus.QUEUED,
        runtime_type="docker",
        cpu_millicores=2_000,
        memory_mb=8_192,
        gpu_count=1,
        gpu_model="RTX4090",
    )

    a100_placement, a100_rejected = choose_placement(a100_task, workers)
    rtx4090_placement, rtx4090_rejected = choose_placement(rtx4090_task, workers)

    assert a100_placement is not None
    assert a100_placement.worker_id == "worker-a100"
    assert a100_placement.gpu_device_ids == ("a100-0", "a100-1")
    assert a100_rejected == {"worker-rtx4090": RejectionReason.GPU_MODEL_MISMATCH}
    assert rtx4090_placement is not None
    assert rtx4090_placement.worker_id == "worker-rtx4090"
    assert rtx4090_placement.gpu_device_ids == ("rtx4090-0",)
    assert rtx4090_rejected == {"worker-a100": RejectionReason.GPU_MODEL_MISMATCH}
