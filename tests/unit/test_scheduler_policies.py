import uuid

import pytest

from core.enums import TaskStatus, WorkerStatus
from models.task import Task
from models.worker import Worker
from scheduler.policies import RejectionReason, evaluate, labels_match, worker_accepts_new_tasks


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
