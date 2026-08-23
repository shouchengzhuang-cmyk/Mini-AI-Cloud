from dataclasses import dataclass
from enum import StrEnum

from core.enums import TaskStatus, WorkerStatus
from models.task import Task
from models.worker import Worker


class RejectionReason(StrEnum):
    """Stable reasons why a worker cannot claim a task."""

    TASK_NOT_FOUND = "task_not_found"
    TASK_NOT_QUEUED = "task_not_queued"
    TASK_CANCELLED = "task_cancelled"
    WORKER_NOT_FOUND = "worker_not_found"
    WORKER_NOT_ONLINE = "worker_not_online"
    WORKER_AT_CAPACITY = "worker_at_capacity"
    INSUFFICIENT_CPU = "insufficient_cpu"
    INSUFFICIENT_MEMORY = "insufficient_memory"
    INSUFFICIENT_GPU = "insufficient_gpu"
    LABEL_MISMATCH = "label_mismatch"


@dataclass(frozen=True, slots=True)
class SchedulingDecision:
    allowed: bool
    reason: RejectionReason | None = None


def labels_match(required: dict[str, str], offered: dict[str, str]) -> bool:
    """Return whether every task label is provided by the worker."""

    return all(offered.get(key) == value for key, value in required.items())


def worker_accepts_new_tasks(worker: Worker | None) -> bool:
    """Check worker-wide conditions that are independent of a particular task."""

    return (
        worker is not None
        and worker.status == WorkerStatus.ONLINE
        and worker.running_tasks < worker.concurrency
    )


def evaluate(worker: Worker | None, task: Task | None) -> SchedulingDecision:
    """Evaluate a task against a worker before the authoritative atomic claim.

    This is intentionally a fast pre-check. ``TaskRepository.claim`` repeats the
    important checks while holding database locks and remains the final authority.
    """

    if worker is None:
        return SchedulingDecision(False, RejectionReason.WORKER_NOT_FOUND)
    if worker.status != WorkerStatus.ONLINE:
        return SchedulingDecision(False, RejectionReason.WORKER_NOT_ONLINE)
    if worker.running_tasks >= worker.concurrency:
        return SchedulingDecision(False, RejectionReason.WORKER_AT_CAPACITY)
    if task is None:
        return SchedulingDecision(False, RejectionReason.TASK_NOT_FOUND)
    if task.status != TaskStatus.QUEUED:
        return SchedulingDecision(False, RejectionReason.TASK_NOT_QUEUED)
    if task.cancel_requested:
        return SchedulingDecision(False, RejectionReason.TASK_CANCELLED)
    if float(worker.reserved_cpu or 0) + task.cpu_limit > worker.cpu_count:
        return SchedulingDecision(False, RejectionReason.INSUFFICIENT_CPU)
    if int(worker.reserved_memory_mb or 0) + task.memory_limit_mb > worker.memory_total_mb:
        return SchedulingDecision(False, RejectionReason.INSUFFICIENT_MEMORY)
    if int(worker.reserved_gpus or 0) + task.gpu_count > worker.gpu_count:
        return SchedulingDecision(False, RejectionReason.INSUFFICIENT_GPU)
    if not labels_match(task.labels, worker.labels):
        return SchedulingDecision(False, RejectionReason.LABEL_MISMATCH)
    return SchedulingDecision(True)
