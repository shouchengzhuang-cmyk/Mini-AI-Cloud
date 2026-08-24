from dataclasses import dataclass, field
from datetime import UTC, datetime
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
    RUNTIME_MISMATCH = "runtime_mismatch"
    TAINT_NOT_TOLERATED = "taint_not_tolerated"
    GPU_MODEL_MISMATCH = "gpu_model_mismatch"
    INSUFFICIENT_GPU_MEMORY = "insufficient_gpu_memory"
    WORKER_OVERCOMMITTED = "worker_overcommitted"


@dataclass(frozen=True, slots=True)
class SchedulingDecision:
    allowed: bool
    reason: RejectionReason | None = None


@dataclass(frozen=True, slots=True)
class GPUDeviceSnapshot:
    id: str
    uuid: str
    model: str
    memory_total_mb: int
    memory_free_mb: int | None = None
    healthy: bool = True
    allocated: bool = False


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    id: str
    status: WorkerStatus
    runtime_types: frozenset[str]
    running_tasks: int
    concurrency: int
    cpu_allocatable_millicores: int
    reserved_cpu_millicores: int
    memory_allocatable_mb: int
    reserved_memory_mb: int
    labels: dict[str, str] = field(default_factory=dict)
    taints: tuple[dict[str, str], ...] = ()
    gpu_devices: tuple[GPUDeviceSnapshot, ...] = ()
    overcommitted: bool = False


@dataclass(frozen=True, slots=True)
class TaskSnapshot:
    id: str
    project_id: str
    status: TaskStatus
    runtime_type: str
    cpu_millicores: int
    memory_mb: int
    gpu_count: int
    gpu_memory_mb: int = 0
    gpu_model: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    tolerations: tuple[dict[str, str], ...] = ()
    priority: int = 50
    queued_at: datetime | None = None
    queue_order: int = 0


@dataclass(frozen=True, slots=True)
class Placement:
    worker_id: str
    gpu_device_ids: tuple[str, ...]
    score: tuple[float, ...]


def effective_priority(task: TaskSnapshot, *, now: datetime, aging_interval_seconds: int) -> int:
    if aging_interval_seconds < 1:
        raise ValueError("aging_interval_seconds must be at least one")
    if task.queued_at is None:
        return task.priority
    queued_at = task.queued_at
    if queued_at.tzinfo is None:
        queued_at = queued_at.replace(tzinfo=UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    waited = max(0.0, (now - queued_at).total_seconds())
    return min(100, task.priority + int(waited // aging_interval_seconds))


def choose_placement(
    task: TaskSnapshot,
    workers: list[WorkerSnapshot],
    *,
    policy: str = "binpack",
) -> tuple[Placement | None, dict[str, RejectionReason]]:
    """Choose a deterministic placement from immutable scheduler snapshots."""

    if policy not in {"binpack", "spread"}:
        raise ValueError("policy must be 'binpack' or 'spread'")
    placements: list[Placement] = []
    rejected: dict[str, RejectionReason] = {}
    for worker in workers:
        reason, device_ids = evaluate_snapshot(worker, task)
        if reason is not None:
            rejected[worker.id] = reason
            continue
        cpu_after = worker.reserved_cpu_millicores + task.cpu_millicores
        memory_after = worker.reserved_memory_mb + task.memory_mb
        cpu_fraction = cpu_after / max(1, worker.cpu_allocatable_millicores)
        memory_fraction = memory_after / max(1, worker.memory_allocatable_mb)
        gpu_fraction = (
            (sum(device.allocated for device in worker.gpu_devices) + task.gpu_count)
            / max(1, len(worker.gpu_devices))
            if worker.gpu_devices
            else 0.0
        )
        dominant = max(cpu_fraction, memory_fraction, gpu_fraction)
        scarce_gpu_penalty = 1.0 if task.gpu_count == 0 and worker.gpu_devices else 0.0
        score: tuple[float, ...]
        if policy == "spread":
            score = (dominant, scarce_gpu_penalty, float(worker.running_tasks))
        else:
            # Higher utilization is better for bin-packing, so negate the score.
            score = (scarce_gpu_penalty, -dominant, -cpu_fraction, -memory_fraction)
        placements.append(Placement(worker_id=worker.id, gpu_device_ids=device_ids, score=score))
    if not placements:
        return None, rejected
    placements.sort(key=lambda item: (*item.score, item.worker_id, item.gpu_device_ids))
    return placements[0], rejected


def evaluate_snapshot(
    worker: WorkerSnapshot, task: TaskSnapshot
) -> tuple[RejectionReason | None, tuple[str, ...]]:
    if worker.status != WorkerStatus.ONLINE:
        return RejectionReason.WORKER_NOT_ONLINE, ()
    if worker.overcommitted:
        return RejectionReason.WORKER_OVERCOMMITTED, ()
    if worker.running_tasks >= worker.concurrency:
        return RejectionReason.WORKER_AT_CAPACITY, ()
    if task.runtime_type not in worker.runtime_types:
        return RejectionReason.RUNTIME_MISMATCH, ()
    if worker.reserved_cpu_millicores + task.cpu_millicores > worker.cpu_allocatable_millicores:
        return RejectionReason.INSUFFICIENT_CPU, ()
    if worker.reserved_memory_mb + task.memory_mb > worker.memory_allocatable_mb:
        return RejectionReason.INSUFFICIENT_MEMORY, ()
    if not labels_match(task.labels, worker.labels):
        return RejectionReason.LABEL_MISMATCH, ()
    if not _taints_tolerated(worker.taints, task.tolerations):
        return RejectionReason.TAINT_NOT_TOLERATED, ()

    available = [device for device in worker.gpu_devices if device.healthy and not device.allocated]
    if task.gpu_model:
        model_devices = [device for device in available if device.model == task.gpu_model]
        if len(model_devices) < task.gpu_count:
            return RejectionReason.GPU_MODEL_MISMATCH, ()
        available = model_devices
    if task.gpu_memory_mb:
        memory_devices = [
            device for device in available if _available_gpu_memory_mb(device) >= task.gpu_memory_mb
        ]
        if len(memory_devices) < task.gpu_count:
            return RejectionReason.INSUFFICIENT_GPU_MEMORY, ()
        available = memory_devices
    if len(available) < task.gpu_count:
        return RejectionReason.INSUFFICIENT_GPU, ()
    # Best-fit keeps larger devices available for workloads that need them.
    available.sort(
        key=lambda device: (
            _available_gpu_memory_mb(device),
            device.memory_total_mb,
            device.uuid,
        )
    )
    return None, tuple(device.id for device in available[: task.gpu_count])


def _available_gpu_memory_mb(device: GPUDeviceSnapshot) -> int:
    return device.memory_total_mb if device.memory_free_mb is None else device.memory_free_mb


def dominant_share(
    *,
    cpu_millicores: int,
    memory_mb: int,
    gpus: int,
    cluster_cpu_millicores: int,
    cluster_memory_mb: int,
    cluster_gpus: int,
) -> float:
    shares = (
        cpu_millicores / max(1, cluster_cpu_millicores),
        memory_mb / max(1, cluster_memory_mb),
        gpus / max(1, cluster_gpus) if cluster_gpus else (1.0 if gpus else 0.0),
    )
    return max(shares)


def _taints_tolerated(
    taints: tuple[dict[str, str], ...], tolerations: tuple[dict[str, str], ...]
) -> bool:
    for taint in taints:
        if taint.get("effect", "NoSchedule") != "NoSchedule":
            continue
        if not any(
            tolerance.get("key") == taint.get("key")
            and tolerance.get("value", taint.get("value")) == taint.get("value")
            for tolerance in tolerations
        ):
            return False
    return True


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
