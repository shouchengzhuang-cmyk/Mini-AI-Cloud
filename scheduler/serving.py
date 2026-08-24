from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ServingPlacementReason(StrEnum):
    INSUFFICIENT_CONTIGUOUS_GPUS = "INSUFFICIENT_CONTIGUOUS_GPUS"
    GPU_MODEL_MISMATCH = "GPU_MODEL_MISMATCH"
    INSUFFICIENT_GPU_MEMORY = "INSUFFICIENT_GPU_MEMORY"


@dataclass(frozen=True, slots=True)
class ServingGPUDeviceSnapshot:
    uuid: str
    index: int
    model: str
    memory_free_mb: int
    fake: bool = False


@dataclass(frozen=True, slots=True)
class ServingWorkerSnapshot:
    id: str
    gpu_devices: tuple[ServingGPUDeviceSnapshot, ...]


@dataclass(frozen=True, slots=True)
class ServingPlacementRequest:
    gpu_count: int
    gpu_model: str | None = None
    gpu_memory_mb: int = 0
    allow_fake: bool = False


@dataclass(frozen=True, slots=True)
class ServingPlacement:
    worker_id: str
    gpu_device_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ServingPlacementExplain:
    reason: ServingPlacementReason
    requested_gpu_count: int
    largest_available_worker_gpu_count: int
    requested_gpu_model: str | None
    required_gpu_memory_mb: int

    def details(self) -> dict[str, object]:
        return {
            "reason": self.reason.value,
            "requested_gpu_count": self.requested_gpu_count,
            "largest_available_worker_gpu_count": self.largest_available_worker_gpu_count,
            "requested_gpu_model": self.requested_gpu_model,
            "required_gpu_memory_mb": self.required_gpu_memory_mb,
        }


def choose_single_node_gang_placement(
    request: ServingPlacementRequest,
    workers: tuple[ServingWorkerSnapshot, ...],
) -> tuple[ServingPlacement | None, ServingPlacementExplain | None]:
    """Choose all requested GPUs from one Worker, or allocate none.

    This helper is deliberately topology-light. It establishes the Phase III
    single-node tensor-parallel invariant while leaving NVLink/PCIe scoring as a
    future extension point.
    """

    if request.gpu_count < 0 or request.gpu_memory_mb < 0:
        raise ValueError("serving GPU requirements must be non-negative")
    if request.gpu_model is not None and not request.gpu_model.strip():
        raise ValueError("gpu_model must not be blank")
    if request.gpu_count == 0:
        if not workers:
            return None, _explain(request, ServingPlacementReason.INSUFFICIENT_CONTIGUOUS_GPUS, 0)
        return ServingPlacement(
            worker_id=min(worker.id for worker in workers), gpu_device_ids=()
        ), None

    available_by_worker = {
        worker.id: tuple(
            device for device in worker.gpu_devices if request.allow_fake or not device.fake
        )
        for worker in workers
    }
    largest_available = max((len(devices) for devices in available_by_worker.values()), default=0)
    if largest_available < request.gpu_count:
        return None, _explain(
            request,
            ServingPlacementReason.INSUFFICIENT_CONTIGUOUS_GPUS,
            largest_available,
        )

    model_by_worker = {
        worker_id: tuple(
            device
            for device in devices
            if request.gpu_model is None or device.model == request.gpu_model
        )
        for worker_id, devices in available_by_worker.items()
    }
    largest_model = max((len(devices) for devices in model_by_worker.values()), default=0)
    if largest_model < request.gpu_count:
        return None, _explain(
            request,
            ServingPlacementReason.GPU_MODEL_MISMATCH,
            largest_model,
        )

    eligible_by_worker = {
        worker_id: tuple(
            device for device in devices if device.memory_free_mb >= request.gpu_memory_mb
        )
        for worker_id, devices in model_by_worker.items()
    }
    largest_eligible = max((len(devices) for devices in eligible_by_worker.values()), default=0)
    if largest_eligible < request.gpu_count:
        return None, _explain(
            request,
            ServingPlacementReason.INSUFFICIENT_GPU_MEMORY,
            largest_eligible,
        )

    candidates: list[tuple[tuple[int, int, str, tuple[str, ...]], ServingPlacement]] = []
    for worker_id, devices in eligible_by_worker.items():
        if len(devices) < request.gpu_count:
            continue
        ordered = sorted(
            devices,
            key=lambda device: (device.memory_free_mb, device.index, device.uuid),
        )
        selected = tuple(ordered[: request.gpu_count])
        device_ids = tuple(device.uuid for device in selected)
        score = (
            len(devices) - request.gpu_count,
            sum(device.memory_free_mb for device in selected),
            worker_id,
            device_ids,
        )
        candidates.append((score, ServingPlacement(worker_id=worker_id, gpu_device_ids=device_ids)))
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1], None


def _explain(
    request: ServingPlacementRequest,
    reason: ServingPlacementReason,
    largest_available: int,
) -> ServingPlacementExplain:
    return ServingPlacementExplain(
        reason=reason,
        requested_gpu_count=request.gpu_count,
        largest_available_worker_gpu_count=largest_available,
        requested_gpu_model=request.gpu_model,
        required_gpu_memory_mb=request.gpu_memory_mb,
    )
