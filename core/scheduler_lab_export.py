"""Fail-closed Mini AI Cloud v2 file exports for GPU Scheduler Lab."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.accelerators import AcceleratorRequest
from core.enums import AcceleratorSelectionPolicy, WorkerStatus
from models.scheduling import GPUDevice
from models.task import Task
from models.worker import Worker

CONTRACT_VERSION = "mini-ai-cloud.gpu-scheduler-lab/v2"


class SchedulerLabExportError(ValueError):
    """Raised when persisted Mini data cannot be represented by the v2 contract."""


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise SchedulerLabExportError(f"{field} must be non-blank canonical text")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise SchedulerLabExportError(f"{field} must not contain control characters")
    return value


def _text_mapping(value: object, *, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise SchedulerLabExportError(f"{field} must be a string mapping")
    result: dict[str, str] = {}
    for key, item in value.items():
        result[_text(key, field=f"{field} key")] = _text(item, field=f"{field}.{key}")
    return dict(sorted(result.items()))


def _text_list(value: object, *, field: str) -> list[str]:
    if not isinstance(value, list):
        raise SchedulerLabExportError(f"{field} must be a list")
    result = [_text(item, field=f"{field}[{index}]") for index, item in enumerate(value)]
    if len(result) != len(set(result)):
        raise SchedulerLabExportError(f"{field} must not contain duplicates")
    return sorted(result)


def _device_payload(device: GPUDevice) -> dict[str, object]:
    if device.fake:
        raise SchedulerLabExportError(
            f"gpu device {device.device_uuid!r} is marked fake and cannot be exported for X0"
        )
    vendor = _text(device.vendor, field=f"gpu device {device.device_uuid!r}.vendor")
    kind = _text(device.accelerator_kind, field=f"gpu device {device.device_uuid!r}.kind")
    if (vendor, kind) not in {("nvidia", "gpu"), ("huawei-ascend", "npu")}:
        raise SchedulerLabExportError(
            f"gpu device {device.device_uuid!r} has unsupported vendor/kind {vendor!r}/{kind!r}"
        )
    if device.memory_total_mb <= 0:
        raise SchedulerLabExportError(
            f"gpu device {device.device_uuid!r}.memory_total_mb must be positive"
        )
    return {
        "device_uuid": _text(device.device_uuid, field="gpu device.device_uuid"),
        "vendor": vendor,
        "kind": kind,
        "model": _text(device.model, field=f"gpu device {device.device_uuid!r}.model"),
        "memory_total_mb": device.memory_total_mb,
        "health": _text(device.health, field=f"gpu device {device.device_uuid!r}.health"),
        "runtime_profiles": _text_list(
            device.runtime_profile_ids,
            field=f"gpu device {device.device_uuid!r}.runtime_profile_ids",
        ),
        "capabilities": _text_list(
            device.capabilities_json,
            field=f"gpu device {device.device_uuid!r}.capabilities_json",
        ),
    }


def _task_accelerator(task: Task) -> AcceleratorRequest | None:
    if task.accelerator_request_json is None:
        if task.gpu_count:
            raise SchedulerLabExportError(
                f"task {task.id} has GPU count but no typed accelerator request; "
                "v1 fallback is forbidden"
            )
        return None
    try:
        request = AcceleratorRequest.model_validate(task.accelerator_request_json)
    except ValueError as exc:
        raise SchedulerLabExportError(
            f"task {task.id} has an invalid typed accelerator request"
        ) from exc
    if request.count != task.gpu_count:
        raise SchedulerLabExportError(
            f"task {task.id} accelerator count does not match persisted gpu_count"
        )
    if request.memory_mb_per_device != task.gpu_memory_mb:
        raise SchedulerLabExportError(
            f"task {task.id} accelerator memory does not match persisted gpu_memory_mb"
        )
    if request.selection_policy is not AcceleratorSelectionPolicy.ANY:
        raise SchedulerLabExportError(
            f"task {task.id} uses {request.selection_policy.value!r}; "
            "Scheduler v2 accepts only 'any'"
        )
    return request


def _task_payload(task: Task) -> dict[str, object]:
    request = _task_accelerator(task)
    common = {
        "id": str(task.id),
        "project_id": str(task.project_id),
        "arrival_time": task.queued_at.isoformat()
        if task.queued_at
        else task.created_at.isoformat(),
        "duration_seconds": task.timeout_seconds,
        "priority": task.priority,
        "workload_type": task.workload_type.value,
        "labels": _text_mapping(task.labels, field=f"task {task.id}.labels"),
    }
    if request is None:
        return common | {
            "gpu_count": 0,
            "allowed_vendors": [],
            "allowed_kinds": [],
            "allowed_models": [],
            "required_capabilities": [],
            "runtime_profile": None,
            "selection_policy": "any",
        }
    return common | {
        "gpu_count": request.count,
        "gpu_memory_mb": request.memory_mb_per_device,
        "allowed_vendors": sorted(vendor.value for vendor in request.allowed_vendors),
        "allowed_kinds": sorted(kind.value for kind in request.allowed_kinds),
        "allowed_models": sorted(request.allowed_models),
        "required_capabilities": sorted(request.required_capabilities),
        "runtime_profile": request.runtime_profile,
        "selection_policy": request.selection_policy.value,
    }


def build_v2_export(
    *,
    workers: Iterable[Worker],
    devices: Iterable[GPUDevice],
    tasks: Iterable[Task],
    producer: str,
) -> dict[str, object]:
    """Build a canonical v2 export from a consistent Mini database snapshot."""

    worker_rows = list(workers)
    grouped_devices: dict[str, list[GPUDevice]] = {}
    for device in devices:
        grouped_devices.setdefault(device.worker_id, []).append(device)
    worker_payloads: list[dict[str, object]] = []
    for worker in sorted(worker_rows, key=lambda item: item.id):
        worker_payloads.append(
            {
                "id": _text(worker.id, field="worker.id"),
                "schedulable": worker.status is WorkerStatus.ONLINE and not worker.overcommitted,
                "labels": _text_mapping(worker.labels, field=f"worker {worker.id}.labels"),
                "gpu_devices": [
                    _device_payload(device)
                    for device in sorted(
                        grouped_devices.get(worker.id, []),
                        key=lambda item: (
                            item.vendor,
                            item.device_index,
                            item.device_uuid,
                            str(item.id),
                        ),
                    )
                ],
            }
        )
    known_workers = {worker.id for worker in worker_rows}
    orphaned = sorted(worker_id for worker_id in grouped_devices if worker_id not in known_workers)
    if orphaned:
        raise SchedulerLabExportError(
            f"export found devices for missing workers: {', '.join(orphaned)}"
        )
    return {
        "contract_version": CONTRACT_VERSION,
        "producer": _text(producer, field="producer"),
        "workers": worker_payloads,
        "tasks": [
            _task_payload(task)
            for task in sorted(tasks, key=lambda item: (item.created_at, str(item.id)))
        ],
    }


async def export_v2_from_session(session: AsyncSession, *, producer: str) -> dict[str, object]:
    """Read a repeatable, read-only view of Mini state and render the v2 payload."""

    workers = list((await session.scalars(select(Worker).order_by(Worker.id))).all())
    devices = list(
        (
            await session.scalars(
                select(GPUDevice).order_by(
                    GPUDevice.worker_id,
                    GPUDevice.vendor,
                    GPUDevice.device_index,
                    GPUDevice.device_uuid,
                    GPUDevice.id,
                )
            )
        ).all()
    )
    tasks = list((await session.scalars(select(Task).order_by(Task.created_at, Task.id))).all())
    return build_v2_export(workers=workers, devices=devices, tasks=tasks, producer=producer)
