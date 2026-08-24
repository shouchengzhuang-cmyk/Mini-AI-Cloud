import os
import socket
import subprocess
from dataclasses import dataclass

import psutil

from worker.gpu_inventory import GPUDevice, GPUInventoryProvider, NvidiaSMIInventoryProvider


@dataclass(frozen=True, slots=True)
class WorkerCapabilities:
    hostname: str
    cpu_count: int
    memory_total_mb: int
    gpu_count: int
    gpu_model: str | None
    gpu_memory_mb: int


def detect_capabilities(
    gpu_provider: GPUInventoryProvider | None = None,
) -> WorkerCapabilities:
    gpu_count, gpu_model, gpu_memory_mb = detect_gpus(gpu_provider)
    return WorkerCapabilities(
        hostname=socket.gethostname(),
        cpu_count=os.cpu_count() or 1,
        memory_total_mb=max(1, int(psutil.virtual_memory().total / (1024 * 1024))),
        gpu_count=gpu_count,
        gpu_model=gpu_model,
        gpu_memory_mb=gpu_memory_mb,
    )


def detect_gpu_devices(
    provider: GPUInventoryProvider | None = None,
) -> tuple[GPUDevice, ...]:
    resolved = provider or NvidiaSMIInventoryProvider(runner=subprocess.run)
    return resolved.list_devices()


def detect_gpus(
    provider: GPUInventoryProvider | None = None,
) -> tuple[int, str | None, int]:
    devices = detect_gpu_devices(provider)
    if not devices:
        return 0, None, 0
    models = ", ".join(dict.fromkeys(device.model for device in devices))
    total_memory = sum(device.memory_total_mb for device in devices)
    return len(devices), models, total_memory
