import os
import socket
import subprocess
from dataclasses import dataclass

import psutil


@dataclass(frozen=True, slots=True)
class WorkerCapabilities:
    hostname: str
    cpu_count: int
    memory_total_mb: int
    gpu_count: int
    gpu_model: str | None
    gpu_memory_mb: int


def detect_capabilities() -> WorkerCapabilities:
    gpu_count, gpu_model, gpu_memory_mb = detect_gpus()
    return WorkerCapabilities(
        hostname=socket.gethostname(),
        cpu_count=os.cpu_count() or 1,
        memory_total_mb=max(1, int(psutil.virtual_memory().total / (1024 * 1024))),
        gpu_count=gpu_count,
        gpu_model=gpu_model,
        gpu_memory_mb=gpu_memory_mb,
    )


def detect_gpus() -> tuple[int, str | None, int]:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return 0, None, 0

    rows = [row.strip() for row in result.stdout.splitlines() if row.strip()]
    models: list[str] = []
    total_memory = 0
    for row in rows:
        model, separator, memory = row.rpartition(",")
        if not separator:
            continue
        models.append(model.strip())
        try:
            total_memory += int(memory.strip())
        except ValueError:
            continue
    if not models:
        return 0, None, 0
    return len(models), ", ".join(dict.fromkeys(models)), total_memory
