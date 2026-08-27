import csv
import io
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from core.config import Settings


@dataclass(frozen=True, slots=True)
class GPUDevice:
    uuid: str
    index: int
    vendor: str
    model: str
    memory_total_mb: int
    memory_free_mb: int
    compute_capability: str | None
    fake: bool = False


class GPUInventoryProvider(Protocol):
    def list_devices(self) -> tuple[GPUDevice, ...]: ...


Runner = Callable[..., subprocess.CompletedProcess[str]]


class NvidiaSMIInventoryProvider:
    """Discover individual NVIDIA devices without requiring NVML bindings."""

    QUERY_FIELDS = (
        "uuid",
        "index",
        "name",
        "memory.total",
        "memory.free",
        "compute_cap",
    )

    def __init__(self, *, runner: Runner | None = None, timeout: float = 5.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._runner = runner
        self.timeout = timeout

    def list_devices(self) -> tuple[GPUDevice, ...]:
        runner = self._runner or subprocess.run
        try:
            result = runner(
                [
                    "nvidia-smi",
                    f"--query-gpu={','.join(self.QUERY_FIELDS)}",
                    "--format=csv,noheader,nounits",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            return ()

        devices: list[GPUDevice] = []
        for row in csv.reader(io.StringIO(result.stdout), skipinitialspace=True):
            if len(row) != len(self.QUERY_FIELDS):
                continue
            raw_uuid, raw_index, raw_model, raw_total, raw_free, raw_capability = (
                item.strip() for item in row
            )
            try:
                index = int(raw_index)
                memory_total_mb = int(raw_total)
                memory_free_mb = int(raw_free)
            except ValueError:
                continue
            if not raw_uuid or not raw_model or index < 0 or memory_total_mb <= 0:
                continue
            devices.append(
                GPUDevice(
                    uuid=raw_uuid,
                    index=index,
                    vendor="nvidia",
                    model=raw_model,
                    memory_total_mb=memory_total_mb,
                    memory_free_mb=min(memory_total_mb, max(0, memory_free_mb)),
                    compute_capability=(
                        raw_capability if raw_capability and raw_capability != "N/A" else None
                    ),
                )
            )
        return tuple(sorted(devices, key=lambda device: (device.index, device.uuid)))


class FakeGPUInventoryProvider:
    """Deterministic development/test inventory that never probes the host."""

    def __init__(
        self,
        *,
        count: int,
        model: str,
        memory_mb: int,
        worker_id: str,
        compute_capability: str = "0.0",
    ) -> None:
        if count < 0:
            raise ValueError("count must not be negative")
        if memory_mb <= 0:
            raise ValueError("memory_mb must be greater than zero")
        if not model.strip():
            raise ValueError("model must not be blank")
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        self.count = count
        self.model = model.strip()
        self.memory_mb = memory_mb
        self.worker_id = worker_id.strip()
        self.compute_capability = compute_capability

    def list_devices(self) -> tuple[GPUDevice, ...]:
        devices = []
        for index in range(self.count):
            device_uuid = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"mini-ai-cloud://fake-gpu/{self.worker_id}/{index}",
            )
            devices.append(
                GPUDevice(
                    uuid=f"FAKE-{device_uuid}",
                    index=index,
                    # A fake device simulates NVIDIA GPU inventory. ``fake`` is
                    # evidence provenance, not a third hardware vendor.
                    vendor="nvidia",
                    model=self.model,
                    memory_total_mb=self.memory_mb,
                    memory_free_mb=self.memory_mb,
                    compute_capability=self.compute_capability,
                    fake=True,
                )
            )
        return tuple(devices)


class NoGPUInventoryProvider:
    def list_devices(self) -> tuple[GPUDevice, ...]:
        return ()


def build_gpu_inventory_provider(settings: Settings, *, worker_id: str) -> GPUInventoryProvider:
    """Select an inventory provider and defensively reject production fake GPUs."""

    if settings.fake_gpu_count:
        if settings.app_env == "production":
            raise ValueError("fake GPU inventory is forbidden in production")
        return FakeGPUInventoryProvider(
            count=settings.fake_gpu_count,
            model=settings.fake_gpu_model,
            memory_mb=settings.fake_gpu_memory_mb,
            worker_id=worker_id,
        )
    return NvidiaSMIInventoryProvider()
