import subprocess
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from core.config import Settings
from worker.gpu_inventory import (
    FakeGPUInventoryProvider,
    NoGPUInventoryProvider,
    NvidiaSMIInventoryProvider,
    build_gpu_inventory_provider,
)


def test_nvidia_inventory_preserves_per_device_details_and_stable_order() -> None:
    completed = subprocess.CompletedProcess(
        args=["nvidia-smi"],
        returncode=0,
        stdout=(
            "GPU-b, 1, NVIDIA A100-SXM4-40GB, 40960, 32000, 8.0\n"
            "GPU-a, 0, NVIDIA RTX 4090, 24564, 22000, 8.9\n"
        ),
    )
    runner = Mock(return_value=completed)

    devices = NvidiaSMIInventoryProvider(runner=runner).list_devices()

    assert [device.uuid for device in devices] == ["GPU-a", "GPU-b"]
    assert devices[0].index == 0
    assert devices[0].vendor == "nvidia"
    assert devices[0].model == "NVIDIA RTX 4090"
    assert devices[0].memory_total_mb == 24_564
    assert devices[0].memory_free_mb == 22_000
    assert devices[0].compute_capability == "8.9"
    assert devices[0].fake is False


def test_nvidia_inventory_degrades_to_empty_when_probe_is_unavailable() -> None:
    runner = Mock(side_effect=FileNotFoundError())

    assert NvidiaSMIInventoryProvider(runner=runner).list_devices() == ()


def test_nvidia_inventory_skips_malformed_rows_and_clamps_free_memory() -> None:
    completed = subprocess.CompletedProcess(
        args=["nvidia-smi"],
        returncode=0,
        stdout=(
            "malformed\n"
            "GPU-bad-index, x, NVIDIA Test, 100, 50, 9.0\n"
            "GPU-valid, 2, NVIDIA Test, 100, 150, N/A\n"
        ),
    )

    devices = NvidiaSMIInventoryProvider(runner=Mock(return_value=completed)).list_devices()

    assert len(devices) == 1
    assert devices[0].uuid == "GPU-valid"
    assert devices[0].memory_free_mb == 100
    assert devices[0].compute_capability is None


def test_fake_inventory_is_deterministic_and_reports_individual_devices() -> None:
    provider = FakeGPUInventoryProvider(
        count=2,
        model="NVIDIA-A100",
        memory_mb=40_960,
        worker_id="worker-a",
        compute_capability="8.0",
    )

    first = provider.list_devices()
    second = provider.list_devices()

    assert first == second
    assert [device.index for device in first] == [0, 1]
    assert len({device.uuid for device in first}) == 2
    assert all(device.uuid.startswith("FAKE-") for device in first)
    assert all(device.model == "NVIDIA-A100" for device in first)
    assert all(device.memory_free_mb == 40_960 for device in first)
    assert all(device.fake for device in first)


def test_no_gpu_inventory_is_empty() -> None:
    assert NoGPUInventoryProvider().list_devices() == ()


def test_provider_factory_uses_fake_only_in_non_production() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        fake_gpu_count=2,
        fake_gpu_model="NVIDIA-A100",
        fake_gpu_memory_mb=40_960,
    )

    provider = build_gpu_inventory_provider(settings, worker_id="worker-test")

    assert isinstance(provider, FakeGPUInventoryProvider)
    assert len(provider.list_devices()) == 2


def test_settings_reject_fake_gpu_inventory_in_production() -> None:
    with pytest.raises(ValidationError, match="FAKE_GPU_COUNT must be zero in production"):
        Settings(
            _env_file=None,
            app_env="production",
            legacy_anonymous_enabled=False,
            fake_gpu_count=1,
        )
