import json
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from core.config import Settings
from core.enums import AcceleratorKind, AcceleratorVendor
from core.runtime_profiles import RuntimeProfileCatalog
from worker.gpu_inventory import (
    AscendNpuSMIInventoryProvider,
    FakeGPUInventoryProvider,
    InventoryProviderRegistry,
    InventoryStatus,
    KubernetesNodeAcceleratorProvider,
    NoGPUInventoryProvider,
    NvidiaSMIInventoryProvider,
    bind_kubernetes_runtime_profiles,
    build_accelerator_inventory_registry,
    build_gpu_inventory_provider,
    parse_ascend_mapping,
    parse_kubernetes_node,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "inventory"
REPOSITORY_ROOT = Path(__file__).parents[2]


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_nvidia_inventory_preserves_details_and_reports_partial_fixture() -> None:
    completed = subprocess.CompletedProcess(
        args=["nvidia-smi"],
        returncode=0,
        stdout=_fixture("nvidia-smi.csv"),
    )
    runner = Mock(return_value=completed)

    result = NvidiaSMIInventoryProvider(runner=runner).discover()

    assert result.status == InventoryStatus.DEGRADED
    assert result.rejected_rows == 1
    assert [device.device_id for device in result.devices] == ["GPU-a", "GPU-b"]
    assert result.devices[0].device_index == 0
    assert result.devices[0].vendor == AcceleratorVendor.NVIDIA
    assert result.devices[0].kind == AcceleratorKind.GPU
    assert result.devices[0].model == "NVIDIA A100-SXM4-40GB"
    assert result.devices[0].memory_total_mb == 40_960
    assert result.devices[0].memory_free_mb == 32_000
    assert result.devices[0].compute_arch == "8.0"
    assert result.devices[0].fake is False


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (FileNotFoundError(), "command_not_found"),
        (subprocess.TimeoutExpired(cmd="nvidia-smi", timeout=5), "command_timeout"),
    ],
)
def test_nvidia_inventory_reports_unavailable_probe(
    failure: BaseException,
    message: str,
) -> None:
    result = NvidiaSMIInventoryProvider(runner=Mock(side_effect=failure)).discover()

    assert result.status == InventoryStatus.UNAVAILABLE
    assert result.devices == ()
    assert result.message == message


def test_nvidia_inventory_bounds_invalid_encoding_and_output_size() -> None:
    invalid_encoding = subprocess.CompletedProcess(
        args=["nvidia-smi"], returncode=0, stdout=b"\xff"
    )
    invalid_result = NvidiaSMIInventoryProvider(
        runner=Mock(return_value=invalid_encoding)
    ).discover()
    oversized = subprocess.CompletedProcess(
        args=["nvidia-smi"], returncode=0, stdout="x" * (1024 * 1024 + 1)
    )
    oversized_result = NvidiaSMIInventoryProvider(runner=Mock(return_value=oversized)).discover()

    assert invalid_result.status == InventoryStatus.UNAVAILABLE
    assert invalid_result.message == "invalid_utf8"
    assert oversized_result.status == InventoryStatus.UNAVAILABLE
    assert oversized_result.message == "output_too_large"


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

    result = NvidiaSMIInventoryProvider(runner=Mock(return_value=completed)).discover()

    assert result.status == InventoryStatus.DEGRADED
    assert result.rejected_rows == 2
    assert len(result.devices) == 1
    assert result.devices[0].device_id == "GPU-valid"
    assert result.devices[0].memory_free_mb == 100
    assert result.devices[0].compute_arch is None


def test_ascend_mapping_parser_accepts_column_reordering_and_partial_rows() -> None:
    standard = parse_ascend_mapping(_fixture("ascend-mapping-v24.txt"))
    reordered = parse_ascend_mapping(_fixture("ascend-mapping-reordered.txt"))

    assert [(entry.npu_id, entry.chip_id, entry.logic_id) for entry in standard.entries] == [
        (0, 0, 0),
        (1, 0, 1),
    ]
    assert standard.rejected_rows == 0
    assert [(entry.npu_id, entry.chip_id, entry.logic_id) for entry in reordered.entries] == [
        (4, 0, 3)
    ]
    assert reordered.rejected_rows == 1


def test_ascend_provider_combines_mapping_and_memory_queries() -> None:
    outputs = {
        ("npu-smi", "info", "-m"): _fixture("ascend-mapping-v24.txt"),
        ("npu-smi", "info", "-t", "memory", "-i", "0"): _fixture("ascend-memory-0.txt"),
        ("npu-smi", "info", "-t", "memory", "-i", "1"): _fixture("ascend-memory-1.txt"),
    }

    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout=outputs[tuple(command)])

    result = AscendNpuSMIInventoryProvider(runner=runner).discover()

    assert result.status == InventoryStatus.AVAILABLE
    assert [device.device_id for device in result.devices] == ["ASCEND-0-0", "ASCEND-1-0"]
    assert all(device.vendor == AcceleratorVendor.HUAWEI_ASCEND for device in result.devices)
    assert all(device.kind == AcceleratorKind.NPU for device in result.devices)
    assert all(device.health == "unknown" for device in result.devices)
    assert result.devices[0].memory_total_mb == 65_536
    assert result.devices[1].memory_free_mb == 63_000
    assert result.devices[0].compute_arch == "Ascend 910B1"


def test_ascend_provider_keeps_partial_devices_when_one_memory_query_fails() -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command == ["npu-smi", "info", "-m"]:
            return subprocess.CompletedProcess(
                command, 0, stdout=_fixture("ascend-mapping-v24.txt")
            )
        if command[-1] == "0":
            return subprocess.CompletedProcess(command, 0, stdout=_fixture("ascend-memory-0.txt"))
        raise subprocess.TimeoutExpired(command, timeout=5)

    result = AscendNpuSMIInventoryProvider(runner=runner).discover()

    assert result.status == InventoryStatus.DEGRADED
    assert [device.device_id for device in result.devices] == ["ASCEND-0-0"]
    assert result.rejected_rows == 1
    assert result.message is not None and "memory query" in result.message


def test_kubernetes_provider_reads_allocatable_labels_and_plugin_resources() -> None:
    node_json = _fixture("kubernetes-node.json")
    runner = Mock(
        return_value=subprocess.CompletedProcess(args=["kubectl"], returncode=0, stdout=node_json)
    )

    result = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node", runner=runner
    ).discover()

    assert result.status == InventoryStatus.AVAILABLE
    assert len(result.devices) == 6
    assert sum(device.vendor == AcceleratorVendor.NVIDIA for device in result.devices) == 2
    assert sum(device.vendor == AcceleratorVendor.HUAWEI_ASCEND for device in result.devices) == 4
    assert {device.kubernetes_resource_name for device in result.devices} == {
        "nvidia.com/gpu",
        "huawei.com/Ascend910",
    }
    assert all("kubernetes-capacity-slot" in device.capabilities for device in result.devices)
    assert all(device.health == "inventory-only" for device in result.devices)
    assert all(device.device_id.startswith("k8s-capacity:node-uid-1:") for device in result.devices)


def test_kubernetes_capacity_binds_only_exact_catalog_resource_contracts() -> None:
    node_json = _fixture("kubernetes-node.json")
    provider = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node",
        runner=Mock(
            return_value=subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=node_json
            )
        ),
    )
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")

    bound = bind_kubernetes_runtime_profiles(provider.list_devices(), catalog)

    nvidia = [device for device in bound if device.vendor == AcceleratorVendor.NVIDIA]
    ascend = [device for device in bound if device.vendor == AcceleratorVendor.HUAWEI_ASCEND]
    assert {device.runtime_profile_ids for device in nvidia} == {("nvidia-vllm-k8s",)}
    assert {device.runtime_profile_ids for device in ascend} == {("ascend-vllm-k8s-a2",)}
    assert all("streaming" in device.capabilities for device in bound)
    assert all("tensor-parallel" in device.capabilities for device in bound)
    assert all(device.health == "inventory-only" for device in bound)


def test_host_cli_inventory_is_not_inferred_as_device_plugin_capacity() -> None:
    provider = NvidiaSMIInventoryProvider(
        runner=Mock(
            return_value=subprocess.CompletedProcess(
                args=["nvidia-smi"],
                returncode=0,
                stdout="GPU-real, 0, NVIDIA A100, 40960, 40000, 8.0\n",
            )
        )
    )
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")

    bound = bind_kubernetes_runtime_profiles(provider.list_devices(), catalog)

    assert bound[0].runtime_profile_ids == ()
    assert bound[0].kubernetes_resource_name is None


def test_kubernetes_parser_rejects_capacity_without_memory_metadata() -> None:
    node = json.loads(_fixture("kubernetes-node.json"))
    del node["metadata"]["labels"]["mind-cluster/npu-chip-memory"]

    parsed = parse_kubernetes_node(node)

    assert len(parsed.devices) == 2
    assert all(device.vendor == AcceleratorVendor.NVIDIA for device in parsed.devices)
    assert parsed.rejected_rows == 4


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
    assert [device.device_index for device in first] == [0, 1]
    assert len({device.device_id for device in first}) == 2
    assert all(device.device_id.startswith("FAKE-") for device in first)
    assert all(device.vendor == AcceleratorVendor.NVIDIA for device in first)
    assert all(device.model == "NVIDIA-A100" for device in first)
    assert all(device.memory_free_mb == 40_960 for device in first)
    assert all(device.fake for device in first)


def test_fake_inventory_rejects_untyped_vendor_spoofing() -> None:
    with pytest.raises(TypeError, match="AcceleratorVendor"):
        FakeGPUInventoryProvider(
            count=1,
            model="spoofed",
            memory_mb=1,
            worker_id="worker-a",
            vendor="nvidia",  # type: ignore[arg-type]
        )


def test_no_accelerator_inventory_is_explicitly_available_and_empty() -> None:
    result = NoGPUInventoryProvider().discover()

    assert result.status == InventoryStatus.AVAILABLE
    assert result.devices == ()
    assert result.message == "accelerator inventory explicitly disabled"


def test_registry_aggregates_provider_status_without_hiding_unavailable() -> None:
    missing = NvidiaSMIInventoryProvider(runner=Mock(side_effect=FileNotFoundError()))
    fake = FakeGPUInventoryProvider(
        count=1,
        model="NVIDIA-A100",
        memory_mb=40_960,
        worker_id="registry-worker",
    )

    snapshot = InventoryProviderRegistry((missing, fake)).snapshot()

    assert snapshot.status == InventoryStatus.DEGRADED
    assert len(snapshot.devices) == 1
    assert snapshot.provider_results[0].status == InventoryStatus.UNAVAILABLE


def test_registry_rejects_same_vendor_device_index_from_multiple_providers() -> None:
    real = NvidiaSMIInventoryProvider(
        runner=Mock(
            return_value=subprocess.CompletedProcess(
                args=["nvidia-smi"],
                returncode=0,
                stdout="GPU-real, 0, NVIDIA A100, 40960, 40000, 8.0\n",
            )
        )
    )
    fake = FakeGPUInventoryProvider(
        count=1,
        model="NVIDIA-A100",
        memory_mb=40_960,
        worker_id="registry-worker",
    )

    with pytest.raises(ValueError, match="duplicate vendor device indexes"):
        InventoryProviderRegistry((real, fake)).snapshot()


def test_provider_factory_uses_fake_override_only_in_non_production() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        accelerator_inventory_providers="nvidia-smi,ascend-npu-smi",
        fake_gpu_count=2,
        fake_gpu_model="NVIDIA-A100",
        fake_gpu_memory_mb=40_960,
    )

    provider = build_gpu_inventory_provider(settings, worker_id="worker-test")

    assert isinstance(provider, FakeGPUInventoryProvider)
    assert len(provider.list_devices()) == 2


def test_registry_factory_preserves_explicit_provider_order() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        accelerator_inventory_providers="ascend-npu-smi,nvidia-smi",
    )

    registry = build_accelerator_inventory_registry(settings, worker_id="worker-test")

    assert [provider.name for provider in registry.providers] == [
        "ascend-npu-smi",
        "nvidia-smi",
    ]


def test_settings_reject_fake_gpu_inventory_in_production() -> None:
    with pytest.raises(ValidationError, match="FAKE_GPU_COUNT must be zero in production"):
        Settings(
            _env_file=None,
            app_env="production",
            legacy_anonymous_enabled=False,
            fake_gpu_count=1,
        )


def test_settings_reject_explicit_fake_provider_in_production() -> None:
    with pytest.raises(
        ValidationError, match="fake accelerator inventory is forbidden in production"
    ):
        Settings(
            _env_file=None,
            app_env="production",
            legacy_anonymous_enabled=False,
            accelerator_inventory_providers="fake",
        )


@pytest.mark.parametrize(
    "providers",
    ["unknown", "none,nvidia-smi", "fake,ascend-npu-smi", "nvidia-smi,nvidia-smi"],
)
def test_settings_reject_invalid_provider_sets(providers: str) -> None:
    with pytest.raises(ValidationError, match="ACCELERATOR_INVENTORY_PROVIDERS"):
        Settings(
            _env_file=None,
            app_env="test",
            accelerator_inventory_providers=providers,
        )
