import asyncio
import json
import subprocess
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

import worker.gpu_inventory as gpu_inventory
from core.config import Settings
from core.enums import AcceleratorKind, AcceleratorVendor
from core.runtime_profiles import (
    KubernetesNodeSelectorRequirement,
    RuntimeProfileCatalog,
    runtime_profile_binding_id,
)
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


def _binding(catalog: RuntimeProfileCatalog, identity: str) -> str:
    entry = next(item for item in catalog.manifest.profiles if item.identity == identity)
    return runtime_profile_binding_id(
        profile_id=entry.profile_id,
        profile_version=entry.profile_version,
        semantic_digest=entry.semantic_digest,
    )


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


async def test_kubernetes_provider_reads_configured_cluster_without_kubectl() -> None:
    node_reader = AsyncMock(return_value=json.loads(_fixture("kubernetes-node.json")))
    pod_reader = AsyncMock(return_value=())

    provider = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        node_reader=node_reader,
        pod_reader=pod_reader,
    )

    snapshot = await InventoryProviderRegistry((provider,)).snapshot_async()
    result = snapshot.provider_results[0]

    node_reader.assert_awaited_once_with(
        node_name="dual-stack-node",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        in_cluster=False,
        request_timeout=5.0,
    )
    pod_reader.assert_awaited_once_with(
        node_name="dual-stack-node",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        in_cluster=False,
        request_timeout=5.0,
    )
    assert result.status == InventoryStatus.AVAILABLE
    assert len(result.devices) == 6
    assert sum(device.vendor == AcceleratorVendor.NVIDIA for device in result.devices) == 2
    assert sum(device.vendor == AcceleratorVendor.HUAWEI_ASCEND for device in result.devices) == 4
    assert {device.kubernetes_resource_name for device in result.devices} == {
        "nvidia.com/gpu",
        "huawei.com/Ascend910",
    }
    assert all(
        dict(device.kubernetes_node_labels)["accelerator.mini-ai-cloud/vendor"] == "nvidia"
        for device in result.devices
    )
    assert all("kubernetes-capacity-slot" in device.capabilities for device in result.devices)
    assert all(device.health == "inventory-only" for device in result.devices)
    assert all(device.device_id.startswith("k8s-capacity:node-uid-1:") for device in result.devices)


async def test_kubernetes_provider_reads_node_via_real_python_client(
    tmp_path: Path,
) -> None:
    request_lines: list[str] = []

    async def serve_node(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request = await reader.readuntil(b"\r\n\r\n")
        request_lines.append(request.split(b"\r\n", 1)[0].decode("ascii"))
        node = json.loads(_fixture("kubernetes-node.json"))
        node.update({"apiVersion": "v1", "kind": "Node"})
        body = json.dumps(node).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            + f"Content-Length: {len(body)}\r\n".encode()
            + b"Content-Type: application/json\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(serve_node, "127.0.0.1", 0)
    socket = server.sockets[0]
    port = int(socket.getsockname()[1])
    kubeconfig = tmp_path / "worker.kubeconfig"
    kubeconfig.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "name": "test-cluster",
                        "cluster": {"server": f"http://127.0.0.1:{port}"},
                    }
                ],
                "contexts": [
                    {
                        "name": "test-context",
                        "context": {"cluster": "test-cluster", "user": "test-user"},
                    }
                ],
                "current-context": "test-context",
                "users": [{"name": "test-user", "user": {}}],
            }
        ),
        encoding="utf-8",
    )
    try:
        result = await KubernetesNodeAcceleratorProvider(
            node_name="dual-stack-node",
            kubeconfig=str(kubeconfig),
            pod_reader=AsyncMock(return_value=()),
        ).discover_async()
    finally:
        server.close()
        await server.wait_closed()

    assert result.status == InventoryStatus.AVAILABLE
    assert len(result.devices) == 6
    assert request_lines == ["GET /api/v1/nodes/dual-stack-node HTTP/1.1"]


async def test_kubernetes_provider_fails_closed_when_api_read_fails() -> None:
    result = await KubernetesNodeAcceleratorProvider(
        node_name="gpu-node-a",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        node_reader=AsyncMock(side_effect=TimeoutError("Kubernetes API unavailable")),
        pod_reader=AsyncMock(return_value=()),
    ).discover_async()

    assert result.status == InventoryStatus.UNAVAILABLE
    assert result.devices == ()
    assert result.message == "kubernetes_api_request_failed"


async def test_kubernetes_provider_fails_closed_when_pod_accounting_fails() -> None:
    result = await KubernetesNodeAcceleratorProvider(
        node_name="gpu-node-a",
        node_reader=AsyncMock(return_value=json.loads(_fixture("kubernetes-node.json"))),
        pod_reader=AsyncMock(side_effect=PermissionError("Pod list forbidden")),
    ).discover_async()

    assert result.status == InventoryStatus.UNAVAILABLE
    assert result.devices == ()
    assert result.message == "kubernetes_api_request_failed"


async def test_kubernetes_api_reader_uses_explicit_kubeconfig_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = json.loads(_fixture("kubernetes-node.json"))
    configuration = Mock()
    api_client = Mock()
    api_client.sanitize_for_serialization.return_value = node
    api_client.close = AsyncMock()
    api = Mock()
    api.read_node = AsyncMock(return_value=Mock())
    load_kube_config = AsyncMock()
    configuration_factory = Mock(return_value=configuration)
    api_client_factory = Mock(return_value=api_client)
    core_api_factory = Mock(return_value=api)
    monkeypatch.setattr(gpu_inventory.client, "Configuration", configuration_factory)
    monkeypatch.setattr(gpu_inventory.client, "ApiClient", api_client_factory)
    monkeypatch.setattr(gpu_inventory.client, "CoreV1Api", core_api_factory)
    monkeypatch.setattr(gpu_inventory.config, "load_kube_config", load_kube_config)

    discovered = await gpu_inventory._read_kubernetes_node(
        node_name="gpu-node-a",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        in_cluster=False,
        request_timeout=3.0,
    )

    assert discovered == node
    load_kube_config.assert_awaited_once_with(
        config_file="/var/run/mini-ai/worker.kubeconfig",
        client_configuration=configuration,
        persist_config=False,
    )
    api_client_factory.assert_called_once_with(configuration=configuration)
    core_api_factory.assert_called_once_with(api_client=api_client)
    api.read_node.assert_awaited_once_with(name="gpu-node-a", _request_timeout=3.0)
    api_client.close.assert_awaited_once_with()


async def test_kubernetes_pod_reader_filters_node_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pods = [{"metadata": {"name": "external-pod"}, "spec": {"nodeName": "gpu-node-a"}}]
    configuration = Mock()
    api_client = Mock()
    api_client.sanitize_for_serialization.return_value = {"items": pods}
    api_client.close = AsyncMock()
    api = Mock()
    api.list_pod_for_all_namespaces = AsyncMock(return_value=Mock())
    load_kube_config = AsyncMock()
    monkeypatch.setattr(gpu_inventory.client, "Configuration", Mock(return_value=configuration))
    monkeypatch.setattr(gpu_inventory.client, "ApiClient", Mock(return_value=api_client))
    monkeypatch.setattr(gpu_inventory.client, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(gpu_inventory.config, "load_kube_config", load_kube_config)

    discovered = await gpu_inventory._read_kubernetes_pods(
        node_name="gpu-node-a",
        kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        in_cluster=False,
        request_timeout=3.0,
    )

    assert discovered == tuple(pods)
    api.list_pod_for_all_namespaces.assert_awaited_once_with(
        field_selector="spec.nodeName=gpu-node-a",
        _request_timeout=3.0,
    )
    api_client.close.assert_awaited_once_with()


@pytest.mark.parametrize(
    ("unschedulable", "conditions"),
    [
        (True, [{"type": "Ready", "status": "True"}]),
        (False, [{"type": "Ready", "status": "False"}]),
        (False, [{"type": "Ready", "status": "Unknown"}]),
        (False, []),
    ],
)
def test_kubernetes_capacity_rejects_unschedulable_or_unready_nodes(
    unschedulable: bool,
    conditions: list[dict[str, str]],
) -> None:
    node = json.loads(_fixture("kubernetes-node.json"))
    node["spec"]["unschedulable"] = unschedulable
    node["status"]["conditions"] = conditions

    parsed = parse_kubernetes_node(node)

    assert parsed.devices == ()
    assert parsed.rejected_rows == 1


async def test_kubernetes_capacity_binds_only_schedulable_catalog_contracts() -> None:
    node_reader = AsyncMock(return_value=json.loads(_fixture("kubernetes-node.json")))
    provider = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node",
        in_cluster=True,
        node_reader=node_reader,
        pod_reader=AsyncMock(return_value=()),
    )
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")

    result = await provider.discover_async()
    bound = bind_kubernetes_runtime_profiles(result.devices, catalog)

    node_reader.assert_awaited_once_with(
        node_name="dual-stack-node",
        kubeconfig=None,
        in_cluster=True,
        request_timeout=5.0,
    )
    nvidia = [device for device in bound if device.vendor == AcceleratorVendor.NVIDIA]
    ascend = [device for device in bound if device.vendor == AcceleratorVendor.HUAWEI_ASCEND]
    assert {device.runtime_profile_ids for device in nvidia} == {
        tuple(
            sorted(
                {
                    _binding(catalog, "nvidia-vllm-k8s@1.0.0"),
                    _binding(catalog, "nvidia-vllm-k8s@2.0.0"),
                }
            )
        )
    }
    assert {device.runtime_profile_ids for device in ascend} == {()}
    assert all(device.capabilities == ("kubernetes-capacity-slot",) for device in nvidia)
    assert all(device.health == "inventory-only" for device in bound)

    ascend_node = json.loads(_fixture("kubernetes-node.json"))
    ascend_node["metadata"]["labels"]["accelerator.mini-ai-cloud/vendor"] = "huawei-ascend"
    ascend_bound = bind_kubernetes_runtime_profiles(
        parse_kubernetes_node(ascend_node).devices,
        catalog,
    )
    ascend = [device for device in ascend_bound if device.vendor == AcceleratorVendor.HUAWEI_ASCEND]
    nvidia = [device for device in ascend_bound if device.vendor == AcceleratorVendor.NVIDIA]
    assert {device.runtime_profile_ids for device in ascend} == {
        tuple(
            sorted(
                {
                    _binding(catalog, "ascend-vllm-k8s-a2@1.0.0"),
                    _binding(catalog, "ascend-vllm-k8s-a2@2.0.0"),
                }
            )
        )
    }
    assert {device.runtime_profile_ids for device in nvidia} == {()}


async def test_kubernetes_provider_deducts_external_pod_accelerator_requests() -> None:
    node_reader = AsyncMock(return_value=json.loads(_fixture("kubernetes-node.json")))
    pod_reader = AsyncMock(
        return_value=(
            {
                "metadata": {"labels": {"app": "external"}},
                "spec": {
                    "nodeName": "dual-stack-node",
                    "containers": [
                        {
                            "resources": {
                                "requests": {
                                    "nvidia.com/gpu": "1",
                                },
                                "limits": {"huawei.com/Ascend910": "2"},
                            }
                        }
                    ],
                },
                "status": {"phase": "Running"},
            },
            {
                "metadata": {
                    "labels": {
                        "mini-ai-cloud/managed": "true",
                        "mini-ai-cloud/cluster-id": "serving-cluster",
                    }
                },
                "spec": {
                    "nodeName": "dual-stack-node",
                    "containers": [{"resources": {"limits": {"nvidia.com/gpu": "1"}}}],
                },
                "status": {"phase": "Running"},
            },
            {
                "metadata": {"labels": {"app": "completed"}},
                "spec": {
                    "nodeName": "dual-stack-node",
                    "containers": [{"resources": {"requests": {"nvidia.com/gpu": "1"}}}],
                },
                "status": {"phase": "Succeeded"},
            },
        )
    )
    provider = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node",
        node_reader=node_reader,
        pod_reader=pod_reader,
        cluster_id="serving-cluster",
        worker_id="worker-a",
    )

    result = await provider.discover_async()

    assert result.status == InventoryStatus.AVAILABLE
    assert sum(device.vendor == AcceleratorVendor.NVIDIA for device in result.devices) == 2
    assert sum(device.vendor == AcceleratorVendor.HUAWEI_ASCEND for device in result.devices) == 4
    externally_allocated = [
        device for device in result.devices if device.health == "externally-allocated"
    ]
    assert sum(device.vendor == AcceleratorVendor.NVIDIA for device in externally_allocated) == 1
    assert (
        sum(device.vendor == AcceleratorVendor.HUAWEI_ASCEND for device in externally_allocated)
        == 2
    )
    assert result.message == "excluded 3 externally requested capacity slot(s)"
    pod_reader.assert_awaited_once_with(
        node_name="dual-stack-node",
        kubeconfig=None,
        in_cluster=False,
        request_timeout=5.0,
    )


@pytest.mark.parametrize(
    ("label", "value"),
    [
        ("accelerator.mini-ai-cloud/vendor", "huawei-ascend"),
        ("nvidia.com/gpu.product", None),
        ("nvidia.com/gpu.count", "0"),
        ("nvidia.com/gpu.compute.major", "7"),
        ("nvidia.com/gpu.sharing-strategy", "time-slicing"),
        ("nvidia.com/mig.strategy", "mixed"),
    ],
)
def test_kubernetes_capacity_rejects_unsatisfied_profile_node_constraints(
    label: str,
    value: str | None,
) -> None:
    node = json.loads(_fixture("kubernetes-node.json"))
    if value is None:
        del node["metadata"]["labels"][label]
    else:
        node["metadata"]["labels"][label] = value
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")

    bound = bind_kubernetes_runtime_profiles(parse_kubernetes_node(node).devices, catalog)

    nvidia = [device for device in bound if device.vendor == AcceleratorVendor.NVIDIA]
    assert nvidia
    expected = (
        ()
        if label == "accelerator.mini-ai-cloud/vendor"
        else (_binding(catalog, "nvidia-vllm-k8s@1.0.0"),)
    )
    assert all(device.runtime_profile_ids == expected for device in nvidia)


@pytest.mark.parametrize(
    ("node_labels", "operator", "values", "expected"),
    [
        ({"feature": "8"}, "Exists", (), True),
        ({}, "Exists", (), False),
        ({}, "DoesNotExist", (), True),
        ({"feature": "8"}, "DoesNotExist", (), False),
        ({"feature": "8"}, "In", ("8", "9"), True),
        ({"feature": "7"}, "In", ("8", "9"), False),
        ({"feature": "7"}, "NotIn", ("8", "9"), True),
        ({}, "NotIn", ("8", "9"), True),
        ({"feature": "8"}, "Gt", ("7",), True),
        ({"feature": "7"}, "Gt", ("7",), False),
        ({"feature": "7"}, "Lt", ("8",), True),
        ({"feature": "not-an-int"}, "Lt", ("8",), False),
    ],
)
def test_kubernetes_node_affinity_operator_matching(
    node_labels: dict[str, str],
    operator: Literal["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"],
    values: tuple[str, ...],
    expected: bool,
) -> None:
    requirement = KubernetesNodeSelectorRequirement(
        key="feature",
        operator=operator,
        values=values,
    )

    assert gpu_inventory._kubernetes_node_matches_requirement(node_labels, requirement) is expected


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


def test_registry_rejects_overlapping_host_and_kubernetes_capacity_sources() -> None:
    node = json.loads(_fixture("kubernetes-node.json"))
    node["status"]["allocatable"] = {
        "cpu": "64",
        "memory": "512Gi",
        "nvidia.com/gpu": "2",
    }
    host = NvidiaSMIInventoryProvider(
        runner=Mock(
            return_value=subprocess.CompletedProcess(
                args=["nvidia-smi"],
                returncode=0,
                stdout="GPU-real, 0, NVIDIA A100, 40960, 40000, 8.0\n",
            )
        )
    )
    kubernetes = KubernetesNodeAcceleratorProvider(
        node_name="dual-stack-node",
        node_reader=AsyncMock(return_value=node),
    )

    with pytest.raises(ValueError, match="cross-authority physical-device aliasing"):
        InventoryProviderRegistry((host, kubernetes))


def test_registry_factory_rejects_mixed_host_and_kubernetes_inventory() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        accelerator_inventory_providers="nvidia-smi,kubernetes-node",
        worker_node_name="gpu-node-a",
    )

    with pytest.raises(ValueError, match="kubernetes-node must be the only"):
        build_accelerator_inventory_registry(settings, worker_id="worker-test")


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


def test_kubernetes_provider_factory_preserves_runtime_cluster_configuration() -> None:
    settings = Settings(
        _env_file=None,
        app_env="test",
        accelerator_inventory_providers="kubernetes-node",
        worker_node_name="gpu-node-a",
        kubernetes_kubeconfig="/var/run/mini-ai/worker.kubeconfig",
        kubernetes_in_cluster=False,
    )

    registry = build_accelerator_inventory_registry(settings, worker_id="worker-test")

    provider = registry.providers[0]
    assert isinstance(provider, KubernetesNodeAcceleratorProvider)
    assert provider.node_name == "gpu-node-a"
    assert provider.kubeconfig == "/var/run/mini-ai/worker.kubeconfig"
    assert provider.in_cluster is False
    assert provider.cluster_id == "mini-ai-cloud-local"
    assert provider.worker_id == "worker-test"


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
