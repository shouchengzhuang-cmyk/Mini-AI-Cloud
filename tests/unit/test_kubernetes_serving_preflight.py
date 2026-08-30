from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from core.enums import AcceleratorVendor
from core.kubernetes_serving_preflight import (
    KubernetesServingPreflightError,
    ReleaseRuntimeProfileContract,
    collect_kubernetes_serving_preflight,
    evaluate_kubernetes_serving_preflight,
    load_release_runtime_profile_contract,
)
from core.runtime_profiles import RuntimeProfileCatalog

REPOSITORY_ROOT = Path(__file__).parents[2]
MANIFEST_PATH = REPOSITORY_ROOT / "runtime_profiles" / "manifest.json"


def _release_contract(vendor: AcceleratorVendor) -> ReleaseRuntimeProfileContract:
    catalog = RuntimeProfileCatalog.from_path(MANIFEST_PATH)
    entry = next(
        item
        for item in catalog.manifest.profiles
        if item.vendor is vendor and item.evidence_status.value == "REAL_HW_NOT_RUN"
    )
    return load_release_runtime_profile_contract(
        MANIFEST_PATH,
        profile_id=entry.profile_id,
        profile_version=entry.profile_version,
        semantic_digest=entry.semantic_digest,
    )


def _namespace() -> dict[str, object]:
    return {
        "metadata": {"name": "model-serving"},
        "status": {"phase": "Active"},
    }


def _nvidia_nodes() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {
                    "name": "sensitive-node-name",
                    "labels": {
                        "accelerator.mini-ai-cloud/vendor": "nvidia",
                        "nvidia.com/gpu.product": "NVIDIA-A100-SXM4-40GB",
                        "nvidia.com/gpu.count": "2",
                        "nvidia.com/gpu.compute.major": "8",
                        "nvidia.com/gpu.sharing-strategy": "none",
                        "nvidia.com/mig.strategy": "none",
                    },
                },
                "status": {
                    "allocatable": {"nvidia.com/gpu": "2"},
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        ]
    }


def _ascend_nodes() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {
                    "name": "sensitive-ascend-node",
                    "labels": {
                        "accelerator.mini-ai-cloud/vendor": "huawei-ascend",
                        "node.kubernetes.io/npu.chip.name": "Ascend910B4-1",
                    },
                },
                "status": {
                    "allocatable": {"huawei.com/Ascend910": "8"},
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        ]
    }


def _nvidia_nodes_without_resource() -> dict[str, object]:
    nodes = _nvidia_nodes()
    items = nodes["items"]
    assert isinstance(items, list)
    node = items[0]
    assert isinstance(node, dict)
    node["status"] = {
        "allocatable": {},
        "conditions": [{"type": "Ready", "status": "True"}],
    }
    return nodes


def _nvidia_nodes_with_zero_resource() -> dict[str, object]:
    nodes = _nvidia_nodes()
    items = nodes["items"]
    assert isinstance(items, list)
    node = items[0]
    assert isinstance(node, dict)
    status = node["status"]
    assert isinstance(status, dict)
    status["allocatable"] = {"nvidia.com/gpu": "0"}
    return nodes


def _nvidia_nodes_without_matching_labels() -> dict[str, object]:
    nodes = _nvidia_nodes()
    items = nodes["items"]
    assert isinstance(items, list)
    node = items[0]
    assert isinstance(node, dict)
    metadata = node["metadata"]
    assert isinstance(metadata, dict)
    labels = metadata["labels"]
    assert isinstance(labels, dict)
    labels["accelerator.mini-ai-cloud/vendor"] = "huawei-ascend"
    return nodes


def _volcano_scheduler_pods() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {
                    "name": "volcano-scheduler-sensitive-suffix",
                    "labels": {"app": "volcano-scheduler"},
                },
                "spec": {
                    "schedulerName": "default-scheduler",
                    "containers": [{"name": "volcano-scheduler"}],
                },
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        ]
    }


def _ready_ascend_plugin() -> dict[str, object]:
    return {
        "items": [
            {
                "status": {
                    "desiredNumberScheduled": 2,
                    "numberReady": 2,
                    "numberAvailable": 2,
                }
            }
        ]
    }


def test_release_manifest_loads_exact_nvidia_and_ascend_static_contracts() -> None:
    nvidia = _release_contract(AcceleratorVendor.NVIDIA)
    ascend = _release_contract(AcceleratorVendor.HUAWEI_ASCEND)

    assert nvidia.profile.identity == "nvidia-vllm-k8s@2.0.0"
    assert nvidia.profile.image.reference.startswith("docker.io/vllm/")
    assert nvidia.profile.image.reference.count("@sha256:") == 1
    assert ascend.profile.identity == "ascend-vllm-k8s-a2@2.0.0"
    assert ascend.profile.kubernetes.scheduler_name == "volcano"
    assert ascend.runtime_handler == "ascend"
    assert ascend.device_plugin_label_selector == "name=ascend-device-plugin-ds"


def test_release_manifest_digest_drift_fails_closed_without_path_disclosure() -> None:
    contract = _release_contract(AcceleratorVendor.NVIDIA)

    with pytest.raises(KubernetesServingPreflightError) as captured:
        load_release_runtime_profile_contract(
            MANIFEST_PATH,
            profile_id=contract.profile.id,
            profile_version=contract.profile.version,
            semantic_digest="sha256:" + "0" * 64,
        )

    assert str(MANIFEST_PATH) not in str(captured.value)
    assert "identity or digest" in str(captured.value)


def test_nvidia_preflight_checks_exact_labels_runtimeclass_and_resource() -> None:
    contract = _release_contract(AcceleratorVendor.NVIDIA)

    result = evaluate_kubernetes_serving_preflight(
        contract,
        namespace_name="model-serving",
        api_ready=True,
        namespace=_namespace(),
        runtime_class={"metadata": {"name": "nvidia"}, "handler": "nvidia"},
        nodes=_nvidia_nodes(),
    )

    assert result["status"] == "KUBERNETES_SERVING_PREFLIGHT_PASS"
    assert result["profile_digest"] == contract.semantic_digest
    assert result["resource_name"] == "nvidia.com/gpu"
    assert result["allocatable_devices"] == 2
    assert result["scheduler_name"] is None
    rendered = str(result).casefold()
    assert "sensitive-node-name" not in rendered
    assert "kubeconfig" not in rendered
    assert "secret" not in rendered


def test_ascend_preflight_checks_plugin_scheduler_chip_and_resource() -> None:
    contract = _release_contract(AcceleratorVendor.HUAWEI_ASCEND)

    result = evaluate_kubernetes_serving_preflight(
        contract,
        namespace_name="model-serving",
        api_ready=True,
        namespace=_namespace(),
        runtime_class={"metadata": {"name": "ascend"}, "handler": "ascend"},
        nodes=_ascend_nodes(),
        scheduler_pods=_volcano_scheduler_pods(),
        device_plugin_daemonsets=_ready_ascend_plugin(),
    )

    assert result["scheduler_name"] == "volcano"
    assert result["scheduler_observed"] is True
    assert result["ready_device_plugin_daemonsets"] == 1
    assert result["allocatable_devices"] == 8
    assert result["evidence_status"] == "REAL_HW_NOT_RUN"


@pytest.mark.parametrize(
    ("runtime_class", "nodes", "scheduler_pods", "message"),
    [
        ({}, _nvidia_nodes(), None, "RuntimeClass is missing"),
        (
            {"metadata": {"name": "nvidia"}},
            _nvidia_nodes_without_resource(),
            None,
            "extended resource",
        ),
        (
            {"metadata": {"name": "nvidia"}},
            _nvidia_nodes_with_zero_resource(),
            None,
            "extended resource",
        ),
        (
            {"metadata": {"name": "nvidia"}},
            _nvidia_nodes_without_matching_labels(),
            None,
            "required node labels",
        ),
    ],
)
def test_nvidia_preflight_fails_closed_for_missing_cluster_contracts(
    runtime_class: dict[str, object],
    nodes: dict[str, object],
    scheduler_pods: dict[str, object] | None,
    message: str,
) -> None:
    with pytest.raises(KubernetesServingPreflightError, match=message):
        evaluate_kubernetes_serving_preflight(
            _release_contract(AcceleratorVendor.NVIDIA),
            namespace_name="model-serving",
            api_ready=True,
            namespace=_namespace(),
            runtime_class=runtime_class,
            nodes=nodes,
            scheduler_pods=scheduler_pods,
        )


@pytest.mark.parametrize(
    ("scheduler_pods", "daemonsets", "message"),
    [
        ({"items": []}, _ready_ascend_plugin(), "schedulerName"),
        (_volcano_scheduler_pods(), {"items": []}, "Device Plugin"),
    ],
)
def test_ascend_preflight_fails_closed_for_missing_scheduler_or_plugin(
    scheduler_pods: dict[str, object],
    daemonsets: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(KubernetesServingPreflightError, match=message):
        evaluate_kubernetes_serving_preflight(
            _release_contract(AcceleratorVendor.HUAWEI_ASCEND),
            namespace_name="model-serving",
            api_ready=True,
            namespace=_namespace(),
            runtime_class={"metadata": {"name": "ascend"}, "handler": "ascend"},
            nodes=_ascend_nodes(),
            scheduler_pods=scheduler_pods,
            device_plugin_daemonsets=daemonsets,
        )


def test_kubectl_failure_does_not_echo_stderr_or_kubeconfig(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_stderr = "token=do-not-print kubeconfig=/sensitive/config"
    monkeypatch.setattr(
        "core.kubernetes_serving_preflight.shutil.which",
        lambda _name: "/usr/local/bin/kubectl",
    )
    monkeypatch.setattr(
        "core.kubernetes_serving_preflight.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=(),
            returncode=1,
            stdout="",
            stderr=secret_stderr,
        ),
    )

    with pytest.raises(KubernetesServingPreflightError) as captured:
        collect_kubernetes_serving_preflight(
            _release_contract(AcceleratorVendor.NVIDIA),
            namespace_name="model-serving",
            kubeconfig=Path("/sensitive/config"),
        )

    assert "do-not-print" not in str(captured.value)
    assert "/sensitive/config" not in str(captured.value)
    assert "Kubernetes API" in str(captured.value)
