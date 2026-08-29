from __future__ import annotations

import copy
import json
import subprocess
import uuid
from pathlib import Path

import httpx
import pytest
import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from core.ascend_runtime import (
    AscendRuntimeAcceptanceContract,
    evaluate_ascend_cluster,
    load_ascend_acceptance_contract,
    parse_npu_smi_list,
    summarize_ascend_device_nodes,
)
from core.runtime_profiles import RuntimeProfile
from scripts.ascend_runtime_acceptance import (
    accept_openai_engine,
    collect_ascend_diagnostic,
)
from scripts.validate_ascend_runtime import validate_repository
from scripts.validate_runtime_profiles import load_profile
from worker.kubernetes_serving_runtime import (
    KubernetesServingLaunchSpec,
    KubernetesServingOwnershipError,
    KubernetesServingRuntimeAdapter,
)

REPOSITORY_ROOT = Path(__file__).parents[2]
CONTRACT_PATH = REPOSITORY_ROOT / "runtime_profiles" / "ascend-vllm-k8s.acceptance.json"
PROFILE_PATH = REPOSITORY_ROOT / "runtime_profiles" / "ascend-vllm-k8s.yaml"


def _contract() -> AscendRuntimeAcceptanceContract:
    return load_ascend_acceptance_contract(CONTRACT_PATH)


def _profile_payload() -> dict[str, object]:
    payload = yaml.safe_load(PROFILE_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _spec(profile: RuntimeProfile) -> KubernetesServingLaunchSpec:
    return KubernetesServingLaunchSpec(
        service_id=uuid.uuid4(),
        replica_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        generation=1,
        execution_id=uuid.uuid4(),
        image=profile.image.reference,
        model="Qwen/Qwen3-0.6B",
        cpu_millicores=1_000,
        memory_mb=8_192,
        accelerator_count=2,
        tensor_parallel_size=2,
        runtime_profile=profile,
        eligible_node_names=("ascend-node-a", "ascend-node-b"),
        profile_environment=(("VLLM_LOGGING_LEVEL", "INFO"),),
    )


def _runtime() -> KubernetesServingRuntimeAdapter:
    return KubernetesServingRuntimeAdapter(
        namespace="ascend-tests",
        cluster_id="ascend-test",
        api=object(),
    )


def test_committed_ascend_profile_and_acceptance_contract_are_consistent() -> None:
    contract = validate_repository(REPOSITORY_ROOT)

    assert contract.profile_identity == "ascend-vllm-k8s-a2@2.0.0"
    assert contract.vllm_ascend_version == "0.23.0"
    assert contract.mindcluster_version == "v26.1.0"
    assert contract.product_generation == "Atlas A2 (Ascend 910B)"
    assert contract.evidence_status == "REAL_HW_NOT_RUN"


def test_ascend_acceptance_requires_both_supported_image_platforms() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    del payload["image_platform_digests"]["linux/arm64"]

    with pytest.raises(ValidationError, match="must cover linux/amd64 and linux/arm64"):
        AscendRuntimeAcceptanceContract.model_validate(payload)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("scheduler", "scheduler_name=volcano"),
        ("annotation", "annotation_key must equal resource_name"),
        ("vendor", "restricted to Huawei Ascend"),
    ],
)
def test_ascend_annotation_visibility_policy_fails_closed(mutation: str, message: str) -> None:
    payload = _profile_payload()
    kubernetes = payload["kubernetes"]
    assert isinstance(kubernetes, dict)
    if mutation == "scheduler":
        kubernetes["scheduler_name"] = None
    elif mutation == "annotation":
        visibility = kubernetes["device_visibility"]
        assert isinstance(visibility, dict)
        visibility["annotation_key"] = "huawei.com/npu"
    else:
        payload["vendor"] = "nvidia"
        payload["kind"] = "gpu"
        node_selector = kubernetes["node_selector"]
        assert isinstance(node_selector, dict)
        node_selector["accelerator.mini-ai-cloud/vendor"] = "nvidia"

    with pytest.raises(ValidationError, match=message):
        RuntimeProfile.model_validate(payload)


def test_ascend_profile_renders_volcano_visibility_and_extended_resource() -> None:
    profile = load_profile(PROFILE_PATH)
    spec = _spec(profile)
    runtime = _runtime()

    runtime._validate_launch_spec(spec)
    labels = runtime._selector_labels(
        spec,
        worker_id="ascend-test-worker",
        worker_session_id=uuid.uuid4(),
    )
    pod = runtime._build_pod(spec, labels)

    container = pod.spec.containers[0]
    assert container.resources.requests["huawei.com/Ascend910"] == "2"
    assert container.resources.requests == container.resources.limits
    assert pod.spec.runtime_class_name == "ascend"
    assert pod.spec.scheduler_name == "volcano"
    required = pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    fields = required.node_selector_terms[0].match_fields
    assert [(item.key, item.operator, tuple(item.values or ())) for item in fields] == [
        ("metadata.name", "In", ("ascend-node-a", "ascend-node-b"))
    ]
    visibility = next(item for item in container.env if item.name == "ASCEND_VISIBLE_DEVICES")
    assert visibility.value is None
    assert visibility.value_from.field_ref.api_version == "v1"
    assert (
        visibility.value_from.field_ref.field_path == "metadata.annotations['huawei.com/Ascend910']"
    )


def test_renderer_keeps_ascend_resource_and_visibility_annotation_configurable() -> None:
    payload = copy.deepcopy(_profile_payload())
    kubernetes = payload["kubernetes"]
    assert isinstance(kubernetes, dict)
    kubernetes["resource_name"] = "huawei.com/npu"
    visibility = kubernetes["device_visibility"]
    assert isinstance(visibility, dict)
    visibility["annotation_key"] = "huawei.com/npu"
    tolerations = kubernetes["tolerations"]
    assert isinstance(tolerations, list) and isinstance(tolerations[0], dict)
    tolerations[0]["key"] = "huawei.com/npu"
    profile = RuntimeProfile.model_validate(payload)

    spec = _spec(profile)
    runtime = _runtime()
    labels = runtime._selector_labels(
        spec,
        worker_id="ascend-test-worker",
        worker_session_id=uuid.uuid4(),
    )
    pod = runtime._build_pod(spec, labels)

    container = pod.spec.containers[0]
    assert container.resources.requests["huawei.com/npu"] == "2"
    visibility_env = next(item for item in container.env if item.name == "ASCEND_VISIBLE_DEVICES")
    assert (
        visibility_env.value_from.field_ref.field_path == "metadata.annotations['huawei.com/npu']"
    )


def test_adoption_hash_rejects_visibility_annotation_field_drift() -> None:
    profile = load_profile(PROFILE_PATH)
    spec = _spec(profile)
    runtime = _runtime()
    labels = runtime._selector_labels(
        spec,
        worker_id="ascend-test-worker",
        worker_session_id=uuid.uuid4(),
    )
    pod = runtime._build_pod(spec, labels)
    visibility = next(
        item for item in pod.spec.containers[0].env if item.name == "ASCEND_VISIBLE_DEVICES"
    )
    visibility.value_from.field_ref.field_path = "metadata.annotations['huawei.com/npu']"

    with pytest.raises(KubernetesServingOwnershipError):
        runtime._validate_observed_pod(pod, expected_labels=labels, expected_spec=spec)


def test_cluster_preflight_requires_ready_plugin_and_atlas_a2_capacity() -> None:
    result = evaluate_ascend_cluster(
        runtime_class={"metadata": {"name": "ascend"}, "handler": "ascend"},
        daemonsets={
            "items": [
                {
                    "metadata": {"name": "ascend-device-plugin-daemonset"},
                    "status": {"desiredNumberScheduled": 2, "numberReady": 2},
                }
            ]
        },
        nodes={
            "items": [
                {
                    "metadata": {
                        "labels": {
                            "accelerator.mini-ai-cloud/vendor": "huawei-ascend",
                            "node.kubernetes.io/npu.chip.name": "Ascend910B4-1",
                        }
                    },
                    "status": {"allocatable": {"huawei.com/Ascend910": "8"}},
                }
            ]
        },
        contract=_contract(),
    )

    assert result["status"] == "CLUSTER_PREFLIGHT_PASS"
    assert result["ready_device_plugin_daemonsets"] == 1
    assert result["allocatable_devices"] == 8
    assert "node_name" not in result


def test_npu_smi_parser_and_device_summary_omit_physical_ids(tmp_path: Path) -> None:
    diagnostic = parse_npu_smi_list(
        """
        Card Count                     : 1
        NPU ID                         : 7
        Product Name                   : Atlas 800I A2
        Serial Number                  : sensitive-serial
        Chip Count                     : 2
        """
    )
    for name in ("davinci0", "davinci1", "davinci_manager", "devmm_svm", "unrelated"):
        (tmp_path / name).touch()
    devices = summarize_ascend_device_nodes(tuple(tmp_path.iterdir()))

    assert diagnostic.card_count == 1
    assert diagnostic.chip_count == 2
    assert devices.indexed_device_count == 2
    assert devices.control_nodes == ("davinci_manager", "devmm_svm")
    rendered = diagnostic.model_dump_json().casefold()
    assert "sensitive-serial" not in rendered
    assert "npu id" not in rendered


def test_diagnostic_reports_real_hw_not_run_without_npu_smi(tmp_path: Path) -> None:
    result = collect_ascend_diagnostic(
        contract=_contract(),
        npu_smi="definitely-missing-npu-smi",
        dev_root=tmp_path,
    )

    assert result["status"] == "REAL_HW_NOT_RUN"
    assert result["reason"] == "npu_smi_unavailable"
    assert result["npu_summary"] is None


def test_diagnostic_accepts_safe_npu_smi_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "davinci0").touch()
    (tmp_path / "davinci_manager").touch()
    monkeypatch.setattr(
        "scripts.ascend_runtime_acceptance.shutil.which",
        lambda _name: "/usr/local/bin/npu-smi",
    )
    monkeypatch.setattr(
        "scripts.ascend_runtime_acceptance.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout=(
                "Card Count : 1\nNPU ID : 0\nProduct Name : Atlas 800I A2\n"
                "Serial Number : omitted\nChip Count : 1\n"
            ),
            stderr="",
        ),
    )

    result = collect_ascend_diagnostic(contract=_contract(), dev_root=tmp_path)

    assert result["status"] == "HARDWARE_OBSERVED"
    assert result["npu_summary"] == {
        "card_count": 1,
        "chip_count": 1,
        "product_names": ["Atlas 800I A2"],
    }


async def test_openai_engine_acceptance_checks_nonstreaming_and_sse() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200)
        if request.url.path == "/version":
            return httpx.Response(200, json={"version": "0.23.0"})
        payload = json.loads(request.content)
        if payload["stream"] is False:
            return httpx.Response(200, json={"choices": [{"message": {"content": "ready"}}]})
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=('data: {"choices":[{"delta":{"content":"ready"}}]}\n\ndata: [DONE]\n\n'),
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://runtime.invalid",
    ) as client:
        result = await accept_openai_engine(client, model="test-model", contract=_contract())

    assert result == {
        "status": "REAL_ENGINE_PASS",
        "profile_identity": "ascend-vllm-k8s-a2@2.0.0",
        "vllm_version": "0.23.0",
        "vllm_ascend_version": "0.23.0",
        "health": "pass",
        "non_streaming": "pass",
        "sse_streaming": "pass",
    }
