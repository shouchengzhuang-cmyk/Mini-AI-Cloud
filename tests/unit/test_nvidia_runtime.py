from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from core.nvidia_runtime import (
    NvidiaRuntimeAcceptanceContract,
    load_nvidia_acceptance_contract,
    parse_nvidia_smi_csv,
    summarize_nvidia_device_nodes,
    validate_nvidia_node_labels,
)
from scripts.nvidia_runtime_acceptance import (
    accept_openai_engine,
    collect_nvidia_diagnostic,
)
from scripts.validate_nvidia_runtime import validate_repository
from scripts.validate_runtime_profiles import load_profile
from worker.kubernetes_serving_runtime import (
    KubernetesServingLaunchSpec,
    KubernetesServingRuntimeAdapter,
)

REPOSITORY_ROOT = Path(__file__).parents[2]
CONTRACT_PATH = REPOSITORY_ROOT / "runtime_profiles" / "nvidia-vllm-k8s.acceptance.json"
PROFILE_PATH = REPOSITORY_ROOT / "runtime_profiles" / "nvidia-vllm-k8s.yaml"


def _contract() -> NvidiaRuntimeAcceptanceContract:
    return load_nvidia_acceptance_contract(CONTRACT_PATH)


def _node_labels() -> dict[str, str]:
    return {
        "nvidia.com/gpu.product": "NVIDIA-A100-SXM4-40GB",
        "nvidia.com/gpu.count": "2",
        "nvidia.com/gpu.compute.major": "8",
        "nvidia.com/gpu.sharing-strategy": "none",
        "nvidia.com/mig.strategy": "none",
    }


def test_committed_nvidia_profile_acceptance_and_fake_manifests_are_consistent() -> None:
    contract = validate_repository(REPOSITORY_ROOT)

    assert contract.profile_identity == "nvidia-vllm-k8s@2.0.0"
    assert contract.vllm_version == "0.28.0"
    assert contract.device_plugin_version == "0.20.0"
    assert contract.evidence_status == "REAL_HW_NOT_RUN"


def test_nvidia_acceptance_requires_both_supported_image_platforms() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    del payload["image_platform_digests"]["linux/arm64"]

    with pytest.raises(ValidationError, match="must cover linux/amd64 and linux/arm64"):
        NvidiaRuntimeAcceptanceContract.model_validate(payload)


def test_nvidia_profile_renders_gfd_affinity_and_extended_resource() -> None:
    profile = load_profile(PROFILE_PATH)
    spec = KubernetesServingLaunchSpec(
        service_id=uuid.uuid4(),
        replica_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        generation=1,
        execution_id=uuid.uuid4(),
        image=profile.image.reference,
        model="Qwen/Qwen3-0.6B",
        cpu_millicores=1_000,
        memory_mb=4_096,
        accelerator_count=2,
        tensor_parallel_size=2,
        runtime_profile=profile,
        profile_environment=(("VLLM_LOGGING_LEVEL", "INFO"),),
        eligible_node_names=("nvidia-node-a", "nvidia-node-b"),
    )
    runtime = KubernetesServingRuntimeAdapter(
        namespace="nvidia-tests",
        cluster_id="nvidia-test",
        api=object(),
    )

    runtime._validate_launch_spec(spec)
    labels = runtime._selector_labels(
        spec,
        worker_id="nvidia-test-worker",
        worker_session_id=uuid.uuid4(),
    )
    pod = runtime._build_pod(spec, labels)

    container = pod.spec.containers[0]
    assert container.resources.requests["nvidia.com/gpu"] == "2"
    assert container.resources.requests == container.resources.limits
    assert pod.spec.runtime_class_name == "nvidia"
    assert pod.spec.tolerations[0].key == "nvidia.com/gpu"
    required = pod.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    expressions = required.node_selector_terms[0].match_expressions
    fields = required.node_selector_terms[0].match_fields
    assert [(item.key, item.operator, tuple(item.values or ())) for item in fields] == [
        ("metadata.name", "In", ("nvidia-node-a", "nvidia-node-b"))
    ]
    assert {(item.key, item.operator, tuple(item.values or ())) for item in expressions} == {
        ("nvidia.com/gpu.product", "Exists", ()),
        ("nvidia.com/gpu.count", "Gt", ("0",)),
        ("nvidia.com/gpu.compute.major", "Gt", ("7",)),
        ("nvidia.com/gpu.sharing-strategy", "In", ("none",)),
        ("nvidia.com/mig.strategy", "In", ("none",)),
    }


def test_nvidia_gfd_labels_accept_only_unshared_non_mig_capacity() -> None:
    validate_nvidia_node_labels(_node_labels(), requested_count=2, contract=_contract())


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("nvidia.com/gpu.product", "", "product label"),
        ("nvidia.com/gpu.product", "NVIDIA-A100-SHARED", "shared resource"),
        ("nvidia.com/gpu.count", "1", "below the requested"),
        ("nvidia.com/gpu.compute.major", "7", "below the NVIDIA profile"),
        ("nvidia.com/gpu.sharing-strategy", "time-slicing", "sharing strategy"),
        ("nvidia.com/mig.strategy", "mixed", "MIG strategy"),
    ],
)
def test_nvidia_gfd_labels_fail_closed(key: str, value: str, message: str) -> None:
    labels = _node_labels()
    labels[key] = value

    with pytest.raises(ValueError, match=message):
        validate_nvidia_node_labels(labels, requested_count=2, contract=_contract())


def test_nvidia_smi_parser_and_device_summary_omit_physical_ids(tmp_path: Path) -> None:
    diagnostics = parse_nvidia_smi_csv("NVIDIA A100, 575.57.08, 40960, 8.0\n")
    for name in ("nvidia0", "nvidia1", "nvidiactl", "nvidia-uvm", "dxg", "unrelated"):
        (tmp_path / name).touch()
    devices = summarize_nvidia_device_nodes(tuple(tmp_path.iterdir()))

    assert diagnostics[0].model == "NVIDIA A100"
    assert diagnostics[0].memory_total_mb == 40_960
    assert devices.indexed_device_count == 2
    assert devices.control_nodes == ("nvidia-uvm", "nvidiactl")
    assert devices.wsl_dxg_present is True
    assert "uuid" not in diagnostics[0].model_dump_json().casefold()


def test_diagnostic_reports_real_hw_not_run_without_nvidia_smi(tmp_path: Path) -> None:
    result = collect_nvidia_diagnostic(
        contract=_contract(),
        nvidia_smi="definitely-missing-nvidia-smi",
        dev_root=tmp_path,
    )

    assert result["status"] == "REAL_HW_NOT_RUN"
    assert result["reason"] == "nvidia_smi_unavailable"
    assert result["gpus"] == []


def test_diagnostic_accepts_the_wsl_dxg_device_interface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "dxg").touch()
    monkeypatch.setattr(
        "scripts.nvidia_runtime_acceptance.shutil.which",
        lambda _name: "/usr/lib/wsl/lib/nvidia-smi",
    )
    monkeypatch.setattr(
        "scripts.nvidia_runtime_acceptance.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout="NVIDIA A100, 575.57.08, 40960, 8.0\n",
            stderr="",
        ),
    )

    result = collect_nvidia_diagnostic(contract=_contract(), dev_root=tmp_path)

    assert result["status"] == "HARDWARE_OBSERVED"
    assert result["device_nodes"] == {
        "indexed_device_count": 0,
        "control_nodes": [],
        "wsl_dxg_present": True,
    }


async def test_openai_engine_acceptance_checks_nonstreaming_and_sse() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200)
        if request.url.path == "/version":
            return httpx.Response(200, json={"version": "0.28.0"})
        payload = json.loads(request.content)
        if payload["stream"] is False:
            return httpx.Response(
                200,
                json={"choices": [{"message": {"role": "assistant", "content": "ready"}}]},
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=('data: {"choices":[{"delta":{"content":"ready"}}]}\n\ndata: [DONE]\n\n'),
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://runtime.invalid",
    ) as client:
        result = await accept_openai_engine(
            client,
            model="test-model",
            contract=_contract(),
        )

    assert result == {
        "status": "REAL_ENGINE_PASS",
        "profile_identity": "nvidia-vllm-k8s@2.0.0",
        "vllm_version": "0.28.0",
        "health": "pass",
        "non_streaming": "pass",
        "sse_streaming": "pass",
    }
