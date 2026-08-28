from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from core.runtime_profiles import RuntimeProfile
from scripts.validate_runtime_profiles import (
    RuntimeProfileContractError,
    load_profiles,
    validate_generated_files,
)

REPOSITORY_ROOT = Path(__file__).parents[2]


def _profile_payload(name: str = "nvidia-vllm-k8s.example.yaml") -> dict[str, Any]:
    payload = yaml.safe_load(
        (REPOSITORY_ROOT / "runtime_profiles" / name).read_text(encoding="utf-8")
    )
    assert isinstance(payload, dict)
    return payload


def test_committed_runtime_profiles_and_generated_files_are_valid() -> None:
    profiles = load_profiles(REPOSITORY_ROOT)

    validate_generated_files(profiles, REPOSITORY_ROOT)

    assert {loaded.profile.vendor.value for loaded in profiles} == {"nvidia", "huawei-ascend"}
    assert all(loaded.profile.image.reference.count("@sha256:") == 1 for loaded in profiles)
    assert {loaded.profile.evidence_status.value for loaded in profiles} == {
        "SCHEMA_READY",
        "REAL_HW_NOT_RUN",
    }
    assert {loaded.profile.identity for loaded in profiles} == {
        "ascend-vllm-k8s-a2@1.0.0",
        "ascend-vllm-k8s-a2@2.0.0",
        "nvidia-vllm-k8s@1.0.0",
        "nvidia-vllm-k8s@2.0.0",
    }


def test_runtime_profile_model_is_frozen() -> None:
    profile = RuntimeProfile.model_validate(_profile_payload())

    with pytest.raises(ValidationError, match="frozen"):
        profile.engine = "different"  # type: ignore[misc]


def test_image_must_be_digest_pinned() -> None:
    payload = _profile_payload()
    payload["image"]["reference"] = "registry.example.invalid/mini-ai-cloud/vllm:1.0.0"

    with pytest.raises(ValidationError, match="pinned by sha256 digest"):
        RuntimeProfile.model_validate(payload)


@pytest.mark.parametrize("field", ["privileged", "hostPID", "hostNetwork"])
def test_privileged_and_host_namespaces_are_forbidden(field: str) -> None:
    payload = _profile_payload()
    payload["kubernetes"]["security"][field] = True

    with pytest.raises(ValidationError):
        RuntimeProfile.model_validate(payload)


def test_host_path_is_forbidden() -> None:
    payload = _profile_payload()
    payload["kubernetes"]["security"]["hostPath"] = ["/dev"]

    with pytest.raises(ValidationError):
        RuntimeProfile.model_validate(payload)


def test_command_must_match_the_exact_allowlist() -> None:
    payload = _profile_payload()
    payload["process"]["command"] = ["sh", "-c", "vllm serve"]

    with pytest.raises(ValidationError, match="command must exactly match"):
        RuntimeProfile.model_validate(payload)


@pytest.mark.parametrize("name", ["MODEL_TOKEN", "CUDA_VISIBLE_DEVICES"])
def test_credentials_and_device_allocation_env_are_forbidden(name: str) -> None:
    payload = _profile_payload()
    payload["process"]["env_allowlist"].append(name)

    with pytest.raises(ValidationError, match="environment"):
        RuntimeProfile.model_validate(payload)


def test_vendor_kind_and_node_selector_must_agree() -> None:
    payload = _profile_payload()
    payload["kind"] = "npu"

    with pytest.raises(ValidationError, match="not compatible"):
        RuntimeProfile.model_validate(payload)

    payload = _profile_payload()
    payload["kubernetes"]["node_selector"]["accelerator.mini-ai-cloud/vendor"] = "huawei-ascend"
    with pytest.raises(ValidationError, match="node_selector must set"):
        RuntimeProfile.model_validate(payload)


def test_node_affinity_requirements_fail_closed() -> None:
    payload = _profile_payload("nvidia-vllm-k8s.yaml")
    payload["kubernetes"]["node_affinity"][0]["values"] = ["unexpected"]
    with pytest.raises(ValidationError, match="must not set values"):
        RuntimeProfile.model_validate(payload)

    payload = _profile_payload("nvidia-vllm-k8s.yaml")
    payload["kubernetes"]["node_affinity"][1]["values"] = ["01"]
    with pytest.raises(ValidationError, match="canonical integers"):
        RuntimeProfile.model_validate(payload)

    payload = _profile_payload("nvidia-vllm-k8s.yaml")
    payload["kubernetes"]["node_affinity"][1]["key"] = "invalid key"
    with pytest.raises(ValidationError, match="must not contain whitespace"):
        RuntimeProfile.model_validate(payload)


def test_semantic_digest_changes_with_profile_semantics_not_yaml_formatting() -> None:
    profile = RuntimeProfile.model_validate(_profile_payload())
    reformatted = RuntimeProfile.model_validate(_profile_payload())
    changed_payload = _profile_payload()
    changed_payload["limitations"].append("Additional release boundary.")
    changed = RuntimeProfile.model_validate(changed_payload)

    assert profile.semantic_digest() == reformatted.semantic_digest()
    assert profile.semantic_digest() != changed.semantic_digest()


def test_positive_evidence_status_requires_a_reference() -> None:
    payload = _profile_payload()
    payload["evidence_status"] = "REAL_ENGINE_PASS"

    with pytest.raises(ValidationError, match="requires evidence_references"):
        RuntimeProfile.model_validate(payload)


def test_stale_manifest_is_rejected(tmp_path: Path) -> None:
    profile_root = tmp_path / "runtime_profiles"
    profile_root.mkdir()
    for name in (
        "nvidia-vllm-k8s.example.yaml",
        "ascend-vllm-k8s.example.yaml",
        "schema.json",
        "manifest.json",
    ):
        (profile_root / name).write_bytes(
            (REPOSITORY_ROOT / "runtime_profiles" / name).read_bytes()
        )
    profiles = load_profiles(tmp_path)
    manifest_path = profile_root / "manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(RuntimeProfileContractError, match=r"manifest\.json is stale"):
        validate_generated_files(profiles, tmp_path)
