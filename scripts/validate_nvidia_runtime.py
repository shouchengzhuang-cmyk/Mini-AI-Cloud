from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, cast

import yaml  # type: ignore[import-untyped]

from core.nvidia_runtime import (
    NvidiaRuntimeAcceptanceContract,
    load_nvidia_acceptance_contract,
    validate_nvidia_profile,
)
from scripts.validate_runtime_profiles import load_profile

NVIDIA_PROFILE_PATH = Path("runtime_profiles/nvidia-vllm-k8s.yaml")
NVIDIA_ACCEPTANCE_PATH = Path("runtime_profiles/nvidia-vllm-k8s.acceptance.json")
FAKE_PLUGIN_PATH = Path("deploy/nvidia-runtime/00-fake-device-plugin.yaml")
FAKE_ALLOCATION_PATH = Path("deploy/nvidia-runtime/10-fake-device-allocation.yaml")


def validate_repository(repository_root: Path) -> NvidiaRuntimeAcceptanceContract:
    profile = load_profile(repository_root / NVIDIA_PROFILE_PATH)
    contract = load_nvidia_acceptance_contract(repository_root / NVIDIA_ACCEPTANCE_PATH)
    validate_nvidia_profile(profile, contract)

    plugin = _load_single_yaml(repository_root / FAKE_PLUGIN_PATH)
    allocation = _load_single_yaml(repository_root / FAKE_ALLOCATION_PATH)
    _validate_fake_device_plugin(plugin, contract)
    _validate_fake_allocation(allocation, contract)
    return contract


def _load_single_yaml(path: Path) -> dict[str, Any]:
    documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    if len(documents) != 1 or not isinstance(documents[0], dict):
        raise ValueError(f"{path}: expected exactly one YAML mapping")
    return cast(dict[str, Any], documents[0])


def _validate_fake_device_plugin(
    manifest: dict[str, Any],
    contract: NvidiaRuntimeAcceptanceContract,
) -> None:
    if manifest.get("apiVersion") != "apps/v1" or manifest.get("kind") != "DaemonSet":
        raise ValueError("fake device plugin manifest must be an apps/v1 DaemonSet")
    pod_spec = manifest["spec"]["template"]["spec"]
    containers = pod_spec.get("containers", [])
    if len(containers) != 1:
        raise ValueError("fake device plugin DaemonSet must have exactly one container")
    container = containers[0]
    if container.get("image") != contract.fake_device_plugin.plugin_image:
        raise ValueError("fake device plugin image does not match the pinned acceptance contract")
    if container.get("securityContext", {}).get("privileged") is not True:
        raise ValueError("Kubernetes sample device plugin requires an explicit privileged boundary")
    annotation = manifest["spec"]["template"]["metadata"].get("annotations", {})
    if annotation.get("mini-ai-cloud/test-scope") != "fake-device-plugin-only":
        raise ValueError("fake device plugin manifest must declare its evidence scope")


def _validate_fake_allocation(
    manifest: dict[str, Any],
    contract: NvidiaRuntimeAcceptanceContract,
) -> None:
    if manifest.get("apiVersion") != "v1" or manifest.get("kind") != "Pod":
        raise ValueError("fake allocation manifest must be a v1 Pod")
    pod_spec = manifest["spec"]
    containers = pod_spec.get("containers", [])
    if len(containers) != 1:
        raise ValueError("fake allocation Pod must have exactly one container")
    container = containers[0]
    if container.get("image") != contract.fake_device_plugin.probe_image:
        raise ValueError("fake allocation probe image does not match the acceptance contract")
    resources = container.get("resources", {})
    requests = resources.get("requests", {})
    limits = resources.get("limits", {})
    resource_name = contract.fake_device_plugin.resource_name
    if requests.get(resource_name) != "1" or limits.get(resource_name) != "1":
        raise ValueError("fake allocation Pod must request and limit exactly one fake resource")
    if requests != limits:
        raise ValueError("fake allocation Pod requests and limits must be identical")
    security = container.get("securityContext", {})
    if (
        security.get("privileged") is not False
        or security.get("allowPrivilegeEscalation") is not False
    ):
        raise ValueError("fake allocation workload must remain non-privileged")
    if pod_spec.get("automountServiceAccountToken") is not False:
        raise ValueError("fake allocation workload must not mount a service account token")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the NVIDIA Kubernetes runtime contract")
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[1])
    args = parser.parse_args(argv)
    contract = validate_repository(args.root.resolve())
    print(
        "Validated NVIDIA runtime contract "
        f"{contract.profile_identity}; evidence_status={contract.evidence_status}."
    )


if __name__ == "__main__":
    main()
