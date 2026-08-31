from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.ascend_runtime import (
    load_ascend_acceptance_contract,
    validate_ascend_profile,
)
from core.enums import AcceleratorVendor
from core.kubernetes_names import validate_kubernetes_dns_subdomain
from core.nvidia_runtime import (
    load_nvidia_acceptance_contract,
    validate_nvidia_profile,
)
from core.runtime_profiles import (
    RuntimeProfile,
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
)


class KubernetesServingPreflightError(RuntimeError):
    """A fail-closed, operator-safe Kubernetes serving preflight failure."""


@dataclass(frozen=True, slots=True)
class ReleaseRuntimeProfileContract:
    profile: RuntimeProfile
    semantic_digest: str
    runtime_handler: str | None = None
    device_plugin_label_selector: str | None = None
    required_label_prefixes: tuple[tuple[str, str], ...] = ()


def load_release_runtime_profile_contract(
    manifest_path: Path,
    *,
    profile_id: str,
    profile_version: str,
    semantic_digest: str,
) -> ReleaseRuntimeProfileContract:
    """Load one exact manifest entry and verify its pinned vendor acceptance contract."""

    try:
        catalog = RuntimeProfileCatalog.from_path(manifest_path)
        profile = catalog.load_exact(
            profile_id=profile_id,
            profile_version=profile_version,
            semantic_digest=semantic_digest,
        )
    except (OSError, RuntimeProfileCompatibilityError, ValueError) as error:
        raise KubernetesServingPreflightError(
            "runtime profile manifest identity or digest verification failed"
        ) from error

    try:
        contract_path = _acceptance_contract_path(manifest_path, profile)
        if profile.vendor is AcceleratorVendor.NVIDIA:
            nvidia_contract = load_nvidia_acceptance_contract(contract_path)
            validate_nvidia_profile(profile, nvidia_contract)
            return ReleaseRuntimeProfileContract(
                profile=profile,
                semantic_digest=semantic_digest,
            )
        if profile.vendor is AcceleratorVendor.HUAWEI_ASCEND:
            ascend_contract = load_ascend_acceptance_contract(contract_path)
            validate_ascend_profile(profile, ascend_contract)
            return ReleaseRuntimeProfileContract(
                profile=profile,
                semantic_digest=semantic_digest,
                runtime_handler=ascend_contract.cluster.runtime_handler,
                device_plugin_label_selector=(
                    ascend_contract.cluster.plugin_daemonset_label_selector
                ),
                required_label_prefixes=(
                    (
                        ascend_contract.cluster.chip_name_label,
                        ascend_contract.cluster.chip_name_prefix,
                    ),
                ),
            )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise KubernetesServingPreflightError(
            "vendor runtime acceptance contract verification failed"
        ) from error
    raise KubernetesServingPreflightError("runtime profile vendor is not supported by preflight")


def evaluate_kubernetes_serving_preflight(
    contract: ReleaseRuntimeProfileContract,
    *,
    namespace_name: str,
    api_ready: bool,
    namespace: Mapping[str, Any],
    runtime_class: Mapping[str, Any],
    nodes: Mapping[str, Any],
    scheduler_pods: Mapping[str, Any] | None = None,
    device_plugin_daemonsets: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    """Evaluate allowlisted cluster observations without returning cluster identities."""

    if not api_ready:
        raise KubernetesServingPreflightError("Kubernetes API readiness check failed")
    validate_kubernetes_dns_subdomain(namespace_name, field_name="namespace_name")
    _require_named_object(namespace, namespace_name, "workload namespace")
    status = namespace.get("status")
    if not isinstance(status, Mapping) or status.get("phase") != "Active":
        raise KubernetesServingPreflightError("workload namespace is not Active")

    profile = contract.profile
    runtime_class_name = profile.kubernetes.runtime_class_name
    _require_named_object(runtime_class, runtime_class_name, "RuntimeClass")
    if contract.runtime_handler is not None and runtime_class.get("handler") != (
        contract.runtime_handler
    ):
        raise KubernetesServingPreflightError("RuntimeClass handler does not match the contract")

    node_items = _mapping_items(nodes, "Node", allow_empty=False)
    matching_nodes = 0
    allocatable_nodes = 0
    allocatable_devices = 0
    for node in node_items:
        metadata = node.get("metadata")
        node_status = node.get("status")
        if not isinstance(metadata, Mapping) or not isinstance(node_status, Mapping):
            continue
        labels = metadata.get("labels")
        allocatable = node_status.get("allocatable")
        if not _condition_is_true(node_status, "Ready") or not isinstance(labels, Mapping):
            continue
        if not _node_matches_contract(
            profile,
            labels,
            required_label_prefixes=contract.required_label_prefixes,
        ):
            continue
        matching_nodes += 1
        if not isinstance(allocatable, Mapping):
            continue
        count = _canonical_resource_count(allocatable.get(profile.kubernetes.resource_name))
        if count > 0:
            allocatable_nodes += 1
            allocatable_devices += count
    if matching_nodes == 0:
        raise KubernetesServingPreflightError("required node labels are not observable")
    if allocatable_nodes == 0:
        raise KubernetesServingPreflightError(
            "required extended resource is not allocatable on matching nodes"
        )

    ready_device_plugins: int | None = None
    if contract.device_plugin_label_selector is not None:
        ready_device_plugins = _ready_daemonset_count(device_plugin_daemonsets)
        if ready_device_plugins == 0:
            raise KubernetesServingPreflightError("required Device Plugin DaemonSet is not ready")

    scheduler_name = profile.kubernetes.scheduler_name
    if scheduler_name is not None and not _scheduler_is_observable(
        scheduler_pods,
        scheduler_name=scheduler_name,
    ):
        raise KubernetesServingPreflightError(
            "required schedulerName is not observable in the cluster"
        )

    result: dict[str, object] = {
        "status": "KUBERNETES_SERVING_PREFLIGHT_PASS",
        "api_ready": True,
        "namespace": namespace_name,
        "profile_identity": profile.identity,
        "profile_digest": contract.semantic_digest,
        "image_reference": profile.image.reference,
        "runtime_class": runtime_class_name,
        "resource_name": profile.kubernetes.resource_name,
        "scheduler_name": scheduler_name,
        "scheduler_observed": True,
        "matching_nodes": matching_nodes,
        "allocatable_nodes": allocatable_nodes,
        "allocatable_devices": allocatable_devices,
        "evidence_status": profile.evidence_status.value,
    }
    if ready_device_plugins is not None:
        result["ready_device_plugin_daemonsets"] = ready_device_plugins
    return result


def collect_kubernetes_serving_preflight(
    contract: ReleaseRuntimeProfileContract,
    *,
    namespace_name: str,
    kubectl: str = "kubectl",
    kubeconfig: Path | None = None,
) -> dict[str, object]:
    """Collect bounded Kubernetes observations using kubectl without echoing credentials."""

    validate_kubernetes_dns_subdomain(namespace_name, field_name="namespace_name")
    executable = shutil.which(kubectl)
    if executable is None:
        raise KubernetesServingPreflightError("kubectl is unavailable")
    prefix = [executable]
    if kubeconfig is not None:
        prefix.extend(("--kubeconfig", str(kubeconfig)))

    readyz = _kubectl_text((*prefix, "get", "--raw=/readyz"), "Kubernetes API")
    api_ready = readyz.strip().casefold().endswith("ok")
    namespace = _kubectl_json(
        (*prefix, "get", "namespace", namespace_name, "-o", "json"),
        "workload namespace",
    )
    runtime_class_name = contract.profile.kubernetes.runtime_class_name
    runtime_class = _kubectl_json(
        (*prefix, "get", "runtimeclass", runtime_class_name, "-o", "json"),
        "RuntimeClass",
    )
    nodes = _kubectl_json(
        (*prefix, "get", "nodes", "-o", "json"),
        "Node capacity",
    )

    scheduler_pods: dict[str, Any] | None = None
    if contract.profile.kubernetes.scheduler_name is not None:
        scheduler_pods = _kubectl_json(
            (*prefix, "get", "pods", "--all-namespaces", "-o", "json"),
            "scheduler observation",
        )

    device_plugin_daemonsets: dict[str, Any] | None = None
    if contract.device_plugin_label_selector is not None:
        device_plugin_daemonsets = _kubectl_json(
            (
                *prefix,
                "get",
                "daemonsets",
                "--all-namespaces",
                "-l",
                contract.device_plugin_label_selector,
                "-o",
                "json",
            ),
            "Device Plugin readiness",
        )

    return evaluate_kubernetes_serving_preflight(
        contract,
        namespace_name=namespace_name,
        api_ready=api_ready,
        namespace=namespace,
        runtime_class=runtime_class,
        nodes=nodes,
        scheduler_pods=scheduler_pods,
        device_plugin_daemonsets=device_plugin_daemonsets,
    )


def _acceptance_contract_path(manifest_path: Path, profile: RuntimeProfile) -> Path:
    references = tuple(
        reference
        for reference in profile.evidence_references
        if reference.startswith("runtime_profiles/") and reference.endswith(".acceptance.json")
    )
    if len(references) != 1:
        raise ValueError("profile must reference exactly one runtime acceptance contract")
    resolved_manifest = manifest_path.resolve(strict=True)
    manifest_directory = resolved_manifest.parent
    repository_root = manifest_directory.parent
    contract_path = (repository_root / Path(*references[0].split("/"))).resolve(strict=True)
    if contract_path.parent != manifest_directory or not contract_path.is_file():
        raise ValueError("runtime acceptance contract escapes the manifest directory")
    return contract_path


def _require_named_object(
    payload: Mapping[str, Any],
    expected_name: str,
    description: str,
) -> None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("name") != expected_name:
        raise KubernetesServingPreflightError(f"{description} is missing")


def _mapping_items(
    payload: Mapping[str, Any] | None,
    description: str,
    *,
    allow_empty: bool,
) -> tuple[Mapping[str, Any], ...]:
    if payload is None:
        if allow_empty:
            return ()
        raise KubernetesServingPreflightError(f"{description} list is missing")
    items = payload.get("items")
    if not isinstance(items, list) or any(not isinstance(item, Mapping) for item in items):
        raise KubernetesServingPreflightError(f"{description} list is invalid")
    if not allow_empty and not items:
        raise KubernetesServingPreflightError(f"{description} list is empty")
    return tuple(items)


def _node_matches_contract(
    profile: RuntimeProfile,
    labels: Mapping[str, Any],
    *,
    required_label_prefixes: tuple[tuple[str, str], ...],
) -> bool:
    if any(labels.get(key) != value for key, value in profile.kubernetes.node_selector.items()):
        return False
    for key, prefix in required_label_prefixes:
        value = labels.get(key)
        if not isinstance(value, str) or not value.startswith(prefix):
            return False
    for requirement in profile.kubernetes.node_affinity:
        raw_value = labels.get(requirement.key)
        operator = requirement.operator
        if operator == "Exists" and raw_value is None:
            return False
        if operator == "DoesNotExist" and raw_value is not None:
            return False
        if operator == "In" and raw_value not in requirement.values:
            return False
        if operator == "NotIn" and (raw_value is None or raw_value in requirement.values):
            return False
        if operator in {"Gt", "Lt"}:
            if not isinstance(raw_value, str) or not raw_value.isdigit():
                return False
            label_number = int(raw_value)
            expected_number = int(requirement.values[0])
            if operator == "Gt" and label_number <= expected_number:
                return False
            if operator == "Lt" and label_number >= expected_number:
                return False
    return True


def _canonical_resource_count(value: object) -> int:
    if not isinstance(value, str) or not value.isdigit() or str(int(value)) != value:
        return 0
    return int(value)


def _ready_daemonset_count(payload: Mapping[str, Any] | None) -> int:
    ready = 0
    for daemonset in _mapping_items(
        payload,
        "Device Plugin DaemonSet",
        allow_empty=True,
    ):
        status = daemonset.get("status")
        if not isinstance(status, Mapping):
            continue
        desired = status.get("desiredNumberScheduled")
        available = status.get("numberAvailable")
        number_ready = status.get("numberReady")
        if (
            isinstance(desired, int)
            and desired > 0
            and number_ready == desired
            and available in (None, desired)
        ):
            ready += 1
    return ready


def _scheduler_is_observable(
    payload: Mapping[str, Any] | None,
    *,
    scheduler_name: str,
) -> bool:
    for pod in _mapping_items(payload, "scheduler Pod", allow_empty=True):
        spec = pod.get("spec")
        status = pod.get("status")
        if not isinstance(spec, Mapping) or not isinstance(status, Mapping):
            continue
        if status.get("phase") != "Running" or not _condition_is_true(status, "Ready"):
            continue
        metadata = pod.get("metadata")
        identifiers: list[str] = []
        if isinstance(metadata, Mapping):
            name = metadata.get("name")
            if isinstance(name, str):
                identifiers.append(name)
            labels = metadata.get("labels")
            if isinstance(labels, Mapping):
                identifiers.extend(
                    str(value)
                    for key, value in labels.items()
                    if isinstance(key, str) and isinstance(value, str)
                )
        containers = spec.get("containers")
        if isinstance(containers, list):
            identifiers.extend(
                str(container.get("name"))
                for container in containers
                if isinstance(container, Mapping) and isinstance(container.get("name"), str)
            )
        needle = scheduler_name.casefold()
        if any(
            needle in identifier.casefold()
            and ("scheduler" in identifier.casefold() or identifier.casefold() == needle)
            for identifier in identifiers
        ):
            return True
    return False


def _condition_is_true(status: Mapping[str, Any], condition_type: str) -> bool:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return False
    return any(
        isinstance(condition, Mapping)
        and condition.get("type") == condition_type
        and condition.get("status") == "True"
        for condition in conditions
    )


def _kubectl_text(command: tuple[str, ...], description: str) -> str:
    completed = _run_kubectl(command, description)
    return completed.stdout


def _kubectl_json(command: tuple[str, ...], description: str) -> dict[str, Any]:
    completed = _run_kubectl(command, description)
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError) as error:
        raise KubernetesServingPreflightError(
            f"Kubernetes API returned invalid JSON for {description}"
        ) from error
    if not isinstance(payload, dict):
        raise KubernetesServingPreflightError(
            f"Kubernetes API returned an invalid object for {description}"
        )
    return payload


def _run_kubectl(command: tuple[str, ...], description: str) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise KubernetesServingPreflightError(
            f"kubectl failed while checking {description}"
        ) from error
    if completed.returncode != 0:
        raise KubernetesServingPreflightError(
            f"Kubernetes API request failed while checking {description}"
        )
    return completed
