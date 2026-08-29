from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, model_validator

from core.enums import AllocationAuthority
from core.runtime_profiles import RuntimeProfile

_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:\.post[0-9]+)?$")
_INDEXED_DEVICE_PATTERN = re.compile(r"^davinci[0-9]+$")


class FrozenAscendContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class AscendClusterContract(FrozenAscendContract):
    runtime_class_name: Literal["ascend"]
    runtime_handler: Literal["ascend"]
    scheduler_name: Literal["volcano"]
    plugin_daemonset_label_selector: Literal["name=ascend-device-plugin-ds"]
    vendor_node_label: Literal["accelerator.mini-ai-cloud/vendor"]
    vendor_node_value: Literal["huawei-ascend"]
    chip_name_label: Literal["node.kubernetes.io/npu.chip.name"]
    chip_name_prefix: Literal["Ascend910B"]


class AscendVisibilityContract(FrozenAscendContract):
    environment_name: Literal["ASCEND_VISIBLE_DEVICES"]
    source: Literal["pod-annotation"]
    annotation_key: Literal["huawei.com/Ascend910"]
    scheduling_mode: Literal["mindcluster-volcano-full-card"]


class AscendRuntimeAcceptanceContract(FrozenAscendContract):
    schema_version: Literal["1.0.0"]
    profile_identity: str = Field(pattern=r"^[a-z][a-z0-9-]{2,63}@[1-9][0-9]*\.[0-9]+\.[0-9]+$")
    image_reference: str = Field(pattern=r"^quay\.io/ascend/vllm-ascend@sha256:[0-9a-f]{64}$")
    image_platform_digests: dict[Literal["linux/amd64", "linux/arm64"], str]
    product_generation: Literal["Atlas A2 (Ascend 910B)"]
    vllm_ascend_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    vllm_ascend_commit: str = Field(pattern=_COMMIT_PATTERN.pattern)
    vllm_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    python_version: Literal["3.12"]
    cann_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    torch_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    torch_npu_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    mindcluster_version: str = Field(pattern=r"^v[0-9]+\.[0-9]+\.[0-9]+$")
    resource_name: Literal["huawei.com/Ascend910"]
    allocation_authority: Literal[AllocationAuthority.KUBERNETES_DEVICE_PLUGIN]
    cluster: AscendClusterContract
    visibility: AscendVisibilityContract
    evidence_status: Literal["REAL_HW_NOT_RUN"]
    observed_at: date
    sources: tuple[AnyHttpUrl, ...] = Field(min_length=5, max_length=16)

    @model_validator(mode="after")
    def validate_digests_and_sources(self) -> Self:
        if set(self.image_platform_digests) != {"linux/amd64", "linux/arm64"}:
            raise ValueError("image platform digests must cover linux/amd64 and linux/arm64")
        if any(
            not _DIGEST_PATTERN.fullmatch(value) for value in self.image_platform_digests.values()
        ):
            raise ValueError("image platform digests must be canonical sha256 values")
        if len(set(self.image_platform_digests.values())) != len(self.image_platform_digests):
            raise ValueError("image platform digests must be unique")
        if self.visibility.annotation_key != self.resource_name:
            raise ValueError("Ascend visibility annotation must match the resource name")
        source_values = [str(source) for source in self.sources]
        if len(set(source_values)) != len(source_values):
            raise ValueError("Ascend runtime sources must be unique")
        return self


class AscendNpuDiagnostic(FrozenAscendContract):
    card_count: int = Field(ge=1, le=1_024)
    chip_count: int = Field(ge=1, le=65_536)
    product_names: tuple[str, ...]


class AscendDeviceNodeSummary(FrozenAscendContract):
    indexed_device_count: int = Field(ge=0)
    control_nodes: tuple[str, ...]


def load_ascend_acceptance_contract(path: Path) -> AscendRuntimeAcceptanceContract:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return AscendRuntimeAcceptanceContract.model_validate(payload)


def validate_ascend_profile(
    profile: RuntimeProfile,
    contract: AscendRuntimeAcceptanceContract,
) -> None:
    if profile.identity != contract.profile_identity:
        raise ValueError("Ascend acceptance contract profile identity does not match")
    if profile.image.reference != contract.image_reference:
        raise ValueError("Ascend acceptance contract image does not match the profile")
    if profile.vendor.value != "huawei-ascend" or profile.kind.value != "npu":
        raise ValueError("Ascend runtime profile must use vendor=huawei-ascend and kind=npu")
    if profile.engine != "vllm-ascend":
        raise ValueError("Ascend runtime profile must use engine=vllm-ascend")
    if profile.kubernetes.resource_name != contract.resource_name:
        raise ValueError("Ascend resource name does not match the acceptance contract")
    if profile.allocation_authority != contract.allocation_authority:
        raise ValueError("Ascend allocation authority does not match the acceptance contract")
    if profile.evidence_status.value != contract.evidence_status:
        raise ValueError("Ascend profile evidence status does not match the acceptance contract")
    if profile.kubernetes.runtime_class_name != contract.cluster.runtime_class_name:
        raise ValueError("Ascend RuntimeClass does not match the acceptance contract")
    if profile.kubernetes.scheduler_name != contract.cluster.scheduler_name:
        raise ValueError("Ascend scheduler does not match the MindCluster contract")
    visibility = profile.kubernetes.device_visibility
    if visibility is None or visibility.model_dump(mode="json") != contract.visibility.model_dump(
        mode="json"
    ):
        raise ValueError("Ascend device visibility does not match the acceptance contract")
    tolerations = {
        (item.key, item.operator, item.value, item.effect)
        for item in profile.kubernetes.tolerations
    }
    if tolerations != {(contract.resource_name, "Exists", None, "NoSchedule")}:
        raise ValueError("Ascend runtime profile toleration contract does not match")
    if tuple(profile.compatibility.hardware_families) != (contract.product_generation,):
        raise ValueError("Ascend product generation does not match the acceptance contract")


def parse_npu_smi_list(output: str) -> AscendNpuDiagnostic:
    values: dict[str, list[str]] = {}
    for line in output.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        values.setdefault(key.strip(), []).append(value.strip())
    card_values = values.get("Card Count", [])
    chip_values = values.get("Chip Count", [])
    if len(card_values) != 1 or not card_values[0].isdigit():
        raise ValueError("npu-smi output omitted a canonical Card Count")
    if not chip_values or any(not value.isdigit() for value in chip_values):
        raise ValueError("npu-smi output omitted canonical Chip Count values")
    card_count = int(card_values[0])
    product_names = tuple(
        value for value in values.get("Product Name", []) if value and value.upper() != "NA"
    )
    return AscendNpuDiagnostic(
        card_count=card_count,
        chip_count=sum(int(value) for value in chip_values),
        product_names=product_names,
    )


def summarize_ascend_device_nodes(paths: tuple[Path, ...]) -> AscendDeviceNodeSummary:
    names = sorted({path.name for path in paths})
    indexed = tuple(name for name in names if _INDEXED_DEVICE_PATTERN.fullmatch(name))
    control = tuple(name for name in names if name in {"davinci_manager", "devmm_svm", "hisi_hdc"})
    return AscendDeviceNodeSummary(
        indexed_device_count=len(indexed),
        control_nodes=control,
    )


def evaluate_ascend_cluster(
    *,
    runtime_class: Mapping[str, Any],
    daemonsets: Mapping[str, Any],
    nodes: Mapping[str, Any],
    contract: AscendRuntimeAcceptanceContract,
) -> dict[str, object]:
    metadata = runtime_class.get("metadata")
    if (
        not isinstance(metadata, Mapping)
        or metadata.get("name") != contract.cluster.runtime_class_name
    ):
        raise ValueError("Ascend RuntimeClass is missing or has an unexpected name")
    if runtime_class.get("handler") != contract.cluster.runtime_handler:
        raise ValueError("Ascend RuntimeClass handler does not match the contract")

    daemonset_items = _mapping_items(daemonsets, "Device Plugin DaemonSet")
    ready_daemonsets = 0
    for item in daemonset_items:
        status = item.get("status")
        if not isinstance(status, Mapping):
            continue
        desired = status.get("desiredNumberScheduled")
        ready = status.get("numberReady")
        if isinstance(desired, int) and desired > 0 and ready == desired:
            ready_daemonsets += 1
    if ready_daemonsets == 0:
        raise ValueError("no ready Ascend Device Plugin DaemonSet was observed")

    node_items = _mapping_items(nodes, "Node")
    allocatable_nodes = 0
    allocatable_devices = 0
    for item in node_items:
        node_metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(node_metadata, Mapping) or not isinstance(status, Mapping):
            continue
        labels = node_metadata.get("labels")
        allocatable = status.get("allocatable")
        if not isinstance(labels, Mapping) or not isinstance(allocatable, Mapping):
            continue
        if labels.get(contract.cluster.vendor_node_label) != contract.cluster.vendor_node_value:
            continue
        chip_name = labels.get(contract.cluster.chip_name_label)
        if not isinstance(chip_name, str) or not chip_name.startswith(
            contract.cluster.chip_name_prefix
        ):
            continue
        count = _canonical_resource_count(allocatable.get(contract.resource_name))
        if count > 0:
            allocatable_nodes += 1
            allocatable_devices += count
    if allocatable_nodes == 0:
        raise ValueError("no Atlas A2 node advertises the configured Ascend resource")
    return {
        "status": "CLUSTER_PREFLIGHT_PASS",
        "profile_identity": contract.profile_identity,
        "runtime_class": contract.cluster.runtime_class_name,
        "scheduler_name": contract.cluster.scheduler_name,
        "resource_name": contract.resource_name,
        "ready_device_plugin_daemonsets": ready_daemonsets,
        "allocatable_nodes": allocatable_nodes,
        "allocatable_devices": allocatable_devices,
    }


def _mapping_items(payload: Mapping[str, Any], description: str) -> tuple[Mapping[str, Any], ...]:
    items = payload.get("items")
    if (
        not isinstance(items, list)
        or not items
        or any(not isinstance(item, Mapping) for item in items)
    ):
        raise ValueError(f"{description} list is empty or invalid")
    return tuple(items)


def _canonical_resource_count(value: object) -> int:
    if not isinstance(value, str) or not value.isdigit() or str(int(value)) != value:
        return 0
    return int(value)
