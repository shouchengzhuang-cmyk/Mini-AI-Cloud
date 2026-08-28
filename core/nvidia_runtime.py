from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Literal, Self

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, model_validator

from core.enums import AllocationAuthority
from core.runtime_profiles import RuntimeProfile

_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+(?:\.[0-9]+)?$")
_INDEXED_DEVICE_PATTERN = re.compile(r"^nvidia[0-9]+$")


class FrozenNvidiaContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class NvidiaGfdContract(FrozenNvidiaContract):
    minimum_gpu_count: int = Field(ge=1, le=1_024)
    minimum_compute_major: int = Field(ge=1, le=99)
    product_label_required: Literal[True] = True
    sharing_strategy: Literal["none"] = "none"
    mig_strategy: Literal["none"] = "none"


class NvidiaFakeDevicePluginContract(FrozenNvidiaContract):
    resource_name: Literal["example.com/resource"]
    plugin_image: str = Field(pattern=r"^registry\.k8s\.io/.+@sha256:[0-9a-f]{64}$")
    probe_image: str = Field(pattern=r"^registry\.k8s\.io/.+@sha256:[0-9a-f]{64}$")
    scope: Literal["Kubernetes device-plugin allocation only; not NVIDIA hardware or vLLM evidence"]


class NvidiaRuntimeAcceptanceContract(FrozenNvidiaContract):
    schema_version: Literal["1.0.0"]
    profile_identity: str = Field(pattern=r"^[a-z][a-z0-9-]{2,63}@[1-9][0-9]*\.[0-9]+\.[0-9]+$")
    image_reference: str = Field(pattern=r"^docker\.io/.+@sha256:[0-9a-f]{64}$")
    image_platform_digests: dict[Literal["linux/amd64", "linux/arm64"], str]
    vllm_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    vllm_commit: str = Field(pattern=_COMMIT_PATTERN.pattern)
    python_version: str = Field(pattern=r"^3\.[0-9]+$")
    cuda_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    nccl_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    device_plugin_version: str = Field(pattern=_VERSION_PATTERN.pattern)
    resource_name: Literal["nvidia.com/gpu"]
    allocation_authority: Literal[AllocationAuthority.KUBERNETES_DEVICE_PLUGIN]
    gfd_contract: NvidiaGfdContract
    fake_device_plugin: NvidiaFakeDevicePluginContract
    evidence_status: Literal["REAL_HW_NOT_RUN"]
    observed_at: date
    sources: tuple[AnyHttpUrl, ...] = Field(min_length=3, max_length=16)

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
        source_values = [str(source) for source in self.sources]
        if len(set(source_values)) != len(source_values):
            raise ValueError("NVIDIA runtime sources must be unique")
        return self


class NvidiaGpuDiagnostic(FrozenNvidiaContract):
    model: str = Field(min_length=1, max_length=128)
    driver_version: str = Field(pattern=r"^[0-9]+(?:\.[0-9]+){1,3}$")
    memory_total_mb: int = Field(ge=1)
    compute_capability: str = Field(pattern=r"^[0-9]+\.[0-9]+$")


class NvidiaDeviceNodeSummary(FrozenNvidiaContract):
    indexed_device_count: int = Field(ge=0)
    control_nodes: tuple[str, ...]
    wsl_dxg_present: bool = False


def load_nvidia_acceptance_contract(path: Path) -> NvidiaRuntimeAcceptanceContract:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return NvidiaRuntimeAcceptanceContract.model_validate(payload)


def validate_nvidia_profile(
    profile: RuntimeProfile,
    contract: NvidiaRuntimeAcceptanceContract,
) -> None:
    if profile.identity != contract.profile_identity:
        raise ValueError("NVIDIA acceptance contract profile identity does not match")
    if profile.image.reference != contract.image_reference:
        raise ValueError("NVIDIA acceptance contract image does not match the profile")
    if profile.vendor.value != "nvidia" or profile.kind.value != "gpu":
        raise ValueError("NVIDIA runtime profile must use vendor=nvidia and kind=gpu")
    if profile.engine != "vllm" or profile.kubernetes.resource_name != contract.resource_name:
        raise ValueError("NVIDIA runtime profile engine or resource contract does not match")
    if profile.allocation_authority != contract.allocation_authority:
        raise ValueError("NVIDIA allocation authority does not match the acceptance contract")
    if profile.evidence_status.value != contract.evidence_status:
        raise ValueError("NVIDIA profile evidence status does not match the acceptance contract")
    if profile.kubernetes.runtime_class_name != "nvidia":
        raise ValueError("NVIDIA runtime profile must select RuntimeClass nvidia")
    tolerations = {
        (item.key, item.operator, item.value, item.effect)
        for item in profile.kubernetes.tolerations
    }
    if tolerations != {("nvidia.com/gpu", "Exists", None, "NoSchedule")}:
        raise ValueError("NVIDIA runtime profile toleration contract does not match")

    requirements = {
        requirement.key: (requirement.operator, requirement.values)
        for requirement in profile.kubernetes.node_affinity
    }
    expected = {
        "nvidia.com/gpu.product": ("Exists", ()),
        "nvidia.com/gpu.count": (
            "Gt",
            (str(contract.gfd_contract.minimum_gpu_count - 1),),
        ),
        "nvidia.com/gpu.compute.major": (
            "Gt",
            (str(contract.gfd_contract.minimum_compute_major - 1),),
        ),
        "nvidia.com/gpu.sharing-strategy": (
            "In",
            (contract.gfd_contract.sharing_strategy,),
        ),
        "nvidia.com/mig.strategy": ("In", (contract.gfd_contract.mig_strategy,)),
    }
    if requirements != expected:
        raise ValueError("NVIDIA runtime profile GFD node affinity does not match the contract")


def validate_nvidia_node_labels(
    labels: Mapping[str, str],
    *,
    requested_count: int,
    contract: NvidiaRuntimeAcceptanceContract,
) -> None:
    if requested_count < 1:
        raise ValueError("requested_count must be greater than zero")
    product = labels.get("nvidia.com/gpu.product", "").strip()
    if contract.gfd_contract.product_label_required and not product:
        raise ValueError("GPU Feature Discovery product label is missing")
    if product.upper().endswith("-SHARED"):
        raise ValueError("GPU Feature Discovery product label describes a shared resource")
    gpu_count = _canonical_label_integer(labels, "nvidia.com/gpu.count")
    minimum_count = max(requested_count, contract.gfd_contract.minimum_gpu_count)
    if gpu_count < minimum_count:
        raise ValueError("GPU Feature Discovery count is below the requested accelerator count")
    compute_major = _canonical_label_integer(labels, "nvidia.com/gpu.compute.major")
    if compute_major < contract.gfd_contract.minimum_compute_major:
        raise ValueError("GPU compute capability is below the NVIDIA profile minimum")
    if labels.get("nvidia.com/gpu.sharing-strategy") != contract.gfd_contract.sharing_strategy:
        raise ValueError("NVIDIA sharing strategy must be none")
    if labels.get("nvidia.com/mig.strategy") != contract.gfd_contract.mig_strategy:
        raise ValueError("NVIDIA MIG strategy must be none")


def parse_nvidia_smi_csv(output: str) -> tuple[NvidiaGpuDiagnostic, ...]:
    rows = csv.reader(StringIO(output))
    diagnostics: list[NvidiaGpuDiagnostic] = []
    for row in rows:
        if not row or all(not value.strip() for value in row):
            continue
        if len(row) != 4:
            raise ValueError("nvidia-smi diagnostic output must have exactly four columns")
        model, driver_version, memory_total_mb, compute_capability = (
            value.strip() for value in row
        )
        diagnostics.append(
            NvidiaGpuDiagnostic(
                model=model,
                driver_version=driver_version,
                memory_total_mb=int(memory_total_mb),
                compute_capability=compute_capability,
            )
        )
    if not diagnostics:
        raise ValueError("nvidia-smi diagnostic output contains no GPUs")
    return tuple(diagnostics)


def summarize_nvidia_device_nodes(paths: tuple[Path, ...]) -> NvidiaDeviceNodeSummary:
    names = sorted({path.name for path in paths})
    indexed = tuple(name for name in names if _INDEXED_DEVICE_PATTERN.fullmatch(name))
    control = tuple(name for name in names if name in {"nvidiactl", "nvidia-modeset", "nvidia-uvm"})
    return NvidiaDeviceNodeSummary(
        indexed_device_count=len(indexed),
        control_nodes=control,
        wsl_dxg_present="dxg" in names,
    )


def _canonical_label_integer(labels: Mapping[str, str], name: str) -> int:
    value = labels.get(name, "")
    if not value.isdigit() or str(int(value)) != value:
        raise ValueError(f"{name} must be a canonical integer label")
    return int(value)
