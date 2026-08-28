from __future__ import annotations

import hashlib
import json
import re
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.accelerators import vendor_kind_is_compatible
from core.enums import AcceleratorKind, AcceleratorVendor, AllocationAuthority
from core.image_policy import ImageReferenceError, canonicalize_image_reference

PROFILE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
PROFILE_VERSION_PATTERN = re.compile(r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
KUBERNETES_RESOURCE_PATTERN = re.compile(
    r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?/[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?$"
)
KUBERNETES_NAME_PATTERN = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")
VENDOR_NODE_SELECTOR = "accelerator.mini-ai-cloud/vendor"

_FORBIDDEN_ENVIRONMENT_FRAGMENTS = (
    "API_KEY",
    "CREDENTIAL",
    "PASSWORD",
    "PRIVATE_KEY",
    "SECRET",
    "TOKEN",
)
_ALLOCATION_ENVIRONMENT_NAMES = {
    "ASCEND_DEVICE_ID",
    "ASCEND_RT_VISIBLE_DEVICES",
    "ASCEND_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
    "NVIDIA_VISIBLE_DEVICES",
}
_FORBIDDEN_COMMAND_EXECUTABLES = {
    "bash",
    "cmd",
    "dash",
    "powershell",
    "pwsh",
    "sh",
    "zsh",
}
_FORBIDDEN_COMMAND_ARGUMENT_FRAGMENTS = (
    "--api-key",
    "--credential",
    "--password",
    "--secret",
    "--token",
)
_EVIDENCE_REFERENCE_REQUIRED = {
    "MANIFEST_VALIDATED",
    "SIMULATED",
    "REAL_ENGINE_PASS",
    "REAL_CONTROL_PLANE_PASS",
    "REAL_DUAL_BACKEND_PASS",
}

NonEmptyText = Annotated[str, Field(min_length=1, max_length=512)]


class RuntimeProfileEvidenceStatus(StrEnum):
    SCHEMA_READY = "SCHEMA_READY"
    MANIFEST_VALIDATED = "MANIFEST_VALIDATED"
    SIMULATED = "SIMULATED"
    REAL_ENGINE_PASS = "REAL_ENGINE_PASS"
    REAL_CONTROL_PLANE_PASS = "REAL_CONTROL_PLANE_PASS"
    REAL_DUAL_BACKEND_PASS = "REAL_DUAL_BACKEND_PASS"
    REAL_HW_NOT_RUN = "REAL_HW_NOT_RUN"
    BLOCKED = "BLOCKED"


class CompatibilityPolicy(StrEnum):
    PROFILE_OWNED = "profile-owned"
    PINNED = "pinned"
    RECORDED_AT_RUNTIME = "recorded-at-runtime"


class FrozenContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


class RuntimeImage(FrozenContractModel):
    reference: str = Field(min_length=1, max_length=512)
    digest_required: Literal[True] = True

    @field_validator("reference")
    @classmethod
    def require_canonical_digest_reference(cls, value: str) -> str:
        try:
            image = canonicalize_image_reference(value)
        except (ImageReferenceError, ValueError) as error:
            raise ValueError(str(error)) from error
        if image.digest is None:
            raise ValueError("runtime profile image must be pinned by sha256 digest")
        return image.canonical


class RuntimeSecurityContext(FrozenContractModel):
    privileged: Literal[False] = False
    host_pid: Literal[False] = Field(default=False, alias="hostPID")
    host_network: Literal[False] = Field(default=False, alias="hostNetwork")
    host_path: tuple[str, ...] = Field(default=(), max_length=0, alias="hostPath")
    allow_privilege_escalation: Literal[False] = False


class KubernetesToleration(FrozenContractModel):
    key: str = Field(min_length=1, max_length=253)
    operator: Literal["Equal", "Exists"] = "Equal"
    value: str | None = Field(default=None, max_length=63)
    effect: Literal["NoSchedule", "PreferNoSchedule", "NoExecute"]

    @model_validator(mode="after")
    def validate_operator_value(self) -> Self:
        if self.operator == "Equal" and not self.value:
            raise ValueError("Equal tolerations require a value")
        if self.operator == "Exists" and self.value is not None:
            raise ValueError("Exists tolerations must not set a value")
        return self


class KubernetesDeviceVisibility(FrozenContractModel):
    environment_name: Literal["ASCEND_VISIBLE_DEVICES"]
    source: Literal["pod-annotation"]
    annotation_key: str = Field(min_length=3, max_length=253)
    scheduling_mode: Literal["mindcluster-volcano-full-card"]

    @field_validator("annotation_key")
    @classmethod
    def validate_annotation_key(cls, value: str) -> str:
        if not KUBERNETES_RESOURCE_PATTERN.fullmatch(value):
            raise ValueError("annotation_key must be a qualified Kubernetes annotation")
        return value


class KubernetesRuntime(FrozenContractModel):
    runtime_class_name: str = Field(min_length=1, max_length=253)
    scheduler_name: str | None = Field(default=None, min_length=1, max_length=253)
    resource_name: str = Field(min_length=3, max_length=253)
    node_selector: dict[str, str] = Field(min_length=1, max_length=32)
    tolerations: tuple[KubernetesToleration, ...] = Field(default=(), max_length=32)
    device_visibility: KubernetesDeviceVisibility | None = None
    requests_equal_limits: Literal[True] = True
    security: RuntimeSecurityContext

    @field_validator("runtime_class_name")
    @classmethod
    def validate_runtime_class_name(cls, value: str) -> str:
        if not KUBERNETES_NAME_PATTERN.fullmatch(value):
            raise ValueError("runtime_class_name must be a Kubernetes DNS subdomain")
        return value

    @field_validator("scheduler_name")
    @classmethod
    def validate_scheduler_name(cls, value: str | None) -> str | None:
        if value is not None and not KUBERNETES_NAME_PATTERN.fullmatch(value):
            raise ValueError("scheduler_name must be a Kubernetes DNS subdomain")
        return value

    @field_validator("resource_name")
    @classmethod
    def validate_resource_name(cls, value: str) -> str:
        if not KUBERNETES_RESOURCE_PATTERN.fullmatch(value):
            raise ValueError("resource_name must be a qualified Kubernetes extended resource")
        return value

    @field_validator("node_selector")
    @classmethod
    def validate_node_selector(cls, value: dict[str, str]) -> dict[str, str]:
        for key, selector_value in value.items():
            if not key or len(key) > 253 or not selector_value or len(selector_value) > 63:
                raise ValueError(
                    "node_selector keys and values must be non-empty Kubernetes labels"
                )
            if any(character.isspace() or ord(character) < 32 for character in key):
                raise ValueError("node_selector keys must not contain whitespace")
            if any(character.isspace() or ord(character) < 32 for character in selector_value):
                raise ValueError("node_selector values must not contain whitespace")
        return value


class RuntimeProcess(FrozenContractModel):
    command: tuple[NonEmptyText, ...] = Field(min_length=1, max_length=32)
    command_allowlist: tuple[tuple[NonEmptyText, ...], ...] = Field(min_length=1, max_length=16)
    env_allowlist: tuple[str, ...] = Field(default=(), max_length=64)
    api_protocol: Literal["openai"] = "openai"
    visible_devices_source: Literal["device-plugin"] = "device-plugin"

    @model_validator(mode="after")
    def validate_allowlists(self) -> Self:
        if len(set(self.command_allowlist)) != len(self.command_allowlist):
            raise ValueError("command_allowlist entries must be unique")
        if self.command not in self.command_allowlist:
            raise ValueError("command must exactly match one command_allowlist entry")
        executable = self.command[0].rsplit("/", maxsplit=1)[-1].casefold()
        if executable in _FORBIDDEN_COMMAND_EXECUTABLES:
            raise ValueError("shell interpreters are not valid runtime profile commands")
        for argument in self.command:
            normalized = argument.casefold()
            if any(fragment in normalized for fragment in _FORBIDDEN_COMMAND_ARGUMENT_FRAGMENTS):
                raise ValueError("runtime profile commands must not contain credential arguments")
            if any(ord(character) < 32 for character in argument):
                raise ValueError("runtime profile commands must not contain control characters")

        if len(set(self.env_allowlist)) != len(self.env_allowlist):
            raise ValueError("env_allowlist entries must be unique")
        for name in self.env_allowlist:
            if not ENVIRONMENT_NAME_PATTERN.fullmatch(name):
                raise ValueError(f"invalid environment variable name: {name}")
            if name in _ALLOCATION_ENVIRONMENT_NAMES:
                raise ValueError(f"device allocation environment is plugin-owned: {name}")
            if any(fragment in name for fragment in _FORBIDDEN_ENVIRONMENT_FRAGMENTS):
                raise ValueError(f"credential-like environment variable is forbidden: {name}")
        return self


class HttpProbe(FrozenContractModel):
    path: str = Field(pattern=r"^/[A-Za-z0-9_./-]*$", max_length=256)
    port: int = Field(ge=1, le=65_535)
    initial_delay_seconds: int = Field(ge=0, le=3_600)
    period_seconds: int = Field(ge=1, le=300)
    timeout_seconds: int = Field(ge=1, le=60)
    failure_threshold: int = Field(ge=1, le=60)


class RuntimeProbes(FrozenContractModel):
    health: HttpProbe
    readiness: HttpProbe


class TensorParallelCapability(FrozenContractModel):
    supported: bool
    scope: Literal["single-node"] = "single-node"
    minimum_size: int = Field(ge=1, le=1_024)
    maximum_size: int | None = Field(default=None, ge=1, le=1_024)
    maximum_size_source: Literal["profile", "runtime-observation"]

    @model_validator(mode="after")
    def validate_size_source(self) -> Self:
        if self.maximum_size_source == "profile" and self.maximum_size is None:
            raise ValueError("profile-owned tensor parallel limits require maximum_size")
        if self.maximum_size_source == "runtime-observation" and self.maximum_size is not None:
            raise ValueError("runtime-observed tensor parallel limits must omit maximum_size")
        if self.maximum_size is not None and self.maximum_size < self.minimum_size:
            raise ValueError("maximum_size must be greater than or equal to minimum_size")
        if not self.supported and (self.minimum_size != 1 or self.maximum_size not in (None, 1)):
            raise ValueError("unsupported tensor parallel profiles may only describe size one")
        return self


class RuntimeCapabilities(FrozenContractModel):
    model_architectures: tuple[NonEmptyText, ...] = Field(default=(), max_length=128)
    dtypes: tuple[NonEmptyText, ...] = Field(min_length=1, max_length=32)
    features: tuple[NonEmptyText, ...] = Field(default=(), max_length=64)
    tensor_parallel: TensorParallelCapability

    @model_validator(mode="after")
    def validate_unique_capabilities(self) -> Self:
        for name, values in (
            ("model_architectures", self.model_architectures),
            ("dtypes", self.dtypes),
            ("features", self.features),
        ):
            if len(set(values)) != len(values):
                raise ValueError(f"{name} entries must be unique")
        return self


class RuntimeCompatibility(FrozenContractModel):
    hardware_families: tuple[NonEmptyText, ...] = Field(default=(), max_length=64)
    python: Literal[CompatibilityPolicy.PROFILE_OWNED]
    vllm_version: Literal[CompatibilityPolicy.PINNED]
    vendor_plugin_version: Literal[CompatibilityPolicy.PINNED]
    driver_version: Literal[CompatibilityPolicy.RECORDED_AT_RUNTIME]
    toolkit_version: Literal[CompatibilityPolicy.RECORDED_AT_RUNTIME]


class RuntimeProfile(FrozenContractModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    id: str = Field(pattern=PROFILE_ID_PATTERN.pattern)
    version: str = Field(pattern=PROFILE_VERSION_PATTERN.pattern)
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    execution_mode: Literal["kubernetes"]
    engine: str = Field(pattern=r"^[a-z][a-z0-9-]{1,63}$")
    image: RuntimeImage
    kubernetes: KubernetesRuntime
    process: RuntimeProcess
    probes: RuntimeProbes
    compatibility: RuntimeCompatibility
    capabilities: RuntimeCapabilities
    allocation_authority: Literal[AllocationAuthority.KUBERNETES_DEVICE_PLUGIN]
    evidence_status: RuntimeProfileEvidenceStatus
    evidence_references: tuple[NonEmptyText, ...] = Field(default=(), max_length=64)
    limitations: tuple[NonEmptyText, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def validate_profile_invariants(self) -> Self:
        if not vendor_kind_is_compatible(self.vendor, self.kind):
            raise ValueError(f"{self.vendor.value} is not compatible with kind={self.kind.value}")
        selected_vendor = self.kubernetes.node_selector.get(VENDOR_NODE_SELECTOR)
        if selected_vendor != self.vendor.value:
            raise ValueError(f"node_selector must set {VENDOR_NODE_SELECTOR}={self.vendor.value}")
        visibility = self.kubernetes.device_visibility
        if visibility is not None:
            if self.vendor != AcceleratorVendor.HUAWEI_ASCEND or self.kind != AcceleratorKind.NPU:
                raise ValueError("device_visibility is restricted to Huawei Ascend NPU profiles")
            if self.kubernetes.scheduler_name != "volcano":
                raise ValueError("Ascend annotation visibility requires scheduler_name=volcano")
            if visibility.annotation_key != self.kubernetes.resource_name:
                raise ValueError("device visibility annotation_key must equal resource_name")
        if (
            self.evidence_status.value in _EVIDENCE_REFERENCE_REQUIRED
            and not self.evidence_references
        ):
            raise ValueError(f"{self.evidence_status.value} requires evidence_references")
        if len(set(self.evidence_references)) != len(self.evidence_references):
            raise ValueError("evidence_references entries must be unique")
        if len(set(self.limitations)) != len(self.limitations):
            raise ValueError("limitations entries must be unique")
        return self

    @property
    def identity(self) -> str:
        return f"{self.id}@{self.version}"

    def semantic_digest(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json", by_alias=True),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def generated_runtime_profile_schema() -> dict[str, object]:
    return RuntimeProfile.model_json_schema(by_alias=True)
