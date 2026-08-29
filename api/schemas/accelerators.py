import re
from collections.abc import Set

from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from api.schemas.common import RequestModel
from core.accelerators import kind_for_vendor, vendor_for_kind
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
)

_CONTRACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
LEGACY_GPU_FIELDS = frozenset({"gpu_count", "gpu_memory_mb", "gpu_model"})


class AcceleratorRequest(RequestModel):
    """Vendor-neutral accelerator request contract.

    The normalized request remains authoritative even when legacy ``gpu_*``
    columns are populated for aggregate resource accounting compatibility.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "count": 1,
                    "memory_mb_per_device": 24_000,
                    "allowed_vendors": ["nvidia", "huawei-ascend"],
                    "allowed_kinds": ["gpu", "npu"],
                    "allowed_models": [],
                    "required_capabilities": ["bf16"],
                    "runtime_profile": None,
                    "selection_policy": "any",
                }
            ]
        }
    )

    count: StrictInt = Field(default=0, ge=0, le=64)
    memory_mb_per_device: StrictInt = Field(default=0, ge=0, le=1_048_576)
    allowed_vendors: list[AcceleratorVendor] = Field(default_factory=list, max_length=2)
    allowed_kinds: list[AcceleratorKind] = Field(default_factory=list, max_length=2)
    allowed_models: list[StrictStr] = Field(default_factory=list, max_length=64)
    required_capabilities: list[StrictStr] = Field(default_factory=list, max_length=64)
    runtime_profile: StrictStr | None = Field(default=None, min_length=1, max_length=128)
    selection_policy: AcceleratorSelectionPolicy = AcceleratorSelectionPolicy.ANY

    @field_validator("allowed_vendors", "allowed_kinds")
    @classmethod
    def validate_unique_enum_values(cls, values: list[object]) -> list[object]:
        if len(values) != len(set(values)):
            raise ValueError("accelerator vendor and kind constraints must be unique")
        return values

    @field_validator("allowed_models")
    @classmethod
    def validate_models(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            model = value.strip()
            if not model or len(model) > 255 or any(ord(character) < 32 for character in model):
                raise ValueError("allowed_models entries must be non-blank and contain no controls")
            normalized.append(model)
        if len(normalized) != len(set(normalized)):
            raise ValueError("allowed_models must be unique")
        return normalized

    @field_validator("required_capabilities")
    @classmethod
    def validate_capabilities(cls, values: list[str]) -> list[str]:
        normalized = [value.strip() for value in values]
        if any(_CONTRACT_NAME.fullmatch(value) is None for value in normalized):
            raise ValueError("required_capabilities entries use an unsupported format")
        if len(normalized) != len(set(normalized)):
            raise ValueError("required_capabilities must be unique")
        return normalized

    @field_validator("runtime_profile")
    @classmethod
    def validate_runtime_profile(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if _CONTRACT_NAME.fullmatch(normalized) is None:
            raise ValueError("runtime_profile uses an unsupported format")
        return normalized

    @model_validator(mode="after")
    def validate_contract(self) -> "AcceleratorRequest":
        constrained = bool(
            self.memory_mb_per_device
            or self.allowed_vendors
            or self.allowed_kinds
            or self.allowed_models
            or self.required_capabilities
            or self.runtime_profile is not None
            or self.selection_policy != AcceleratorSelectionPolicy.ANY
        )
        if self.count == 0:
            if constrained:
                raise ValueError("accelerator constraints require count greater than zero")
            return self
        if not self.allowed_vendors or not self.allowed_kinds:
            raise ValueError("count greater than zero requires allowed_vendors and allowed_kinds")

        vendors = set(self.allowed_vendors)
        kinds = set(self.allowed_kinds)
        for vendor in vendors:
            if kind_for_vendor(vendor) not in kinds:
                raise ValueError(f"allowed_kinds does not include the kind for {vendor.value}")
        for kind in kinds:
            if vendor_for_kind(kind) not in vendors:
                raise ValueError(f"allowed_vendors does not include the vendor for {kind.value}")

        if self.selection_policy == AcceleratorSelectionPolicy.NVIDIA_ONLY and vendors != {
            AcceleratorVendor.NVIDIA
        }:
            raise ValueError("nvidia-only requires allowed_vendors=['nvidia']")
        if self.selection_policy == AcceleratorSelectionPolicy.ASCEND_ONLY and vendors != {
            AcceleratorVendor.HUAWEI_ASCEND
        }:
            raise ValueError("ascend-only requires allowed_vendors=['huawei-ascend']")
        if (
            self.selection_policy == AcceleratorSelectionPolicy.PREFER_NVIDIA
            and AcceleratorVendor.NVIDIA not in vendors
        ):
            raise ValueError("prefer-nvidia requires nvidia in allowed_vendors")
        if (
            self.selection_policy == AcceleratorSelectionPolicy.PREFER_ASCEND
            and AcceleratorVendor.HUAWEI_ASCEND not in vendors
        ):
            raise ValueError("prefer-ascend requires huawei-ascend in allowed_vendors")
        return self

    @classmethod
    def from_legacy_gpu(
        cls,
        *,
        gpu_count: int,
        gpu_memory_mb: int,
        gpu_model: str | None,
    ) -> "AcceleratorRequest":
        if gpu_count == 0:
            if gpu_memory_mb or gpu_model is not None:
                raise ValueError("gpu_model and gpu_memory_mb require gpu_count greater than zero")
            return cls()
        return cls(
            count=gpu_count,
            memory_mb_per_device=gpu_memory_mb,
            allowed_vendors=[AcceleratorVendor.NVIDIA],
            allowed_kinds=[AcceleratorKind.GPU],
            allowed_models=[] if gpu_model is None else [gpu_model],
            selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
        )

    def is_legacy_gpu_compatible(self) -> bool:
        if self.count == 0:
            return True
        return (
            set(self.allowed_vendors) == {AcceleratorVendor.NVIDIA}
            and set(self.allowed_kinds) == {AcceleratorKind.GPU}
            and len(self.allowed_models) <= 1
            and not self.required_capabilities
            and self.runtime_profile is None
            and self.selection_policy
            in {AcceleratorSelectionPolicy.ANY, AcceleratorSelectionPolicy.NVIDIA_ONLY}
        )

    def legacy_gpu_values(self) -> tuple[int, int, str | None]:
        if not self.is_legacy_gpu_compatible():
            raise ValueError(
                "the accelerator request cannot be represented by the v0.4 NVIDIA GPU fields"
            )
        if self.count == 0:
            return 0, 0, None
        return (
            self.count,
            self.memory_mb_per_device,
            self.allowed_models[0] if self.allowed_models else None,
        )


def reconcile_legacy_gpu_fields(
    *,
    accelerator: AcceleratorRequest | None,
    gpu_count: int,
    gpu_memory_mb: int,
    gpu_model: str | None,
    fields_set: Set[str],
) -> tuple[int, int, str | None]:
    legacy_values = (gpu_count, gpu_memory_mb, gpu_model)
    legacy_was_supplied = bool(LEGACY_GPU_FIELDS.intersection(fields_set))
    if accelerator is None:
        AcceleratorRequest.from_legacy_gpu(
            gpu_count=gpu_count,
            gpu_memory_mb=gpu_memory_mb,
            gpu_model=gpu_model,
        )
        return legacy_values

    if legacy_was_supplied:
        if not accelerator.is_legacy_gpu_compatible():
            raise ValueError("legacy gpu_* fields cannot be combined with a non-GPU accelerator")
        if accelerator.legacy_gpu_values() != legacy_values:
            raise ValueError("accelerator conflicts with legacy gpu_* fields")
        return legacy_values
    if accelerator.is_legacy_gpu_compatible():
        return accelerator.legacy_gpu_values()
    return (
        accelerator.count,
        accelerator.memory_mb_per_device,
        accelerator.allowed_models[0] if len(accelerator.allowed_models) == 1 else None,
    )


def require_current_execution_support(accelerator: AcceleratorRequest | None) -> None:
    """Compatibility hook retained for callers introduced before A9.

    A9 admits the complete validated contract. Runtime, profile, model-variant,
    quota and capacity constraints are enforced by vendor-aware admission.
    """

    del accelerator
