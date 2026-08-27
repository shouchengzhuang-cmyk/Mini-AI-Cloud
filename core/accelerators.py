from dataclasses import dataclass

from core.enums import AcceleratorKind, AcceleratorVendor

_VENDOR_KINDS = {
    AcceleratorVendor.NVIDIA: AcceleratorKind.GPU,
    AcceleratorVendor.HUAWEI_ASCEND: AcceleratorKind.NPU,
}


def kind_for_vendor(vendor: AcceleratorVendor) -> AcceleratorKind:
    return _VENDOR_KINDS[vendor]


def vendor_for_kind(kind: AcceleratorKind) -> AcceleratorVendor:
    for vendor, vendor_kind in _VENDOR_KINDS.items():
        if vendor_kind == kind:
            return vendor
    raise ValueError(f"unsupported accelerator kind: {kind}")


def vendor_kind_is_compatible(vendor: AcceleratorVendor, kind: AcceleratorKind) -> bool:
    return _VENDOR_KINDS.get(vendor) == kind


@dataclass(frozen=True, slots=True)
class AcceleratorDevice:
    """Vendor-neutral accelerator inventory value object.

    A2 persists this vocabulary without renaming the existing ``gpu_devices``
    table. Provider discovery and vendor runtime behavior remain later M6 work.
    """

    device_id: str
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    model: str
    memory_total_mb: int
    memory_free_mb: int
    health: str = "healthy"
    compute_arch: str | None = None
    runtime_profile_ids: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    fake: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.vendor, AcceleratorVendor):
            raise TypeError("vendor must be an AcceleratorVendor")
        if not isinstance(self.kind, AcceleratorKind):
            raise TypeError("kind must be an AcceleratorKind")
        if not self.device_id.strip():
            raise ValueError("device_id must not be blank")
        if not vendor_kind_is_compatible(self.vendor, self.kind):
            raise ValueError(
                f"{self.vendor.value} devices must use kind={kind_for_vendor(self.vendor)}"
            )
        if not self.model.strip():
            raise ValueError("model must not be blank")
        if self.memory_total_mb <= 0:
            raise ValueError("memory_total_mb must be greater than zero")
        if not 0 <= self.memory_free_mb <= self.memory_total_mb:
            raise ValueError("memory_free_mb must be between zero and memory_total_mb")
        if not self.health.strip():
            raise ValueError("health must not be blank")
        if len(self.runtime_profile_ids) != len(set(self.runtime_profile_ids)):
            raise ValueError("runtime_profile_ids must be unique")
        if any(not profile_id.strip() for profile_id in self.runtime_profile_ids):
            raise ValueError("runtime_profile_ids must not contain blank values")
        if len(self.capabilities) != len(set(self.capabilities)):
            raise ValueError("capabilities must be unique")
        if any(not capability.strip() for capability in self.capabilities):
            raise ValueError("capabilities must not contain blank values")
