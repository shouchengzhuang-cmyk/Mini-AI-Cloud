import re
from dataclasses import dataclass

from core.enums import AcceleratorKind, AcceleratorVendor

_VENDOR_KINDS = {
    AcceleratorVendor.NVIDIA: AcceleratorKind.GPU,
    AcceleratorVendor.HUAWEI_ASCEND: AcceleratorKind.NPU,
}

_KUBERNETES_RESOURCE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?/[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?$"
)
_DISCOVERED_HEALTH_STATES = {"healthy", "unknown", "inventory-only"}


def _validate_inventory_text(name: str, value: str, *, maximum: int) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be canonical non-blank text")
    if len(value) > maximum:
        raise ValueError(f"{name} must not exceed {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} must not contain control characters")


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
    # Legacy field name; Kubernetes values are immutable profile@version#digest bindings.
    runtime_profile_ids: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    fake: bool = False
    device_index: int = 0
    kubernetes_resource_name: str | None = None
    kubernetes_node_labels: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.vendor, AcceleratorVendor):
            raise TypeError("vendor must be an AcceleratorVendor")
        if not isinstance(self.kind, AcceleratorKind):
            raise TypeError("kind must be an AcceleratorKind")
        _validate_inventory_text("device_id", self.device_id, maximum=255)
        if not vendor_kind_is_compatible(self.vendor, self.kind):
            raise ValueError(
                f"{self.vendor.value} devices must use kind={kind_for_vendor(self.vendor)}"
            )
        _validate_inventory_text("model", self.model, maximum=255)
        if self.memory_total_mb <= 0:
            raise ValueError("memory_total_mb must be greater than zero")
        if not 0 <= self.memory_free_mb <= self.memory_total_mb:
            raise ValueError("memory_free_mb must be between zero and memory_total_mb")
        if self.health not in _DISCOVERED_HEALTH_STATES:
            raise ValueError("health must be healthy, unknown, or inventory-only")
        if self.compute_arch is not None:
            _validate_inventory_text("compute_arch", self.compute_arch, maximum=128)
        if len(self.runtime_profile_ids) > 64:
            raise ValueError("runtime_profile_ids must not contain more than 64 values")
        if len(self.runtime_profile_ids) != len(set(self.runtime_profile_ids)):
            raise ValueError("runtime_profile_ids must be unique")
        for profile_binding in self.runtime_profile_ids:
            _validate_inventory_text("runtime_profile_binding", profile_binding, maximum=255)
        if len(self.capabilities) > 128:
            raise ValueError("capabilities must not contain more than 128 values")
        if len(self.capabilities) != len(set(self.capabilities)):
            raise ValueError("capabilities must be unique")
        for capability in self.capabilities:
            _validate_inventory_text("capability", capability, maximum=128)
        if self.device_index < 0:
            raise ValueError("device_index must not be negative")
        if self.kubernetes_resource_name is not None:
            _validate_inventory_text(
                "kubernetes_resource_name", self.kubernetes_resource_name, maximum=255
            )
            if not _KUBERNETES_RESOURCE.fullmatch(self.kubernetes_resource_name):
                raise ValueError("kubernetes_resource_name must be a qualified resource name")
        label_keys = [key for key, _ in self.kubernetes_node_labels]
        if len(label_keys) != len(set(label_keys)):
            raise ValueError("kubernetes_node_labels keys must be unique")
        for key, value in self.kubernetes_node_labels:
            _validate_inventory_text("kubernetes_node_label key", key, maximum=253)
            if any(character.isspace() for character in key):
                raise ValueError("kubernetes_node_label keys must not contain whitespace")
            if value != value.strip() or len(value) > 63:
                raise ValueError(
                    "kubernetes_node_label values must be canonical and at most 63 characters"
                )
            if any(character.isspace() or ord(character) < 32 for character in value):
                raise ValueError(
                    "kubernetes_node_label values must not contain whitespace or controls"
                )

    @property
    def uuid(self) -> str:
        """Legacy internal alias retained while v0.4 GPU consumers migrate."""

        return self.device_id

    @property
    def index(self) -> int:
        """Legacy internal alias retained for exact-device Docker bindings."""

        return self.device_index

    @property
    def compute_capability(self) -> str | None:
        """Legacy NVIDIA name for the vendor-neutral compute architecture."""

        return self.compute_arch
