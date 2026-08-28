from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from core.accelerators import vendor_kind_is_compatible
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
)


class AdmissionRejectionReason(StrEnum):
    """Stable, vendor-specific reasons why an admission candidate was rejected."""

    NVIDIA_POLICY_EXCLUDED = "nvidia_policy_excluded"
    ASCEND_POLICY_EXCLUDED = "ascend_policy_excluded"
    NVIDIA_VENDOR_NOT_ALLOWED = "nvidia_vendor_not_allowed"
    ASCEND_VENDOR_NOT_ALLOWED = "ascend_vendor_not_allowed"
    NVIDIA_KIND_NOT_ALLOWED = "nvidia_kind_not_allowed"
    ASCEND_KIND_NOT_ALLOWED = "ascend_kind_not_allowed"
    NVIDIA_MODEL_INCOMPATIBLE = "nvidia_model_incompatible"
    ASCEND_MODEL_INCOMPATIBLE = "ascend_model_incompatible"
    NVIDIA_PROFILE_INCOMPATIBLE = "nvidia_profile_incompatible"
    ASCEND_PROFILE_INCOMPATIBLE = "ascend_profile_incompatible"
    NVIDIA_PROFILE_UNAVAILABLE = "nvidia_profile_unavailable"
    ASCEND_PROFILE_UNAVAILABLE = "ascend_profile_unavailable"
    NVIDIA_MODEL_VARIANT_INCOMPATIBLE = "nvidia_model_variant_incompatible"
    ASCEND_MODEL_VARIANT_INCOMPATIBLE = "ascend_model_variant_incompatible"
    NVIDIA_UNHEALTHY = "nvidia_unhealthy"
    ASCEND_UNHEALTHY = "ascend_unhealthy"
    NVIDIA_CAPABILITY_MISMATCH = "nvidia_capability_mismatch"
    ASCEND_CAPABILITY_MISMATCH = "ascend_capability_mismatch"
    NVIDIA_QUOTA_EXCEEDED = "nvidia_quota_exceeded"
    ASCEND_QUOTA_EXCEEDED = "ascend_quota_exceeded"
    NVIDIA_CAPACITY_UNAVAILABLE = "nvidia_capacity_unavailable"
    ASCEND_CAPACITY_UNAVAILABLE = "ascend_capacity_unavailable"


class _RejectionCategory(StrEnum):
    POLICY_EXCLUDED = "policy_excluded"
    VENDOR_NOT_ALLOWED = "vendor_not_allowed"
    KIND_NOT_ALLOWED = "kind_not_allowed"
    MODEL_INCOMPATIBLE = "model_incompatible"
    PROFILE_INCOMPATIBLE = "profile_incompatible"
    PROFILE_UNAVAILABLE = "profile_unavailable"
    MODEL_VARIANT_INCOMPATIBLE = "model_variant_incompatible"
    UNHEALTHY = "unhealthy"
    CAPABILITY_MISMATCH = "capability_mismatch"
    QUOTA_EXCEEDED = "quota_exceeded"
    CAPACITY_UNAVAILABLE = "capacity_unavailable"


_VENDOR_REASONS = {
    AcceleratorVendor.NVIDIA: {
        _RejectionCategory.POLICY_EXCLUDED: AdmissionRejectionReason.NVIDIA_POLICY_EXCLUDED,
        _RejectionCategory.VENDOR_NOT_ALLOWED: AdmissionRejectionReason.NVIDIA_VENDOR_NOT_ALLOWED,
        _RejectionCategory.KIND_NOT_ALLOWED: AdmissionRejectionReason.NVIDIA_KIND_NOT_ALLOWED,
        _RejectionCategory.MODEL_INCOMPATIBLE: (AdmissionRejectionReason.NVIDIA_MODEL_INCOMPATIBLE),
        _RejectionCategory.PROFILE_INCOMPATIBLE: (
            AdmissionRejectionReason.NVIDIA_PROFILE_INCOMPATIBLE
        ),
        _RejectionCategory.PROFILE_UNAVAILABLE: (
            AdmissionRejectionReason.NVIDIA_PROFILE_UNAVAILABLE
        ),
        _RejectionCategory.MODEL_VARIANT_INCOMPATIBLE: (
            AdmissionRejectionReason.NVIDIA_MODEL_VARIANT_INCOMPATIBLE
        ),
        _RejectionCategory.UNHEALTHY: AdmissionRejectionReason.NVIDIA_UNHEALTHY,
        _RejectionCategory.CAPABILITY_MISMATCH: (
            AdmissionRejectionReason.NVIDIA_CAPABILITY_MISMATCH
        ),
        _RejectionCategory.QUOTA_EXCEEDED: AdmissionRejectionReason.NVIDIA_QUOTA_EXCEEDED,
        _RejectionCategory.CAPACITY_UNAVAILABLE: (
            AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE
        ),
    },
    AcceleratorVendor.HUAWEI_ASCEND: {
        _RejectionCategory.POLICY_EXCLUDED: AdmissionRejectionReason.ASCEND_POLICY_EXCLUDED,
        _RejectionCategory.VENDOR_NOT_ALLOWED: AdmissionRejectionReason.ASCEND_VENDOR_NOT_ALLOWED,
        _RejectionCategory.KIND_NOT_ALLOWED: AdmissionRejectionReason.ASCEND_KIND_NOT_ALLOWED,
        _RejectionCategory.MODEL_INCOMPATIBLE: (AdmissionRejectionReason.ASCEND_MODEL_INCOMPATIBLE),
        _RejectionCategory.PROFILE_INCOMPATIBLE: (
            AdmissionRejectionReason.ASCEND_PROFILE_INCOMPATIBLE
        ),
        _RejectionCategory.PROFILE_UNAVAILABLE: (
            AdmissionRejectionReason.ASCEND_PROFILE_UNAVAILABLE
        ),
        _RejectionCategory.MODEL_VARIANT_INCOMPATIBLE: (
            AdmissionRejectionReason.ASCEND_MODEL_VARIANT_INCOMPATIBLE
        ),
        _RejectionCategory.UNHEALTHY: AdmissionRejectionReason.ASCEND_UNHEALTHY,
        _RejectionCategory.CAPABILITY_MISMATCH: (
            AdmissionRejectionReason.ASCEND_CAPABILITY_MISMATCH
        ),
        _RejectionCategory.QUOTA_EXCEEDED: AdmissionRejectionReason.ASCEND_QUOTA_EXCEEDED,
        _RejectionCategory.CAPACITY_UNAVAILABLE: (
            AdmissionRejectionReason.ASCEND_CAPACITY_UNAVAILABLE
        ),
    },
}

_CANONICAL_VENDOR_ORDER = {
    AcceleratorVendor.NVIDIA: 0,
    AcceleratorVendor.HUAWEI_ASCEND: 1,
}


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    """Normalized constraints used by the pure admission policy."""

    count: int
    allowed_vendors: frozenset[AcceleratorVendor]
    allowed_kinds: frozenset[AcceleratorKind]
    allowed_models: frozenset[str] = frozenset()
    required_capabilities: frozenset[str] = frozenset()
    runtime_profile_id: str | None = None
    runtime_profile_version: str | None = None
    runtime_profile_digest: str | None = None
    model_variant_id: str | None = None
    selection_policy: AcceleratorSelectionPolicy = AcceleratorSelectionPolicy.ANY

    def __post_init__(self) -> None:
        if not 1 <= self.count <= 64:
            raise ValueError("count must be between one and 64")
        if not self.allowed_vendors:
            raise ValueError("allowed_vendors must not be empty")
        if not self.allowed_kinds:
            raise ValueError("allowed_kinds must not be empty")
        if any(not isinstance(vendor, AcceleratorVendor) for vendor in self.allowed_vendors):
            raise TypeError("allowed_vendors must contain AcceleratorVendor values")
        if any(not isinstance(kind, AcceleratorKind) for kind in self.allowed_kinds):
            raise TypeError("allowed_kinds must contain AcceleratorKind values")
        if not isinstance(self.selection_policy, AcceleratorSelectionPolicy):
            raise TypeError("selection_policy must be an AcceleratorSelectionPolicy")
        if any(not model.strip() for model in self.allowed_models):
            raise ValueError("allowed_models must not contain blank values")
        if any(not capability.strip() for capability in self.required_capabilities):
            raise ValueError("required_capabilities must not contain blank values")
        _validate_optional_text("runtime_profile_id", self.runtime_profile_id)
        _validate_optional_text("runtime_profile_version", self.runtime_profile_version)
        _validate_optional_text("runtime_profile_digest", self.runtime_profile_digest)
        _validate_optional_text("model_variant_id", self.model_variant_id)
        if self.runtime_profile_id is None and (
            self.runtime_profile_version is not None or self.runtime_profile_digest is not None
        ):
            raise ValueError("runtime profile version and digest require runtime_profile_id")

        required_vendor = _policy_vendor(self.selection_policy)
        if required_vendor is not None and required_vendor not in self.allowed_vendors:
            raise ValueError(
                f"{self.selection_policy.value} requires {required_vendor.value} in allowed_vendors"
            )


@dataclass(frozen=True, slots=True)
class AdmissionCandidate:
    """One homogeneous accelerator capacity pool.

    The policy selects one candidate and never joins multiple pools. Therefore every returned
    gang has one vendor, kind, model, runtime profile and allocation authority by construction.
    """

    candidate_id: str
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    model: str
    runtime_profile_id: str | None
    runtime_profile_version: str | None
    runtime_profile_digest: str | None
    model_variant_id: str | None
    allocation_authority: AllocationAuthority
    available_capacity: int
    available_quota: int
    healthy: bool
    capabilities: frozenset[str] = frozenset()
    profile_ready: bool = True
    model_variant_ready: bool = True
    concrete_device_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name, value in (
            ("candidate_id", self.candidate_id),
            ("model", self.model),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be blank")
        profile_values = (
            self.runtime_profile_id,
            self.runtime_profile_version,
            self.runtime_profile_digest,
        )
        if any(value is not None for value in profile_values) and not all(
            value is not None and value.strip() for value in profile_values
        ):
            raise ValueError("runtime profile identity and digest must be complete")
        _validate_optional_text("model_variant_id", self.model_variant_id)
        if not isinstance(self.vendor, AcceleratorVendor):
            raise TypeError("vendor must be an AcceleratorVendor")
        if not isinstance(self.kind, AcceleratorKind):
            raise TypeError("kind must be an AcceleratorKind")
        if not vendor_kind_is_compatible(self.vendor, self.kind):
            raise ValueError("candidate vendor and kind are incompatible")
        if not isinstance(self.allocation_authority, AllocationAuthority):
            raise TypeError("allocation_authority must be an AllocationAuthority")
        if self.available_capacity < 0:
            raise ValueError("available_capacity must not be negative")
        if self.available_quota < 0:
            raise ValueError("available_quota must not be negative")
        if any(not capability.strip() for capability in self.capabilities):
            raise ValueError("capabilities must not contain blank values")
        if len(self.concrete_device_ids) != len(set(self.concrete_device_ids)):
            raise ValueError("concrete_device_ids must be unique")
        if any(not device_id.strip() for device_id in self.concrete_device_ids):
            raise ValueError("concrete_device_ids must not contain blank values")
        if self.allocation_authority == AllocationAuthority.KUBERNETES_DEVICE_PLUGIN:
            if self.concrete_device_ids:
                raise ValueError("Kubernetes allocation authority must not expose device IDs")
            if self.runtime_profile_id is None:
                raise ValueError("Kubernetes allocation authority requires a runtime profile")
        elif len(self.concrete_device_ids) != self.available_capacity:
            raise ValueError("exact-device capacity must equal concrete_device_ids length")

    @property
    def gang_identity(
        self,
    ) -> tuple[
        AcceleratorVendor,
        AcceleratorKind,
        str,
        str | None,
        str | None,
        str | None,
    ]:
        return (
            self.vendor,
            self.kind,
            self.model,
            self.runtime_profile_id,
            self.runtime_profile_version,
            self.runtime_profile_digest,
        )


@dataclass(frozen=True, slots=True)
class AdmissionRejection:
    candidate_id: str
    vendor: AcceleratorVendor
    reason: AdmissionRejectionReason


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    accelerator_count: int
    selected_candidate: AdmissionCandidate | None
    concrete_device_ids: tuple[str, ...] = ()
    rejections: tuple[AdmissionRejection, ...] = ()

    def __post_init__(self) -> None:
        if not 1 <= self.accelerator_count <= 64:
            raise ValueError("accelerator_count must be between one and 64")
        if self.selected_candidate is None:
            if self.concrete_device_ids:
                raise ValueError("a rejected decision must not contain concrete device IDs")
            return
        if self.selected_candidate.allocation_authority == (
            AllocationAuthority.KUBERNETES_DEVICE_PLUGIN
        ):
            if self.concrete_device_ids:
                raise ValueError("Kubernetes admission must not return concrete device IDs")
            return
        if len(self.concrete_device_ids) != self.accelerator_count:
            raise ValueError("exact-device admission must return one device ID per accelerator")
        if not set(self.concrete_device_ids).issubset(self.selected_candidate.concrete_device_ids):
            raise ValueError("decision device IDs must belong to the selected candidate")

    @property
    def allowed(self) -> bool:
        return self.selected_candidate is not None

    @property
    def reason(self) -> AdmissionRejectionReason | None:
        if self.allowed or not self.rejections:
            return None
        return self.rejections[0].reason


def choose_admission(
    request: AdmissionRequest,
    candidates: Sequence[AdmissionCandidate],
) -> AdmissionDecision:
    """Choose one eligible homogeneous pool using deterministic policy ordering."""

    candidate_ids = [candidate.candidate_id for candidate in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidate_id values must be unique")

    eligible: list[AdmissionCandidate] = []
    rejected: list[tuple[AdmissionCandidate, AdmissionRejectionReason]] = []
    for candidate in candidates:
        reason = evaluate_admission_candidate(request, candidate)
        if reason is None:
            eligible.append(candidate)
        else:
            rejected.append((candidate, reason))

    eligible.sort(key=lambda candidate: _candidate_sort_key(request, candidate))
    rejected.sort(key=lambda item: (*_candidate_sort_key(request, item[0]), item[1].value))
    rejections = tuple(
        AdmissionRejection(
            candidate_id=candidate.candidate_id,
            vendor=candidate.vendor,
            reason=reason,
        )
        for candidate, reason in rejected
    )
    if not eligible:
        return AdmissionDecision(
            accelerator_count=request.count,
            selected_candidate=None,
            rejections=rejections,
        )

    selected = eligible[0]
    concrete_device_ids: tuple[str, ...] = ()
    if selected.allocation_authority == AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE:
        concrete_device_ids = tuple(sorted(selected.concrete_device_ids)[: request.count])
    return AdmissionDecision(
        accelerator_count=request.count,
        selected_candidate=selected,
        concrete_device_ids=concrete_device_ids,
        rejections=rejections,
    )


def evaluate_admission_candidate(
    request: AdmissionRequest,
    candidate: AdmissionCandidate,
) -> AdmissionRejectionReason | None:
    """Apply hard admission filters without considering vendor preference ordering."""

    if not _policy_allows_vendor(request.selection_policy, candidate.vendor):
        return _vendor_reason(candidate.vendor, _RejectionCategory.POLICY_EXCLUDED)
    if candidate.vendor not in request.allowed_vendors:
        return _vendor_reason(candidate.vendor, _RejectionCategory.VENDOR_NOT_ALLOWED)
    if candidate.kind not in request.allowed_kinds:
        return _vendor_reason(candidate.vendor, _RejectionCategory.KIND_NOT_ALLOWED)
    if request.allowed_models and candidate.model not in request.allowed_models:
        return _vendor_reason(candidate.vendor, _RejectionCategory.MODEL_INCOMPATIBLE)
    if not _profile_matches(request, candidate):
        return _vendor_reason(candidate.vendor, _RejectionCategory.PROFILE_INCOMPATIBLE)
    if not candidate.profile_ready:
        return _vendor_reason(candidate.vendor, _RejectionCategory.PROFILE_UNAVAILABLE)
    if (
        request.model_variant_id is not None
        and candidate.model_variant_id != request.model_variant_id
    ) or not candidate.model_variant_ready:
        return _vendor_reason(candidate.vendor, _RejectionCategory.MODEL_VARIANT_INCOMPATIBLE)
    if not candidate.healthy:
        return _vendor_reason(candidate.vendor, _RejectionCategory.UNHEALTHY)
    if not request.required_capabilities.issubset(candidate.capabilities):
        return _vendor_reason(candidate.vendor, _RejectionCategory.CAPABILITY_MISMATCH)
    if candidate.available_quota < request.count:
        return _vendor_reason(candidate.vendor, _RejectionCategory.QUOTA_EXCEEDED)
    if candidate.available_capacity < request.count:
        return _vendor_reason(candidate.vendor, _RejectionCategory.CAPACITY_UNAVAILABLE)
    return None


def _profile_matches(request: AdmissionRequest, candidate: AdmissionCandidate) -> bool:
    if (
        request.runtime_profile_id is not None
        and candidate.runtime_profile_id != request.runtime_profile_id
    ):
        return False
    if (
        request.runtime_profile_version is not None
        and candidate.runtime_profile_version != request.runtime_profile_version
    ):
        return False
    return not (
        request.runtime_profile_digest is not None
        and candidate.runtime_profile_digest != request.runtime_profile_digest
    )


def _candidate_sort_key(
    request: AdmissionRequest,
    candidate: AdmissionCandidate,
) -> tuple[int, int, str, str, str, str, str, str, str]:
    return (
        _preference_rank(request.selection_policy, candidate.vendor),
        _CANONICAL_VENDOR_ORDER[candidate.vendor],
        candidate.kind.value,
        candidate.model,
        candidate.runtime_profile_id or "",
        candidate.runtime_profile_version or "",
        candidate.runtime_profile_digest or "",
        candidate.model_variant_id or "",
        candidate.candidate_id,
    )


def _preference_rank(
    policy: AcceleratorSelectionPolicy,
    vendor: AcceleratorVendor,
) -> int:
    if policy == AcceleratorSelectionPolicy.PREFER_NVIDIA:
        return 0 if vendor == AcceleratorVendor.NVIDIA else 1
    if policy == AcceleratorSelectionPolicy.PREFER_ASCEND:
        return 0 if vendor == AcceleratorVendor.HUAWEI_ASCEND else 1
    return 0


def _policy_vendor(policy: AcceleratorSelectionPolicy) -> AcceleratorVendor | None:
    if policy == AcceleratorSelectionPolicy.NVIDIA_ONLY:
        return AcceleratorVendor.NVIDIA
    if policy == AcceleratorSelectionPolicy.ASCEND_ONLY:
        return AcceleratorVendor.HUAWEI_ASCEND
    return None


def _policy_allows_vendor(
    policy: AcceleratorSelectionPolicy,
    vendor: AcceleratorVendor,
) -> bool:
    required_vendor = _policy_vendor(policy)
    return required_vendor is None or vendor == required_vendor


def _vendor_reason(
    vendor: AcceleratorVendor,
    category: _RejectionCategory,
) -> AdmissionRejectionReason:
    return _VENDOR_REASONS[vendor][category]


def _validate_optional_text(field_name: str, value: str | None) -> None:
    if value is not None and not value.strip():
        raise ValueError(f"{field_name} must not be blank")
