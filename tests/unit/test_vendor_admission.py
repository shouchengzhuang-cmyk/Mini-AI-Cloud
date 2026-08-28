from dataclasses import replace
from typing import Any

import pytest

from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
)
from scheduler.admission import (
    AdmissionCandidate,
    AdmissionRejectionReason,
    AdmissionRequest,
    choose_admission,
)


def _request(
    *,
    policy: AcceleratorSelectionPolicy = AcceleratorSelectionPolicy.ANY,
    count: int = 2,
    **overrides: Any,
) -> AdmissionRequest:
    values: dict[str, Any] = {
        "count": count,
        "allowed_vendors": frozenset({AcceleratorVendor.NVIDIA, AcceleratorVendor.HUAWEI_ASCEND}),
        "allowed_kinds": frozenset({AcceleratorKind.GPU, AcceleratorKind.NPU}),
        "required_capabilities": frozenset({"bf16", "tensor-parallel"}),
        "selection_policy": policy,
    }
    values.update(overrides)
    return AdmissionRequest(**values)


def _candidate(
    vendor: AcceleratorVendor,
    *,
    candidate_id: str | None = None,
    authority: AllocationAuthority = AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE,
    capacity: int = 4,
) -> AdmissionCandidate:
    if vendor == AcceleratorVendor.NVIDIA:
        kind = AcceleratorKind.GPU
        model = "NVIDIA A100"
        profile_id = "nvidia-vllm-k8s"
        profile_digest = "sha256:" + "a" * 64
        variant_id = "variant-nvidia"
        prefix = "GPU"
    else:
        kind = AcceleratorKind.NPU
        model = "Ascend 910B"
        profile_id = "ascend-vllm-k8s-a2"
        profile_digest = "sha256:" + "b" * 64
        variant_id = "variant-ascend"
        prefix = "NPU"
    concrete_device_ids = (
        tuple(f"{prefix}-{index}" for index in range(capacity))
        if authority == AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE
        else ()
    )
    return AdmissionCandidate(
        candidate_id=candidate_id or vendor.value,
        vendor=vendor,
        kind=kind,
        model=model,
        runtime_profile_id=profile_id,
        runtime_profile_version="2.0.0",
        runtime_profile_digest=profile_digest,
        model_variant_id=variant_id,
        allocation_authority=authority,
        available_capacity=capacity,
        available_quota=capacity,
        healthy=True,
        capabilities=frozenset({"bf16", "tensor-parallel"}),
        concrete_device_ids=concrete_device_ids,
    )


@pytest.mark.parametrize(
    ("policy", "expected_vendor", "rejected_reason"),
    [
        (AcceleratorSelectionPolicy.ANY, AcceleratorVendor.NVIDIA, None),
        (AcceleratorSelectionPolicy.NVIDIA_ONLY, AcceleratorVendor.NVIDIA, "ascend"),
        (AcceleratorSelectionPolicy.ASCEND_ONLY, AcceleratorVendor.HUAWEI_ASCEND, "nvidia"),
        (AcceleratorSelectionPolicy.PREFER_NVIDIA, AcceleratorVendor.NVIDIA, None),
        (
            AcceleratorSelectionPolicy.PREFER_ASCEND,
            AcceleratorVendor.HUAWEI_ASCEND,
            None,
        ),
    ],
)
def test_all_selection_policies_filter_and_sort_deterministically(
    policy: AcceleratorSelectionPolicy,
    expected_vendor: AcceleratorVendor,
    rejected_reason: str | None,
) -> None:
    nvidia = _candidate(AcceleratorVendor.NVIDIA)
    ascend = _candidate(AcceleratorVendor.HUAWEI_ASCEND)

    forward = choose_admission(_request(policy=policy), [ascend, nvidia])
    reverse = choose_admission(_request(policy=policy), [nvidia, ascend])

    assert forward.allowed is True
    assert forward.selected_candidate is not None
    assert forward.selected_candidate.vendor == expected_vendor
    assert reverse.selected_candidate == forward.selected_candidate
    if rejected_reason == "ascend":
        assert [item.reason for item in forward.rejections] == [
            AdmissionRejectionReason.ASCEND_POLICY_EXCLUDED
        ]
    elif rejected_reason == "nvidia":
        assert [item.reason for item in forward.rejections] == [
            AdmissionRejectionReason.NVIDIA_POLICY_EXCLUDED
        ]
    else:
        assert forward.rejections == ()


@pytest.mark.parametrize(
    ("policy", "preferred_vendor", "fallback_vendor", "reason"),
    [
        (
            AcceleratorSelectionPolicy.PREFER_NVIDIA,
            AcceleratorVendor.NVIDIA,
            AcceleratorVendor.HUAWEI_ASCEND,
            AdmissionRejectionReason.NVIDIA_QUOTA_EXCEEDED,
        ),
        (
            AcceleratorSelectionPolicy.PREFER_ASCEND,
            AcceleratorVendor.HUAWEI_ASCEND,
            AcceleratorVendor.NVIDIA,
            AdmissionRejectionReason.ASCEND_QUOTA_EXCEEDED,
        ),
    ],
)
def test_preference_does_not_bypass_hard_constraints(
    policy: AcceleratorSelectionPolicy,
    preferred_vendor: AcceleratorVendor,
    fallback_vendor: AcceleratorVendor,
    reason: AdmissionRejectionReason,
) -> None:
    preferred = replace(_candidate(preferred_vendor), available_quota=1)
    fallback = _candidate(fallback_vendor)

    decision = choose_admission(_request(policy=policy), [preferred, fallback])

    assert decision.selected_candidate == fallback
    assert [item.reason for item in decision.rejections] == [reason]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            {"profile_ready": False},
            AdmissionRejectionReason.NVIDIA_PROFILE_UNAVAILABLE,
        ),
        (
            {"model_variant_ready": False},
            AdmissionRejectionReason.NVIDIA_MODEL_VARIANT_INCOMPATIBLE,
        ),
        ({"healthy": False}, AdmissionRejectionReason.NVIDIA_UNHEALTHY),
        (
            {"capabilities": frozenset({"bf16"})},
            AdmissionRejectionReason.NVIDIA_CAPABILITY_MISMATCH,
        ),
        ({"available_quota": 1}, AdmissionRejectionReason.NVIDIA_QUOTA_EXCEEDED),
        (
            {"available_capacity": 1, "concrete_device_ids": ("GPU-0",)},
            AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE,
        ),
    ],
)
def test_profile_variant_health_capability_quota_and_capacity_are_hard_filters(
    mutation: dict[str, Any],
    reason: AdmissionRejectionReason,
) -> None:
    candidate = replace(_candidate(AcceleratorVendor.NVIDIA), **mutation)
    request = _request(
        policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
        allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
        allowed_kinds=frozenset({AcceleratorKind.GPU}),
    )

    decision = choose_admission(request, [candidate])

    assert decision.allowed is False
    assert decision.reason == reason
    assert decision.rejections[0].vendor == AcceleratorVendor.NVIDIA


def test_requested_model_profile_digest_and_variant_are_hard_filters() -> None:
    candidate = _candidate(AcceleratorVendor.HUAWEI_ASCEND)
    base: dict[str, Any] = {
        "policy": AcceleratorSelectionPolicy.ASCEND_ONLY,
        "allowed_vendors": frozenset({AcceleratorVendor.HUAWEI_ASCEND}),
        "allowed_kinds": frozenset({AcceleratorKind.NPU}),
    }

    model_decision = choose_admission(
        _request(allowed_models=frozenset({"Ascend 310P"}), **base),
        [candidate],
    )
    profile_decision = choose_admission(
        _request(
            runtime_profile_id=candidate.runtime_profile_id,
            runtime_profile_digest="sha256:" + "c" * 64,
            **base,
        ),
        [candidate],
    )
    variant_decision = choose_admission(
        _request(model_variant_id="different-variant", **base),
        [candidate],
    )

    assert model_decision.reason == AdmissionRejectionReason.ASCEND_MODEL_INCOMPATIBLE
    assert profile_decision.reason == AdmissionRejectionReason.ASCEND_PROFILE_INCOMPATIBLE
    assert variant_decision.reason == AdmissionRejectionReason.ASCEND_MODEL_VARIANT_INCOMPATIBLE


def test_a_gang_is_never_assembled_from_heterogeneous_capacity_pools() -> None:
    nvidia = replace(
        _candidate(AcceleratorVendor.NVIDIA, capacity=1),
        available_quota=2,
    )
    ascend = replace(
        _candidate(AcceleratorVendor.HUAWEI_ASCEND, capacity=1),
        available_quota=2,
    )

    decision = choose_admission(_request(count=2), [nvidia, ascend])

    assert decision.allowed is False
    assert decision.concrete_device_ids == ()
    assert {item.reason for item in decision.rejections} == {
        AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE,
        AdmissionRejectionReason.ASCEND_CAPACITY_UNAVAILABLE,
    }


def test_exact_device_authority_returns_deterministic_ids_from_one_pool() -> None:
    candidate = replace(
        _candidate(AcceleratorVendor.NVIDIA, candidate_id="pool-b", capacity=3),
        concrete_device_ids=("GPU-2", "GPU-0", "GPU-1"),
    )

    decision = choose_admission(
        _request(
            policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
            allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
            allowed_kinds=frozenset({AcceleratorKind.GPU}),
        ),
        [candidate],
    )

    assert decision.concrete_device_ids == ("GPU-0", "GPU-1")
    assert decision.selected_candidate is not None
    assert decision.selected_candidate.gang_identity == (
        AcceleratorVendor.NVIDIA,
        AcceleratorKind.GPU,
        "NVIDIA A100",
        "nvidia-vllm-k8s",
        "2.0.0",
        "sha256:" + "a" * 64,
    )


def test_exact_device_authority_allows_the_profileless_legacy_docker_path() -> None:
    candidate = replace(
        _candidate(AcceleratorVendor.NVIDIA, capacity=2),
        runtime_profile_id=None,
        runtime_profile_version=None,
        runtime_profile_digest=None,
    )
    request = _request(
        policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
        allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
        allowed_kinds=frozenset({AcceleratorKind.GPU}),
    )

    decision = choose_admission(request, [candidate])

    assert decision.allowed is True
    assert decision.selected_candidate == candidate
    assert decision.concrete_device_ids == ("GPU-0", "GPU-1")


def test_kubernetes_authority_requires_a_complete_runtime_profile() -> None:
    candidate = _candidate(
        AcceleratorVendor.NVIDIA,
        authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
    )

    with pytest.raises(ValueError, match="requires a runtime profile"):
        replace(
            candidate,
            runtime_profile_id=None,
            runtime_profile_version=None,
            runtime_profile_digest=None,
        )


def test_kubernetes_authority_never_accepts_or_returns_concrete_device_ids() -> None:
    candidate = _candidate(
        AcceleratorVendor.HUAWEI_ASCEND,
        authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
    )
    request = _request(
        policy=AcceleratorSelectionPolicy.ASCEND_ONLY,
        allowed_vendors=frozenset({AcceleratorVendor.HUAWEI_ASCEND}),
        allowed_kinds=frozenset({AcceleratorKind.NPU}),
    )

    decision = choose_admission(request, [candidate])

    assert decision.allowed is True
    assert decision.concrete_device_ids == ()
    with pytest.raises(ValueError, match="must not expose device IDs"):
        replace(candidate, concrete_device_ids=("NPU-physical-0",))


def test_candidate_identity_and_input_ids_must_be_unambiguous() -> None:
    with pytest.raises(ValueError, match="vendor and kind are incompatible"):
        replace(_candidate(AcceleratorVendor.NVIDIA), kind=AcceleratorKind.NPU)

    candidate = _candidate(AcceleratorVendor.NVIDIA)
    with pytest.raises(ValueError, match="candidate_id values must be unique"):
        choose_admission(_request(), [candidate, candidate])
