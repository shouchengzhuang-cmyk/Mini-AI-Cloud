from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    ModelAvailabilityStatus,
    WorkerStatus,
)
from core.runtime_profiles import (
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
    runtime_profile_binding_id,
)
from models.model_variant import ModelVariant
from models.service import ReplicaStatus
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.admission import (
    AdmissionRepository,
    InventoryDeviceSnapshot,
    _active_service_accelerators,
    _batch_homogeneous_compatible_devices,
    _batch_pool_available_capacity,
    _classify_deferred_accelerators,
    _DeferredAcceleratorUsage,
    _DeferredReservationTotal,
    _service_candidates,
    _ServiceAcceleratorUsage,
    _ServiceReplicaCommitment,
    typed_quota_available,
)
from repositories.quotas import QuotaSnapshot
from scheduler.admission import AdmissionRejectionReason, AdmissionRequest, choose_admission

REPOSITORY_ROOT = Path(__file__).parents[2]
PROJECT_ID = uuid.UUID("20000000-0000-0000-0000-000000000001")


def _quota_snapshot() -> QuotaSnapshot:
    return QuotaSnapshot(
        quota=ProjectQuota(
            project_id=PROJECT_ID,
            max_gpus=None,
            max_nvidia_gpus=None,
            max_ascend_npus=None,
        ),
        state=ProjectQuotaState(
            project_id=PROJECT_ID,
            reserved_gpus=80,
            reserved_nvidia_gpus=80,
            reserved_ascend_npus=0,
            service_reserved_gpus=20,
            service_reserved_nvidia_gpus=20,
            service_reserved_ascend_npus=0,
        ),
    )


def _nvidia_fixture() -> tuple[
    RuntimeProfileCatalog,
    ModelVariant,
    AdmissionRequest,
    str,
]:
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    entry = next(
        item for item in catalog.manifest.profiles if item.identity == "nvidia-vllm-k8s@2.0.0"
    )
    profile = catalog.load_exact(
        profile_id=entry.profile_id,
        profile_version=entry.profile_version,
        semantic_digest=entry.semantic_digest,
    )
    variant = ModelVariant(
        id=uuid.uuid4(),
        logical_model_id=uuid.uuid4(),
        name="nvidia-tp",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        runtime_profile_id=entry.profile_id,
        runtime_profile_version=entry.profile_version,
        runtime_profile_digest=entry.semantic_digest,
        artifact_source="org/nvidia-model",
        artifact_revision="revision-1",
        artifact_digest="sha256:" + "a" * 64,
        architecture="test-architecture",
        dtype="float16",
        status=ModelAvailabilityStatus.READY,
    )
    request = AdmissionRequest(
        count=2,
        allowed_vendors=frozenset({AcceleratorVendor.NVIDIA}),
        allowed_kinds=frozenset({AcceleratorKind.GPU}),
        runtime_profile_id=entry.profile_id,
        model_variant_id=str(variant.id),
        selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY,
    )
    return catalog, variant, request, profile.kubernetes.resource_name


def _device(
    *,
    worker_id: str,
    node_name: str,
    index: int,
    profile_id: str,
    profile_version: str = "2.0.0",
    profile_digest: str | None = None,
    resource_name: str,
    model: str = "NVIDIA A100",
    capabilities: frozenset[str] = frozenset({"float16", "streaming"}),
) -> InventoryDeviceSnapshot:
    if profile_digest is None:
        catalog = RuntimeProfileCatalog.from_path(
            REPOSITORY_ROOT / "runtime_profiles/manifest.json"
        )
        entry = next(
            item
            for item in catalog.manifest.profiles
            if item.profile_id == profile_id and item.profile_version == profile_version
        )
        profile_digest = entry.semantic_digest
    return InventoryDeviceSnapshot(
        device_id=uuid.uuid4(),
        worker_id=worker_id,
        node_name=node_name,
        worker_session_id=uuid.uuid4(),
        worker_status=WorkerStatus.ONLINE,
        worker_runtime_types=("kubernetes",),
        device_uuid=f"GPU-{worker_id}-{index}",
        health="healthy",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model=model,
        memory_total_mb=40_960,
        memory_free_mb=40_960,
        runtime_profile_ids=(
            runtime_profile_binding_id(
                profile_id=profile_id,
                profile_version=profile_version,
                semantic_digest=profile_digest,
            ),
        ),
        capabilities=capabilities,
        kubernetes_resource_name=resource_name,
        inventory_generation=1,
        last_seen_at=datetime.now(UTC),
    )


def test_unlimited_typed_quota_has_no_synthetic_sixty_four_accelerator_cap() -> None:
    assert typed_quota_available(_quota_snapshot(), AcceleratorVendor.NVIDIA) is None


def test_service_tensor_parallel_capacity_is_not_assembled_across_nodes() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="worker-a",
            node_name="node-a",
            index=0,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        ),
        _device(
            worker_id="worker-b",
            node_name="node-b",
            index=0,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        ),
    ]

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
    )
    decision = choose_admission(request, candidates)

    assert len(candidates) == 1
    assert candidates[0].available_capacity == 0
    assert decision.allowed is False
    assert decision.reason == AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE


def test_service_admission_matches_the_exact_runtime_profile_binding() -> None:
    catalog, latest_variant, base_request, resource_name = _nvidia_fixture()
    legacy_entry = next(
        entry for entry in catalog.manifest.profiles if entry.identity == "nvidia-vllm-k8s@1.0.0"
    )
    legacy_variant = ModelVariant(
        id=uuid.uuid4(),
        logical_model_id=latest_variant.logical_model_id,
        name="nvidia-legacy-profile",
        vendor=latest_variant.vendor,
        kind=latest_variant.kind,
        runtime_profile_id=legacy_entry.profile_id,
        runtime_profile_version=legacy_entry.profile_version,
        runtime_profile_digest=legacy_entry.semantic_digest,
        artifact_source=latest_variant.artifact_source,
        artifact_revision=latest_variant.artifact_revision,
        artifact_digest=latest_variant.artifact_digest,
        architecture=latest_variant.architecture,
        dtype="bfloat16",
        status=latest_variant.status,
    )
    legacy_request = replace(
        base_request,
        count=1,
        model_variant_id=str(legacy_variant.id),
    )
    latest_request = replace(base_request, count=1)
    inventory = [
        _device(
            worker_id="legacy-profile-worker",
            node_name="legacy-profile-node",
            index=0,
            profile_id=legacy_entry.profile_id,
            profile_version=legacy_entry.profile_version,
            profile_digest=legacy_entry.semantic_digest,
            resource_name=resource_name,
            capabilities=frozenset({"bfloat16", "streaming"}),
        )
    ]

    legacy_candidates, _ = _service_candidates(
        variants=[legacy_variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=legacy_request,
        desired_replicas=1,
        requested_dtype="bfloat16",
    )
    latest_candidates, _ = _service_candidates(
        variants=[latest_variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=latest_request,
        desired_replicas=1,
        requested_dtype="float16",
    )

    assert choose_admission(legacy_request, legacy_candidates).allowed is True
    assert choose_admission(latest_request, latest_candidates).allowed is False


def test_service_replicas_aggregate_same_node_slots_across_homogeneous_nodes() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id=f"worker-{node}",
            node_name=f"node-{node}",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for node in ("a", "b")
        for index in range(2)
    ]

    candidates, bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=2,
        requested_dtype="float16",
    )

    decision = choose_admission(request, candidates)
    assert decision.allowed is True
    assert decision.selected_candidate is not None
    assert len(candidates) == 1
    assert candidates[0].available_capacity == request.count
    assert bindings[candidates[0].candidate_id].eligible_node_names == (
        "node-a",
        "node-b",
    )


def test_two_tp_replicas_fit_on_one_four_accelerator_node() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="inventory-worker",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(4)
    ]

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=2,
        requested_dtype="float16",
    )

    assert choose_admission(request, candidates).allowed is True


def test_assigned_node_exactly_preserves_five_three_fragmentation() -> None:
    catalog, variant, base_request, resource_name = _nvidia_fixture()
    request = replace(base_request, count=3)
    inventory = [
        _device(
            worker_id=f"inventory-worker-{node}",
            node_name=f"node-{node}",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for node, capacity in (("a", 5), ("b", 3))
        for index in range(capacity)
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=2,
                eligible_node_names=("node-a", "node-b"),
                assigned_node_name="node-b",
            ),
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=2,
        requested_dtype="float16",
        service_usage=usage,
    )

    assert choose_admission(request, candidates).allowed is False


def test_service_whole_device_capacity_does_not_double_charge_memory_free() -> None:
    catalog, variant, base_request, resource_name = _nvidia_fixture()
    request = replace(base_request, count=3)
    inventory = [
        replace(
            _device(
                worker_id="inventory-worker",
                node_name="node-a",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
            ),
            memory_free_mb=1_000,
        )
        for index in range(4)
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=1,
                eligible_node_names=("node-a",),
                assigned_node_name="node-a",
            ),
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=usage,
        minimum_memory_mb=30_000,
    )

    assert choose_admission(request, candidates).allowed is True


def test_service_scale_credit_uses_static_whole_device_slots() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    request = replace(request, count=1)
    inventory = [
        replace(
            _device(
                worker_id="inventory-worker",
                node_name="node-a",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
            ),
            memory_free_mb=1_000,
        )
        for index in range(4)
    ]

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        # Revalidation excludes the two current replicas from service_usage;
        # all four static slots must remain visible for a 2 -> 4 scale-up.
        desired_replicas=4,
        requested_dtype="float16",
        minimum_memory_mb=30_000,
    )

    assert choose_admission(request, candidates).allowed is True


def test_unassigned_commitment_blocks_pool_until_assignment_is_observed() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="inventory-worker",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(4)
    ]
    pending = _ServiceReplicaCommitment(
        service_id=uuid.uuid4(),
        replica_ordinal=0,
        vendor=AcceleratorVendor.NVIDIA.value,
        model="NVIDIA A100",
        resource_name=resource_name,
        accelerator_count=2,
        eligible_node_names=("node-a",),
    )

    blocked, _ = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=_ServiceAcceleratorUsage(commitments=(pending,)),
    )
    assigned, _ = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=_ServiceAcceleratorUsage(
            commitments=(replace(pending, assigned_node_name="node-a"),)
        ),
    )

    assert choose_admission(request, blocked).allowed is False
    assert choose_admission(request, assigned).allowed is True


def test_existing_service_usage_is_deducted_once_from_a_multi_node_pool() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id=f"worker-{node}",
            node_name=f"node-{node}",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for node in ("a", "b")
        for index in range(2)
    ]
    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=_ServiceAcceleratorUsage(
            commitments=(
                _ServiceReplicaCommitment(
                    service_id=uuid.uuid4(),
                    replica_ordinal=0,
                    vendor=AcceleratorVendor.NVIDIA.value,
                    model="NVIDIA A100",
                    resource_name=resource_name,
                    accelerator_count=2,
                    eligible_node_names=("node-a", "node-b"),
                    assigned_node_name="node-a",
                ),
            ),
        ),
    )

    assert len(candidates) == 1
    assert candidates[0].available_capacity == request.count
    assert choose_admission(request, candidates).allowed is True


def test_each_small_existing_replica_conservatively_fragments_a_tp_slot() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id=f"worker-{node}",
            node_name=f"node-{node}",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for node in ("a", "b")
        for index in range(2)
    ]
    service_id = uuid.uuid4()
    usage = _ServiceAcceleratorUsage(
        commitments=tuple(
            _ServiceReplicaCommitment(
                service_id=service_id,
                replica_ordinal=ordinal,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=1,
                eligible_node_names=("node-a", "node-b"),
                assigned_node_name=f"node-{'a' if ordinal == 0 else 'b'}",
            )
            for ordinal in range(2)
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=usage,
    )

    assert candidates[0].available_capacity == 0
    assert choose_admission(request, candidates).allowed is False


def test_other_accelerator_model_commitment_makes_shared_resource_node_unsafe() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="worker-a",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(2)
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA H100",
                resource_name=resource_name,
                accelerator_count=2,
                eligible_node_names=("node-a",),
                assigned_node_name="node-a",
            ),
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=usage,
    )

    assert choose_admission(request, candidates).allowed is False


def test_other_accelerator_model_commitment_on_disjoint_node_is_ignored() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="worker-a",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(2)
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA H100",
                resource_name=resource_name,
                accelerator_count=2,
                eligible_node_names=("node-b",),
                assigned_node_name="node-b",
            ),
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=usage,
    )

    assert choose_admission(request, candidates).allowed is True


def test_eligible_nodes_are_stable_across_occupancy_and_exclude_incompatible_nodes() -> None:
    catalog, variant, base_request, resource_name = _nvidia_fixture()
    request = AdmissionRequest(
        count=base_request.count,
        allowed_vendors=base_request.allowed_vendors,
        allowed_kinds=base_request.allowed_kinds,
        runtime_profile_id=base_request.runtime_profile_id,
        model_variant_id=base_request.model_variant_id,
        required_capabilities=frozenset({"flash-attention-v9"}),
        selection_policy=base_request.selection_policy,
    )
    inventory = [
        *[
            _device(
                worker_id="worker-old",
                node_name="node-old",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
                capabilities=frozenset({"float16"}),
            )
            for index in range(2)
        ],
        *[
            _device(
                worker_id="worker-fragmented",
                node_name="node-fragmented",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
                capabilities=frozenset({"float16", "flash-attention-v9"}),
            )
            for index in range(2)
        ],
        *[
            _device(
                worker_id="worker-ready",
                node_name="node-ready",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
                capabilities=frozenset({"float16", "flash-attention-v9"}),
            )
            for index in range(2)
        ],
    ]
    deferred = _DeferredAcceleratorUsage(
        by_node_resource={
            (
                "worker-fragmented",
                "node-fragmented",
                AcceleratorVendor.NVIDIA.value,
                resource_name,
            ): 1
        }
    )

    candidates, bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        deferred_usage=deferred,
    )

    decision = choose_admission(request, candidates)
    assert decision.allowed is True
    assert decision.selected_candidate is not None
    assert bindings[decision.selected_candidate.candidate_id].eligible_node_names == (
        "node-fragmented",
        "node-ready",
    )


@pytest.mark.parametrize("unsafe_slot", ["model", "profile", "memory"])
def test_service_rejects_mixed_generic_resource_slots_on_one_node(
    unsafe_slot: str,
) -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    safe = _device(
        worker_id="inventory-worker",
        node_name="node-a",
        index=0,
        profile_id=variant.runtime_profile_id,
        resource_name=resource_name,
    )
    other = _device(
        worker_id="inventory-worker",
        node_name="node-a",
        index=1,
        profile_id=variant.runtime_profile_id,
        resource_name=resource_name,
        model="NVIDIA H100" if unsafe_slot == "model" else "NVIDIA A100",
    )
    if unsafe_slot == "profile":
        other = replace(other, runtime_profile_ids=())
    elif unsafe_slot == "memory":
        other = replace(other, memory_total_mb=20_000, memory_free_mb=20_000)

    candidates, bindings = _service_candidates(
        variants=[variant],
        inventory=[safe, other],
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        minimum_memory_mb=30_000 if unsafe_slot == "memory" else 0,
    )

    assert choose_admission(request, candidates).allowed is False
    assert all(not binding.eligible_node_names for binding in bindings.values())


@pytest.mark.parametrize("unsafe_slot", ["model", "profile", "memory"])
def test_batch_recheck_rejects_mixed_generic_resource_slots(
    unsafe_slot: str,
) -> None:
    catalog, variant, _request, resource_name = _nvidia_fixture()
    profile = catalog.load_exact(
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        semantic_digest=variant.runtime_profile_digest,
    )
    safe = _device(
        worker_id="batch-worker",
        node_name="node-a",
        index=0,
        profile_id=variant.runtime_profile_id,
        resource_name=resource_name,
    )
    other = _device(
        worker_id="batch-worker",
        node_name="node-a",
        index=1,
        profile_id=variant.runtime_profile_id,
        resource_name=resource_name,
        model="NVIDIA H100" if unsafe_slot == "model" else "NVIDIA A100",
    )
    if unsafe_slot == "profile":
        other = replace(other, runtime_profile_ids=())
    elif unsafe_slot == "memory":
        other = replace(other, memory_total_mb=20_000, memory_free_mb=20_000)

    available = _batch_pool_available_capacity(
        inventory=[safe, other],
        worker_id="batch-worker",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        profile_digest=variant.runtime_profile_digest,
        resource_name=resource_name,
        service_usage=_ServiceAcceleratorUsage(),
        deferred_usage=_DeferredAcceleratorUsage(),
        profile_capabilities=(
            frozenset(profile.capabilities.features) | frozenset(profile.capabilities.dtypes)
        ),
        minimum_memory_mb=30_000 if unsafe_slot == "memory" else 0,
    )

    assert available == 0


def test_batch_capacity_keeps_compatibility_and_remaining_on_the_same_node() -> None:
    _catalog, variant, _request, resource_name = _nvidia_fixture()
    inventory = [
        *[
            _device(
                worker_id="batch-worker",
                node_name="node-a",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
            )
            for index in range(2)
        ],
        *[
            replace(
                _device(
                    worker_id="batch-worker",
                    node_name="node-b",
                    index=index,
                    profile_id=variant.runtime_profile_id,
                    resource_name=resource_name,
                ),
                runtime_profile_ids=(),
            )
            for index in range(2)
        ],
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=2,
                eligible_node_names=("node-a",),
                assigned_node_name="node-a",
            ),
        )
    )

    available = _batch_pool_available_capacity(
        inventory=inventory,
        worker_id="batch-worker",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        profile_digest=variant.runtime_profile_digest,
        resource_name=resource_name,
        service_usage=usage,
        deferred_usage=_DeferredAcceleratorUsage(),
    )

    assert available == 0


def test_batch_whole_device_capacity_does_not_double_charge_memory_free() -> None:
    _catalog, variant, _request, resource_name = _nvidia_fixture()
    inventory = [
        replace(
            _device(
                worker_id="batch-worker",
                node_name="node-a",
                index=index,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
            ),
            memory_free_mb=1_000,
        )
        for index in range(4)
    ]
    usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=1,
                eligible_node_names=("node-a",),
                assigned_node_name="node-a",
            ),
        )
    )

    available = _batch_pool_available_capacity(
        inventory=inventory,
        worker_id="batch-worker",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        profile_digest=variant.runtime_profile_digest,
        resource_name=resource_name,
        service_usage=usage,
        deferred_usage=_DeferredAcceleratorUsage(),
        minimum_memory_mb=30_000,
    )

    assert available == 3


def test_batch_compatible_node_is_not_poisoned_by_other_incompatible_node() -> None:
    _catalog, variant, _request, resource_name = _nvidia_fixture()
    required = frozenset({"flash-attention-v9"})
    good = [
        _device(
            worker_id="batch-worker",
            node_name="node-good",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
            capabilities=frozenset({"float16", "flash-attention-v9"}),
        )
        for index in range(2)
    ]
    bad = replace(
        _device(
            worker_id="batch-worker",
            node_name="node-bad",
            index=0,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        ),
        runtime_profile_ids=(),
    )

    compatible = _batch_homogeneous_compatible_devices(
        inventory=[*good, bad],
        worker_id="batch-worker",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        profile_digest=variant.runtime_profile_digest,
        resource_name=resource_name,
        required_capabilities=required,
    )
    available = _batch_pool_available_capacity(
        inventory=[*good, bad],
        worker_id="batch-worker",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        profile_digest=variant.runtime_profile_digest,
        resource_name=resource_name,
        service_usage=_ServiceAcceleratorUsage(),
        deferred_usage=_DeferredAcceleratorUsage(),
        required_capabilities=required,
    )

    assert {device.node_name for device in compatible} == {"node-good"}
    assert available == 2
    assert required.issubset(
        frozenset.intersection(*(device.capabilities for device in compatible))
    )


def test_unlimited_typed_quota_allows_more_than_sixty_four_service_replicas() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    request = AdmissionRequest(
        count=1,
        allowed_vendors=request.allowed_vendors,
        allowed_kinds=request.allowed_kinds,
        runtime_profile_id=request.runtime_profile_id,
        model_variant_id=request.model_variant_id,
        selection_policy=request.selection_policy,
    )
    inventory = [
        _device(
            worker_id="large-worker",
            node_name="large-node",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(65)
    ]

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=65,
        requested_dtype="float16",
    )
    decision = choose_admission(request, candidates)

    assert decision.allowed is True
    assert decision.selected_candidate is not None
    assert decision.selected_candidate.available_quota == 1


def test_deferred_reservations_are_charged_to_their_resolved_resource_pool() -> None:
    catalog = Mock()
    catalog.load_exact.side_effect = lambda **kwargs: SimpleNamespace(
        vendor=AcceleratorVendor.NVIDIA,
        kubernetes=SimpleNamespace(resource_name=f"example.com/{kwargs['profile_id']}"),
    )
    totals = [
        _DeferredReservationTotal(
            worker_id="worker-a",
            vendor=AcceleratorVendor.NVIDIA.value,
            profile_id="profile-a",
            profile_version="1.0.0",
            profile_digest="sha256:" + "a" * 64,
            accelerator_count=2,
        )
    ]

    usage = _classify_deferred_accelerators(
        totals,
        catalog=cast(RuntimeProfileCatalog, catalog),
    )

    assert (
        usage.for_pool(
            worker_id="worker-a",
            vendor=AcceleratorVendor.NVIDIA,
            resource_name="example.com/profile-a",
        )
        == 2
    )
    assert (
        usage.for_pool(
            worker_id="worker-a",
            vendor=AcceleratorVendor.NVIDIA,
            resource_name="example.com/profile-b",
        )
        == 0
    )

    legacy = _classify_deferred_accelerators(
        [
            _DeferredReservationTotal(
                worker_id="worker-a",
                vendor=AcceleratorVendor.NVIDIA.value,
                profile_id=None,
                profile_version=None,
                profile_digest=None,
                accelerator_count=1,
            )
        ],
        catalog=cast(RuntimeProfileCatalog, catalog),
    )
    assert (
        legacy.for_pool(
            worker_id="worker-a",
            vendor=AcceleratorVendor.NVIDIA,
            resource_name="example.com/profile-b",
        )
        == 1
    )


@pytest.mark.asyncio
async def test_authoritative_deferred_pool_query_preserves_exact_resource_identity() -> None:
    catalog = Mock()
    catalog.load_exact.side_effect = lambda **kwargs: SimpleNamespace(
        vendor=AcceleratorVendor.NVIDIA,
        kubernetes=SimpleNamespace(resource_name=f"example.com/{kwargs['profile_id']}"),
    )
    session = AsyncMock(spec=AsyncSession)
    worker_session_id = uuid.uuid4()
    session.get.return_value = SimpleNamespace(node_name="node-a")
    session.execute.return_value = [
        (
            "worker-a",
            worker_session_id,
            "node-a",
            worker_session_id,
            AcceleratorVendor.NVIDIA.value,
            "profile-a",
            "1.0.0",
            "sha256:" + "a" * 64,
            2,
            ["GPU-a", "GPU-b"],
        ),
        (
            "worker-a",
            worker_session_id,
            "node-a",
            worker_session_id,
            AcceleratorVendor.NVIDIA.value,
            None,
            None,
            None,
            1,
            None,
        ),
    ]

    active = await AdmissionRepository.active_deferred_accelerators_for_pool(
        cast(AsyncSession, session),
        catalog=cast(RuntimeProfileCatalog, catalog),
        worker_id="worker-a",
        vendor=AcceleratorVendor.NVIDIA,
        resource_name="example.com/profile-b",
    )

    assert active == 1


def test_deferred_observation_requires_current_worker_session_and_valid_node() -> None:
    catalog = Mock()
    catalog.load_exact.return_value = SimpleNamespace(
        vendor=AcceleratorVendor.NVIDIA,
        kubernetes=SimpleNamespace(resource_name="nvidia.com/gpu"),
    )
    base = _DeferredReservationTotal(
        worker_id="worker-a",
        node_name="node-a",
        worker_session_matches=True,
        vendor=AcceleratorVendor.NVIDIA.value,
        profile_id="nvidia-vllm-k8s",
        profile_version="2.0.0",
        profile_digest="sha256:" + "a" * 64,
        accelerator_count=2,
        observed_device_ids=("GPU-a", "GPU-b"),
    )

    exact = _classify_deferred_accelerators(
        [base],
        catalog=cast(RuntimeProfileCatalog, catalog),
    )
    stale_session = _classify_deferred_accelerators(
        [replace(base, worker_session_matches=False)],
        catalog=cast(RuntimeProfileCatalog, catalog),
    )
    invalid_node = _classify_deferred_accelerators(
        [replace(base, node_name="NOT_A_NODE")],
        catalog=cast(RuntimeProfileCatalog, catalog),
    )

    key = (
        "worker-a",
        "node-a",
        AcceleratorVendor.NVIDIA.value,
        "nvidia.com/gpu",
    )
    assert exact.by_node_resource[key] == 2
    assert (
        stale_session.unknown_by_worker_resource[
            ("worker-a", AcceleratorVendor.NVIDIA.value, "nvidia.com/gpu")
        ]
        == 2
    )
    assert (
        invalid_node.unknown_by_worker_resource[
            ("worker-a", AcceleratorVendor.NVIDIA.value, "nvidia.com/gpu")
        ]
        == 2
    )


@pytest.mark.parametrize("observed_device_ids", [None, ("GPU-a", "GPU-b")])
def test_old_worker_reservation_is_charged_to_current_owner_of_same_node(
    observed_device_ids: tuple[str, ...] | None,
) -> None:
    catalog = Mock()
    catalog.load_exact.return_value = SimpleNamespace(
        vendor=AcceleratorVendor.NVIDIA,
        kubernetes=SimpleNamespace(resource_name="nvidia.com/gpu"),
    )
    reservation = _DeferredReservationTotal(
        worker_id="worker-old",
        node_name="node-a",
        worker_session_matches=True,
        vendor=AcceleratorVendor.NVIDIA.value,
        profile_id="nvidia-vllm-k8s",
        profile_version="2.0.0",
        profile_digest="sha256:" + "a" * 64,
        accelerator_count=2,
        observed_device_ids=observed_device_ids,
    )

    same_node = _classify_deferred_accelerators(
        [reservation],
        catalog=cast(RuntimeProfileCatalog, catalog),
        node_owners={"node-a": "worker-new"},
    )
    different_node = _classify_deferred_accelerators(
        [reservation],
        catalog=cast(RuntimeProfileCatalog, catalog),
        node_owners={"node-b": "worker-new"},
    )

    assert (
        same_node.by_node_resource[
            (
                "worker-new",
                "node-a",
                AcceleratorVendor.NVIDIA.value,
                "nvidia.com/gpu",
            )
        ]
        == 2
    )
    assert different_node == _DeferredAcceleratorUsage()


def test_old_worker_unknown_profile_is_charged_to_current_owner_of_same_node() -> None:
    catalog = Mock()
    catalog.load_exact.side_effect = RuntimeProfileCompatibilityError("runtime profile was removed")
    reservation = _DeferredReservationTotal(
        worker_id="worker-old",
        node_name="node-a",
        worker_session_matches=True,
        vendor=AcceleratorVendor.NVIDIA.value,
        profile_id="removed-profile",
        profile_version="1.0.0",
        profile_digest="sha256:" + "a" * 64,
        accelerator_count=2,
    )

    usage = _classify_deferred_accelerators(
        [reservation],
        catalog=cast(RuntimeProfileCatalog, catalog),
        node_owners={"node-a": "worker-new"},
    )

    assert usage.unknown_by_worker_vendor[("worker-new", AcceleratorVendor.NVIDIA.value)] == 2
    assert (
        usage.for_pool(
            worker_id="worker-new",
            vendor=AcceleratorVendor.NVIDIA,
            resource_name="nvidia.com/gpu",
        )
        == 2
    )


def test_duplicate_inventory_workers_for_one_node_are_not_service_eligible() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id=f"worker-{index}",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(2)
    ]

    candidates, bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
    )

    assert choose_admission(request, candidates).allowed is False
    assert all(not binding.eligible_node_names for binding in bindings.values())


@pytest.mark.asyncio
async def test_active_service_usage_keeps_draining_and_synthesizes_current_gap() -> None:
    service_id = uuid.uuid4()
    resource_name = "nvidia.com/gpu"
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [
        [
            (
                service_id,
                2,
                0,
                AcceleratorVendor.NVIDIA.value,
                "NVIDIA A100",
                resource_name,
                ["node-old"],
                "node-old",
                ReplicaStatus.DRAINING,
                2,
                2,
                2,
            )
        ],
        [
            (
                service_id,
                2,
                AcceleratorVendor.NVIDIA.value,
                "NVIDIA A100",
                resource_name,
                ["node-new"],
                2,
                2,
                1,
            )
        ],
    ]

    usage = await _active_service_accelerators(cast(AsyncSession, session))

    assert len(usage.commitments) == 2
    assert usage.commitments[0].assigned_node_name == "node-old"
    assert usage.commitments[1].eligible_node_names == ("node-new",)
    assert usage.commitments[1].assigned_node_name is None


@pytest.mark.asyncio
async def test_active_service_usage_preserves_old_and_new_generation_snapshots() -> None:
    service_id = uuid.uuid4()
    resource_name = "nvidia.com/gpu"
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = [
        [
            (
                service_id,
                1,
                0,
                AcceleratorVendor.NVIDIA.value,
                "NVIDIA A100",
                resource_name,
                ["node-old"],
                "node-old",
                ReplicaStatus.STOPPING,
                2,
                2,
                2,
            ),
            (
                service_id,
                2,
                0,
                AcceleratorVendor.NVIDIA.value,
                "NVIDIA A100",
                resource_name,
                ["node-new"],
                "node-new",
                ReplicaStatus.RUNNING,
                2,
                2,
                2,
            ),
        ],
        [
            (
                service_id,
                2,
                AcceleratorVendor.NVIDIA.value,
                "NVIDIA A100",
                resource_name,
                ["node-new"],
                2,
                2,
                2,
            )
        ],
    ]

    usage = await _active_service_accelerators(cast(AsyncSession, session))

    assert [item.generation for item in usage.commitments] == [1, 2, 2]
    assert [item.eligible_node_names for item in usage.commitments] == [
        ("node-old",),
        ("node-new",),
        ("node-new",),
    ]
    assert [item.assigned_node_name for item in usage.commitments] == [
        "node-old",
        "node-new",
        None,
    ]


def test_existing_service_commitments_reduce_matching_resource_capacity() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    inventory = [
        _device(
            worker_id="worker-a",
            node_name="node-a",
            index=index,
            profile_id=variant.runtime_profile_id,
            resource_name=resource_name,
        )
        for index in range(4)
    ]
    matching_usage = _ServiceAcceleratorUsage(
        commitments=(
            _ServiceReplicaCommitment(
                service_id=uuid.uuid4(),
                replica_ordinal=0,
                vendor=AcceleratorVendor.NVIDIA.value,
                model="NVIDIA A100",
                resource_name=resource_name,
                accelerator_count=3,
                eligible_node_names=("node-a",),
                assigned_node_name="node-a",
            ),
        )
    )

    candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=matching_usage,
    )

    assert choose_admission(request, candidates).reason == (
        AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE
    )

    other_resource_candidates, _bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=_quota_snapshot(),
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
        service_usage=_ServiceAcceleratorUsage(
            commitments=(
                _ServiceReplicaCommitment(
                    service_id=uuid.uuid4(),
                    replica_ordinal=0,
                    vendor=AcceleratorVendor.NVIDIA.value,
                    model="NVIDIA A100",
                    resource_name="example.com/other",
                    accelerator_count=3,
                    eligible_node_names=("node-a",),
                    assigned_node_name="node-a",
                ),
            )
        ),
    )
    assert choose_admission(request, other_resource_candidates).allowed is True


def test_zero_replica_service_preserves_compatibility_without_capacity_or_quota() -> None:
    catalog, variant, request, resource_name = _nvidia_fixture()
    request = replace(request, count=1)
    inventory = [
        replace(
            _device(
                worker_id="worker-a",
                node_name="node-a",
                index=0,
                profile_id=variant.runtime_profile_id,
                resource_name=resource_name,
            ),
            health="externally-allocated",
        )
    ]
    exhausted_quota = QuotaSnapshot(
        quota=ProjectQuota(
            project_id=PROJECT_ID,
            max_gpus=0,
            max_nvidia_gpus=0,
            max_ascend_npus=0,
        ),
        state=ProjectQuotaState(
            project_id=PROJECT_ID,
            reserved_gpus=0,
            reserved_nvidia_gpus=0,
            reserved_ascend_npus=0,
            service_reserved_gpus=0,
            service_reserved_nvidia_gpus=0,
            service_reserved_ascend_npus=0,
        ),
    )
    zero_candidates, zero_bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=exhausted_quota,
        request=request,
        desired_replicas=0,
        requested_dtype="float16",
    )
    positive_candidates, _positive_bindings = _service_candidates(
        variants=[variant],
        inventory=inventory,
        catalog=catalog,
        quota=exhausted_quota,
        request=request,
        desired_replicas=1,
        requested_dtype="float16",
    )
    incompatible_candidates, _incompatible_bindings = _service_candidates(
        variants=[variant],
        inventory=[replace(inventory[0], runtime_profile_ids=())],
        catalog=catalog,
        quota=exhausted_quota,
        request=request,
        desired_replicas=0,
        requested_dtype="float16",
    )

    assert choose_admission(request, zero_candidates).allowed is True
    assert zero_candidates[0].available_capacity == request.count
    assert zero_candidates[0].available_quota == request.count
    assert zero_bindings[zero_candidates[0].candidate_id].eligible_node_names == ("node-a",)
    assert choose_admission(request, positive_candidates).allowed is False
    assert choose_admission(request, incompatible_candidates).allowed is False
