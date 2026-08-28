from __future__ import annotations

import uuid
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
from core.runtime_profiles import RuntimeProfileCatalog
from models.model_variant import ModelVariant
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.admission import (
    AdmissionRepository,
    InventoryDeviceSnapshot,
    _classify_deferred_accelerators,
    _DeferredReservationTotal,
    _service_candidates,
    _ServiceAcceleratorUsage,
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
    resource_name: str,
) -> InventoryDeviceSnapshot:
    return InventoryDeviceSnapshot(
        device_id=uuid.uuid4(),
        worker_id=worker_id,
        node_name=node_name,
        worker_session_id=uuid.uuid4(),
        worker_status=WorkerStatus.ONLINE,
        worker_runtime_types=("kubernetes",),
        device_uuid=f"GPU-{worker_id}-{index}",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        memory_total_mb=40_960,
        memory_free_mb=40_960,
        runtime_profile_ids=(profile_id,),
        capabilities=frozenset({"float16", "streaming"}),
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

    assert len(candidates) == 2
    assert [candidate.available_capacity for candidate in candidates] == [1, 1]
    assert decision.allowed is False
    assert decision.reason == AdmissionRejectionReason.NVIDIA_CAPACITY_UNAVAILABLE


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
    session.execute.return_value = [
        (
            "worker-a",
            AcceleratorVendor.NVIDIA.value,
            "profile-a",
            "1.0.0",
            "sha256:" + "a" * 64,
            2,
        ),
        (
            "worker-a",
            AcceleratorVendor.NVIDIA.value,
            None,
            None,
            None,
            1,
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
        by_resource={(AcceleratorVendor.NVIDIA.value, resource_name): 3}
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
            by_resource={(AcceleratorVendor.NVIDIA.value, "example.com/other"): 3}
        ),
    )
    assert choose_admission(request, other_resource_candidates).allowed is True
