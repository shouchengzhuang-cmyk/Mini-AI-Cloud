from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
    RuntimeType,
    WorkerStatus,
    WorkloadType,
)
from core.runtime_profiles import RuntimeProfileCatalog, RuntimeProfileCompatibilityError
from models.admission import AdmissionEvent
from models.model_variant import ModelVariant
from models.scheduling import GPUDevice
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.model_variants import ModelVariantRepository
from repositories.quotas import (
    QuotaNotFoundError,
    QuotaRepository,
    QuotaSnapshot,
)
from scheduler.admission import (
    AdmissionCandidate,
    AdmissionDecision,
    AdmissionRejection,
    AdmissionRejectionReason,
    AdmissionRequest,
    choose_admission,
)

_UNBOUNDED_ACCELERATOR_QUOTA = 64
_CANDIDATE_SUMMARY_MAX_BYTES = 15_500


@dataclass(frozen=True, slots=True)
class InventoryDeviceSnapshot:
    device_id: uuid.UUID
    worker_id: str
    worker_session_id: uuid.UUID
    worker_status: WorkerStatus
    worker_runtime_types: tuple[str, ...]
    device_uuid: str
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    model: str
    memory_total_mb: int
    memory_free_mb: int
    runtime_profile_ids: tuple[str, ...]
    capabilities: frozenset[str]
    kubernetes_resource_name: str | None
    inventory_generation: int
    last_seen_at: datetime


@dataclass(frozen=True, slots=True)
class ServiceAdmissionSnapshot:
    logical_model_id: uuid.UUID
    model_variant_id: uuid.UUID
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    selected_model: str
    runtime_profile_id: str
    runtime_profile_version: str
    runtime_profile_digest: str
    allocation_authority: AllocationAuthority
    accelerator_resource_name: str
    selection_policy: AcceleratorSelectionPolicy
    artifact_source: str
    artifact_revision: str
    artifact_digest: str
    dtype: str


@dataclass(frozen=True, slots=True)
class ServiceAdmissionResult:
    snapshot: ServiceAdmissionSnapshot | None
    reason: AdmissionRejectionReason | None
    rejected_vendor: AcceleratorVendor | None
    summary: tuple[dict[str, object], ...]

    @property
    def allowed(self) -> bool:
        return self.snapshot is not None


@dataclass(frozen=True, slots=True)
class _ServiceCandidateBinding:
    variant: ModelVariant
    resource_name: str | None


def typed_quota_available(snapshot: QuotaSnapshot, vendor: AcceleratorVendor) -> int:
    """Return remaining aggregate-and-vendor accelerator quota under a locked snapshot."""

    quota = snapshot.quota
    state = snapshot.state
    aggregate_available = _remaining(
        quota.max_gpus,
        state.reserved_gpus + state.service_reserved_gpus,
    )
    if vendor == AcceleratorVendor.NVIDIA:
        typed_available = _remaining(
            quota.max_nvidia_gpus,
            state.reserved_nvidia_gpus + state.service_reserved_nvidia_gpus,
        )
    else:
        typed_available = _remaining(
            quota.max_ascend_npus,
            state.reserved_ascend_npus + state.service_reserved_ascend_npus,
        )
    return max(0, min(aggregate_available, typed_available))


def candidate_summary(
    candidates: Sequence[AdmissionCandidate],
    decision: AdmissionDecision,
) -> tuple[dict[str, object], ...]:
    """Build a deterministic, redacted admission summary bounded below the DB limit."""

    rejections = {item.candidate_id: item.reason.value for item in decision.rejections}
    items: list[dict[str, object]] = []
    seen: set[str] = set()
    for candidate in sorted(candidates, key=lambda item: item.candidate_id):
        seen.add(candidate.candidate_id)
        items.append(
            {
                "candidate_id": candidate.candidate_id,
                "vendor": candidate.vendor.value,
                "kind": candidate.kind.value,
                "model": candidate.model,
                "runtime_profile_id": candidate.runtime_profile_id,
                "runtime_profile_version": candidate.runtime_profile_version,
                "runtime_profile_digest": candidate.runtime_profile_digest,
                "model_variant_id": candidate.model_variant_id,
                "allocation_authority": candidate.allocation_authority.value,
                "available_capacity": candidate.available_capacity,
                "available_quota": candidate.available_quota,
                "healthy": candidate.healthy,
                "profile_ready": candidate.profile_ready,
                "model_variant_ready": candidate.model_variant_ready,
                "capabilities": sorted(candidate.capabilities),
                "reason": rejections.get(candidate.candidate_id),
            }
        )
    for rejection in sorted(
        (item for item in decision.rejections if item.candidate_id not in seen),
        key=lambda item: (item.candidate_id, item.vendor.value, item.reason.value),
    ):
        items.append(
            {
                "candidate_id": rejection.candidate_id,
                "vendor": rejection.vendor.value,
                "reason": rejection.reason.value,
            }
        )
    return _bounded_summary(items)


class AdmissionRepository:
    @staticmethod
    async def list_healthy_inventory_devices(
        session: AsyncSession,
        *,
        vendors: Sequence[AcceleratorVendor],
        kinds: Sequence[AcceleratorKind],
        minimum_memory_mb: int = 0,
        runtime_type: RuntimeType | None = None,
        for_update: bool = False,
    ) -> list[InventoryDeviceSnapshot]:
        if minimum_memory_mb < 0:
            raise ValueError("minimum_memory_mb must not be negative")
        query = (
            select(GPUDevice, Worker)
            .join(Worker, Worker.id == GPUDevice.worker_id)
            .where(
                Worker.status == WorkerStatus.ONLINE,
                Worker.overcommitted.is_(False),
                GPUDevice.health == "healthy",
                GPUDevice.inventory_generation == Worker.inventory_generation,
                GPUDevice.memory_free_mb >= minimum_memory_mb,
                GPUDevice.vendor.in_(tuple(vendor.value for vendor in vendors)),
                GPUDevice.accelerator_kind.in_(tuple(kind.value for kind in kinds)),
            )
            .order_by(
                GPUDevice.vendor,
                GPUDevice.accelerator_kind,
                GPUDevice.model,
                GPUDevice.worker_id,
                GPUDevice.device_uuid,
                GPUDevice.id,
            )
        )
        if for_update:
            query = query.with_for_update()
        rows = list((await session.execute(query)).all())
        result: list[InventoryDeviceSnapshot] = []
        for device, worker in rows:
            runtime_types = tuple(sorted(set(worker.runtime_types or [])))
            if runtime_type is not None and runtime_type.value not in runtime_types:
                continue
            result.append(
                InventoryDeviceSnapshot(
                    device_id=device.id,
                    worker_id=worker.id,
                    worker_session_id=worker.worker_session_id,
                    worker_status=worker.status,
                    worker_runtime_types=runtime_types,
                    device_uuid=device.device_uuid,
                    vendor=AcceleratorVendor(device.vendor),
                    kind=AcceleratorKind(device.accelerator_kind),
                    model=device.model,
                    memory_total_mb=device.memory_total_mb,
                    memory_free_mb=device.memory_free_mb,
                    runtime_profile_ids=tuple(sorted(set(device.runtime_profile_ids or []))),
                    capabilities=frozenset(device.capabilities_json or []),
                    kubernetes_resource_name=device.kubernetes_resource_name,
                    inventory_generation=device.inventory_generation,
                    last_seen_at=device.last_seen_at,
                )
            )
        return result

    @staticmethod
    async def record_event(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        workload_type: WorkloadType,
        workload_id: uuid.UUID,
        policy: AcceleratorSelectionPolicy,
        outcome: str,
        reason: str,
        summary: Sequence[dict[str, object]],
        selected_candidate: AdmissionCandidate | None = None,
        execution_id: uuid.UUID | None = None,
    ) -> AdmissionEvent:
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("admission event reason must not be blank")
        selected_profile_id: str | None = None
        selected_profile_version: str | None = None
        selected_profile_digest: str | None = None
        selected_variant_id: uuid.UUID | None = None
        if selected_candidate is not None:
            if (
                selected_candidate.runtime_profile_id is None
                or selected_candidate.runtime_profile_version is None
                or selected_candidate.runtime_profile_digest is None
            ):
                raise ValueError("selected admission event requires a complete runtime profile")
            selected_profile_id = selected_candidate.runtime_profile_id
            selected_profile_version = selected_candidate.runtime_profile_version
            selected_profile_digest = selected_candidate.runtime_profile_digest
            if selected_candidate.model_variant_id is not None:
                selected_variant_id = uuid.UUID(selected_candidate.model_variant_id)
        event = AdmissionEvent(
            project_id=project_id,
            workload_type=workload_type.value,
            workload_id=workload_id,
            execution_id=execution_id,
            policy=policy.value,
            outcome=outcome.strip(),
            reason=normalized_reason,
            selected_vendor=(
                selected_candidate.vendor.value if selected_candidate is not None else None
            ),
            selected_kind=(
                selected_candidate.kind.value if selected_candidate is not None else None
            ),
            selected_model=(selected_candidate.model if selected_candidate is not None else None),
            runtime_profile_id=selected_profile_id,
            runtime_profile_version=selected_profile_version,
            runtime_profile_digest=selected_profile_digest,
            model_variant_id=selected_variant_id,
            allocation_authority=(
                selected_candidate.allocation_authority.value
                if selected_candidate is not None
                else None
            ),
            candidate_summary=list(_bounded_summary(list(summary))),
            occurred_at=await database_utcnow(session),
        )
        session.add(event)
        await session.flush()
        return event

    @staticmethod
    async def admit_logical_model_service(
        session: AsyncSession,
        *,
        catalog: RuntimeProfileCatalog,
        project_id: uuid.UUID,
        service_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        request: AdmissionRequest,
        minimum_memory_mb: int,
        desired_replicas: int,
        requested_dtype: str,
    ) -> ServiceAdmissionResult:
        if desired_replicas < 0:
            raise ValueError("desired_replicas must not be negative")
        try:
            quota = await QuotaRepository.get_locked(session, project_id=project_id)
        except QuotaNotFoundError:
            quota = await QuotaRepository.initialize(session, project_id=project_id)

        variants = await ModelVariantRepository.list_ready_candidates(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            allowed_vendors=tuple(sorted(request.allowed_vendors, key=lambda item: item.value)),
            allowed_kinds=tuple(sorted(request.allowed_kinds, key=lambda item: item.value)),
            runtime_profile_id=request.runtime_profile_id,
        )
        inventory = await AdmissionRepository.list_healthy_inventory_devices(
            session,
            vendors=tuple(sorted(request.allowed_vendors, key=lambda item: item.value)),
            kinds=tuple(sorted(request.allowed_kinds, key=lambda item: item.value)),
            minimum_memory_mb=minimum_memory_mb,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
        )
        candidates, bindings = _service_candidates(
            variants=variants,
            inventory=inventory,
            catalog=catalog,
            quota=quota,
            request=request,
            desired_replicas=desired_replicas,
            requested_dtype=requested_dtype,
        )
        if candidates:
            decision = choose_admission(request, candidates)
        else:
            decision = _missing_variant_decision(request)

        if not decision.allowed:
            return await _record_rejected_service_admission(
                session,
                project_id=project_id,
                service_id=service_id,
                request=request,
                candidates=candidates,
                decision=decision,
            )

        selected = decision.selected_candidate
        assert selected is not None
        binding = bindings[selected.candidate_id]
        variant = binding.variant
        locked_variant = await ModelVariantRepository.revalidate_ready_for_reservation(
            session,
            project_id=project_id,
            logical_model_id=logical_model_id,
            expected_variant_id=variant.id,
            expected_vendor=variant.vendor,
            expected_kind=variant.kind,
            expected_runtime_profile_id=variant.runtime_profile_id,
            expected_runtime_profile_version=variant.runtime_profile_version,
            expected_artifact_digest=variant.artifact_digest,
            expected_runtime_profile_digest=variant.runtime_profile_digest,
        )
        if locked_variant is None or binding.resource_name is None:
            stale_reason = _variant_reason(selected.vendor)
            stale_decision = AdmissionDecision(
                accelerator_count=request.count,
                selected_candidate=None,
                rejections=(
                    AdmissionRejection(
                        candidate_id=selected.candidate_id,
                        vendor=selected.vendor,
                        reason=stale_reason,
                    ),
                    *decision.rejections,
                ),
            )
            return await _record_rejected_service_admission(
                session,
                project_id=project_id,
                service_id=service_id,
                request=request,
                candidates=candidates,
                decision=stale_decision,
            )

        summary = candidate_summary(candidates, decision)
        await AdmissionRepository.record_event(
            session,
            project_id=project_id,
            workload_type=WorkloadType.MODEL_SERVICE,
            workload_id=service_id,
            policy=request.selection_policy,
            outcome="admitted",
            reason="admitted",
            summary=summary,
            selected_candidate=selected,
        )
        snapshot = ServiceAdmissionSnapshot(
            logical_model_id=logical_model_id,
            model_variant_id=locked_variant.id,
            vendor=locked_variant.vendor,
            kind=locked_variant.kind,
            selected_model=selected.model,
            runtime_profile_id=locked_variant.runtime_profile_id,
            runtime_profile_version=locked_variant.runtime_profile_version,
            runtime_profile_digest=locked_variant.runtime_profile_digest,
            allocation_authority=selected.allocation_authority,
            accelerator_resource_name=binding.resource_name,
            selection_policy=request.selection_policy,
            artifact_source=locked_variant.artifact_source,
            artifact_revision=locked_variant.artifact_revision,
            artifact_digest=locked_variant.artifact_digest,
            dtype=locked_variant.dtype,
        )
        return ServiceAdmissionResult(
            snapshot=snapshot,
            reason=None,
            rejected_vendor=None,
            summary=summary,
        )


def _service_candidates(
    *,
    variants: Sequence[ModelVariant],
    inventory: Sequence[InventoryDeviceSnapshot],
    catalog: RuntimeProfileCatalog,
    quota: QuotaSnapshot,
    request: AdmissionRequest,
    desired_replicas: int,
    requested_dtype: str,
) -> tuple[list[AdmissionCandidate], dict[str, _ServiceCandidateBinding]]:
    candidates: list[AdmissionCandidate] = []
    bindings: dict[str, _ServiceCandidateBinding] = {}
    commitment_divisor = max(1, desired_replicas)
    for variant in variants:
        manifest_entry = None
        try:
            manifest_entry = catalog.resolve_compatible(
                profile_id=variant.runtime_profile_id,
                profile_version=variant.runtime_profile_version,
                semantic_digest=variant.runtime_profile_digest,
                vendor=variant.vendor,
                kind=variant.kind,
                architecture=variant.architecture,
                dtype=variant.dtype,
            )
        except RuntimeProfileCompatibilityError:
            pass
        variant_ready = (
            requested_dtype == "auto" or requested_dtype == variant.dtype
        ) and len(variant.artifact_source) <= 512
        relevant = [
            device
            for device in inventory
            if device.vendor == variant.vendor and device.kind == variant.kind
        ]
        profiled = [
            device
            for device in relevant
            if variant.runtime_profile_id in device.runtime_profile_ids
            and device.kubernetes_resource_name is not None
        ]
        groups: dict[tuple[str, str, tuple[str, ...]], list[InventoryDeviceSnapshot]] = {}
        for device in profiled:
            capabilities = set(device.capabilities)
            capabilities.add(variant.dtype)
            if manifest_entry is not None:
                capabilities.update(manifest_entry.features)
            key = (
                device.model,
                device.kubernetes_resource_name or "",
                tuple(sorted(capabilities)),
            )
            groups.setdefault(key, []).append(device)

        available_quota = typed_quota_available(quota, variant.vendor) // commitment_divisor
        for index, (key, devices) in enumerate(sorted(groups.items()), start=1):
            model, resource_name, capabilities = key
            candidate_id = f"{variant.id}:{index}"
            candidate = AdmissionCandidate(
                candidate_id=candidate_id,
                vendor=variant.vendor,
                kind=variant.kind,
                model=model,
                runtime_profile_id=variant.runtime_profile_id,
                runtime_profile_version=variant.runtime_profile_version,
                runtime_profile_digest=variant.runtime_profile_digest,
                model_variant_id=str(variant.id),
                allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
                available_capacity=len(devices) // commitment_divisor,
                available_quota=available_quota,
                healthy=True,
                capabilities=frozenset(capabilities),
                profile_ready=manifest_entry is not None,
                model_variant_ready=variant_ready,
            )
            candidates.append(candidate)
            bindings[candidate_id] = _ServiceCandidateBinding(
                variant=variant,
                resource_name=resource_name,
            )

        if groups:
            continue
        candidate_id = f"{variant.id}:unavailable"
        profile_ready = manifest_entry is not None and not relevant
        candidate = AdmissionCandidate(
            candidate_id=candidate_id,
            vendor=variant.vendor,
            kind=variant.kind,
            model=(sorted(request.allowed_models)[0] if request.allowed_models else "unavailable"),
            runtime_profile_id=variant.runtime_profile_id,
            runtime_profile_version=variant.runtime_profile_version,
            runtime_profile_digest=variant.runtime_profile_digest,
            model_variant_id=str(variant.id),
            allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
            available_capacity=0,
            available_quota=available_quota,
            healthy=True,
            capabilities=request.required_capabilities,
            profile_ready=profile_ready,
            model_variant_ready=variant_ready,
        )
        candidates.append(candidate)
        bindings[candidate_id] = _ServiceCandidateBinding(variant=variant, resource_name=None)
    return candidates, bindings


async def _record_rejected_service_admission(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    service_id: uuid.UUID,
    request: AdmissionRequest,
    candidates: Sequence[AdmissionCandidate],
    decision: AdmissionDecision,
) -> ServiceAdmissionResult:
    reason = decision.reason or _variant_reason(_ordered_vendors(request)[0])
    rejected_vendor = (
        decision.rejections[0].vendor
        if decision.rejections
        else _ordered_vendors(request)[0]
    )
    summary = candidate_summary(candidates, decision)
    await AdmissionRepository.record_event(
        session,
        project_id=project_id,
        workload_type=WorkloadType.MODEL_SERVICE,
        workload_id=service_id,
        policy=request.selection_policy,
        outcome="rejected",
        reason=reason.value,
        summary=summary,
    )
    return ServiceAdmissionResult(
        snapshot=None,
        reason=reason,
        rejected_vendor=rejected_vendor,
        summary=summary,
    )


def _missing_variant_decision(request: AdmissionRequest) -> AdmissionDecision:
    vendor = _ordered_vendors(request)[0]
    if request.model_variant_id is not None:
        reason = _variant_reason(vendor)
    elif request.runtime_profile_id is not None:
        reason = _profile_reason(vendor)
    else:
        reason = _variant_reason(vendor)
    return AdmissionDecision(
        accelerator_count=request.count,
        selected_candidate=None,
        rejections=(
            AdmissionRejection(
                candidate_id="no-ready-model-variant",
                vendor=vendor,
                reason=reason,
            ),
        ),
    )


def _ordered_vendors(request: AdmissionRequest) -> list[AcceleratorVendor]:
    vendors = sorted(
        request.allowed_vendors,
        key=lambda vendor: 0 if vendor == AcceleratorVendor.NVIDIA else 1,
    )
    preferred = None
    if request.selection_policy in {
        AcceleratorSelectionPolicy.NVIDIA_ONLY,
        AcceleratorSelectionPolicy.PREFER_NVIDIA,
    }:
        preferred = AcceleratorVendor.NVIDIA
    elif request.selection_policy in {
        AcceleratorSelectionPolicy.ASCEND_ONLY,
        AcceleratorSelectionPolicy.PREFER_ASCEND,
    }:
        preferred = AcceleratorVendor.HUAWEI_ASCEND
    if preferred in vendors:
        vendors.remove(preferred)
        vendors.insert(0, preferred)
    return vendors


def _variant_reason(vendor: AcceleratorVendor) -> AdmissionRejectionReason:
    if vendor == AcceleratorVendor.NVIDIA:
        return AdmissionRejectionReason.NVIDIA_MODEL_VARIANT_INCOMPATIBLE
    return AdmissionRejectionReason.ASCEND_MODEL_VARIANT_INCOMPATIBLE


def _profile_reason(vendor: AcceleratorVendor) -> AdmissionRejectionReason:
    if vendor == AcceleratorVendor.NVIDIA:
        return AdmissionRejectionReason.NVIDIA_PROFILE_UNAVAILABLE
    return AdmissionRejectionReason.ASCEND_PROFILE_UNAVAILABLE


def _remaining(limit: int | None, used: int) -> int:
    if limit is None:
        return _UNBOUNDED_ACCELERATOR_QUOTA
    return max(0, limit - used)


def _bounded_summary(items: list[dict[str, object]]) -> tuple[dict[str, object], ...]:
    result: list[dict[str, object]] = []
    for item in items:
        if _summary_size([*result, item]) > _CANDIDATE_SUMMARY_MAX_BYTES:
            break
        result.append(item)
    omitted = len(items) - len(result)
    if omitted:
        marker: dict[str, object] = {"truncated": True, "omitted_candidates": omitted}
        while result and _summary_size([*result, marker]) > _CANDIDATE_SUMMARY_MAX_BYTES:
            result.pop()
            marker["omitted_candidates"] = len(items) - len(result)
        if _summary_size([*result, marker]) <= _CANDIDATE_SUMMARY_MAX_BYTES:
            result.append(marker)
    return tuple(result)


def _summary_size(items: list[dict[str, object]]) -> int:
    return len(
        json.dumps(
            items,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
