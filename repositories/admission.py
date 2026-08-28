from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.accelerators import kind_for_vendor
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
    RuntimeType,
    WorkerStatus,
    WorkloadType,
)
from core.runtime_profiles import (
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
    RuntimeProfileManifestEntry,
)
from models.admission import AdmissionEvent
from models.model_variant import ModelVariant
from models.scheduling import GPUDevice, ResourceReservation
from models.service import ModelService
from models.task import Task
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

_CANDIDATE_SUMMARY_MAX_BYTES = 15_500


@dataclass(frozen=True, slots=True)
class InventoryDeviceSnapshot:
    device_id: uuid.UUID
    worker_id: str
    node_name: str
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
class BatchAdmissionSnapshot:
    worker_id: str
    worker_session_id: uuid.UUID
    vendor: AcceleratorVendor
    kind: AcceleratorKind
    selected_model: str
    runtime_profile_id: str
    runtime_profile_version: str
    runtime_profile_digest: str
    allocation_authority: AllocationAuthority
    accelerator_resource_name: str
    selection_policy: AcceleratorSelectionPolicy


@dataclass(frozen=True, slots=True)
class BatchAdmissionResult:
    snapshot: BatchAdmissionSnapshot | None
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


@dataclass(frozen=True, slots=True)
class _DeferredReservationTotal:
    worker_id: str
    vendor: str
    profile_id: str | None
    profile_version: str | None
    profile_digest: str | None
    accelerator_count: int


@dataclass(frozen=True, slots=True)
class _DeferredAcceleratorUsage:
    by_worker_resource: Mapping[tuple[str, str, str], int] = field(default_factory=dict)
    unknown_by_worker_vendor: Mapping[tuple[str, str], int] = field(default_factory=dict)

    def for_pool(self, *, worker_id: str, vendor: AcceleratorVendor, resource_name: str) -> int:
        vendor_value = vendor.value
        return self.by_worker_resource.get(
            (worker_id, vendor_value, resource_name),
            0,
        ) + self.unknown_by_worker_vendor.get((worker_id, vendor_value), 0)


@dataclass(frozen=True, slots=True)
class _ServiceAcceleratorUsage:
    by_resource: Mapping[tuple[str, str], int] = field(default_factory=dict)
    unknown_by_vendor: Mapping[str, int] = field(default_factory=dict)
    unknown_vendor: int = 0

    def for_pool(self, *, vendor: AcceleratorVendor, resource_name: str) -> int:
        vendor_value = vendor.value
        return (
            self.by_resource.get((vendor_value, resource_name), 0)
            + self.unknown_by_vendor.get(vendor_value, 0)
            + self.unknown_vendor
        )


def typed_quota_available(
    snapshot: QuotaSnapshot,
    vendor: AcceleratorVendor,
) -> int | None:
    """Return finite remaining typed quota, or ``None`` when both limits are unlimited."""

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
    finite_limits = tuple(
        available for available in (aggregate_available, typed_available) if available is not None
    )
    if not finite_limits:
        return None
    return max(0, min(finite_limits))


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
        accepted_health = (
            ("healthy", "inventory-only")
            if runtime_type == RuntimeType.KUBERNETES
            else ("healthy",)
        )
        query = (
            select(GPUDevice, Worker)
            .join(Worker, Worker.id == GPUDevice.worker_id)
            .where(
                Worker.status == WorkerStatus.ONLINE,
                Worker.overcommitted.is_(False),
                GPUDevice.health.in_(accepted_health),
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
                    node_name=worker.node_name or worker.id,
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
    async def active_deferred_accelerators_for_pool(
        session: AsyncSession,
        *,
        catalog: RuntimeProfileCatalog,
        worker_id: str,
        vendor: AcceleratorVendor,
        resource_name: str,
    ) -> int:
        """Return active deferred usage for one exact Kubernetes resource pool."""

        normalized_resource = resource_name.strip()
        if not normalized_resource:
            raise ValueError("resource_name must not be blank")
        usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            worker_ids=frozenset({worker_id}),
        )
        return usage.for_pool(
            worker_id=worker_id,
            vendor=vendor,
            resource_name=normalized_resource,
        )

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
    async def admit_batch_task(
        session: AsyncSession,
        *,
        catalog: RuntimeProfileCatalog,
        task: Task,
        request: AdmissionRequest,
        allowed_worker_ids: frozenset[str] | None = None,
    ) -> BatchAdmissionResult:
        """Select one immutable Kubernetes accelerator pool without binding device rows."""

        if task.runtime_type != RuntimeType.KUBERNETES:
            raise ValueError("vendor-aware batch admission requires runtime_type='kubernetes'")
        try:
            quota = await QuotaRepository.get_locked(session, project_id=task.project_id)
        except QuotaNotFoundError:
            quota = await QuotaRepository.initialize(session, project_id=task.project_id)
        inventory = await AdmissionRepository.list_healthy_inventory_devices(
            session,
            vendors=tuple(sorted(request.allowed_vendors, key=lambda item: item.value)),
            kinds=tuple(sorted(request.allowed_kinds, key=lambda item: item.value)),
            minimum_memory_mb=task.gpu_memory_mb,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
        )
        if allowed_worker_ids is not None:
            inventory = [device for device in inventory if device.worker_id in allowed_worker_ids]

        active_deferred = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            worker_ids=frozenset(device.worker_id for device in inventory),
        )

        candidates: list[AdmissionCandidate] = []
        bindings: dict[str, tuple[InventoryDeviceSnapshot, str, str, str]] = {}
        groups: dict[
            tuple[str, AcceleratorVendor, AcceleratorKind, str, str, str, str, str],
            list[InventoryDeviceSnapshot],
        ] = {}
        latest_entries: dict[
            tuple[AcceleratorVendor, AcceleratorKind, str], RuntimeProfileManifestEntry
        ] = {}
        for entry in catalog.manifest.profiles:
            latest_key = (entry.vendor, entry.kind, entry.profile_id)
            current = latest_entries.get(latest_key)
            is_newer = current is None or _profile_version_key(
                entry.profile_version
            ) > _profile_version_key(current.profile_version)
            if is_newer:
                latest_entries[latest_key] = entry
        for device in inventory:
            for entry in latest_entries.values():
                if (
                    entry.profile_id not in device.runtime_profile_ids
                    or entry.vendor != device.vendor
                    or entry.kind != device.kind
                    or (
                        request.runtime_profile_id is not None
                        and entry.profile_id != request.runtime_profile_id
                    )
                ):
                    continue
                profile = catalog.load_exact(
                    profile_id=entry.profile_id,
                    profile_version=entry.profile_version,
                    semantic_digest=entry.semantic_digest,
                )
                if device.kubernetes_resource_name != profile.kubernetes.resource_name:
                    continue
                group_key = (
                    device.worker_id,
                    device.vendor,
                    device.kind,
                    device.model,
                    entry.profile_id,
                    entry.profile_version,
                    entry.semantic_digest,
                    profile.kubernetes.resource_name,
                )
                groups.setdefault(group_key, []).append(device)

        for index, (group_key, devices) in enumerate(
            sorted(groups.items(), key=lambda item: item[0])
        ):
            (
                worker_id,
                vendor,
                kind,
                model,
                profile_id,
                profile_version,
                profile_digest,
                resource_name,
            ) = group_key
            profile = catalog.load_exact(
                profile_id=profile_id,
                profile_version=profile_version,
                semantic_digest=profile_digest,
            )
            capabilities = (
                frozenset(
                    set.intersection(*(set(device.capabilities) for device in devices))
                    if devices
                    else set()
                )
                | frozenset(profile.capabilities.features)
                | frozenset(profile.capabilities.dtypes)
            )
            candidate_id = f"{worker_id}:{profile_id}@{profile_version}:{model}:{index}"
            finite_quota = typed_quota_available(quota, vendor)
            candidate = AdmissionCandidate(
                candidate_id=candidate_id,
                vendor=vendor,
                kind=kind,
                model=model,
                runtime_profile_id=profile_id,
                runtime_profile_version=profile_version,
                runtime_profile_digest=profile_digest,
                model_variant_id=None,
                allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN,
                available_capacity=max(
                    0,
                    len(devices)
                    - active_deferred.for_pool(
                        worker_id=worker_id,
                        vendor=vendor,
                        resource_name=resource_name,
                    ),
                ),
                available_quota=(request.count if finite_quota is None else finite_quota),
                healthy=True,
                capabilities=capabilities,
            )
            candidates.append(candidate)
            bindings[candidate_id] = (devices[0], resource_name, profile_id, profile_version)

        decision = (
            choose_admission(request, candidates) if candidates else _missing_pool_decision(request)
        )
        summary = candidate_summary(candidates, decision)
        if not decision.allowed or decision.selected_candidate is None:
            reason = decision.reason or _profile_reason(_ordered_vendors(request)[0])
            rejected_vendor = (
                decision.rejections[0].vendor
                if decision.rejections
                else _ordered_vendors(request)[0]
            )
            await AdmissionRepository.record_event(
                session,
                project_id=task.project_id,
                workload_type=WorkloadType.BATCH_JOB,
                workload_id=task.id,
                policy=request.selection_policy,
                outcome="rejected",
                reason=reason.value,
                summary=summary,
            )
            return BatchAdmissionResult(None, reason, rejected_vendor, summary)

        selected = decision.selected_candidate
        device, resource_name, _profile_id, _profile_version = bindings[selected.candidate_id]
        await AdmissionRepository.record_event(
            session,
            project_id=task.project_id,
            workload_type=WorkloadType.BATCH_JOB,
            workload_id=task.id,
            policy=request.selection_policy,
            outcome="admitted",
            reason="admitted",
            summary=summary,
            selected_candidate=selected,
        )
        assert selected.runtime_profile_id is not None
        assert selected.runtime_profile_version is not None
        assert selected.runtime_profile_digest is not None
        return BatchAdmissionResult(
            snapshot=BatchAdmissionSnapshot(
                worker_id=device.worker_id,
                worker_session_id=device.worker_session_id,
                vendor=selected.vendor,
                kind=selected.kind,
                selected_model=selected.model,
                runtime_profile_id=selected.runtime_profile_id,
                runtime_profile_version=selected.runtime_profile_version,
                runtime_profile_digest=selected.runtime_profile_digest,
                allocation_authority=selected.allocation_authority,
                accelerator_resource_name=resource_name,
                selection_policy=request.selection_policy,
            ),
            reason=None,
            rejected_vendor=None,
            summary=summary,
        )

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
        service_usage = await _active_service_accelerators(session)
        deferred_usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            worker_ids=frozenset(device.worker_id for device in inventory),
        )
        candidates, bindings = _service_candidates(
            variants=variants,
            inventory=inventory,
            catalog=catalog,
            quota=quota,
            request=request,
            desired_replicas=desired_replicas,
            requested_dtype=requested_dtype,
            service_usage=service_usage,
            deferred_usage=deferred_usage,
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

    @staticmethod
    async def revalidate_logical_model_service_scale(
        session: AsyncSession,
        *,
        catalog: RuntimeProfileCatalog,
        service: ModelService,
        desired_replicas: int,
    ) -> ServiceAdmissionResult:
        """Re-admit one immutable logical-service snapshot before a positive scale-up."""

        if desired_replicas <= service.desired_replicas:
            raise ValueError("service scale revalidation requires a positive replica delta")
        try:
            vendor = (
                AcceleratorVendor(service.selected_vendor)
                if service.selected_vendor is not None
                else None
            )
        except ValueError:
            vendor = None
        try:
            kind = (
                AcceleratorKind(service.selected_kind)
                if service.selected_kind is not None
                else None
            )
        except ValueError:
            kind = None
        try:
            policy = (
                AcceleratorSelectionPolicy(service.selection_policy)
                if service.selection_policy is not None
                else None
            )
        except ValueError:
            policy = None
        invalid_snapshot = (
            service.logical_model_id is None
            or service.model_variant_id is None
            or vendor is None
            or kind is None
            or kind != kind_for_vendor(vendor)
            or service.selected_model is None
            or not service.selected_model.strip()
            or service.runtime_profile_id is None
            or not service.runtime_profile_id.strip()
            or service.runtime_profile_version is None
            or not service.runtime_profile_version.strip()
            or service.runtime_profile_digest is None
            or not service.runtime_profile_digest.strip()
            or service.accelerator_resource_name is None
            or not service.accelerator_resource_name.strip()
            or policy is None
            or service.runtime_type != RuntimeType.KUBERNETES
            or not 1 <= service.gpu_count <= 64
            or service.tensor_parallel_size != service.gpu_count
            or service.allocation_authority != AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value
        )
        if invalid_snapshot:
            return await _record_invalid_service_snapshot(
                session,
                service=service,
                vendor=vendor,
                policy=policy,
            )
        assert service.logical_model_id is not None
        assert service.model_variant_id is not None
        assert vendor is not None
        assert kind is not None
        assert service.selected_model is not None
        assert service.runtime_profile_id is not None
        assert service.runtime_profile_version is not None
        assert service.runtime_profile_digest is not None
        assert service.accelerator_resource_name is not None
        assert policy is not None
        allowed_vendors = {vendor}
        if policy == AcceleratorSelectionPolicy.PREFER_NVIDIA:
            allowed_vendors.add(AcceleratorVendor.NVIDIA)
        elif policy == AcceleratorSelectionPolicy.PREFER_ASCEND:
            allowed_vendors.add(AcceleratorVendor.HUAWEI_ASCEND)
        request = AdmissionRequest(
            count=service.gpu_count,
            allowed_vendors=frozenset(allowed_vendors),
            allowed_kinds=frozenset(kind_for_vendor(item) for item in allowed_vendors),
            allowed_models=frozenset({service.selected_model}),
            runtime_profile_id=service.runtime_profile_id,
            runtime_profile_version=service.runtime_profile_version,
            runtime_profile_digest=service.runtime_profile_digest,
            model_variant_id=str(service.model_variant_id),
            selection_policy=policy,
        )

        variant = await ModelVariantRepository.get(
            session,
            project_id=service.project_id,
            logical_model_id=service.logical_model_id,
            variant_id=service.model_variant_id,
            for_update=False,
        )
        invalid_snapshot = not _service_variant_snapshot_matches(
            service,
            variant,
            vendor=vendor,
            kind=kind,
        )
        if invalid_snapshot:
            return await _record_rejected_service_admission(
                session,
                project_id=service.project_id,
                service_id=service.id,
                request=request,
                candidates=(),
                decision=_single_service_rejection(
                    request,
                    vendor=vendor,
                    reason=_variant_reason(vendor),
                    candidate_id="stale-service-admission-snapshot",
                ),
            )
        assert variant is not None

        try:
            quota = await QuotaRepository.get_locked(session, project_id=service.project_id)
        except QuotaNotFoundError:
            quota = await QuotaRepository.initialize(session, project_id=service.project_id)
        inventory = await AdmissionRepository.list_healthy_inventory_devices(
            session,
            vendors=(vendor,),
            kinds=(kind,),
            minimum_memory_mb=service.gpu_memory_mb,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
        )
        service_usage = await _active_service_accelerators(
            session,
            exclude_service_id=service.id,
        )
        deferred_usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            worker_ids=frozenset(device.worker_id for device in inventory),
        )
        candidates, bindings = _service_candidates(
            variants=(variant,),
            inventory=inventory,
            catalog=catalog,
            quota=quota,
            request=request,
            desired_replicas=desired_replicas,
            requested_dtype=service.dtype,
            service_usage=service_usage,
            deferred_usage=deferred_usage,
            quota_credit=service.gpu_count * service.desired_replicas,
        )
        decision = (
            choose_admission(request, candidates)
            if candidates
            else _missing_variant_decision(request)
        )
        if not decision.allowed or decision.selected_candidate is None:
            return await _record_rejected_service_admission(
                session,
                project_id=service.project_id,
                service_id=service.id,
                request=request,
                candidates=candidates,
                decision=decision,
            )

        selected = decision.selected_candidate
        binding = bindings[selected.candidate_id]
        locked_variant = await ModelVariantRepository.revalidate_ready_for_reservation(
            session,
            project_id=service.project_id,
            logical_model_id=service.logical_model_id,
            expected_variant_id=variant.id,
            expected_vendor=variant.vendor,
            expected_kind=variant.kind,
            expected_runtime_profile_id=variant.runtime_profile_id,
            expected_runtime_profile_version=variant.runtime_profile_version,
            expected_artifact_digest=variant.artifact_digest,
            expected_runtime_profile_digest=variant.runtime_profile_digest,
        )
        if not _service_variant_snapshot_matches(
            service,
            locked_variant,
            vendor=vendor,
            kind=kind,
        ):
            return await _record_rejected_service_admission(
                session,
                project_id=service.project_id,
                service_id=service.id,
                request=request,
                candidates=candidates,
                decision=_single_service_rejection(
                    request,
                    vendor=vendor,
                    reason=_variant_reason(vendor),
                    candidate_id=selected.candidate_id,
                ),
            )
        assert locked_variant is not None
        if (
            selected.vendor != vendor
            or selected.kind != kind
            or selected.model != service.selected_model
            or selected.runtime_profile_id != service.runtime_profile_id
            or selected.runtime_profile_version != service.runtime_profile_version
            or selected.runtime_profile_digest != service.runtime_profile_digest
            or selected.model_variant_id != str(service.model_variant_id)
            or selected.allocation_authority != AllocationAuthority.KUBERNETES_DEVICE_PLUGIN
            or binding.resource_name != service.accelerator_resource_name
        ):
            return await _record_rejected_service_admission(
                session,
                project_id=service.project_id,
                service_id=service.id,
                request=request,
                candidates=candidates,
                decision=_single_service_rejection(
                    request,
                    vendor=vendor,
                    reason=_profile_reason(vendor),
                    candidate_id=selected.candidate_id,
                ),
            )

        summary = candidate_summary(candidates, decision)
        await AdmissionRepository.record_event(
            session,
            project_id=service.project_id,
            workload_type=WorkloadType.MODEL_SERVICE,
            workload_id=service.id,
            policy=request.selection_policy,
            outcome="admitted",
            reason="scale_up_admitted",
            summary=summary,
            selected_candidate=selected,
        )
        return ServiceAdmissionResult(
            snapshot=ServiceAdmissionSnapshot(
                logical_model_id=service.logical_model_id,
                model_variant_id=locked_variant.id,
                vendor=vendor,
                kind=kind,
                selected_model=selected.model,
                runtime_profile_id=locked_variant.runtime_profile_id,
                runtime_profile_version=locked_variant.runtime_profile_version,
                runtime_profile_digest=locked_variant.runtime_profile_digest,
                allocation_authority=selected.allocation_authority,
                accelerator_resource_name=service.accelerator_resource_name,
                selection_policy=request.selection_policy,
                artifact_source=locked_variant.artifact_source,
                artifact_revision=locked_variant.artifact_revision,
                artifact_digest=locked_variant.artifact_digest,
                dtype=locked_variant.dtype,
            ),
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
    service_usage: _ServiceAcceleratorUsage | None = None,
    deferred_usage: _DeferredAcceleratorUsage | None = None,
    quota_credit: int = 0,
) -> tuple[list[AdmissionCandidate], dict[str, _ServiceCandidateBinding]]:
    if quota_credit < 0:
        raise ValueError("quota_credit must not be negative")
    candidates: list[AdmissionCandidate] = []
    bindings: dict[str, _ServiceCandidateBinding] = {}
    service_usage = service_usage or _ServiceAcceleratorUsage()
    deferred_usage = deferred_usage or _DeferredAcceleratorUsage()
    commitment_divisor = max(1, desired_replicas)
    for variant in variants:
        manifest_entry = None
        runtime_profile = None
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
            runtime_profile = catalog.load_exact(
                profile_id=variant.runtime_profile_id,
                profile_version=variant.runtime_profile_version,
                semantic_digest=variant.runtime_profile_digest,
            )
        except RuntimeProfileCompatibilityError:
            pass
        variant_ready = (requested_dtype == "auto" or requested_dtype == variant.dtype) and len(
            variant.artifact_source
        ) <= 512
        relevant = [
            device
            for device in inventory
            if device.vendor == variant.vendor and device.kind == variant.kind
        ]
        profiled = [
            device
            for device in relevant
            if variant.runtime_profile_id in device.runtime_profile_ids
            and runtime_profile is not None
            and device.kubernetes_resource_name == runtime_profile.kubernetes.resource_name
        ]
        groups: dict[
            tuple[str, str, str, str, tuple[str, ...]],
            list[InventoryDeviceSnapshot],
        ] = {}
        for device in profiled:
            capabilities_set = set(device.capabilities)
            capabilities_set.add(variant.dtype)
            if manifest_entry is not None:
                capabilities_set.update(manifest_entry.features)
            key = (
                device.worker_id,
                device.node_name,
                device.model,
                device.kubernetes_resource_name or "",
                tuple(sorted(capabilities_set)),
            )
            groups.setdefault(key, []).append(device)

        finite_quota = typed_quota_available(quota, variant.vendor)
        if finite_quota is not None:
            finite_quota += quota_credit
        available_quota = (
            request.count if finite_quota is None else finite_quota // commitment_divisor
        )
        for index, (key, devices) in enumerate(sorted(groups.items()), start=1):
            worker_id, _node_name, model, resource_name, capabilities = key
            candidate_id = f"{variant.id}:{index}"
            occupied = service_usage.for_pool(
                vendor=variant.vendor,
                resource_name=resource_name,
            ) + deferred_usage.for_pool(
                worker_id=worker_id,
                vendor=variant.vendor,
                resource_name=resource_name,
            )
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
                available_capacity=max(0, len(devices) - occupied) // commitment_divisor,
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


def _service_variant_snapshot_matches(
    service: ModelService,
    variant: ModelVariant | None,
    *,
    vendor: AcceleratorVendor,
    kind: AcceleratorKind,
) -> bool:
    return (
        variant is not None
        and variant.id == service.model_variant_id
        and variant.logical_model_id == service.logical_model_id
        and variant.vendor == vendor
        and variant.kind == kind
        and variant.runtime_profile_id == service.runtime_profile_id
        and variant.runtime_profile_version == service.runtime_profile_version
        and variant.runtime_profile_digest == service.runtime_profile_digest
        and variant.artifact_source == service.model
        and variant.artifact_revision == service.model_revision
        and variant.dtype == service.dtype
    )


def _single_service_rejection(
    request: AdmissionRequest,
    *,
    vendor: AcceleratorVendor,
    reason: AdmissionRejectionReason,
    candidate_id: str,
) -> AdmissionDecision:
    return AdmissionDecision(
        accelerator_count=request.count,
        selected_candidate=None,
        rejections=(
            AdmissionRejection(
                candidate_id=candidate_id,
                vendor=vendor,
                reason=reason,
            ),
        ),
    )


async def _record_invalid_service_snapshot(
    session: AsyncSession,
    *,
    service: ModelService,
    vendor: AcceleratorVendor | None,
    policy: AcceleratorSelectionPolicy | None,
) -> ServiceAdmissionResult:
    reason = _variant_reason(vendor) if vendor is not None else None
    reason_value = reason.value if reason is not None else "invalid_service_admission_snapshot"
    summary: tuple[dict[str, object], ...] = (
        {
            "candidate_id": "stale-service-admission-snapshot",
            "reason": reason_value,
        },
    )
    await AdmissionRepository.record_event(
        session,
        project_id=service.project_id,
        workload_type=WorkloadType.MODEL_SERVICE,
        workload_id=service.id,
        policy=policy or AcceleratorSelectionPolicy.ANY,
        outcome="rejected",
        reason=reason_value,
        summary=summary,
    )
    return ServiceAdmissionResult(
        snapshot=None,
        reason=reason,
        rejected_vendor=vendor,
        summary=summary,
    )


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
        decision.rejections[0].vendor if decision.rejections else _ordered_vendors(request)[0]
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


def _missing_pool_decision(request: AdmissionRequest) -> AdmissionDecision:
    return AdmissionDecision(
        accelerator_count=request.count,
        selected_candidate=None,
        rejections=tuple(
            AdmissionRejection(
                candidate_id=f"no-ready-{vendor.value}-pool",
                vendor=vendor,
                reason=_profile_reason(vendor),
            )
            for vendor in _ordered_vendors(request)
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


async def _active_deferred_accelerators(
    session: AsyncSession,
    *,
    catalog: RuntimeProfileCatalog,
    worker_ids: frozenset[str],
) -> _DeferredAcceleratorUsage:
    if not worker_ids:
        return _DeferredAcceleratorUsage()
    rows = await session.execute(
        select(
            ResourceReservation.worker_id,
            ResourceReservation.requested_vendor,
            ResourceReservation.requested_profile_id,
            ResourceReservation.requested_profile_version,
            ResourceReservation.requested_profile_digest,
            func.coalesce(func.sum(ResourceReservation.gpu_count), 0),
        )
        .where(
            ResourceReservation.worker_id.in_(worker_ids),
            ResourceReservation.released_at.is_(None),
            ResourceReservation.allocation_authority
            == AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value,
            ResourceReservation.requested_vendor.is_not(None),
        )
        .group_by(
            ResourceReservation.worker_id,
            ResourceReservation.requested_vendor,
            ResourceReservation.requested_profile_id,
            ResourceReservation.requested_profile_version,
            ResourceReservation.requested_profile_digest,
        )
    )
    totals = [
        _DeferredReservationTotal(
            worker_id=worker_id,
            vendor=vendor,
            profile_id=profile_id,
            profile_version=profile_version,
            profile_digest=profile_digest,
            accelerator_count=int(accelerator_count),
        )
        for (
            worker_id,
            vendor,
            profile_id,
            profile_version,
            profile_digest,
            accelerator_count,
        ) in rows
        if vendor is not None
    ]
    return _classify_deferred_accelerators(totals, catalog=catalog)


def _classify_deferred_accelerators(
    totals: Sequence[_DeferredReservationTotal],
    *,
    catalog: RuntimeProfileCatalog,
) -> _DeferredAcceleratorUsage:
    by_worker_resource: dict[tuple[str, str, str], int] = {}
    unknown_by_worker_vendor: dict[tuple[str, str], int] = {}
    for total in totals:
        resource_name: str | None = None
        if (
            total.profile_id is not None
            and total.profile_version is not None
            and total.profile_digest is not None
        ):
            try:
                profile = catalog.load_exact(
                    profile_id=total.profile_id,
                    profile_version=total.profile_version,
                    semantic_digest=total.profile_digest,
                )
                if profile.vendor.value == total.vendor:
                    resource_name = profile.kubernetes.resource_name
            except RuntimeProfileCompatibilityError:
                pass
        if resource_name is None:
            fallback_key = (total.worker_id, total.vendor)
            unknown_by_worker_vendor[fallback_key] = (
                unknown_by_worker_vendor.get(fallback_key, 0) + total.accelerator_count
            )
            continue
        resource_key = (total.worker_id, total.vendor, resource_name)
        by_worker_resource[resource_key] = (
            by_worker_resource.get(resource_key, 0) + total.accelerator_count
        )
    return _DeferredAcceleratorUsage(
        by_worker_resource=by_worker_resource,
        unknown_by_worker_vendor=unknown_by_worker_vendor,
    )


async def _active_service_accelerators(
    session: AsyncSession,
    *,
    exclude_service_id: uuid.UUID | None = None,
) -> _ServiceAcceleratorUsage:
    query = select(
        ModelService.selected_vendor,
        ModelService.accelerator_resource_name,
        ModelService.gpu_count,
        ModelService.tensor_parallel_size,
        ModelService.desired_replicas,
    ).where(
        ModelService.runtime_type == RuntimeType.KUBERNETES,
        ModelService.desired_replicas > 0,
        ModelService.gpu_count > 0,
    )
    if exclude_service_id is not None:
        query = query.where(ModelService.id != exclude_service_id)
    rows = await session.execute(query.order_by(ModelService.id))
    by_resource: dict[tuple[str, str], int] = {}
    unknown_by_vendor: dict[str, int] = {}
    unknown_vendor = 0
    for vendor, resource_name, gpu_count, tensor_parallel_size, desired_replicas in rows:
        accelerator_count = max(int(gpu_count), int(tensor_parallel_size)) * int(desired_replicas)
        normalized_resource = (resource_name or "").strip()
        if vendor is None:
            unknown_vendor += accelerator_count
        elif not normalized_resource:
            unknown_by_vendor[vendor] = unknown_by_vendor.get(vendor, 0) + accelerator_count
        else:
            resource_key = (vendor, normalized_resource)
            by_resource[resource_key] = by_resource.get(resource_key, 0) + accelerator_count
    return _ServiceAcceleratorUsage(
        by_resource=by_resource,
        unknown_by_vendor=unknown_by_vendor,
        unknown_vendor=unknown_vendor,
    )


def _remaining(limit: int | None, used: int) -> int | None:
    if limit is None:
        return None
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


def _profile_version_key(value: str) -> tuple[int, int, int]:
    major, minor, patch = value.split(".")
    return int(major), int(minor), int(patch)
