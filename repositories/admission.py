from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import or_, select
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
from core.kubernetes_names import validate_kubernetes_dns_subdomain
from core.runtime_profiles import (
    RuntimeProfile,
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
    runtime_profile_binding_id,
)
from models.admission import AdmissionEvent
from models.model_variant import ModelVariant
from models.scheduling import GPUDevice, ResourceReservation
from models.service import ModelService, ReplicaStatus, ServiceReplica
from models.task import Task
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.gateway_model_names import lock_gateway_model_namespace
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


def _runtime_profile_capabilities(profile: RuntimeProfile) -> frozenset[str]:
    capabilities = set(profile.capabilities.features) | set(profile.capabilities.dtypes)
    if profile.capabilities.tensor_parallel.supported:
        capabilities.add("tensor-parallel")
    return frozenset(capabilities)


@dataclass(frozen=True, slots=True)
class InventoryDeviceSnapshot:
    device_id: uuid.UUID
    worker_id: str
    node_name: str
    worker_session_id: uuid.UUID
    worker_status: WorkerStatus
    worker_runtime_types: tuple[str, ...]
    device_uuid: str
    health: str
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
    eligible_node_names: tuple[str, ...]


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
    eligible_node_names: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _DeferredReservationTotal:
    worker_id: str
    vendor: str
    profile_id: str | None
    profile_version: str | None
    profile_digest: str | None
    accelerator_count: int
    node_name: str | None = None
    observed_device_ids: tuple[str, ...] | None = None
    worker_session_matches: bool = False


@dataclass(frozen=True, slots=True)
class _DeferredAcceleratorUsage:
    by_node_resource: Mapping[tuple[str, str, str, str], int] = field(default_factory=dict)
    unknown_by_worker_resource: Mapping[tuple[str, str, str], int] = field(default_factory=dict)
    unknown_by_worker_vendor: Mapping[tuple[str, str], int] = field(default_factory=dict)
    unknown_by_vendor_resource: Mapping[tuple[str, str], int] = field(default_factory=dict)
    unknown_by_vendor: Mapping[str, int] = field(default_factory=dict)

    def for_pool(self, *, worker_id: str, vendor: AcceleratorVendor, resource_name: str) -> int:
        vendor_value = vendor.value
        exact = sum(
            count
            for (
                candidate_worker,
                _node,
                candidate_vendor,
                candidate_resource,
            ), count in self.by_node_resource.items()
            if candidate_worker == worker_id
            and candidate_vendor == vendor_value
            and candidate_resource == resource_name
        )
        return (
            exact
            + self.unknown_by_worker_resource.get(
                (worker_id, vendor_value, resource_name),
                0,
            )
            + self.unknown_by_worker_vendor.get((worker_id, vendor_value), 0)
            + self.unknown_by_vendor_resource.get((vendor_value, resource_name), 0)
            + self.unknown_by_vendor.get(vendor_value, 0)
        )


@dataclass(frozen=True, slots=True)
class _ServiceReplicaCommitment:
    service_id: uuid.UUID
    replica_ordinal: int
    vendor: str | None
    model: str | None
    resource_name: str | None
    accelerator_count: int
    eligible_node_names: tuple[str, ...] | None
    assigned_node_name: str | None = None
    generation: int = 1


@dataclass(frozen=True, slots=True)
class _ServiceAcceleratorUsage:
    commitments: tuple[_ServiceReplicaCommitment, ...] = ()


def _plan_physical_pool_remaining(
    *,
    inventory: Sequence[InventoryDeviceSnapshot],
    vendor: AcceleratorVendor,
    model: str,
    resource_name: str,
    service_usage: _ServiceAcceleratorUsage,
    deferred_usage: _DeferredAcceleratorUsage,
) -> dict[tuple[str, str], int] | None:
    """Plan existing commitments once across one physical accelerator pool.

    A ``None`` result means an incomplete or impossible service snapshot makes
    the pool unsafe to admit. Existing replica gangs are charged only to their
    observed Kubernetes node; guessing an unobserved placement can over-admit
    fragmented tensor-parallel capacity.
    """

    candidate_node_keys = {
        (device.worker_id, device.node_name)
        for device in inventory
        if device.vendor == vendor
        and device.model == model
        and device.kubernetes_resource_name == resource_name
    }
    if any(
        len({device.worker_id for device in inventory if device.node_name == node_name}) != 1
        for node_name in {
            candidate_node_name for _worker_id, candidate_node_name in candidate_node_keys
        }
    ):
        return None
    devices = [
        device
        for device in inventory
        if device.vendor == vendor
        and device.kubernetes_resource_name == resource_name
        and (device.worker_id, device.node_name) in candidate_node_keys
    ]
    remaining: dict[tuple[str, str], int] = {}
    for device in devices:
        node_key = (device.worker_id, device.node_name)
        remaining[node_key] = remaining.get(node_key, 0) + 1

    vendor_value = vendor.value
    if (
        deferred_usage.unknown_by_vendor_resource.get((vendor_value, resource_name), 0) > 0
        or deferred_usage.unknown_by_vendor.get(vendor_value, 0) > 0
    ):
        return None
    relevant_workers = {key[0] for key in remaining}
    if any(
        count > 0
        and worker_id in relevant_workers
        and deferred_vendor == vendor_value
        and deferred_resource == resource_name
        for (
            worker_id,
            deferred_vendor,
            deferred_resource,
        ), count in deferred_usage.unknown_by_worker_resource.items()
    ) or any(
        count > 0 and worker_id in relevant_workers and deferred_vendor == vendor_value
        for (worker_id, deferred_vendor), count in deferred_usage.unknown_by_worker_vendor.items()
    ):
        return None
    for (
        worker_id,
        node_name,
        deferred_vendor,
        deferred_resource,
    ), deferred in deferred_usage.by_node_resource.items():
        if deferred_vendor != vendor_value or deferred_resource != resource_name:
            continue
        node_key = (worker_id, node_name)
        if remaining.get(node_key, 0) < deferred:
            return None
        remaining[node_key] -= deferred

    for commitment in service_usage.commitments:
        if commitment.vendor is None:
            return None
        if commitment.vendor != vendor.value:
            continue
        if commitment.resource_name is None:
            return None
        if commitment.resource_name != resource_name:
            continue
        if not commitment.eligible_node_names:
            return None
        if commitment.assigned_node_name is None:
            if set(commitment.eligible_node_names).intersection(
                node_name for _worker_id, node_name in remaining
            ):
                return None
            continue
        if commitment.assigned_node_name not in set(commitment.eligible_node_names or ()):
            return None
        assigned_keys = [
            node_key for node_key in remaining if node_key[1] == commitment.assigned_node_name
        ]
        if not assigned_keys:
            continue
        if len(assigned_keys) != 1:
            return None
        assigned_key = assigned_keys[0]
        if commitment.model is None or commitment.model != model:
            remaining[assigned_key] = 0
            continue
        if remaining.get(assigned_key, 0) < commitment.accelerator_count:
            return None
        remaining[assigned_key] -= commitment.accelerator_count
    return remaining


def _batch_homogeneous_compatible_devices(
    *,
    inventory: Sequence[InventoryDeviceSnapshot],
    worker_id: str,
    vendor: AcceleratorVendor,
    kind: AcceleratorKind,
    model: str,
    profile_id: str,
    profile_version: str,
    profile_digest: str,
    resource_name: str,
    required_capabilities: frozenset[str] = frozenset(),
    profile_capabilities: frozenset[str] = frozenset(),
    minimum_memory_mb: int = 0,
) -> list[InventoryDeviceSnapshot]:
    binding_id = runtime_profile_binding_id(
        profile_id=profile_id,
        profile_version=profile_version,
        semantic_digest=profile_digest,
    )
    candidate_node_keys = {
        (device.worker_id, device.node_name)
        for device in inventory
        if device.worker_id == worker_id
        and device.health in {"healthy", "inventory-only"}
        and device.vendor == vendor
        and device.kind == kind
        and device.model == model
        and device.kubernetes_resource_name == resource_name
        and binding_id in device.runtime_profile_ids
    }
    compatible_devices: list[InventoryDeviceSnapshot] = []
    for node_key in sorted(candidate_node_keys):
        pool_devices = [
            device
            for device in inventory
            if (device.worker_id, device.node_name) == node_key
            and device.kubernetes_resource_name == resource_name
        ]
        if not pool_devices or not all(
            device.vendor == vendor
            and device.kind == kind
            and device.model == model
            and device.memory_total_mb >= minimum_memory_mb
            and binding_id in device.runtime_profile_ids
            and required_capabilities.issubset(
                frozenset(device.capabilities) | profile_capabilities
            )
            for device in pool_devices
        ):
            continue
        compatible_devices.extend(
            device for device in pool_devices if device.health in {"healthy", "inventory-only"}
        )
    return compatible_devices


def _batch_pool_available_capacity(
    *,
    inventory: Sequence[InventoryDeviceSnapshot],
    worker_id: str,
    vendor: AcceleratorVendor,
    kind: AcceleratorKind,
    model: str,
    profile_id: str,
    profile_version: str,
    profile_digest: str,
    resource_name: str,
    service_usage: _ServiceAcceleratorUsage,
    deferred_usage: _DeferredAcceleratorUsage,
    required_capabilities: frozenset[str] = frozenset(),
    profile_capabilities: frozenset[str] = frozenset(),
    minimum_memory_mb: int = 0,
) -> int:
    remaining = _plan_physical_pool_remaining(
        inventory=inventory,
        vendor=vendor,
        model=model,
        resource_name=resource_name,
        service_usage=service_usage,
        deferred_usage=deferred_usage,
    )
    if remaining is None:
        return 0
    compatible_by_node: dict[tuple[str, str], int] = {}
    for device in _batch_homogeneous_compatible_devices(
        inventory=inventory,
        worker_id=worker_id,
        vendor=vendor,
        kind=kind,
        model=model,
        profile_id=profile_id,
        profile_version=profile_version,
        profile_digest=profile_digest,
        resource_name=resource_name,
        required_capabilities=required_capabilities,
        profile_capabilities=profile_capabilities,
        minimum_memory_mb=minimum_memory_mb,
    ):
        node_key = (device.worker_id, device.node_name)
        compatible_by_node[node_key] = compatible_by_node.get(node_key, 0) + 1
    return sum(
        max(
            0,
            compatible_count
            - (
                sum(
                    1
                    for device in inventory
                    if (device.worker_id, device.node_name) == node_key
                    and device.vendor == vendor
                    and device.kubernetes_resource_name == resource_name
                )
                - remaining.get(node_key, 0)
            ),
        )
        for node_key, compatible_count in compatible_by_node.items()
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
        include_unavailable: bool = False,
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
        if not include_unavailable:
            query = query.where(GPUDevice.health.in_(accepted_health))
        if for_update:
            query = query.with_for_update()
        rows = list((await session.execute(query)).all())
        result: list[InventoryDeviceSnapshot] = []
        for device, worker in rows:
            runtime_types = tuple(sorted(set(worker.runtime_types or [])))
            if runtime_type is not None and runtime_type.value not in runtime_types:
                continue
            if runtime_type == RuntimeType.KUBERNETES:
                try:
                    node_name = validate_kubernetes_dns_subdomain(
                        worker.node_name,
                        field_name="worker node_name",
                    )
                except (TypeError, ValueError):
                    continue
            else:
                node_name = worker.node_name or worker.id
            result.append(
                InventoryDeviceSnapshot(
                    device_id=device.id,
                    worker_id=worker.id,
                    node_name=node_name,
                    worker_session_id=worker.worker_session_id,
                    worker_status=worker.status,
                    worker_runtime_types=runtime_types,
                    device_uuid=device.device_uuid,
                    health=device.health,
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
        worker = await session.get(Worker, worker_id)
        if worker is None or worker.node_name is None:
            return 0
        try:
            node_name = validate_kubernetes_dns_subdomain(
                worker.node_name,
                field_name="worker node_name",
            )
        except (TypeError, ValueError):
            return 0
        usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            node_owners={node_name: worker_id},
            vendors=frozenset({vendor}),
        )
        return usage.for_pool(
            worker_id=worker_id,
            vendor=vendor,
            resource_name=normalized_resource,
        )

    @staticmethod
    async def available_batch_accelerators_for_pool(
        session: AsyncSession,
        *,
        catalog: RuntimeProfileCatalog,
        worker_id: str,
        vendor: AcceleratorVendor,
        kind: AcceleratorKind,
        model: str,
        profile_id: str,
        profile_version: str,
        profile_digest: str,
        resource_name: str,
        minimum_memory_mb: int,
        required_capabilities: frozenset[str] = frozenset(),
    ) -> int:
        """Recompute one worker's batch capacity with the shared pool planner."""

        normalized_resource = resource_name.strip()
        if not normalized_resource:
            raise ValueError("resource_name must not be blank")
        try:
            profile = catalog.load_exact(
                profile_id=profile_id,
                profile_version=profile_version,
                semantic_digest=profile_digest,
            )
        except RuntimeProfileCompatibilityError:
            return 0
        if (
            profile.vendor != vendor
            or profile.kind != kind
            or profile.kubernetes.resource_name != normalized_resource
        ):
            return 0
        profile_capabilities = _runtime_profile_capabilities(profile)
        inventory = await AdmissionRepository.list_healthy_inventory_devices(
            session,
            vendors=(vendor,),
            kinds=(kind,),
            minimum_memory_mb=0,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
            include_unavailable=True,
        )
        service_usage = await _active_service_accelerators(session)
        deferred_usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            node_owners=_unique_inventory_node_owners(inventory),
            vendors=frozenset({vendor}),
        )
        return _batch_pool_available_capacity(
            inventory=inventory,
            worker_id=worker_id,
            vendor=vendor,
            kind=kind,
            model=model,
            profile_id=profile_id,
            profile_version=profile_version,
            profile_digest=profile_digest,
            resource_name=normalized_resource,
            service_usage=service_usage,
            deferred_usage=deferred_usage,
            required_capabilities=required_capabilities,
            profile_capabilities=profile_capabilities,
            minimum_memory_mb=minimum_memory_mb,
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
            minimum_memory_mb=0,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
            include_unavailable=True,
        )
        candidate_inventory = [
            device for device in inventory if device.health in {"healthy", "inventory-only"}
        ]
        if allowed_worker_ids is not None:
            candidate_inventory = [
                device for device in candidate_inventory if device.worker_id in allowed_worker_ids
            ]

        active_deferred = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            node_owners=_unique_inventory_node_owners(inventory),
            vendors=request.allowed_vendors,
        )
        active_services = await _active_service_accelerators(session)

        candidates: list[AdmissionCandidate] = []
        bindings: dict[str, tuple[InventoryDeviceSnapshot, str, str, str]] = {}
        groups: dict[
            tuple[str, AcceleratorVendor, AcceleratorKind, str, str, str, str, str],
            list[InventoryDeviceSnapshot],
        ] = {}
        for device in candidate_inventory:
            for entry in catalog.manifest.profiles:
                binding_id = runtime_profile_binding_id(
                    profile_id=entry.profile_id,
                    profile_version=entry.profile_version,
                    semantic_digest=entry.semantic_digest,
                )
                if (
                    binding_id not in device.runtime_profile_ids
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
            profile_capabilities = _runtime_profile_capabilities(profile)
            compatible_devices = _batch_homogeneous_compatible_devices(
                inventory=inventory,
                worker_id=worker_id,
                vendor=vendor,
                kind=kind,
                model=model,
                profile_id=profile_id,
                profile_version=profile_version,
                profile_digest=profile_digest,
                resource_name=resource_name,
                required_capabilities=request.required_capabilities,
                profile_capabilities=profile_capabilities,
                minimum_memory_mb=task.gpu_memory_mb,
            )
            capabilities = (
                frozenset(
                    set.intersection(*(set(device.capabilities) for device in compatible_devices))
                    if compatible_devices
                    else set()
                )
                | profile_capabilities
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
                available_capacity=_batch_pool_available_capacity(
                    inventory=inventory,
                    worker_id=worker_id,
                    vendor=vendor,
                    kind=kind,
                    model=model,
                    profile_id=profile_id,
                    profile_version=profile_version,
                    profile_digest=profile_digest,
                    resource_name=resource_name,
                    service_usage=active_services,
                    deferred_usage=active_deferred,
                    required_capabilities=request.required_capabilities,
                    profile_capabilities=profile_capabilities,
                    minimum_memory_mb=task.gpu_memory_mb,
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
        # Service creation serializes the project namespace before quota rows.
        # Match that global lock order here so logical admission cannot deadlock
        # a concurrent direct service create (Project -> quota/state).
        await lock_gateway_model_namespace(session, project_id=project_id)
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
            minimum_memory_mb=0,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
            include_unavailable=True,
        )
        service_usage = await _active_service_accelerators(session)
        deferred_usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            node_owners=_unique_inventory_node_owners(inventory),
            vendors=request.allowed_vendors,
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
            minimum_memory_mb=minimum_memory_mb,
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
            eligible_node_names=binding.eligible_node_names,
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
            minimum_memory_mb=0,
            runtime_type=RuntimeType.KUBERNETES,
            for_update=True,
            include_unavailable=True,
        )
        service_usage = await _active_service_accelerators(
            session,
            exclude_service_id=service.id,
        )
        deferred_usage = await _active_deferred_accelerators(
            session,
            catalog=catalog,
            node_owners=_unique_inventory_node_owners(inventory),
            vendors=frozenset({vendor}),
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
            minimum_memory_mb=service.gpu_memory_mb,
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
                eligible_node_names=binding.eligible_node_names,
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
    minimum_memory_mb: int = 0,
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
        runtime_profile_binding = None
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
            runtime_profile_binding = runtime_profile_binding_id(
                profile_id=variant.runtime_profile_id,
                profile_version=variant.runtime_profile_version,
                semantic_digest=variant.runtime_profile_digest,
            )
        except RuntimeProfileCompatibilityError:
            pass
        variant_ready = (requested_dtype == "auto" or requested_dtype == variant.dtype) and len(
            variant.artifact_source
        ) <= 512
        profile_capabilities = (
            _runtime_profile_capabilities(runtime_profile)
            if runtime_profile is not None
            else frozenset()
        )
        relevant = [
            device
            for device in inventory
            if device.vendor == variant.vendor and device.kind == variant.kind
        ]
        profiled: list[InventoryDeviceSnapshot] = []
        effective_capabilities: dict[uuid.UUID, frozenset[str]] = {}
        for device in relevant:
            capabilities = set(device.capabilities)
            capabilities.update(profile_capabilities)
            frozen_capabilities = frozenset(capabilities)
            effective_capabilities[device.device_id] = frozen_capabilities
            if (
                device.health not in {"healthy", "inventory-only"}
                or runtime_profile_binding is None
                or runtime_profile_binding not in device.runtime_profile_ids
                or runtime_profile is None
                or device.kubernetes_resource_name != runtime_profile.kubernetes.resource_name
            ):
                continue
            if not request.required_capabilities.issubset(frozen_capabilities):
                continue
            profiled.append(device)
        groups: dict[tuple[str, str], list[InventoryDeviceSnapshot]] = {}
        for device in profiled:
            key = (
                device.model,
                device.kubernetes_resource_name or "",
            )
            groups.setdefault(key, []).append(device)

        finite_quota = typed_quota_available(quota, variant.vendor)
        if finite_quota is not None:
            finite_quota += quota_credit
        available_quota = (
            request.count if finite_quota is None else finite_quota // commitment_divisor
        )
        for index, (key, devices) in enumerate(sorted(groups.items()), start=1):
            model, resource_name = key
            candidate_id = f"{variant.id}:{index}"
            candidate_node_keys = {(device.worker_id, device.node_name) for device in devices}
            compatible_node_keys: set[tuple[str, str]] = set()
            for node_key in candidate_node_keys:
                pool_devices = [
                    device
                    for device in inventory
                    if (device.worker_id, device.node_name) == node_key
                    and device.kubernetes_resource_name == resource_name
                ]
                if pool_devices and all(
                    device.vendor == variant.vendor
                    and device.kind == variant.kind
                    and device.model == model
                    and device.memory_total_mb >= minimum_memory_mb
                    and runtime_profile_binding in device.runtime_profile_ids
                    and request.required_capabilities.issubset(
                        effective_capabilities[device.device_id]
                    )
                    for device in pool_devices
                ):
                    compatible_node_keys.add(node_key)
            workers_by_node_name: dict[str, set[str]] = {}
            for worker_id, node_name in compatible_node_keys:
                workers_by_node_name.setdefault(node_name, set()).add(worker_id)
            ambiguous_node_names = {
                node_name
                for node_name, worker_ids in workers_by_node_name.items()
                if len(worker_ids) != 1
            }
            compatible_node_keys = {
                node_key
                for node_key in compatible_node_keys
                if node_key[1] not in ambiguous_node_names
            }
            compatible_devices = [
                device
                for device in devices
                if (device.worker_id, device.node_name) in compatible_node_keys
            ]
            compatible_counts: dict[tuple[str, str], int] = {}
            for device in compatible_devices:
                node_key = (device.worker_id, device.node_name)
                compatible_counts[node_key] = compatible_counts.get(node_key, 0) + 1
            remaining = _plan_physical_pool_remaining(
                inventory=inventory,
                vendor=variant.vendor,
                model=model,
                resource_name=resource_name,
                service_usage=service_usage,
                deferred_usage=deferred_usage,
            )
            available_by_node: dict[tuple[str, str], int] = {}
            for node_key, compatible_count in compatible_counts.items():
                physical_count = sum(
                    1
                    for device in inventory
                    if (device.worker_id, device.node_name) == node_key
                    and device.vendor == variant.vendor
                    and device.kubernetes_resource_name == resource_name
                )
                used_count = physical_count - (remaining or {}).get(node_key, 0)
                available_by_node[node_key] = max(0, compatible_count - used_count)
            available_replica_slots = sum(
                count // request.count for count in available_by_node.values()
            )
            capabilities = (
                set.intersection(
                    *(
                        set(effective_capabilities[device.device_id])
                        for device in compatible_devices
                    )
                )
                if compatible_devices
                else set(request.required_capabilities)
            )
            eligible_node_names = tuple(
                sorted({node_name for _worker_id, node_name in compatible_node_keys})
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
                available_capacity=(available_replica_slots * request.count) // commitment_divisor,
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
                eligible_node_names=eligible_node_names,
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
    node_owners: Mapping[str, str],
    vendors: frozenset[AcceleratorVendor],
) -> _DeferredAcceleratorUsage:
    if not node_owners or not vendors:
        return _DeferredAcceleratorUsage()
    rows = await session.execute(
        select(
            ResourceReservation.worker_id,
            ResourceReservation.worker_session_id,
            Worker.node_name,
            Worker.worker_session_id,
            ResourceReservation.requested_vendor,
            ResourceReservation.requested_profile_id,
            ResourceReservation.requested_profile_version,
            ResourceReservation.requested_profile_digest,
            ResourceReservation.gpu_count,
            ResourceReservation.observed_device_ids_json,
        )
        .join(Worker, Worker.id == ResourceReservation.worker_id)
        .where(
            ResourceReservation.released_at.is_(None),
            ResourceReservation.allocation_authority
            == AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value,
            ResourceReservation.requested_vendor.is_not(None),
            ResourceReservation.requested_vendor.in_(
                tuple(sorted(vendor.value for vendor in vendors))
            ),
        )
        .order_by(
            ResourceReservation.worker_id,
            ResourceReservation.id,
            ResourceReservation.requested_vendor,
        )
    )
    totals = [
        _DeferredReservationTotal(
            worker_id=worker_id,
            node_name=node_name,
            worker_session_matches=(reservation_worker_session_id == current_worker_session_id),
            vendor=vendor,
            profile_id=profile_id,
            profile_version=profile_version,
            profile_digest=profile_digest,
            accelerator_count=int(accelerator_count),
            observed_device_ids=(
                tuple(observed_device_ids)
                if isinstance(observed_device_ids, list)
                and all(isinstance(value, str) for value in observed_device_ids)
                else None
            ),
        )
        for (
            worker_id,
            reservation_worker_session_id,
            node_name,
            current_worker_session_id,
            vendor,
            profile_id,
            profile_version,
            profile_digest,
            accelerator_count,
            observed_device_ids,
        ) in rows
        if vendor is not None
    ]
    return _classify_deferred_accelerators(
        totals,
        catalog=catalog,
        node_owners=node_owners,
    )


def _classify_deferred_accelerators(
    totals: Sequence[_DeferredReservationTotal],
    *,
    catalog: RuntimeProfileCatalog,
    node_owners: Mapping[str, str] | None = None,
) -> _DeferredAcceleratorUsage:
    by_node_resource: dict[tuple[str, str, str, str], int] = {}
    unknown_by_worker_resource: dict[tuple[str, str, str], int] = {}
    unknown_by_worker_vendor: dict[tuple[str, str], int] = {}
    unknown_by_vendor_resource: dict[tuple[str, str], int] = {}
    unknown_by_vendor: dict[str, int] = {}
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
        stable_node = total.worker_session_matches and _is_valid_kubernetes_node_name(
            total.node_name
        )
        if node_owners is None:
            current_owner = total.worker_id if stable_node else None
        else:
            current_owner = node_owners.get(total.node_name or "") if stable_node else None
            if stable_node and current_owner is None:
                # The reservation belongs to a different physical Kubernetes node.
                continue
        if resource_name is None:
            if node_owners is not None:
                if not stable_node or current_owner is None:
                    unknown_by_vendor[total.vendor] = (
                        unknown_by_vendor.get(total.vendor, 0) + total.accelerator_count
                    )
                    continue
                fallback_key = (current_owner, total.vendor)
            else:
                fallback_key = (total.worker_id, total.vendor)
            unknown_by_worker_vendor[fallback_key] = (
                unknown_by_worker_vendor.get(fallback_key, 0) + total.accelerator_count
            )
            continue
        if stable_node and current_owner is not None:
            assert total.node_name is not None
            resource_key = (
                current_owner,
                total.node_name,
                total.vendor,
                resource_name,
            )
            by_node_resource[resource_key] = (
                by_node_resource.get(resource_key, 0) + total.accelerator_count
            )
            continue
        if node_owners is not None:
            global_resource_key = (total.vendor, resource_name)
            unknown_by_vendor_resource[global_resource_key] = (
                unknown_by_vendor_resource.get(global_resource_key, 0) + total.accelerator_count
            )
            continue
        fallback_resource_key = (total.worker_id, total.vendor, resource_name)
        unknown_by_worker_resource[fallback_resource_key] = (
            unknown_by_worker_resource.get(fallback_resource_key, 0) + total.accelerator_count
        )
    return _DeferredAcceleratorUsage(
        by_node_resource=by_node_resource,
        unknown_by_worker_resource=unknown_by_worker_resource,
        unknown_by_worker_vendor=unknown_by_worker_vendor,
        unknown_by_vendor_resource=unknown_by_vendor_resource,
        unknown_by_vendor=unknown_by_vendor,
    )


def _unique_inventory_node_owners(
    inventory: Sequence[InventoryDeviceSnapshot],
) -> dict[str, str]:
    workers_by_node: dict[str, set[str]] = {}
    for device in inventory:
        workers_by_node.setdefault(device.node_name, set()).add(device.worker_id)
    return {
        node_name: next(iter(worker_ids))
        for node_name, worker_ids in workers_by_node.items()
        if len(worker_ids) == 1
    }


def _is_valid_kubernetes_node_name(value: str | None) -> bool:
    if value is None:
        return False
    try:
        validate_kubernetes_dns_subdomain(value, field_name="worker node_name")
    except (TypeError, ValueError):
        return False
    return True


async def _active_service_accelerators(
    session: AsyncSession,
    *,
    exclude_service_id: uuid.UUID | None = None,
) -> _ServiceAcceleratorUsage:
    commitments: list[_ServiceReplicaCommitment] = []
    current_materialized: dict[tuple[uuid.UUID, int], int] = {}
    replica_query = (
        select(
            ServiceReplica.service_id,
            ServiceReplica.generation,
            ServiceReplica.ordinal,
            ServiceReplica.selected_vendor,
            ServiceReplica.selected_model,
            ServiceReplica.accelerator_resource_name,
            ServiceReplica.eligible_node_names,
            ServiceReplica.assigned_node_name,
            ServiceReplica.status,
            ModelService.generation,
            ModelService.gpu_count,
            ModelService.tensor_parallel_size,
        )
        .join(ModelService, ModelService.id == ServiceReplica.service_id)
        .where(
            ModelService.runtime_type == RuntimeType.KUBERNETES,
            or_(ModelService.gpu_count > 0, ModelService.selected_vendor.is_not(None)),
            ServiceReplica.status.in_(
                (
                    ReplicaStatus.PENDING,
                    ReplicaStatus.STARTING,
                    ReplicaStatus.LOADING,
                    ReplicaStatus.RUNNING,
                    ReplicaStatus.DRAINING,
                    ReplicaStatus.STOPPING,
                )
            ),
        )
    )
    if exclude_service_id is not None:
        replica_query = replica_query.where(
            or_(
                ServiceReplica.service_id != exclude_service_id,
                ServiceReplica.generation != ModelService.generation,
                ServiceReplica.status.in_((ReplicaStatus.DRAINING, ReplicaStatus.STOPPING)),
            )
        )
    replica_rows = await session.execute(
        replica_query.order_by(
            ServiceReplica.service_id,
            ServiceReplica.generation,
            ServiceReplica.ordinal,
        )
    )
    for (
        service_id,
        replica_generation,
        replica_ordinal,
        vendor,
        model,
        resource_name,
        eligible_node_names,
        assigned_node_name,
        replica_status,
        service_generation,
        gpu_count,
        tensor_parallel_size,
    ) in replica_rows:
        accelerators_per_replica = max(int(gpu_count), int(tensor_parallel_size))
        commitments.append(
            _service_commitment(
                service_id=service_id,
                generation=int(replica_generation),
                replica_ordinal=int(replica_ordinal),
                vendor=vendor,
                model=model,
                resource_name=resource_name,
                accelerator_count=accelerators_per_replica,
                eligible_node_names=eligible_node_names,
                assigned_node_name=assigned_node_name,
            )
        )
        if int(replica_generation) == int(service_generation) and replica_status in {
            ReplicaStatus.PENDING,
            ReplicaStatus.STARTING,
            ReplicaStatus.LOADING,
            ReplicaStatus.RUNNING,
        }:
            current_key = (service_id, int(service_generation))
            current_materialized[current_key] = current_materialized.get(current_key, 0) + 1

    service_query = select(
        ModelService.id,
        ModelService.generation,
        ModelService.selected_vendor,
        ModelService.selected_model,
        ModelService.accelerator_resource_name,
        ModelService.eligible_node_names,
        ModelService.gpu_count,
        ModelService.tensor_parallel_size,
        ModelService.desired_replicas,
    ).where(
        ModelService.runtime_type == RuntimeType.KUBERNETES,
        ModelService.desired_replicas > 0,
        or_(ModelService.gpu_count > 0, ModelService.selected_vendor.is_not(None)),
    )
    if exclude_service_id is not None:
        service_query = service_query.where(ModelService.id != exclude_service_id)
    service_rows = await session.execute(service_query.order_by(ModelService.id))
    for (
        service_id,
        generation,
        vendor,
        model,
        resource_name,
        eligible_node_names,
        gpu_count,
        tensor_parallel_size,
        desired_replicas,
    ) in service_rows:
        materialized = current_materialized.get((service_id, int(generation)), 0)
        missing = max(0, int(desired_replicas) - materialized)
        accelerators_per_replica = max(int(gpu_count), int(tensor_parallel_size))
        for replica_ordinal in range(materialized, materialized + missing):
            commitments.append(
                _service_commitment(
                    service_id=service_id,
                    generation=int(generation),
                    replica_ordinal=replica_ordinal,
                    vendor=vendor,
                    model=model,
                    resource_name=resource_name,
                    accelerator_count=accelerators_per_replica,
                    eligible_node_names=eligible_node_names,
                    assigned_node_name=None,
                )
            )
    return _ServiceAcceleratorUsage(commitments=tuple(commitments))


def _service_commitment(
    *,
    service_id: uuid.UUID,
    generation: int,
    replica_ordinal: int,
    vendor: object,
    model: object,
    resource_name: object,
    accelerator_count: int,
    eligible_node_names: object,
    assigned_node_name: object,
) -> _ServiceReplicaCommitment:
    normalized_nodes = (
        tuple(eligible_node_names)
        if isinstance(eligible_node_names, list)
        and eligible_node_names
        and all(isinstance(value, str) for value in eligible_node_names)
        and tuple(eligible_node_names) == tuple(sorted(set(eligible_node_names)))
        else None
    )

    def normalized(value: object) -> str | None:
        if isinstance(value, AcceleratorVendor):
            return value.value
        return value.strip() if isinstance(value, str) and value.strip() else None

    return _ServiceReplicaCommitment(
        service_id=service_id,
        generation=generation,
        replica_ordinal=replica_ordinal,
        vendor=normalized(vendor),
        model=normalized(model),
        resource_name=normalized(resource_name),
        accelerator_count=accelerator_count,
        eligible_node_names=normalized_nodes,
        assigned_node_name=normalized(assigned_node_name),
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
