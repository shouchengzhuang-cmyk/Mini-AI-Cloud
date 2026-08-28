from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import urlsplit

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.pagination import CursorKey
from core.enums import AcceleratorVendor, RuntimeType
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServiceStatus,
    ServingRuntime,
)
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.outbox import OutboxRepository
from repositories.quotas import QuotaRepository

ACTIVE_REPLICA_STATUSES = frozenset(
    {
        ReplicaStatus.PENDING,
        ReplicaStatus.STARTING,
        ReplicaStatus.LOADING,
        ReplicaStatus.RUNNING,
    }
)
NONTERMINAL_REPLICA_STATUSES = ACTIVE_REPLICA_STATUSES | {
    ReplicaStatus.DRAINING,
    ReplicaStatus.STOPPING,
}


@dataclass(frozen=True, slots=True)
class ServiceCounts:
    actual_replicas: int = 0
    healthy_replicas: int = 0


@dataclass(frozen=True, slots=True)
class EndpointSelection:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    endpoint_url: str


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    services_seen: int = 0
    replicas_created: int = 0
    replicas_stopping: int = 0
    replicas_stopped: int = 0
    services_updated: int = 0

    def __add__(self, other: ReconcileResult) -> ReconcileResult:
        return ReconcileResult(
            services_seen=self.services_seen + other.services_seen,
            replicas_created=self.replicas_created + other.replicas_created,
            replicas_stopping=self.replicas_stopping + other.replicas_stopping,
            replicas_stopped=self.replicas_stopped + other.replicas_stopped,
            services_updated=self.services_updated + other.services_updated,
        )


@dataclass(frozen=True, slots=True)
class LeaseRecoveryResult:
    services_seen: int = 0
    replicas_lost: int = 0
    replicas_stopped: int = 0


class ServiceRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        model: str,
        runtime: ServingRuntime,
        runtime_type: RuntimeType,
        image: str | None,
        cpu_millicores: int,
        memory_mb: int,
        gpu_count: int,
        gpu_memory_mb: int,
        desired_replicas: int,
        registered_model_id: uuid.UUID | None = None,
        model_revision: str | None = None,
        gpu_model: str | None = None,
        tensor_parallel_size: int | None = None,
        dtype: str = "auto",
        gpu_memory_utilization: float = 0.9,
        max_model_len: int | None = None,
        autoscaling_enabled: bool = False,
        autoscaling_min_replicas: int = 1,
        autoscaling_max_replicas: int = 4,
        autoscaling_target_concurrency: int = 8,
        autoscaling_cooldown_seconds: int = 60,
        service_id: uuid.UUID | None = None,
        logical_model_id: uuid.UUID | None = None,
        model_variant_id: uuid.UUID | None = None,
        selected_vendor: AcceleratorVendor | str | None = None,
        selected_kind: str | None = None,
        selected_model: str | None = None,
        runtime_profile_id: str | None = None,
        runtime_profile_version: str | None = None,
        runtime_profile_digest: str | None = None,
        allocation_authority: str | None = None,
        accelerator_resource_name: str | None = None,
        selection_policy: str | None = None,
    ) -> ModelService:
        vendor_value = (
            selected_vendor.value
            if isinstance(selected_vendor, AcceleratorVendor)
            else selected_vendor
        )
        await QuotaRepository.replace_service_commitment(
            session,
            project_id=project_id,
            current_replicas=0,
            desired_replicas=desired_replicas,
            cpu_millicores=cpu_millicores,
            memory_mb=memory_mb,
            gpu_count=gpu_count,
            accelerator_vendor=vendor_value,
        )
        now = await database_utcnow(session)
        service = ModelService(
            id=service_id or uuid.uuid4(),
            project_id=project_id,
            registered_model_id=registered_model_id,
            name=name,
            model=model,
            model_revision=model_revision,
            runtime=runtime,
            runtime_type=runtime_type,
            image=image,
            cpu_millicores=cpu_millicores,
            memory_mb=memory_mb,
            gpu_count=gpu_count,
            gpu_memory_mb=gpu_memory_mb,
            gpu_model=gpu_model,
            logical_model_id=logical_model_id,
            model_variant_id=model_variant_id,
            selected_vendor=vendor_value,
            selected_kind=selected_kind,
            selected_model=selected_model,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version=runtime_profile_version,
            runtime_profile_digest=runtime_profile_digest,
            allocation_authority=allocation_authority,
            accelerator_resource_name=accelerator_resource_name,
            selection_policy=selection_policy,
            tensor_parallel_size=(
                tensor_parallel_size if tensor_parallel_size is not None else max(1, gpu_count)
            ),
            dtype=dtype,
            gpu_memory_utilization=gpu_memory_utilization,
            max_model_len=max_model_len,
            desired_replicas=desired_replicas,
            autoscaling_enabled=autoscaling_enabled,
            autoscaling_min_replicas=autoscaling_min_replicas,
            autoscaling_max_replicas=autoscaling_max_replicas,
            autoscaling_target_concurrency=autoscaling_target_concurrency,
            autoscaling_cooldown_seconds=autoscaling_cooldown_seconds,
            generation=1,
            status=ServiceStatus.PENDING,
            round_robin_cursor=0,
            scheduling_details={},
            created_at=now,
            updated_at=now,
        )
        session.add(service)
        await session.flush()
        _add_service_event(
            session,
            aggregate_id=service.id,
            aggregate_type="service",
            event_type="service.reconcile",
            payload={
                "service_id": str(service.id),
                "project_id": str(project_id),
                "generation": service.generation,
                "reason": "created",
            },
            available_at=now,
        )
        return service

    @staticmethod
    async def get(
        session: AsyncSession,
        service_id: uuid.UUID,
        *,
        project_id: uuid.UUID | None = None,
        for_update: bool = False,
    ) -> ModelService | None:
        query = select(ModelService).where(ModelService.id == service_id)
        if project_id is not None:
            query = query.where(ModelService.project_id == project_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_by_name(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        for_update: bool = False,
    ) -> ModelService | None:
        query = select(ModelService).where(
            ModelService.project_id == project_id,
            ModelService.name == name,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list_services(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        status: ServiceStatus | None,
        limit: int,
        offset: int,
        after: CursorKey | None = None,
    ) -> list[ModelService]:
        query = select(ModelService).where(ModelService.project_id == project_id)
        if status is not None:
            query = query.where(ModelService.status == status)
        if after is not None:
            query = query.where(
                or_(
                    ModelService.created_at < after.created_at,
                    (ModelService.created_at == after.created_at)
                    & (ModelService.id < after.item_id),
                )
            )
        return list(
            await session.scalars(
                query.order_by(ModelService.created_at.desc(), ModelService.id.desc())
                .limit(limit)
                .offset(0 if after is not None else offset)
            )
        )

    @staticmethod
    async def count_services(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        status: ServiceStatus | None,
    ) -> int:
        query = select(func.count(ModelService.id)).where(ModelService.project_id == project_id)
        if status is not None:
            query = query.where(ModelService.status == status)
        return int(await session.scalar(query) or 0)

    @staticmethod
    async def list_replicas(
        session: AsyncSession,
        service_id: uuid.UUID,
        *,
        generation: int | None = None,
        for_update: bool = False,
    ) -> list[ServiceReplica]:
        query = (
            select(ServiceReplica)
            .where(ServiceReplica.service_id == service_id)
            .order_by(ServiceReplica.generation, ServiceReplica.ordinal, ServiceReplica.id)
        )
        if generation is not None:
            query = query.where(ServiceReplica.generation == generation)
        if for_update:
            query = query.with_for_update()
        return list(await session.scalars(query))

    @staticmethod
    async def counts_for_service_ids(
        session: AsyncSession, service_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, ServiceCounts]:
        if not service_ids:
            return {}
        rows = await session.execute(
            select(
                ServiceReplica.service_id,
                ServiceReplica.status,
                ServiceReplica.health,
                ServiceReplica.endpoint_url,
            )
            .join(ModelService, ModelService.id == ServiceReplica.service_id)
            .where(
                ServiceReplica.service_id.in_(service_ids),
                ServiceReplica.generation == ModelService.generation,
            )
        )
        mutable = {service_id: [0, 0] for service_id in service_ids}
        for service_id, status, health, endpoint_url in rows:
            if status != ReplicaStatus.RUNNING:
                continue
            mutable[service_id][0] += 1
            if health == ReplicaHealth.HEALTHY and endpoint_url:
                mutable[service_id][1] += 1
        return {
            service_id: ServiceCounts(actual_replicas=values[0], healthy_replicas=values[1])
            for service_id, values in mutable.items()
        }

    @staticmethod
    async def set_desired_replicas(
        session: AsyncSession,
        *,
        service_id: uuid.UUID,
        project_id: uuid.UUID,
        desired_replicas: int,
    ) -> ModelService | None:
        service = await ServiceRepository.get(
            session, service_id, project_id=project_id, for_update=True
        )
        if service is None:
            return None
        if service.desired_replicas == desired_replicas:
            return service

        await QuotaRepository.replace_service_commitment(
            session,
            project_id=project_id,
            current_replicas=service.desired_replicas,
            desired_replicas=desired_replicas,
            cpu_millicores=service.cpu_millicores,
            memory_mb=service.memory_mb,
            gpu_count=service.gpu_count,
            accelerator_vendor=service.selected_vendor,
        )
        now = await database_utcnow(session)
        service.desired_replicas = desired_replicas
        service.status = ServiceStatus.STOPPING if desired_replicas == 0 else ServiceStatus.PENDING
        if desired_replicas == 0:
            service.scheduling_reason = None
            service.scheduling_details = {}
        service.stopped_at = None
        service.updated_at = now
        service.version += 1
        _add_service_event(
            session,
            aggregate_id=service.id,
            aggregate_type="service",
            event_type="service.reconcile",
            payload={
                "service_id": str(service.id),
                "project_id": str(project_id),
                "generation": service.generation,
                "reason": "scaled",
                "desired_replicas": desired_replicas,
            },
            available_at=now,
        )
        return service

    @staticmethod
    def reconcile_candidates_query(limit: int) -> Select[tuple[ModelService]]:
        active_count = (
            select(func.count(ServiceReplica.id))
            .where(
                ServiceReplica.service_id == ModelService.id,
                ServiceReplica.generation == ModelService.generation,
                ServiceReplica.status.in_(ACTIVE_REPLICA_STATUSES),
            )
            .correlate(ModelService)
            .scalar_subquery()
        )
        superseded_active_exists = (
            select(ServiceReplica.id)
            .where(
                ServiceReplica.service_id == ModelService.id,
                ServiceReplica.generation != ModelService.generation,
                ServiceReplica.status.in_(ACTIVE_REPLICA_STATUSES),
            )
            .exists()
        )
        unhealthy_current_exists = (
            select(ServiceReplica.id)
            .where(
                ServiceReplica.service_id == ModelService.id,
                ServiceReplica.generation == ModelService.generation,
                ServiceReplica.status == ReplicaStatus.RUNNING,
                ServiceReplica.health == ReplicaHealth.UNHEALTHY,
            )
            .exists()
        )
        return (
            select(ModelService)
            .where(
                or_(
                    ModelService.status == ServiceStatus.PENDING,
                    ModelService.desired_replicas != active_count,
                    superseded_active_exists,
                    unhealthy_current_exists,
                )
            )
            .order_by(ModelService.updated_at, ModelService.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    @staticmethod
    async def reconcile_batch(
        session: AsyncSession,
        *,
        limit: int,
        drain_timeout_seconds: float = 30.0,
        kubernetes_drain_timeout_seconds: float | None = None,
    ) -> ReconcileResult:
        if drain_timeout_seconds < 0:
            raise ValueError("drain_timeout_seconds must not be negative")
        if kubernetes_drain_timeout_seconds is not None and kubernetes_drain_timeout_seconds < 0:
            raise ValueError("kubernetes_drain_timeout_seconds must not be negative")
        services = list(await session.scalars(ServiceRepository.reconcile_candidates_query(limit)))
        if not services:
            return ReconcileResult()
        now = await database_utcnow(session)
        result = ReconcileResult()
        for service in services:
            resolved_drain_timeout = (
                kubernetes_drain_timeout_seconds
                if service.runtime_type == RuntimeType.KUBERNETES
                and kubernetes_drain_timeout_seconds is not None
                else drain_timeout_seconds
            )
            result += await ServiceRepository.reconcile_locked(
                session,
                service,
                now=now,
                drain_timeout_seconds=resolved_drain_timeout,
            )
        return result

    @staticmethod
    def expired_lease_services_query(limit: int, *, now: datetime) -> Select[tuple[ModelService]]:
        expired_replica_exists = (
            select(ServiceReplica.id)
            .where(
                ServiceReplica.service_id == ModelService.id,
                ServiceReplica.status.in_(
                    {
                        ReplicaStatus.STARTING,
                        ReplicaStatus.LOADING,
                        ReplicaStatus.RUNNING,
                        ReplicaStatus.DRAINING,
                        ReplicaStatus.STOPPING,
                    }
                ),
                ServiceReplica.lease_expires_at.is_not(None),
                ServiceReplica.lease_expires_at < now,
            )
            .exists()
        )
        return (
            select(ModelService)
            .where(expired_replica_exists)
            .order_by(ModelService.updated_at, ModelService.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    @staticmethod
    async def recover_expired_leases(session: AsyncSession, *, limit: int) -> LeaseRecoveryResult:
        """Fence expired service executions and make their desired state reconcilable.

        The service row is locked before replica rows, matching the normal
        reconciliation lock order. A stopping execution is considered stopped;
        a starting or running execution is considered lost and will be replaced
        by the following reconciliation pass when the service still desires it.
        """

        now = await database_utcnow(session)
        services = list(
            await session.scalars(ServiceRepository.expired_lease_services_query(limit, now=now))
        )
        lost = 0
        stopped = 0
        for service in services:
            replicas = list(
                await session.scalars(
                    select(ServiceReplica)
                    .where(
                        ServiceReplica.service_id == service.id,
                        ServiceReplica.status.in_(
                            {
                                ReplicaStatus.STARTING,
                                ReplicaStatus.LOADING,
                                ReplicaStatus.RUNNING,
                                ReplicaStatus.DRAINING,
                                ReplicaStatus.STOPPING,
                            }
                        ),
                        ServiceReplica.lease_expires_at.is_not(None),
                        ServiceReplica.lease_expires_at < now,
                    )
                    .order_by(ServiceReplica.lease_expires_at, ServiceReplica.id)
                    .with_for_update()
                )
            )
            for replica in replicas:
                status = (
                    ReplicaStatus.STOPPED
                    if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                    else ReplicaStatus.LOST
                )
                replica.status = status
                replica.health = ReplicaHealth.UNHEALTHY
                replica.endpoint_url = None
                replica.lease_expires_at = None
                replica.health_probe_token = None
                replica.health_probe_expires_at = None
                replica.active_requests = 0
                replica.error_code = "LEASE_EXPIRED"
                replica.error_message = "service replica lease expired"
                replica.stopped_at = now
                replica.updated_at = now
                replica.version += 1
                lost += int(status == ReplicaStatus.LOST)
                stopped += int(status == ReplicaStatus.STOPPED)
                _add_service_event(
                    session,
                    aggregate_id=replica.id,
                    aggregate_type="service_replica",
                    event_type="service.replica.lease_expired",
                    payload={
                        "service_id": str(service.id),
                        "project_id": str(service.project_id),
                        "replica_id": str(replica.id),
                        "generation": replica.generation,
                        "execution_id": (
                            str(replica.execution_id) if replica.execution_id else None
                        ),
                        "status": status.value,
                    },
                    available_at=now,
                )
            if replicas:
                await _refresh_service_status(session, service, now)
        return LeaseRecoveryResult(
            services_seen=len(services),
            replicas_lost=lost,
            replicas_stopped=stopped,
        )

    @staticmethod
    async def reconcile_locked(
        session: AsyncSession,
        service: ModelService,
        *,
        now: datetime | None = None,
        drain_timeout_seconds: float = 30.0,
    ) -> ReconcileResult:
        if drain_timeout_seconds < 0:
            raise ValueError("drain_timeout_seconds must not be negative")
        changed_at = now or await database_utcnow(session)
        replicas = await ServiceRepository.list_replicas(session, service.id, for_update=True)
        created = 0
        stopping = 0
        stopped = 0

        for replica in replicas:
            if replica.generation == service.generation:
                continue
            if replica.status in NONTERMINAL_REPLICA_STATUSES:
                requested, immediate = _request_replica_stop(
                    session,
                    service,
                    replica,
                    changed_at,
                    reason="generation superseded",
                    drain_timeout_seconds=drain_timeout_seconds,
                )
                stopping += requested
                stopped += immediate

        current = [item for item in replicas if item.generation == service.generation]
        for replica in current:
            if replica.status != ReplicaStatus.RUNNING or replica.health != ReplicaHealth.UNHEALTHY:
                continue
            requested, immediate = _request_replica_stop(
                session,
                service,
                replica,
                changed_at,
                reason="replica health threshold exceeded",
                drain_timeout_seconds=drain_timeout_seconds,
            )
            stopping += requested
            stopped += immediate

        active = [item for item in current if item.status in ACTIVE_REPLICA_STATUSES]
        excess = len(active) - service.desired_replicas
        if excess > 0:
            for replica in sorted(active, key=_scale_down_order)[:excess]:
                requested, immediate = _request_replica_stop(
                    session,
                    service,
                    replica,
                    changed_at,
                    reason="replica count reduced",
                    drain_timeout_seconds=drain_timeout_seconds,
                )
                stopping += requested
                stopped += immediate

        active = [item for item in current if item.status in ACTIVE_REPLICA_STATUSES]
        missing = service.desired_replicas - len(active)
        next_ordinal = max((item.ordinal for item in current), default=-1) + 1
        for ordinal in range(next_ordinal, next_ordinal + max(0, missing)):
            replica = ServiceReplica(
                service_id=service.id,
                runtime=service.runtime,
                generation=service.generation,
                ordinal=ordinal,
                status=ReplicaStatus.PENDING,
                health=ReplicaHealth.UNKNOWN,
                active_requests=0,
                logical_model_id=service.logical_model_id,
                model_variant_id=service.model_variant_id,
                selected_vendor=service.selected_vendor,
                selected_kind=service.selected_kind,
                selected_model=service.selected_model,
                runtime_profile_id=service.runtime_profile_id,
                runtime_profile_version=service.runtime_profile_version,
                runtime_profile_digest=service.runtime_profile_digest,
                allocation_authority=service.allocation_authority,
                accelerator_resource_name=service.accelerator_resource_name,
                selection_policy=service.selection_policy,
                model_revision=service.model_revision,
                image_digest=_image_digest(service.image),
                created_at=changed_at,
                updated_at=changed_at,
            )
            session.add(replica)
            await session.flush()
            current.append(replica)
            created += 1
            _add_service_event(
                session,
                aggregate_id=replica.id,
                aggregate_type="service_replica",
                event_type="service.replica.created",
                payload={
                    "service_id": str(service.id),
                    "project_id": str(service.project_id),
                    "replica_id": str(replica.id),
                    "generation": replica.generation,
                    "ordinal": replica.ordinal,
                },
                available_at=changed_at,
            )

        derived = _derive_service_status(service, current)
        service_changed = service.status != derived or created > 0 or stopping > 0 or stopped > 0
        if service_changed:
            service.status = derived
            service.updated_at = changed_at
            service.version += 1
            service.stopped_at = changed_at if derived == ServiceStatus.STOPPED else None
        return ReconcileResult(
            services_seen=1,
            replicas_created=created,
            replicas_stopping=stopping,
            replicas_stopped=stopped,
            services_updated=int(service_changed),
        )

    @staticmethod
    async def bind_replica_execution(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        worker_id: str,
        worker_session_id: uuid.UUID | None = None,
        execution_id: uuid.UUID,
        lease_expires_at: datetime,
    ) -> bool:
        if not await _worker_session_matches(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        ):
            return False
        service, replica = await _lock_service_and_replica(session, replica_id)
        if replica is None or replica.generation != generation:
            return False
        if (
            service is None
            or service.generation != generation
            or service.desired_replicas == 0
            or replica.status not in {ReplicaStatus.PENDING, ReplicaStatus.STARTING}
            or (replica.execution_id is not None and replica.execution_id != execution_id)
            or (replica.worker_id is not None and replica.worker_id != worker_id)
        ):
            return False
        first_claim = replica.execution_id is None
        now = await database_utcnow(session)
        if lease_expires_at.tzinfo is None:
            raise ValueError("lease_expires_at must include a timezone")
        if lease_expires_at <= now:
            return False
        replica.worker_id = worker_id
        replica.execution_id = execution_id
        replica.lease_expires_at = lease_expires_at
        replica.status = ReplicaStatus.STARTING
        replica.health = ReplicaHealth.UNKNOWN
        replica.active_requests = 0
        replica.container_started_at = None
        replica.ready_at = None
        replica.drain_started_at = None
        replica.drain_deadline = None
        replica.error_code = None
        replica.error_message = None
        if first_claim:
            replica.started_at = now
        replica.updated_at = now
        replica.version += 1
        return True

    @staticmethod
    async def renew_replica_lease(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        lease_expires_at: datetime,
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        if not await _worker_session_matches(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        ):
            return False
        service, replica = await _lock_service_and_replica(session, replica_id)
        if (
            service is None
            or replica is None
            or replica.generation != generation
            or replica.execution_id != execution_id
            or (worker_id is not None and replica.worker_id != worker_id)
        ):
            return False
        draining = replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
        if not draining and (
            service.generation != generation
            or service.desired_replicas == 0
            or replica.status
            not in {ReplicaStatus.STARTING, ReplicaStatus.LOADING, ReplicaStatus.RUNNING}
        ):
            return False
        now = await database_utcnow(session)
        if lease_expires_at.tzinfo is None:
            raise ValueError("lease_expires_at must include a timezone")
        if lease_expires_at <= now:
            return False
        replica.lease_expires_at = lease_expires_at
        replica.updated_at = now
        replica.version += 1
        return True

    @staticmethod
    async def mark_replica_loading(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        endpoint_url: str,
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        """Persist that the runtime started but the model is not ready yet."""

        replica, service = await _owned_current_replica(
            session,
            replica_id=replica_id,
            generation=generation,
            execution_id=execution_id,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        )
        if replica is None or service is None or replica.status != ReplicaStatus.STARTING:
            return False
        _validate_endpoint_url(endpoint_url)
        now = await database_utcnow(session)
        replica.status = ReplicaStatus.LOADING
        replica.endpoint_url = endpoint_url
        replica.container_started_at = now
        if replica.started_at is None:
            replica.started_at = now
        replica.error_code = None
        replica.error_message = None
        replica.updated_at = now
        replica.version += 1
        await _refresh_service_status(session, service, now)
        return True

    @staticmethod
    async def mark_replica_running(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        endpoint_url: str,
        model_revision: str | None = None,
        image_digest: str | None = None,
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        replica, service = await _owned_current_replica(
            session,
            replica_id=replica_id,
            generation=generation,
            execution_id=execution_id,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        )
        if (
            replica is None
            or service is None
            or replica.status not in {ReplicaStatus.STARTING, ReplicaStatus.LOADING}
        ):
            return False
        _validate_endpoint_url(endpoint_url)
        now = await database_utcnow(session)
        replica.status = ReplicaStatus.RUNNING
        replica.endpoint_url = endpoint_url
        if replica.container_started_at is None:
            replica.container_started_at = now
        if replica.started_at is None:
            replica.started_at = replica.container_started_at
        replica.ready_at = now
        if model_revision is not None:
            replica.model_revision = model_revision
        if image_digest is not None:
            if not image_digest.startswith("sha256:") or len(image_digest) != 71:
                raise ValueError("image_digest must be a sha256 digest")
            replica.image_digest = image_digest
        replica.error_code = None
        replica.error_message = None
        replica.updated_at = now
        replica.version += 1
        await _refresh_service_status(session, service, now)
        return True

    @staticmethod
    async def record_replica_health(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        health: ReplicaHealth,
        error_message: str | None = None,
        failure_threshold: int = 1,
        probe_token: uuid.UUID | None = None,
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        replica, service = await _owned_current_replica(
            session,
            replica_id=replica_id,
            generation=generation,
            execution_id=execution_id,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        )
        if replica is None or service is None or replica.status != ReplicaStatus.RUNNING:
            return False
        if probe_token is not None and replica.health_probe_token != probe_token:
            return False
        if health not in {ReplicaHealth.HEALTHY, ReplicaHealth.UNHEALTHY}:
            raise ValueError("reported replica health must be healthy or unhealthy")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least one")
        now = await database_utcnow(session)
        if probe_token is not None:
            replica.health_probe_token = None
            replica.health_probe_expires_at = None
        replica.last_health_at = now
        if health == ReplicaHealth.HEALTHY:
            replica.health = ReplicaHealth.HEALTHY
            replica.health_failure_count = 0
            replica.error_code = None
        else:
            replica.health_failure_count += 1
            if replica.health_failure_count >= failure_threshold:
                replica.health = ReplicaHealth.UNHEALTHY
                replica.error_code = "REPLICA_UNHEALTHY"
        replica.error_message = error_message
        replica.updated_at = now
        replica.version += 1
        await _refresh_service_status(session, service, now)
        return True

    @staticmethod
    async def mark_replica_terminal(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        status: ReplicaStatus,
        error_code: str | None = None,
        error_message: str | None = None,
        worker_id: str | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> bool:
        if status not in {ReplicaStatus.STOPPED, ReplicaStatus.FAILED, ReplicaStatus.LOST}:
            raise ValueError("terminal replica status must be stopped, failed or lost")
        if not await _worker_session_matches(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        ):
            return False
        service, replica = await _lock_service_and_replica(session, replica_id)
        if (
            replica is None
            or replica.generation != generation
            or replica.execution_id != execution_id
            or replica.status not in NONTERMINAL_REPLICA_STATUSES
        ):
            return False
        if service is None:
            return False
        now = await database_utcnow(session)
        replica.status = status
        replica.health = ReplicaHealth.UNHEALTHY
        replica.endpoint_url = None
        replica.lease_expires_at = None
        replica.health_probe_token = None
        replica.health_probe_expires_at = None
        replica.active_requests = 0
        if error_code is not None or status == ReplicaStatus.STOPPED:
            replica.error_code = error_code
        replica.error_message = error_message
        replica.stopped_at = now
        replica.updated_at = now
        replica.version += 1
        _add_service_event(
            session,
            aggregate_id=replica.id,
            aggregate_type="service_replica",
            event_type="service.replica.terminal",
            payload={
                "service_id": str(service.id),
                "replica_id": str(replica.id),
                "generation": replica.generation,
                "execution_id": str(execution_id),
                "status": status.value,
            },
            available_at=now,
        )
        await _refresh_service_status(session, service, now)
        return True

    @staticmethod
    async def choose_healthy_endpoint(
        session: AsyncSession,
        *,
        service_id: uuid.UUID,
        project_id: uuid.UUID,
    ) -> EndpointSelection | None:
        service = await ServiceRepository.get(
            session, service_id, project_id=project_id, for_update=True
        )
        if service is None or service.desired_replicas == 0:
            return None
        replicas = list(
            await session.scalars(
                select(ServiceReplica)
                .where(
                    ServiceReplica.service_id == service.id,
                    ServiceReplica.generation == service.generation,
                    ServiceReplica.status == ReplicaStatus.RUNNING,
                    ServiceReplica.health == ReplicaHealth.HEALTHY,
                    ServiceReplica.endpoint_url.is_not(None),
                    ServiceReplica.endpoint_url != "",
                    ServiceReplica.execution_id.is_not(None),
                )
                .order_by(ServiceReplica.ordinal, ServiceReplica.id)
            )
        )
        if not replicas:
            return None
        replica = replicas[service.round_robin_cursor % len(replicas)]
        assert replica.execution_id is not None
        assert replica.endpoint_url is not None
        now = await database_utcnow(session)
        replica.active_requests += 1
        replica.updated_at = now
        replica.version += 1
        service.round_robin_cursor += 1
        service.updated_at = now
        service.version += 1
        return EndpointSelection(
            service_id=service.id,
            replica_id=replica.id,
            generation=replica.generation,
            execution_id=replica.execution_id,
            endpoint_url=replica.endpoint_url,
        )

    @staticmethod
    async def release_endpoint_request(
        session: AsyncSession,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
    ) -> bool:
        """Release one request acquired by ``choose_healthy_endpoint``.

        A release remains valid after scale-down or generation replacement while
        that execution still tracks an active request. Terminal recovery clears
        leaked counters, after which a late release is safely rejected. The
        execution fence prevents it from mutating a newer replica owner.
        """

        _service, replica = await _lock_service_and_replica(session, replica_id)
        if (
            replica is None
            or replica.generation != generation
            or replica.execution_id != execution_id
            or replica.active_requests <= 0
        ):
            return False
        now = await database_utcnow(session)
        replica.active_requests -= 1
        replica.updated_at = now
        replica.version += 1
        return True


def _scale_down_order(replica: ServiceReplica) -> tuple[int, int]:
    if replica.status == ReplicaStatus.PENDING:
        rank = 0
    elif replica.status in {ReplicaStatus.STARTING, ReplicaStatus.LOADING}:
        rank = 1
    elif replica.health == ReplicaHealth.UNHEALTHY:
        rank = 2
    elif replica.health == ReplicaHealth.UNKNOWN:
        rank = 3
    else:
        rank = 4
    return rank, -replica.ordinal


def _request_replica_stop(
    session: AsyncSession,
    service: ModelService,
    replica: ServiceReplica,
    now: datetime,
    *,
    reason: str,
    drain_timeout_seconds: float,
) -> tuple[int, int]:
    if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}:
        return 0, 0

    replica.error_message = reason
    replica.updated_at = now
    replica.version += 1
    if replica.status == ReplicaStatus.PENDING and replica.execution_id is None:
        replica.status = ReplicaStatus.STOPPED
        replica.stopped_at = now
        _add_service_event(
            session,
            aggregate_id=replica.id,
            aggregate_type="service_replica",
            event_type="service.replica.terminal",
            payload={
                "service_id": str(service.id),
                "replica_id": str(replica.id),
                "generation": replica.generation,
                "status": ReplicaStatus.STOPPED.value,
                "reason": reason,
            },
            available_at=now,
        )
        return 0, 1

    if replica.status == ReplicaStatus.RUNNING:
        replica.status = ReplicaStatus.DRAINING
        replica.drain_started_at = now
        replica.drain_deadline = now + timedelta(seconds=drain_timeout_seconds)
        _add_service_event(
            session,
            aggregate_id=replica.id,
            aggregate_type="service_replica",
            event_type="service.replica.drain_requested",
            payload={
                "service_id": str(service.id),
                "project_id": str(service.project_id),
                "replica_id": str(replica.id),
                "generation": replica.generation,
                "execution_id": str(replica.execution_id) if replica.execution_id else None,
                "active_requests": replica.active_requests,
                "drain_deadline": replica.drain_deadline.isoformat(),
                "reason": reason,
            },
            available_at=now,
        )
        return 1, 0

    replica.health = ReplicaHealth.UNHEALTHY
    replica.endpoint_url = None
    replica.status = ReplicaStatus.STOPPING
    _add_service_event(
        session,
        aggregate_id=replica.id,
        aggregate_type="service_replica",
        event_type="service.replica.stop_requested",
        payload={
            "service_id": str(service.id),
            "project_id": str(service.project_id),
            "replica_id": str(replica.id),
            "generation": replica.generation,
            "execution_id": str(replica.execution_id) if replica.execution_id else None,
            "reason": reason,
        },
        available_at=now,
    )
    return 1, 0


def _derive_service_status(service: ModelService, replicas: list[ServiceReplica]) -> ServiceStatus:
    current = [item for item in replicas if item.generation == service.generation]
    active = [item for item in current if item.status in ACTIVE_REPLICA_STATUSES]
    stopping = any(
        item.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING} for item in replicas
    )
    running = [item for item in current if item.status == ReplicaStatus.RUNNING]
    healthy = [
        item for item in running if item.health == ReplicaHealth.HEALTHY and item.endpoint_url
    ]
    if service.desired_replicas == 0:
        return ServiceStatus.STOPPING if active or stopping else ServiceStatus.STOPPED
    if len(healthy) >= service.desired_replicas and len(running) >= service.desired_replicas:
        return ServiceStatus.RUNNING
    if healthy:
        return ServiceStatus.DEGRADED
    if any(item.health == ReplicaHealth.UNHEALTHY for item in running):
        return ServiceStatus.DEGRADED
    if active or stopping:
        return ServiceStatus.DEPLOYING
    return ServiceStatus.FAILED


async def _owned_current_replica(
    session: AsyncSession,
    *,
    replica_id: uuid.UUID,
    generation: int,
    execution_id: uuid.UUID,
    worker_id: str | None = None,
    worker_session_id: uuid.UUID | None = None,
) -> tuple[ServiceReplica | None, ModelService | None]:
    if not await _worker_session_matches(
        session,
        worker_id=worker_id,
        worker_session_id=worker_session_id,
    ):
        return None, None
    service, replica = await _lock_service_and_replica(session, replica_id)
    if replica is None or replica.generation != generation or replica.execution_id != execution_id:
        return None, None
    if worker_id is not None and replica.worker_id != worker_id:
        return None, None
    if service is None or service.generation != generation or service.desired_replicas == 0:
        return None, None
    return replica, service


async def _worker_session_matches(
    session: AsyncSession,
    *,
    worker_id: str | None,
    worker_session_id: uuid.UUID | None,
) -> bool:
    if worker_session_id is None:
        return True
    if worker_id is None:
        raise ValueError("worker_id is required when worker_session_id is provided")
    worker = await session.get(Worker, worker_id, with_for_update=True)
    return worker is not None and worker.worker_session_id == worker_session_id


async def _lock_service_and_replica(
    session: AsyncSession, replica_id: uuid.UUID
) -> tuple[ModelService | None, ServiceReplica | None]:
    """Lock a replica owner in the same order used by reconciliation.

    The initial lookup is only a pointer read. ``service_id`` is immutable, and
    all ownership fields are re-read after both rows are locked.
    """

    service_id = await session.scalar(
        select(ServiceReplica.service_id).where(ServiceReplica.id == replica_id)
    )
    if service_id is None:
        return None, None
    service = await session.get(ModelService, service_id, with_for_update=True)
    replica = await session.get(ServiceReplica, replica_id, with_for_update=True)
    if replica is None or replica.service_id != service_id:
        return service, None
    return service, replica


def _add_service_event(
    session: AsyncSession,
    *,
    aggregate_id: uuid.UUID,
    aggregate_type: str,
    event_type: str,
    payload: dict[str, object],
    available_at: datetime,
) -> None:
    event = OutboxRepository.add(
        session,
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload,
        available_at=available_at,
    )
    event.aggregate_type = aggregate_type


def _validate_endpoint_url(endpoint_url: str) -> None:
    parsed = urlsplit(endpoint_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or bool(parsed.query)
        or bool(parsed.fragment)
        or parsed.path not in {"", "/"}
    ):
        raise ValueError(
            "endpoint_url must be an absolute HTTP(S) origin without path, credentials, "
            "query or fragment"
        )


def _image_digest(image: str | None) -> str | None:
    if image is None:
        return None
    _name, separator, digest = image.rpartition("@")
    if separator and digest.startswith("sha256:") and len(digest) == 71:
        return digest
    return None


async def _refresh_service_status(
    session: AsyncSession, service: ModelService, now: datetime
) -> None:
    replicas = await ServiceRepository.list_replicas(session, service.id)
    status = _derive_service_status(service, replicas)
    if service.status == status:
        return
    service.status = status
    service.updated_at = now
    service.version += 1
    service.stopped_at = now if status == ServiceStatus.STOPPED else None
