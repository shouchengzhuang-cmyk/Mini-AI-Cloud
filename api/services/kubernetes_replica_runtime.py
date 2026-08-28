from __future__ import annotations

import asyncio
import hashlib
import os
import re
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, func, select

from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
    ErrorCode,
    RuntimeType,
    WorkerStatus,
)
from core.logging import get_logger
from core.metrics import (
    K8S_SERVING_LAUNCH_FAILURES,
    K8S_SERVING_LAUNCHES,
    K8S_SERVING_PODS,
    K8S_SERVING_RECONCILE_DURATION,
    K8S_SERVING_REPLACEMENTS,
)
from core.runtime_profiles import (
    RuntimeProfile,
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
)
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServingRuntime,
)
from models.worker import Worker
from repositories.clock import database_utcnow
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository
from worker.kubernetes_serving_runtime import (
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    PROJECT_ID_LABEL,
    REPLICA_ID_LABEL,
    SERVICE_ID_LABEL,
    WORKER_ID_LABEL,
    KubernetesServingHandle,
    KubernetesServingLaunchSpec,
    KubernetesServingOwnershipIdentity,
    KubernetesServingRecoveryConflict,
    KubernetesServingRuntime,
    KubernetesServingState,
)

_BACKOFF_REASON = "KUBERNETES_SERVING_BACKOFF"
_IMAGE_REQUIRED_REASON = "KUBERNETES_SERVING_IMAGE_REQUIRED"
_RUNTIME_PROFILE_INVALID_REASON = "KUBERNETES_RUNTIME_PROFILE_INVALID"
_MAX_ERROR_MESSAGE_BYTES = 4096
_MAX_BACKOFF_SECONDS = 300.0
_MAX_RECOVERY_CONFLICT_WARNINGS = 20
_KUBERNETES_LABEL_VALUE = re.compile(r"^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
_POD_METRIC_STATES = ("unknown", "not_ready", "ready", "terminating")
_ACTIVE_RUNTIME_STATUSES = frozenset(
    {
        ReplicaStatus.STARTING,
        ReplicaStatus.LOADING,
        ReplicaStatus.RUNNING,
        ReplicaStatus.DRAINING,
        ReplicaStatus.STOPPING,
    }
)
_IMAGE_PULL_REASONS = frozenset(
    {
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RegistryUnavailable",
    }
)
_CONTAINER_START_REASONS = frozenset(
    {
        "CreateContainerConfigError",
        "CreateContainerError",
        "RunContainerError",
    }
)


class StaleKubernetesServingController(RuntimeError):
    """The stable virtual Worker has been claimed by a newer controller session."""


@dataclass(frozen=True, slots=True)
class KubernetesReplicaClaim:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    project_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    image: str
    model: str
    cpu_millicores: int
    memory_mb: int
    claimed_at: datetime
    accelerator_count: int = 0
    tensor_parallel_size: int = 1
    runtime_profile: RuntimeProfile | None = None
    replacement_reason: str | None = None


@dataclass(frozen=True, slots=True)
class KubernetesRuntimeRunResult:
    claimed: int = 0
    launched: int = 0
    ready: int = 0
    stopped: int = 0
    failed: int = 0
    stale: int = 0
    recovered: int = 0
    orphans_cleaned: int = 0
    recovery_conflicts: int = 0
    waiting_backoff: int = 0


@dataclass(slots=True)
class _ManagedReplica:
    claim: KubernetesReplicaClaim
    handle: KubernetesServingHandle
    startup_deadline: datetime
    published: bool = False
    expected_stop: bool = False
    stop_requested_at: datetime | None = None
    metric_state: str = "unknown"


class KubernetesReplicaRuntimeController:
    """Converge fenced profile-backed and explicitly enabled Fake replicas onto Pods.

    The controller uses a stable virtual Worker id and a process-scoped Worker
    session.  PostgreSQL remains the ownership authority; Kubernetes labels are
    only sufficient to recover the concrete Pod/Service handles after restart.
    A stale controller session deliberately drops local handles without deleting
    resources, because a newer session may already have adopted them.
    """

    def __init__(
        self,
        database: Database,
        runtime: KubernetesServingRuntime,
        *,
        app_env: str,
        cluster_id: str,
        image: str | None,
        runtime_profile_catalog: RuntimeProfileCatalog | None = None,
        fake_enabled: bool = False,
        worker_id: str | None = None,
        batch_size: int = 10,
        startup_timeout_seconds: float = 120.0,
        drain_timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 1.0,
        lease_seconds: float = 300.0,
        failure_backoff_seconds: float = 5.0,
        termination_grace_seconds: float = 30.0,
        fake_startup_delay_seconds: float = 0.0,
        fake_chunk_delay_seconds: float = 0.0,
    ) -> None:
        if fake_enabled and app_env not in {"development", "test"}:
            raise ValueError("Kubernetes Fake serving is prohibited outside development and test")
        resolved_cluster_id = cluster_id.strip()
        if not _KUBERNETES_LABEL_VALUE.fullmatch(resolved_cluster_id):
            raise ValueError("cluster_id must be a Kubernetes label value")
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if startup_timeout_seconds <= 0 or poll_interval_seconds <= 0 or lease_seconds <= 0:
            raise ValueError("Kubernetes serving timeouts must be positive")
        if drain_timeout_seconds < 0 or termination_grace_seconds < 0:
            raise ValueError("Kubernetes serving drain timeouts must not be negative")
        if failure_backoff_seconds <= 0:
            raise ValueError("failure_backoff_seconds must be positive")
        if lease_seconds <= max(startup_timeout_seconds, poll_interval_seconds * 2):
            raise ValueError("lease_seconds must exceed startup and polling timeouts")
        if fake_startup_delay_seconds < 0 or fake_chunk_delay_seconds < 0:
            raise ValueError("Fake inference delays must not be negative")
        resolved_worker_id = (
            worker_id.strip() if worker_id is not None else _stable_worker_id(resolved_cluster_id)
        )
        if not _KUBERNETES_LABEL_VALUE.fullmatch(resolved_worker_id):
            raise ValueError("worker_id must be a Kubernetes label value")

        self.database = database
        self.runtime = runtime
        self.cluster_id = resolved_cluster_id
        self.default_image = image.strip() if image is not None and image.strip() else None
        self.runtime_profile_catalog = runtime_profile_catalog
        self.fake_enabled = fake_enabled
        self.worker_id = resolved_worker_id
        self.worker_session_id = uuid.uuid4()
        self.batch_size = batch_size
        self.startup_timeout_seconds = startup_timeout_seconds
        self.drain_timeout_seconds = drain_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self.failure_backoff_seconds = failure_backoff_seconds
        self.termination_grace_seconds = termination_grace_seconds
        self.fake_startup_delay_seconds = fake_startup_delay_seconds
        self.fake_chunk_delay_seconds = fake_chunk_delay_seconds
        self._handles: dict[uuid.UUID, _ManagedReplica] = {}
        self._cycle_lock = asyncio.Lock()
        self._registered = False
        self._recovered = False
        self._admission_ready = False
        self._closing = False
        self._closed = False
        self.logger = get_logger("kubernetes_replica_runtime")

    @property
    def active_pod_count(self) -> int:
        return len(self._handles)

    @property
    def admission_ready(self) -> bool:
        """Whether this controller has a current session and completed recovery."""

        return (
            self._admission_ready
            and self._registered
            and self._recovered
            and not self._closing
            and not self._closed
        )

    @staticmethod
    def claim_candidates_query(
        limit: int,
        *,
        fake_enabled: bool = True,
    ) -> Select[tuple[ModelService]]:
        supported_runtimes = {ServingRuntime.VLLM}
        if fake_enabled:
            supported_runtimes.add(ServingRuntime.FAKE)
        pending_exists = (
            select(ServiceReplica.id)
            .where(
                ServiceReplica.service_id == ModelService.id,
                ServiceReplica.generation == ModelService.generation,
                ServiceReplica.status == ReplicaStatus.PENDING,
            )
            .exists()
        )
        return (
            select(ModelService)
            .where(
                ModelService.runtime.in_(supported_runtimes),
                ModelService.runtime_type == RuntimeType.KUBERNETES,
                ModelService.desired_replicas > 0,
                pending_exists,
            )
            .order_by(ModelService.updated_at, ModelService.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def startup(self) -> KubernetesRuntimeRunResult:
        """Register the current session, adopt resources, and renew leases first."""

        async with self._cycle_lock:
            if self._closed or self._closing:
                raise RuntimeError("Kubernetes replica runtime is closed")
            if self._recovered:
                return KubernetesRuntimeRunResult()
            try:
                await self._register_worker()
                (
                    recovered,
                    orphans_cleaned,
                    recovery_conflicts,
                ) = await self._recover_managed_resources()
                stale = await self._renew_active_leases()
                self._recovered = True
                if not await self._session_is_current():
                    self._handles.clear()
                    raise StaleKubernetesServingController(
                        "Worker session changed during Kubernetes serving startup"
                    )
                self._admission_ready = True
                return KubernetesRuntimeRunResult(
                    recovered=recovered,
                    orphans_cleaned=orphans_cleaned,
                    recovery_conflicts=recovery_conflicts,
                    stale=stale,
                )
            except asyncio.CancelledError:
                self._admission_ready = False
                raise
            except Exception:
                self._admission_ready = False
                raise

    async def run_once(self) -> KubernetesRuntimeRunResult:
        async with self._cycle_lock:
            started_at = time.monotonic()
            if self._closed or self._closing:
                raise RuntimeError("Kubernetes replica runtime is closed")
            try:
                startup_result = KubernetesRuntimeRunResult()
                if not self._recovered:
                    await self._register_worker()
                    (
                        recovered,
                        orphans_cleaned,
                        recovery_conflicts,
                    ) = await self._recover_managed_resources()
                    startup_result = KubernetesRuntimeRunResult(
                        recovered=recovered,
                        orphans_cleaned=orphans_cleaned,
                        recovery_conflicts=recovery_conflicts,
                    )
                    self._recovered = True
                else:
                    await self._heartbeat_worker()

                stopped = await self._stop_requested_replicas()
                ready, failed, inspected_stale = await self._inspect_managed_replicas()
                lease_stale = await self._renew_active_leases()
                claims, waiting_backoff = await self._claim_pending_replicas()
                for claim in claims:
                    if claim.replacement_reason is not None:
                        K8S_SERVING_REPLACEMENTS.labels(reason=claim.replacement_reason).inc()
                outcomes = await asyncio.gather(*(self._launch(claim) for claim in claims))
                for outcome in outcomes:
                    K8S_SERVING_LAUNCHES.labels(outcome=_launch_metric_outcome(outcome)).inc()
                result = KubernetesRuntimeRunResult(
                    claimed=len(claims),
                    launched=sum(outcome in {"launched", "ready"} for outcome in outcomes),
                    ready=ready + sum(outcome == "ready" for outcome in outcomes),
                    stopped=stopped,
                    failed=failed + sum(outcome == "failed" for outcome in outcomes),
                    stale=(
                        startup_result.stale
                        + inspected_stale
                        + lease_stale
                        + sum(outcome == "stale" for outcome in outcomes)
                    ),
                    recovered=startup_result.recovered,
                    orphans_cleaned=startup_result.orphans_cleaned,
                    recovery_conflicts=startup_result.recovery_conflicts,
                    waiting_backoff=waiting_backoff,
                )
                if any(
                    (
                        result.claimed,
                        result.ready,
                        result.stopped,
                        result.failed,
                        result.stale,
                        result.recovered,
                        result.orphans_cleaned,
                        result.recovery_conflicts,
                    )
                ):
                    self.logger.info(
                        "Kubernetes serving runtime cycle completed",
                        worker_id=self.worker_id,
                        claimed=result.claimed,
                        launched=result.launched,
                        ready=result.ready,
                        stopped=result.stopped,
                        failed=result.failed,
                        stale=result.stale,
                        recovered=result.recovered,
                        orphans_cleaned=result.orphans_cleaned,
                        recovery_conflicts=result.recovery_conflicts,
                        waiting_backoff=result.waiting_backoff,
                    )
                if not await self._session_is_current():
                    self._handles.clear()
                    self._admission_ready = False
                    return result
                self._admission_ready = True
                return result
            except asyncio.CancelledError:
                self._admission_ready = False
                raise
            except Exception:
                self._admission_ready = False
                raise
            finally:
                self._publish_pod_metrics()
                K8S_SERVING_RECONCILE_DURATION.observe(max(0.0, time.monotonic() - started_at))

    async def close(self) -> None:
        """Release clients without deleting healthy Pods owned by this cluster."""

        self._admission_ready = False
        self._closing = True
        async with self._cycle_lock:
            if self._closed:
                return
            self._closed = True
            if self._registered:
                async with self.database.session() as session, session.begin():
                    await WorkerRepository.set_status(
                        session,
                        self.worker_id,
                        WorkerStatus.OFFLINE,
                        worker_session_id=self.worker_session_id,
                    )
            self._handles.clear()
            await self.runtime.close()

    def _publish_pod_metrics(self) -> None:
        counts = dict.fromkeys(_POD_METRIC_STATES, 0)
        for item in self._handles.values():
            state = "terminating" if item.expected_stop else item.metric_state
            counts[state] += 1
        for state in _POD_METRIC_STATES:
            K8S_SERVING_PODS.labels(state=state).set(counts[state])

    async def _register_worker(self) -> None:
        runtime_version = await self.runtime.version()
        async with self.database.session() as session, session.begin():
            await WorkerRepository.register(
                session,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
                hostname=socket.gethostname(),
                node_name=self.cluster_id,
                concurrency=self.batch_size,
                cpu_count=max(1, os.cpu_count() or 1),
                memory_total_mb=16,
                docker_version=runtime_version,
                labels={
                    "managed-by": "kubernetes-serving-runtime",
                    "runtime": RuntimeType.KUBERNETES.value,
                    "workload": "model-service",
                    "cluster-id": self.cluster_id,
                },
                runtime_types=[RuntimeType.KUBERNETES.value],
                gpu_count=0,
                gpu_model=None,
                gpu_memory_mb=0,
            )
            await WorkerRepository.drain(
                session,
                self.worker_id,
                reason="dedicated Kubernetes service runtime; batch placement disabled",
            )
        self._registered = True

    async def _heartbeat_worker(self) -> None:
        async with self.database.session() as session, session.begin():
            worker = await WorkerRepository.heartbeat(
                session,
                self.worker_id,
                self.active_pod_count,
                worker_session_id=self.worker_session_id,
            )
        if worker is None:
            self._handles.clear()
            raise StaleKubernetesServingController(
                "Kubernetes serving Worker session is owned by a newer controller"
            )

    async def _session_is_current(self) -> bool:
        async with self.database.session() as session:
            worker = await session.get(Worker, self.worker_id)
            return worker is not None and worker.worker_session_id == self.worker_session_id

    async def _force_cleanup_if_current(self, handle: KubernetesServingHandle) -> bool:
        """Delete resources only while this session owns the locked Worker row."""

        async with self.database.session() as session, session.begin():
            worker = await session.get(Worker, self.worker_id, with_for_update=True)
            if worker is None or worker.worker_session_id != self.worker_session_id:
                return False
            await self.runtime.force_cleanup(handle)
        return True

    async def _request_stop_if_current(self, handle: KubernetesServingHandle) -> bool:
        """Begin graceful deletion while holding the Worker session fence."""

        async with self.database.session() as session, session.begin():
            worker = await session.get(Worker, self.worker_id, with_for_update=True)
            if worker is None or worker.worker_session_id != self.worker_session_id:
                return False
            await self.runtime.request_stop(handle)
        return True

    async def _prepare_and_start_if_current(
        self,
        spec: KubernetesServingLaunchSpec,
    ) -> KubernetesServingHandle | None:
        """Create resources while preventing a newer session from passing registration."""

        async with self.database.session() as session, session.begin():
            worker = await session.get(Worker, self.worker_id, with_for_update=True)
            if worker is None or worker.worker_session_id != self.worker_session_id:
                return None
            handle = await self.runtime.prepare(
                spec,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )
            return await self.runtime.start(handle)

    async def _recover_managed_resources(self) -> tuple[int, int, int]:
        handles = list(await self.runtime.list_managed(worker_id=self.worker_id))
        conflicts = tuple(getattr(self.runtime, "recovery_conflicts", ()))
        self._record_recovery_conflicts(conflicts)
        async with self.database.session() as session, session.begin():
            worker = await session.get(Worker, self.worker_id, with_for_update=True)
            if worker is None or worker.worker_session_id != self.worker_session_id:
                raise StaleKubernetesServingController(
                    "Worker session changed during Kubernetes serving recovery"
                )
            rows = list(
                await session.execute(
                    select(ServiceReplica, ModelService)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ModelService.runtime.in_(
                            {ServingRuntime.VLLM}
                            | ({ServingRuntime.FAKE} if self.fake_enabled else set())
                        ),
                        ModelService.runtime_type == RuntimeType.KUBERNETES,
                        ServiceReplica.worker_id == self.worker_id,
                        ServiceReplica.execution_id.is_not(None),
                        ServiceReplica.status.in_(_ACTIVE_RUNTIME_STATUSES),
                    )
                    .with_for_update()
                )
            )
            for replica, _service in rows:
                if replica.status == ReplicaStatus.STARTING and replica.started_at is None:
                    # Persist the best available claim-time approximation for
                    # pre-upgrade rows before lease renewal advances updated_at.
                    replica.started_at = replica.updated_at
        replicas_by_execution = {
            replica.execution_id: (replica, service)
            for replica, service in rows
            if replica.execution_id is not None
        }
        matched_replica_ids: set[uuid.UUID] = set()
        for conflict in conflicts:
            ownership = conflict.ownership
            if ownership is None:
                continue
            pair = replicas_by_execution.get(ownership.execution_id)
            if pair is not None and _ownership_matches_replica(
                ownership,
                pair[0],
                pair[1].project_id,
                self.worker_id,
                self.cluster_id,
            ):
                # The workload contract is unsafe to adopt, but its complete
                # identity fence proves that this active DB execution still has
                # a concrete Kubernetes resource. Quarantine it for operator
                # repair instead of converting uncertainty into deletion/loss.
                matched_replica_ids.add(pair[0].id)
        recovered = 0
        orphans_cleaned = 0
        for handle in handles:
            execution_id = _label_uuid(handle, EXECUTION_ID_LABEL)
            pair = replicas_by_execution.get(execution_id) if execution_id is not None else None
            if pair is None or not _labels_match_replica(
                handle,
                pair[0],
                pair[1].project_id,
                self.worker_id,
            ):
                if not await self._force_cleanup_if_current(handle):
                    self._handles.clear()
                    raise StaleKubernetesServingController(
                        "Worker session changed during Kubernetes orphan recovery"
                    )
                orphans_cleaned += 1
                continue
            replica, service = pair
            assert replica.execution_id is not None
            try:
                claim = _claim_from_models(
                    replica,
                    service,
                    self.default_image,
                    self.runtime_profile_catalog,
                )
            except RuntimeProfileCompatibilityError as error:
                self.logger.warning(
                    "Kubernetes serving replica profile rejected during recovery",
                    service_id=str(service.id),
                    replica_id=str(replica.id),
                    reason=_bounded_message(str(error)),
                )
                matched_replica_ids.add(replica.id)
                continue
            if claim is None:
                if not await self._force_cleanup_if_current(handle):
                    raise StaleKubernetesServingController(
                        "Worker session changed during Kubernetes recovery"
                    )
                await self._mark_terminal(
                    KubernetesReplicaClaim(
                        service_id=service.id,
                        replica_id=replica.id,
                        project_id=service.project_id,
                        generation=replica.generation,
                        execution_id=replica.execution_id,
                        image="unavailable",
                        model=service.model,
                        cpu_millicores=service.cpu_millicores,
                        memory_mb=service.memory_mb,
                        claimed_at=_replica_claimed_at(replica),
                    ),
                    status=ReplicaStatus.FAILED,
                    error_code=ErrorCode.CONTAINER_START_FAILED.value,
                    error_message="Kubernetes serving image is unavailable during recovery",
                    apply_backoff=True,
                )
                orphans_cleaned += 1
                continue
            item = _ManagedReplica(
                claim=claim,
                handle=handle,
                startup_deadline=claim.claimed_at + timedelta(seconds=self.startup_timeout_seconds),
                # DRAINING is entered from RUNNING and intentionally retains
                # its endpoint while active requests finish. Treat it as an
                # already-published replica so recovery cannot attempt a second
                # RUNNING transition and delete an otherwise healthy Pod.
                published=replica.status in {ReplicaStatus.RUNNING, ReplicaStatus.DRAINING},
                # A DB drain state does not prove that Kubernetes accepted the
                # delete request before this process restarted. Re-issuing the
                # idempotent graceful stop is safer than skipping directly to
                # force cleanup.
                expected_stop=False,
                stop_requested_at=None,
            )
            self._handles[replica.id] = item
            matched_replica_ids.add(replica.id)
            recovered += 1

        for replica, service in rows:
            if replica.id in matched_replica_ids:
                continue
            assert replica.execution_id is not None
            try:
                claim = _claim_from_models(
                    replica,
                    service,
                    self.default_image,
                    self.runtime_profile_catalog,
                )
            except RuntimeProfileCompatibilityError:
                claim = None
            if claim is None:
                claim = KubernetesReplicaClaim(
                    service_id=service.id,
                    replica_id=replica.id,
                    project_id=service.project_id,
                    generation=replica.generation,
                    execution_id=replica.execution_id,
                    image="unavailable",
                    model=service.model,
                    cpu_millicores=service.cpu_millicores,
                    memory_mb=service.memory_mb,
                    claimed_at=_replica_claimed_at(replica),
                )
            terminal_status = (
                ReplicaStatus.STOPPED
                if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                else ReplicaStatus.LOST
            )
            await self._mark_terminal(
                claim,
                status=terminal_status,
                error_code=(
                    None
                    if terminal_status == ReplicaStatus.STOPPED
                    else ErrorCode.WORKER_LOST.value
                ),
                error_message="Kubernetes serving controller restarted without a managed Pod",
                apply_backoff=terminal_status == ReplicaStatus.LOST,
                launch_failure=replica.status
                in {
                    ReplicaStatus.STARTING,
                    ReplicaStatus.LOADING,
                },
            )
        return recovered, orphans_cleaned, len(conflicts)

    def _record_recovery_conflicts(
        self,
        conflicts: tuple[KubernetesServingRecoveryConflict, ...],
    ) -> None:
        for conflict in conflicts[:_MAX_RECOVERY_CONFLICT_WARNINGS]:
            self.logger.warning(
                "Kubernetes serving managed resource quarantined",
                resource_kind=conflict.resource_kind,
                resource_name=conflict.resource_name,
                reason=conflict.reason,
                detail=conflict.message,
            )
        omitted = len(conflicts) - _MAX_RECOVERY_CONFLICT_WARNINGS
        if omitted > 0:
            self.logger.warning(
                "Additional Kubernetes serving recovery conflicts suppressed",
                omitted=omitted,
            )

    async def _claim_pending_replicas(
        self,
    ) -> tuple[list[KubernetesReplicaClaim], int]:
        claims: list[KubernetesReplicaClaim] = []
        waiting_backoff = 0
        async with self.database.session() as session, session.begin():
            services = list(
                await session.scalars(
                    self.claim_candidates_query(
                        self.batch_size,
                        fake_enabled=self.fake_enabled,
                    )
                )
            )
            if not services:
                return [], 0
            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            for service in services:
                if len(claims) >= self.batch_size:
                    break
                if not _retry_is_due(service, now):
                    waiting_backoff += 1
                    continue
                if (
                    service.runtime == ServingRuntime.FAKE
                    and (service.image or self.default_image) is None
                ):
                    _record_scheduling_reason(
                        service,
                        reason=_IMAGE_REQUIRED_REASON,
                        details={},
                        now=now,
                    )
                    continue
                already_started = int(
                    await session.scalar(
                        select(func.count(ServiceReplica.id)).where(
                            ServiceReplica.service_id == service.id,
                            ServiceReplica.generation == service.generation,
                            ServiceReplica.status.in_(
                                {
                                    ReplicaStatus.STARTING,
                                    ReplicaStatus.LOADING,
                                    ReplicaStatus.RUNNING,
                                }
                            ),
                        )
                    )
                    or 0
                )
                claimable = min(
                    self.batch_size - len(claims),
                    max(0, service.desired_replicas - already_started),
                )
                if claimable == 0:
                    continue
                replicas = list(
                    await session.scalars(
                        select(ServiceReplica)
                        .where(
                            ServiceReplica.service_id == service.id,
                            ServiceReplica.generation == service.generation,
                            ServiceReplica.status == ReplicaStatus.PENDING,
                        )
                        .order_by(ServiceReplica.ordinal, ServiceReplica.id)
                        .limit(claimable)
                    )
                )
                for replica in replicas:
                    try:
                        runtime_profile = _runtime_profile_from_replica_snapshot(
                            replica=replica,
                            service_runtime=service.runtime,
                            catalog=self.runtime_profile_catalog,
                        )
                    except RuntimeProfileCompatibilityError as error:
                        _record_scheduling_reason(
                            service,
                            reason=_RUNTIME_PROFILE_INVALID_REASON,
                            details={"message": _bounded_message(str(error))},
                            now=now,
                        )
                        continue
                    image = (
                        runtime_profile.image.reference
                        if runtime_profile is not None
                        else service.image or self.default_image
                    )
                    if image is None:
                        _record_scheduling_reason(
                            service,
                            reason=_IMAGE_REQUIRED_REASON,
                            details={},
                            now=now,
                        )
                        continue
                    execution_id = uuid.uuid4()
                    accepted = await ServiceRepository.bind_replica_execution(
                        session,
                        replica_id=replica.id,
                        generation=service.generation,
                        worker_id=self.worker_id,
                        worker_session_id=self.worker_session_id,
                        execution_id=execution_id,
                        lease_expires_at=lease_expires_at,
                    )
                    if accepted:
                        assert replica.started_at is not None
                        claim = _claim_from_models(
                            replica,
                            service,
                            self.default_image,
                            self.runtime_profile_catalog,
                        )
                        if claim is None:
                            raise RuntimeError("claimed Kubernetes replica has no launch image")
                        claims.append(
                            KubernetesReplicaClaim(
                                service_id=claim.service_id,
                                replica_id=claim.replica_id,
                                project_id=claim.project_id,
                                generation=claim.generation,
                                execution_id=claim.execution_id,
                                image=claim.image,
                                model=claim.model,
                                cpu_millicores=claim.cpu_millicores,
                                memory_mb=claim.memory_mb,
                                claimed_at=claim.claimed_at,
                                accelerator_count=claim.accelerator_count,
                                tensor_parallel_size=claim.tensor_parallel_size,
                                runtime_profile=claim.runtime_profile,
                                replacement_reason=_replacement_metric_reason(service, replica),
                            )
                        )
        return claims, waiting_backoff

    async def _launch(self, claim: KubernetesReplicaClaim) -> str:
        spec = KubernetesServingLaunchSpec(
            service_id=claim.service_id,
            replica_id=claim.replica_id,
            project_id=claim.project_id,
            generation=claim.generation,
            execution_id=claim.execution_id,
            image=claim.image,
            model=claim.model,
            cpu_millicores=claim.cpu_millicores,
            memory_mb=claim.memory_mb,
            startup_delay_seconds=self.fake_startup_delay_seconds,
            chunk_delay_seconds=self.fake_chunk_delay_seconds,
            container_port=8000,
            accelerator_count=claim.accelerator_count,
            tensor_parallel_size=claim.tensor_parallel_size,
            runtime_profile=claim.runtime_profile,
        )
        handle: KubernetesServingHandle | None = None
        try:
            handle = await self._prepare_and_start_if_current(spec)
            if handle is None:
                return "stale"
            item = _ManagedReplica(
                claim=claim,
                handle=handle,
                startup_deadline=claim.claimed_at + timedelta(seconds=self.startup_timeout_seconds),
            )
            self._handles[claim.replica_id] = item
            endpoint_url = handle.endpoint_url
            if endpoint_url is not None and not await self._publish_loading(claim, endpoint_url):
                self._handles.pop(claim.replica_id, None)
                # Rejection may mean a newer controller session adopted this exact
                # execution. Never delete a resource after losing the DB fence.
                if not await self._force_cleanup_if_current(handle):
                    return "stale"
                return "stale"
            state = await self.runtime.inspect(handle)
            item.metric_state = _pod_metric_state(state)
            return await self._advance_item(item, state)
        except asyncio.CancelledError:
            # Kubernetes labels make an in-flight launch recoverable. Preserve it
            # for the next cycle/controller instead of turning cancellation into
            # deletion. Re-running startup discovery also finds partial creates.
            self._recovered = False
            raise
        except Exception as exc:
            self._handles.pop(claim.replica_id, None)
            if handle is not None:
                if not await self._force_cleanup_if_current(handle):
                    return "stale"
            else:
                # prepare may create a Pod before failing to create its Service.
                # Force one discovery pass next cycle so that partial resource
                # cannot remain a steady-state orphan.
                self._recovered = False
                if not await self._session_is_current():
                    return "stale"
            accepted = await self._mark_terminal(
                claim,
                status=ReplicaStatus.FAILED,
                error_code=ErrorCode.CONTAINER_START_FAILED.value,
                error_message=_bounded_message(
                    "Kubernetes serving Pod launch failed",
                    type(exc).__name__,
                ),
                apply_backoff=True,
                launch_failure=True,
            )
            return "failed" if accepted else "stale"

    async def _inspect_managed_replicas(self) -> tuple[int, int, int]:
        ready = failed = stale = 0
        for item in list(self._handles.values()):
            try:
                state = await self.runtime.inspect(item.handle)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning(
                    "Kubernetes serving resource inspection failed",
                    replica_id=str(item.claim.replica_id),
                    error_type=type(exc).__name__,
                )
                continue
            item.metric_state = _pod_metric_state(state)
            outcome = await self._advance_item(item, state)
            ready += int(outcome == "ready")
            failed += int(outcome == "failed")
            stale += int(outcome == "stale")
        return ready, failed, stale

    async def _advance_item(
        self,
        item: _ManagedReplica,
        state: KubernetesServingState,
    ) -> str:
        claim = item.claim
        if state.missing:
            status = ReplicaStatus.STOPPED if item.expected_stop else ReplicaStatus.LOST
            if not item.expected_stop and not await self._force_cleanup_if_current(item.handle):
                self._handles.pop(claim.replica_id, None)
                return "stale"
            accepted = await self._mark_terminal(
                claim,
                status=status,
                error_code=(None if item.expected_stop else ErrorCode.WORKER_LOST.value),
                error_message=(
                    "Kubernetes serving Pod stopped after drain"
                    if item.expected_stop
                    else "Kubernetes serving Pod disappeared"
                ),
                apply_backoff=not item.expected_stop,
                launch_failure=not item.published and not item.expected_stop,
            )
            self._handles.pop(claim.replica_id, None)
            if not accepted:
                return "stale"
            return "stopped" if item.expected_stop else "failed"

        if state.deleting and not item.expected_stop:
            if not await self._force_cleanup_if_current(item.handle):
                self._handles.pop(claim.replica_id, None)
                return "stale"
            accepted = await self._mark_terminal(
                claim,
                status=ReplicaStatus.LOST,
                error_code=ErrorCode.WORKER_LOST.value,
                error_message="Kubernetes serving Pod was deleted outside the controller",
                apply_backoff=True,
                launch_failure=not item.published,
            )
            self._handles.pop(claim.replica_id, None)
            return "failed" if accepted else "stale"

        if item.expected_stop:
            if await self._stop_grace_expired(item):
                if not await self._force_cleanup_if_current(item.handle):
                    self._handles.pop(claim.replica_id, None)
                    return "stale"
                accepted = await self._mark_terminal(
                    claim,
                    status=ReplicaStatus.STOPPED,
                    error_code=None,
                    error_message="Kubernetes serving Pod force-stopped after drain deadline",
                    apply_backoff=False,
                )
                self._handles.pop(claim.replica_id, None)
                return "stopped" if accepted else "stale"
            return "stopping"

        terminal = _terminal_failure(state)
        if terminal is not None:
            error_code, message = terminal
            if not await self._force_cleanup_if_current(item.handle):
                self._handles.pop(claim.replica_id, None)
                return "stale"
            accepted = await self._mark_terminal(
                claim,
                status=ReplicaStatus.FAILED,
                error_code=error_code,
                error_message=message,
                apply_backoff=True,
                launch_failure=not item.published,
            )
            self._handles.pop(claim.replica_id, None)
            return "failed" if accepted else "stale"

        endpoint_url = state.endpoint_url or item.handle.endpoint_url
        if endpoint_url is not None and not item.published:
            await self._publish_loading(claim, endpoint_url)
        if state.ready and endpoint_url is not None:
            if item.published:
                accepted = await self._record_health(claim, ReplicaHealth.HEALTHY)
                # A healthy Pod can remain ready while the database replica is
                # draining.  Health writes are intentionally rejected once the
                # service desires zero replicas; lease renewal below remains the
                # ownership check and will report a real fence loss.
                return "healthy" if accepted else "unchanged"
            accepted = await self._publish_ready(
                claim,
                endpoint_url,
                state.image_digest or item.handle.image_digest,
            )
            if accepted:
                item.published = True
                return "ready"
            if not await self._force_cleanup_if_current(item.handle):
                self._handles.pop(claim.replica_id, None)
                return "stale"
            self._handles.pop(claim.replica_id, None)
            return "stale"

        if item.published and state.running and not state.ready:
            await self._record_health(
                claim,
                ReplicaHealth.UNHEALTHY,
                error_message="Kubernetes readiness condition is false",
                failure_threshold=1,
            )
            return "unhealthy"

        if datetime.now(UTC) >= item.startup_deadline:
            if not await self._force_cleanup_if_current(item.handle):
                self._handles.pop(claim.replica_id, None)
                return "stale"
            accepted = await self._mark_terminal(
                claim,
                status=ReplicaStatus.FAILED,
                error_code=ErrorCode.MODEL_LOAD_TIMEOUT.value,
                error_message="Kubernetes serving Pod did not become ready before startup timeout",
                apply_backoff=True,
                launch_failure=not item.published,
            )
            self._handles.pop(claim.replica_id, None)
            return "failed" if accepted else "stale"
        return "launched"

    async def _stop_requested_replicas(self) -> int:
        async with self.database.session() as session:
            now = await database_utcnow(session)
            replicas = list(
                await session.scalars(
                    select(ServiceReplica)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ModelService.runtime == ServingRuntime.FAKE,
                        ModelService.runtime_type == RuntimeType.KUBERNETES,
                        ServiceReplica.worker_id == self.worker_id,
                        ServiceReplica.execution_id.is_not(None),
                        ServiceReplica.status.in_({ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}),
                    )
                )
            )
        stopped = 0
        for replica in replicas:
            assert replica.execution_id is not None
            drain_deadline = self._effective_drain_deadline(replica)
            if (
                replica.status != ReplicaStatus.STOPPING
                and replica.active_requests > 0
                and drain_deadline > _as_utc(now)
            ):
                continue
            item = self._handles.get(replica.id)
            if item is None:
                accepted = await self._mark_terminal_values(
                    service_id=replica.service_id,
                    replica_id=replica.id,
                    generation=replica.generation,
                    execution_id=replica.execution_id,
                    status=ReplicaStatus.STOPPED,
                    error_code=None,
                    error_message=replica.error_message or "Kubernetes replica stop completed",
                    apply_backoff=False,
                )
                stopped += int(accepted)
                continue
            force = replica.active_requests > 0 and drain_deadline <= _as_utc(now)
            if item.expected_stop and not force:
                continue
            if force:
                if not await self._force_cleanup_if_current(item.handle):
                    self._handles.clear()
                    raise StaleKubernetesServingController(
                        "Worker session changed before Kubernetes Pod force cleanup"
                    )
                accepted = await self._mark_terminal_values(
                    service_id=replica.service_id,
                    replica_id=replica.id,
                    generation=replica.generation,
                    execution_id=replica.execution_id,
                    status=ReplicaStatus.STOPPED,
                    error_code=None,
                    error_message="Kubernetes replica force-stopped after drain timeout",
                    apply_backoff=False,
                )
                self._handles.pop(replica.id, None)
                stopped += int(accepted)
            else:
                if not await self._request_stop_if_current(item.handle):
                    self._handles.clear()
                    raise StaleKubernetesServingController(
                        "Worker session changed before Kubernetes Pod drain"
                    )
                item.expected_stop = True
                item.stop_requested_at = datetime.now(UTC)
                item.metric_state = "terminating"
        return stopped

    def _effective_drain_deadline(self, replica: ServiceReplica) -> datetime:
        """Honor the earliest repository or Kubernetes-specific drain cap."""

        base_time = replica.drain_started_at or replica.updated_at
        configured_deadline = _as_utc(base_time) + timedelta(seconds=self.drain_timeout_seconds)
        if replica.drain_deadline is None:
            return configured_deadline
        return min(configured_deadline, _as_utc(replica.drain_deadline))

    async def _stop_grace_expired(self, item: _ManagedReplica) -> bool:
        if item.stop_requested_at is None:
            return False
        return datetime.now(UTC) >= item.stop_requested_at + timedelta(
            seconds=self.termination_grace_seconds
        )

    async def _renew_active_leases(self) -> int:
        if not self._handles:
            return 0
        stale: list[uuid.UUID] = []
        async with self.database.session() as session, session.begin():
            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            for replica_id, item in self._handles.items():
                claim = item.claim
                renewed = await ServiceRepository.renew_replica_lease(
                    session,
                    replica_id=claim.replica_id,
                    generation=claim.generation,
                    execution_id=claim.execution_id,
                    lease_expires_at=lease_expires_at,
                    worker_id=self.worker_id,
                    worker_session_id=self.worker_session_id,
                )
                if not renewed:
                    stale.append(replica_id)
        # Never delete here: rejection may be caused by a newer controller
        # session which has already adopted the same Kubernetes resource.
        for replica_id in stale:
            self._handles.pop(replica_id, None)
        return len(stale)

    async def _publish_loading(self, claim: KubernetesReplicaClaim, endpoint_url: str) -> bool:
        async with self.database.session() as session, session.begin():
            return await ServiceRepository.mark_replica_loading(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                endpoint_url=endpoint_url,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )

    async def _publish_ready(
        self,
        claim: KubernetesReplicaClaim,
        endpoint_url: str,
        image_digest: str | None,
    ) -> bool:
        async with self.database.session() as session, session.begin():
            accepted = await ServiceRepository.mark_replica_running(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                endpoint_url=endpoint_url,
                image_digest=image_digest,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )
            if accepted:
                accepted = await ServiceRepository.record_replica_health(
                    session,
                    replica_id=claim.replica_id,
                    generation=claim.generation,
                    execution_id=claim.execution_id,
                    health=ReplicaHealth.HEALTHY,
                    worker_id=self.worker_id,
                    worker_session_id=self.worker_session_id,
                )
            if accepted:
                service = await session.get(ModelService, claim.service_id, with_for_update=True)
                if service is not None and service.scheduling_reason in {
                    _BACKOFF_REASON,
                    _IMAGE_REQUIRED_REASON,
                }:
                    now = await database_utcnow(session)
                    service.scheduling_reason = None
                    service.scheduling_details = {}
                    service.updated_at = now
                    service.version += 1
            return accepted

    async def _record_health(
        self,
        claim: KubernetesReplicaClaim,
        health: ReplicaHealth,
        *,
        error_message: str | None = None,
        failure_threshold: int = 1,
    ) -> bool:
        async with self.database.session() as session, session.begin():
            return await ServiceRepository.record_replica_health(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                health=health,
                error_message=error_message,
                failure_threshold=failure_threshold,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )

    async def _mark_terminal(
        self,
        claim: KubernetesReplicaClaim,
        *,
        status: ReplicaStatus,
        error_code: str | None,
        error_message: str,
        apply_backoff: bool,
        launch_failure: bool = False,
    ) -> bool:
        return await self._mark_terminal_values(
            service_id=claim.service_id,
            replica_id=claim.replica_id,
            generation=claim.generation,
            execution_id=claim.execution_id,
            status=status,
            error_code=error_code,
            error_message=error_message,
            apply_backoff=apply_backoff,
            launch_failure=launch_failure,
        )

    async def _mark_terminal_values(
        self,
        *,
        service_id: uuid.UUID,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        status: ReplicaStatus,
        error_code: str | None,
        error_message: str,
        apply_backoff: bool,
        launch_failure: bool = False,
    ) -> bool:
        failure_reason: str | None = None
        async with self.database.session() as session, session.begin():
            service = await session.get(ModelService, service_id, with_for_update=True)
            accepted = await ServiceRepository.mark_replica_terminal(
                session,
                replica_id=replica_id,
                generation=generation,
                execution_id=execution_id,
                status=status,
                error_code=error_code,
                error_message=_bounded_message(error_message),
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )
            if accepted and apply_backoff and service is not None:
                now = await database_utcnow(session)
                previous_failures = _backoff_failure_count(service)
                failures = min(previous_failures + 1, 64)
                delay = min(
                    _MAX_BACKOFF_SECONDS,
                    self.failure_backoff_seconds * (2 ** min(failures - 1, 6)),
                )
                service.scheduling_reason = _BACKOFF_REASON
                service.scheduling_details = {
                    "failure_count": failures,
                    "last_failure_code": error_code or "UNKNOWN",
                    "retry_not_before": (now + timedelta(seconds=delay)).isoformat(),
                }
                service.updated_at = now
                service.version += 1
                failure_reason = _failure_metric_reason(error_code, status)
        if failure_reason is not None and launch_failure:
            K8S_SERVING_LAUNCH_FAILURES.labels(reason=failure_reason).inc()
        return accepted


def _claim_from_models(
    replica: ServiceReplica,
    service: ModelService,
    default_image: str | None,
    runtime_profile_catalog: RuntimeProfileCatalog | None,
) -> KubernetesReplicaClaim | None:
    if replica.execution_id is None:
        return None
    runtime_profile = _runtime_profile_from_replica_snapshot(
        replica=replica,
        service_runtime=service.runtime,
        catalog=runtime_profile_catalog,
    )
    image = runtime_profile.image.reference if runtime_profile is not None else service.image
    image = image or default_image
    if image is None:
        return None
    accelerator_count = service.tensor_parallel_size if runtime_profile is not None else 0
    tensor_parallel_size = service.tensor_parallel_size if runtime_profile is not None else 1
    return KubernetesReplicaClaim(
        service_id=service.id,
        replica_id=replica.id,
        project_id=service.project_id,
        generation=replica.generation,
        execution_id=replica.execution_id,
        image=image,
        model=service.model,
        cpu_millicores=service.cpu_millicores,
        memory_mb=service.memory_mb,
        claimed_at=_replica_claimed_at(replica),
        accelerator_count=accelerator_count,
        tensor_parallel_size=tensor_parallel_size,
        runtime_profile=runtime_profile,
    )


def _runtime_profile_from_replica_snapshot(
    *,
    replica: ServiceReplica,
    service_runtime: ServingRuntime,
    catalog: RuntimeProfileCatalog | None,
) -> RuntimeProfile | None:
    snapshot = {
        "model_variant_id": replica.model_variant_id,
        "selected_vendor": replica.selected_vendor,
        "selected_kind": replica.selected_kind,
        "selected_model": replica.selected_model,
        "runtime_profile_id": replica.runtime_profile_id,
        "runtime_profile_version": replica.runtime_profile_version,
        "runtime_profile_digest": replica.runtime_profile_digest,
        "allocation_authority": replica.allocation_authority,
        "accelerator_resource_name": replica.accelerator_resource_name,
        "selection_policy": replica.selection_policy,
    }
    has_snapshot = any(value is not None for value in snapshot.values())
    if service_runtime == ServingRuntime.FAKE:
        if has_snapshot:
            raise RuntimeProfileCompatibilityError(
                "Fake Kubernetes replicas must not carry an accelerator snapshot"
            )
        return None
    if service_runtime != ServingRuntime.VLLM:
        raise RuntimeProfileCompatibilityError(
            f"unsupported Kubernetes serving runtime: {service_runtime.value}"
        )

    missing = sorted(
        name
        for name, value in snapshot.items()
        if value is None or (isinstance(value, str) and not value.strip())
    )
    if missing:
        raise RuntimeProfileCompatibilityError(
            f"incomplete replica accelerator snapshot: {', '.join(missing)}"
        )
    if catalog is None:
        raise RuntimeProfileCompatibilityError("runtime profile catalog is unavailable")

    assert replica.selected_vendor is not None
    assert replica.selected_kind is not None
    assert replica.runtime_profile_id is not None
    assert replica.runtime_profile_version is not None
    assert replica.runtime_profile_digest is not None
    assert replica.allocation_authority is not None
    assert replica.accelerator_resource_name is not None
    assert replica.selection_policy is not None
    try:
        vendor = AcceleratorVendor(replica.selected_vendor)
        kind = AcceleratorKind(replica.selected_kind)
        authority = AllocationAuthority(replica.allocation_authority)
        policy = AcceleratorSelectionPolicy(replica.selection_policy)
    except ValueError as error:
        raise RuntimeProfileCompatibilityError(
            "replica accelerator snapshot contains an unsupported enum value"
        ) from error
    if authority != AllocationAuthority.KUBERNETES_DEVICE_PLUGIN:
        raise RuntimeProfileCompatibilityError(
            "Kubernetes accelerator launch requires kubernetes_device_plugin authority"
        )
    if (
        policy == AcceleratorSelectionPolicy.NVIDIA_ONLY and vendor != AcceleratorVendor.NVIDIA
    ) or (
        policy == AcceleratorSelectionPolicy.ASCEND_ONLY
        and vendor != AcceleratorVendor.HUAWEI_ASCEND
    ):
        raise RuntimeProfileCompatibilityError(
            "replica selection policy does not match the selected vendor"
        )

    profile = catalog.load_exact(
        profile_id=replica.runtime_profile_id,
        profile_version=replica.runtime_profile_version,
        semantic_digest=replica.runtime_profile_digest,
    )
    if profile.vendor is not vendor or profile.kind is not kind:
        raise RuntimeProfileCompatibilityError(
            "replica vendor/kind does not match the runtime profile"
        )
    if profile.allocation_authority is not authority:
        raise RuntimeProfileCompatibilityError(
            "replica allocation authority does not match the runtime profile"
        )
    if profile.kubernetes.resource_name != replica.accelerator_resource_name:
        raise RuntimeProfileCompatibilityError(
            "replica accelerator resource does not match the runtime profile"
        )
    return profile


def _replica_claimed_at(replica: ServiceReplica) -> datetime:
    if replica.started_at is not None:
        return _as_utc(replica.started_at)
    if replica.status == ReplicaStatus.STARTING:
        return _as_utc(replica.updated_at)
    return _as_utc(replica.container_started_at or replica.created_at)


def _stable_worker_id(cluster_id: str) -> str:
    prefix = "kubernetes-serving-"
    candidate = f"{prefix}{cluster_id}"
    if len(candidate) <= 63:
        return candidate
    digest = hashlib.sha256(cluster_id.encode("utf-8")).hexdigest()[:10]
    head_length = 63 - len(prefix) - len(digest) - 1
    return f"{prefix}{cluster_id[:head_length]}-{digest}"


def _label_uuid(handle: KubernetesServingHandle, key: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(handle.labels.get(key, ""))
    except ValueError:
        return None


def _labels_match_replica(
    handle: KubernetesServingHandle,
    replica: ServiceReplica,
    project_id: uuid.UUID,
    worker_id: str,
) -> bool:
    return (
        handle.labels.get(SERVICE_ID_LABEL) == str(replica.service_id)
        and handle.labels.get(REPLICA_ID_LABEL) == str(replica.id)
        and handle.labels.get(PROJECT_ID_LABEL) == str(project_id)
        and handle.labels.get(GENERATION_LABEL) == str(replica.generation)
        and handle.labels.get(EXECUTION_ID_LABEL) == str(replica.execution_id)
        and handle.labels.get(WORKER_ID_LABEL) == worker_id
    )


def _ownership_matches_replica(
    ownership: KubernetesServingOwnershipIdentity,
    replica: ServiceReplica,
    project_id: uuid.UUID,
    worker_id: str,
    cluster_id: str,
) -> bool:
    return (
        ownership.service_id == replica.service_id
        and ownership.replica_id == replica.id
        and ownership.project_id == project_id
        and ownership.generation == replica.generation
        and ownership.execution_id == replica.execution_id
        and ownership.worker_id == worker_id
        and ownership.cluster_id == cluster_id
    )


def _terminal_failure(state: KubernetesServingState) -> tuple[str, str] | None:
    reason = (state.reason or "").strip()
    if state.oom_killed or reason == "OOMKilled":
        return (
            ErrorCode.OOM_KILLED.value,
            "Kubernetes serving Pod was terminated by the out-of-memory killer",
        )
    if reason in _IMAGE_PULL_REASONS:
        return (
            ErrorCode.IMAGE_PULL_FAILED.value,
            _bounded_message("Kubernetes serving image pull failed", reason),
        )
    if reason in _CONTAINER_START_REASONS:
        return (
            ErrorCode.CONTAINER_START_FAILED.value,
            _bounded_message("Kubernetes serving container failed to start", reason),
        )
    if state.phase in {"Failed", "Succeeded"} or (
        state.exit_code is not None and not state.running
    ):
        return (
            ErrorCode.REPLICA_UNHEALTHY.value,
            _bounded_message(
                "Kubernetes serving Pod exited unexpectedly",
                reason or f"exit-{state.exit_code}",
            ),
        )
    return None


def _retry_is_due(service: ModelService, now: datetime) -> bool:
    if service.scheduling_reason != _BACKOFF_REASON:
        return True
    raw = dict(service.scheduling_details or {}).get("retry_not_before")
    if not isinstance(raw, str):
        return True
    try:
        retry_at = datetime.fromisoformat(raw)
    except ValueError:
        return True
    return _as_utc(retry_at) <= _as_utc(now)


def _backoff_failure_count(service: ModelService) -> int:
    if service.scheduling_reason != _BACKOFF_REASON:
        return 0
    raw = dict(service.scheduling_details or {}).get("failure_count")
    return raw if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0 else 0


def _record_scheduling_reason(
    service: ModelService,
    *,
    reason: str,
    details: dict[str, object],
    now: datetime,
) -> None:
    if service.scheduling_reason == reason and service.scheduling_details == details:
        return
    service.scheduling_reason = reason
    service.scheduling_details = details
    service.updated_at = now
    service.version += 1


def _bounded_message(*parts: str) -> str:
    normalized = (
        ": ".join(" ".join(part.replace("\x00", " ").split()) for part in parts if part.strip())
        or "Kubernetes serving runtime failure"
    )
    encoded = normalized.encode("utf-8")
    if len(encoded) <= _MAX_ERROR_MESSAGE_BYTES:
        return normalized
    return encoded[:_MAX_ERROR_MESSAGE_BYTES].decode("utf-8", "ignore")


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _launch_metric_outcome(outcome: str) -> str:
    if outcome in {"launched", "ready"}:
        return "success"
    if outcome == "failed":
        return "failure"
    return "fenced"


def _pod_metric_state(state: KubernetesServingState) -> str:
    """Map the last successful Pod observation onto non-overlapping gauge states."""

    if state.deleting:
        return "terminating"
    if state.ready:
        return "ready"
    return "not_ready"


def _replacement_metric_reason(service: ModelService, replica: ServiceReplica) -> str | None:
    """Identify a post-terminal claim without treating ordinary scale-out as replacement."""

    if replica.ordinal < service.desired_replicas:
        return None
    if _backoff_failure_count(service) > 0:
        return "failure_backoff"
    return "terminal_replica"


def _failure_metric_reason(error_code: str | None, status: ReplicaStatus) -> str:
    if error_code == ErrorCode.IMAGE_PULL_FAILED.value:
        return "image_pull"
    if error_code == ErrorCode.OOM_KILLED.value:
        return "oom"
    if error_code == ErrorCode.MODEL_LOAD_TIMEOUT.value:
        return "startup_timeout"
    if status == ReplicaStatus.LOST or error_code == ErrorCode.WORKER_LOST.value:
        return "pod_missing"
    if error_code == ErrorCode.CONTAINER_START_FAILED.value:
        return "launch_error"
    return "pod_failed"
