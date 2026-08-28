from __future__ import annotations

import asyncio
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx
import psutil
from sqlalchemy import Select, func, or_, select

from core.database import Database
from core.enums import AcceleratorKind, AcceleratorVendor, ErrorCode, RuntimeType, WorkerStatus
from core.logging import get_logger
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServingRuntime,
)
from repositories.clock import database_utcnow
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository
from scheduler.serving import (
    ServingGPUDeviceSnapshot,
    ServingPlacementRequest,
    ServingWorkerSnapshot,
    choose_single_node_gang_placement,
)
from worker.gpu_inventory import GPUDevice, GPUInventoryProvider, NvidiaSMIInventoryProvider
from worker.vllm_runtime import (
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    REPLICA_ID_LABEL,
    SERVICE_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    VLLMContainerHandle,
    VLLMLaunchRequest,
    VLLMReplicaRuntime,
    build_vllm_launch_spec,
)


@dataclass(frozen=True, slots=True)
class VLLMReplicaClaim:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    project_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    image: str
    model: str
    model_revision: str | None
    cpu_millicores: int
    memory_mb: int
    gpu_device_ids: tuple[str, ...]
    tensor_parallel_size: int
    dtype: str
    gpu_memory_utilization: float
    max_model_len: int | None


@dataclass(frozen=True, slots=True)
class VLLMRuntimeRunResult:
    claimed: int = 0
    launched: int = 0
    ready: int = 0
    stopped: int = 0
    failed: int = 0
    stale: int = 0
    recovered: int = 0
    waiting_capacity: int = 0


@dataclass(slots=True)
class _ManagedReplica:
    claim: VLLMReplicaClaim
    handle: VLLMContainerHandle
    startup_deadline: float
    published: bool = False


class VLLMReplicaRuntimeController:
    """Fenced Docker executor for long-running vLLM service replicas.

    The controller registers a dedicated, draining Worker identity so the batch
    scheduler cannot place tasks onto capacity owned by this service executor.
    Deployments must run it on a dedicated serving node; enabling it is explicit.
    Each process registration creates a new Worker session. Containers from an
    older session are stopped and fenced ``lost`` before replacement intent is
    created by the normal service reconciler.
    """

    def __init__(
        self,
        database: Database,
        runtime: VLLMReplicaRuntime,
        *,
        http_client: httpx.AsyncClient | None = None,
        inventory_provider: GPUInventoryProvider | None = None,
        worker_id: str | None = None,
        batch_size: int = 10,
        ready_timeout_seconds: float = 600.0,
        probe_timeout_seconds: float = 3.0,
        lease_seconds: float = 900.0,
        allow_fake_gpu_inventory: bool = False,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if ready_timeout_seconds <= 0 or probe_timeout_seconds <= 0:
            raise ValueError("vLLM readiness timeouts must be positive")
        if lease_seconds <= probe_timeout_seconds * 2:
            raise ValueError("lease_seconds must exceed two readiness probe timeouts")
        resolved_worker_id = worker_id or f"vllm-docker-{socket.gethostname()}"
        if not resolved_worker_id.strip() or len(resolved_worker_id) > 255:
            raise ValueError("worker_id must contain 1 to 255 characters")

        self.database = database
        self.runtime = runtime
        self.http_client = http_client or httpx.AsyncClient(
            follow_redirects=False,
            trust_env=False,
        )
        self._owns_http_client = http_client is None
        self.inventory_provider = inventory_provider or NvidiaSMIInventoryProvider()
        self.worker_id = resolved_worker_id.strip()
        self.worker_session_id = uuid.uuid4()
        self.batch_size = batch_size
        self.ready_timeout_seconds = ready_timeout_seconds
        self.probe_timeout = httpx.Timeout(probe_timeout_seconds)
        self.lease_seconds = lease_seconds
        self.allow_fake_gpu_inventory = allow_fake_gpu_inventory
        self._devices: tuple[GPUDevice, ...] = ()
        self._cpu_total_millicores = max(1, os.cpu_count() or 1) * 1000
        self._memory_total_mb = max(16, int(psutil.virtual_memory().total // (1024 * 1024)))
        self._handles: dict[uuid.UUID, _ManagedReplica] = {}
        self._cycle_lock = asyncio.Lock()
        self._registered = False
        self._closed = False
        self.logger = get_logger("vllm_replica_runtime")

    @property
    def active_container_count(self) -> int:
        return len(self._handles)

    @staticmethod
    def claim_candidates_query(limit: int) -> Select[tuple[ModelService]]:
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
                ModelService.runtime == ServingRuntime.VLLM,
                ModelService.runtime_type == RuntimeType.DOCKER,
                ModelService.desired_replicas > 0,
                pending_exists,
            )
            .order_by(ModelService.updated_at, ModelService.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def run_once(self) -> VLLMRuntimeRunResult:
        async with self._cycle_lock:
            if self._closed:
                raise RuntimeError("vLLM replica runtime is closed")
            recovered = await self._ensure_worker()
            stopped = await self._stop_requested_replicas()
            failed = await self._fail_exited_replicas()
            stale = await self._renew_active_leases()
            ready, startup_failed, startup_stale = await self._advance_starting_replicas()
            claims, waiting_capacity = await self._claim_pending_replicas()
            outcomes = await asyncio.gather(*(self._launch(claim) for claim in claims))
            result = VLLMRuntimeRunResult(
                claimed=len(claims),
                launched=sum(outcome in {"launched", "ready"} for outcome in outcomes),
                ready=ready + sum(outcome == "ready" for outcome in outcomes),
                stopped=stopped,
                failed=failed + startup_failed + sum(outcome == "failed" for outcome in outcomes),
                stale=stale + startup_stale + sum(outcome == "stale" for outcome in outcomes),
                recovered=recovered,
                waiting_capacity=waiting_capacity,
            )
            if any(
                (
                    result.claimed,
                    result.ready,
                    result.stopped,
                    result.failed,
                    result.stale,
                    result.recovered,
                )
            ):
                self.logger.info(
                    "vLLM replica runtime cycle completed",
                    worker_id=self.worker_id,
                    claimed=result.claimed,
                    launched=result.launched,
                    ready=result.ready,
                    stopped=result.stopped,
                    failed=result.failed,
                    stale=result.stale,
                    recovered=result.recovered,
                    waiting_capacity=result.waiting_capacity,
                )
            return result

    async def close(self) -> None:
        async with self._cycle_lock:
            if self._closed:
                return
            self._closed = True
            managed = list(self._handles.values())
            await asyncio.gather(
                *(
                    self._terminate_handle(
                        item,
                        status=ReplicaStatus.LOST,
                        error_message="vLLM runtime controller stopped",
                    )
                    for item in managed
                ),
                return_exceptions=True,
            )
            if self._registered:
                async with self.database.session() as session, session.begin():
                    await WorkerRepository.set_status(
                        session,
                        self.worker_id,
                        WorkerStatus.OFFLINE,
                        worker_session_id=self.worker_session_id,
                    )
            if self._owns_http_client:
                await self.http_client.aclose()
            await self.runtime.close()

    async def _ensure_worker(self) -> int:
        if self._registered:
            async with self.database.session() as session, session.begin():
                worker = await WorkerRepository.heartbeat(
                    session,
                    self.worker_id,
                    self.active_container_count,
                    worker_session_id=self.worker_session_id,
                )
            if worker is None:
                raise RuntimeError("vLLM runtime worker session is stale")
            return 0

        devices = await asyncio.to_thread(self.inventory_provider.list_devices)
        if any(device.fake for device in devices) and not self.allow_fake_gpu_inventory:
            raise ValueError("fake GPU inventory cannot launch real vLLM containers")
        if any(
            device.vendor != AcceleratorVendor.NVIDIA or device.kind != AcceleratorKind.GPU
            for device in devices
        ):
            raise ValueError("Docker vLLM runtime only accepts NVIDIA GPU inventory")
        if len({device.uuid for device in devices}) != len(devices):
            raise ValueError("vLLM GPU inventory contains duplicate device UUIDs")
        self._devices = tuple(sorted(devices, key=lambda item: (item.index, item.uuid)))
        docker_version = await self.runtime.version()
        async with self.database.session() as session, session.begin():
            worker = await WorkerRepository.register(
                session,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
                hostname=socket.gethostname(),
                node_name=socket.gethostname(),
                concurrency=self.batch_size,
                cpu_count=max(1, self._cpu_total_millicores // 1000),
                memory_total_mb=self._memory_total_mb,
                docker_version=docker_version,
                labels={
                    "managed-by": "vllm-replica-runtime",
                    "runtime": RuntimeType.DOCKER.value,
                    "workload": "model-service",
                },
                runtime_types=[RuntimeType.DOCKER.value],
                gpu_count=len(self._devices),
                gpu_model=", ".join(dict.fromkeys(item.model for item in self._devices)) or None,
                gpu_memory_mb=sum(item.memory_total_mb for item in self._devices),
            )
            await WorkerRepository.replace_gpu_inventory(
                session,
                worker_id=self.worker_id,
                worker_session_id=worker.worker_session_id,
                devices=[
                    {
                        "uuid": item.uuid,
                        "index": item.index,
                        "vendor": item.vendor.value,
                        "accelerator_kind": item.kind.value,
                        "model": item.model,
                        "memory_total_mb": item.memory_total_mb,
                        "memory_free_mb": item.memory_free_mb,
                        "compute_capability": item.compute_capability,
                        "compute_arch": item.compute_arch,
                        "runtime_profile_ids": list(item.runtime_profile_ids),
                        "capabilities": list(item.capabilities),
                        "kubernetes_resource_name": item.kubernetes_resource_name,
                        "fake": item.fake,
                    }
                    for item in self._devices
                ],
            )
            await WorkerRepository.drain(
                session,
                self.worker_id,
                reason="dedicated vLLM service runtime; batch placement disabled",
            )
        self._registered = True
        return await self._recover_owned_orphans()

    async def _recover_owned_orphans(self) -> int:
        containers = list(await self.runtime.list_managed(worker_id=self.worker_id))
        async with self.database.session() as session:
            replicas = list(
                await session.scalars(
                    select(ServiceReplica)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ModelService.runtime == ServingRuntime.VLLM,
                        ModelService.runtime_type == RuntimeType.DOCKER,
                        ServiceReplica.worker_id == self.worker_id,
                        ServiceReplica.execution_id.is_not(None),
                        ServiceReplica.status.in_(
                            {
                                ReplicaStatus.STARTING,
                                ReplicaStatus.LOADING,
                                ReplicaStatus.RUNNING,
                                ReplicaStatus.DRAINING,
                                ReplicaStatus.STOPPING,
                            }
                        ),
                    )
                )
            )
        replicas_by_execution = {
            replica.execution_id: replica
            for replica in replicas
            if replica.execution_id is not None
        }
        recovered_ids: set[uuid.UUID] = set()
        recovered = 0
        for handle in containers:
            execution_id = _label_uuid(handle, EXECUTION_ID_LABEL)
            replica = replicas_by_execution.get(execution_id) if execution_id is not None else None
            try:
                await self.runtime.stop(handle)
                await self.runtime.cleanup(handle)
            except Exception as exc:
                self.logger.error(
                    "failed to clean old-session vLLM container",
                    worker_id=self.worker_id,
                    container_id=handle.object_id,
                    error_type=type(exc).__name__,
                )
                continue
            if replica is None or not _labels_match_replica(handle, replica):
                recovered += 1
                continue
            assert replica.execution_id is not None
            status = (
                ReplicaStatus.STOPPED
                if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                else ReplicaStatus.LOST
            )
            accepted = await self._mark_terminal_values(
                replica_id=replica.id,
                generation=replica.generation,
                execution_id=replica.execution_id,
                status=status,
                error_message="vLLM runtime restarted; old worker session container removed",
            )
            recovered_ids.add(replica.id)
            recovered += int(accepted)

        for replica in replicas:
            if replica.id in recovered_ids:
                continue
            assert replica.execution_id is not None
            status = (
                ReplicaStatus.STOPPED
                if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                else ReplicaStatus.LOST
            )
            accepted = await self._mark_terminal_values(
                replica_id=replica.id,
                generation=replica.generation,
                execution_id=replica.execution_id,
                status=status,
                error_message="vLLM runtime restarted without a managed container",
            )
            recovered += int(accepted)
        return recovered

    async def _claim_pending_replicas(self) -> tuple[list[VLLMReplicaClaim], int]:
        used_cpu = sum(item.claim.cpu_millicores for item in self._handles.values())
        used_memory = sum(item.claim.memory_mb for item in self._handles.values())
        used_devices = {
            device_id for item in self._handles.values() for device_id in item.claim.gpu_device_ids
        }
        available_cpu = max(0, self._cpu_total_millicores - used_cpu)
        available_memory = max(0, self._memory_total_mb - used_memory)
        available_devices = {
            device.uuid: device for device in self._devices if device.uuid not in used_devices
        }
        claims: list[VLLMReplicaClaim] = []
        waiting_capacity = 0

        async with self.database.session() as session, session.begin():
            services = list(await session.scalars(self.claim_candidates_query(self.batch_size)))
            if not services:
                return [], 0
            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            for service in services:
                if len(claims) >= self.batch_size:
                    break
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
                    placement, explain = choose_single_node_gang_placement(
                        ServingPlacementRequest(
                            gpu_count=service.gpu_count,
                            gpu_model=service.gpu_model,
                            gpu_memory_mb=service.gpu_memory_mb,
                            allow_fake=self.allow_fake_gpu_inventory,
                        ),
                        (
                            ServingWorkerSnapshot(
                                id=self.worker_id,
                                gpu_devices=tuple(
                                    ServingGPUDeviceSnapshot(
                                        uuid=device.uuid,
                                        index=device.index,
                                        model=device.model,
                                        memory_free_mb=device.memory_free_mb,
                                        fake=device.fake,
                                    )
                                    for device in available_devices.values()
                                ),
                            ),
                        ),
                    )
                    capacity_failure: tuple[str, dict[str, object]] | None = None
                    if service.image is None:
                        capacity_failure = ("VLLM_IMAGE_REQUIRED", {})
                    elif service.cpu_millicores > available_cpu:
                        capacity_failure = (
                            "INSUFFICIENT_CPU",
                            {
                                "requested_cpu_millicores": service.cpu_millicores,
                                "available_cpu_millicores": available_cpu,
                            },
                        )
                    elif service.memory_mb > available_memory:
                        capacity_failure = (
                            "INSUFFICIENT_MEMORY",
                            {
                                "requested_memory_mb": service.memory_mb,
                                "available_memory_mb": available_memory,
                            },
                        )
                    elif placement is None:
                        assert explain is not None
                        capacity_failure = (explain.reason.value, explain.details())
                    if capacity_failure is not None:
                        _record_scheduling_explain(
                            service,
                            reason=capacity_failure[0],
                            details=capacity_failure[1],
                            now=now,
                        )
                        waiting_capacity += 1
                        break
                    assert placement is not None
                    assert placement.worker_id == self.worker_id
                    assert service.image is not None
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
                    if not accepted:
                        continue
                    device_ids = placement.gpu_device_ids
                    _clear_scheduling_explain(service, now=now)
                    available_cpu -= service.cpu_millicores
                    available_memory -= service.memory_mb
                    for device_id in device_ids:
                        available_devices.pop(device_id, None)
                    claims.append(
                        VLLMReplicaClaim(
                            service_id=service.id,
                            replica_id=replica.id,
                            project_id=service.project_id,
                            generation=service.generation,
                            execution_id=execution_id,
                            image=service.image,
                            model=service.model,
                            model_revision=service.model_revision,
                            cpu_millicores=service.cpu_millicores,
                            memory_mb=service.memory_mb,
                            gpu_device_ids=device_ids,
                            tensor_parallel_size=service.tensor_parallel_size,
                            dtype=service.dtype,
                            gpu_memory_utilization=service.gpu_memory_utilization,
                            max_model_len=service.max_model_len,
                        )
                    )
        return claims, waiting_capacity

    async def _launch(self, claim: VLLMReplicaClaim) -> str:
        launch_spec = build_vllm_launch_spec(
            VLLMLaunchRequest(
                service_id=claim.service_id,
                replica_id=claim.replica_id,
                project_id=claim.project_id,
                execution_id=claim.execution_id,
                generation=claim.generation,
                image=claim.image,
                model=claim.model,
                gpu_device_ids=claim.gpu_device_ids,
                revision=claim.model_revision,
                tensor_parallel_size=claim.tensor_parallel_size,
                dtype=claim.dtype,
                gpu_memory_utilization=claim.gpu_memory_utilization,
                max_model_len=claim.max_model_len,
            )
        )
        handle: VLLMContainerHandle | None = None
        try:
            handle = await self.runtime.prepare(
                launch_spec,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
                cpu_millicores=claim.cpu_millicores,
                memory_mb=claim.memory_mb,
            )
            handle = await self.runtime.start(handle)
            if handle.endpoint_url is None:
                raise RuntimeError("vLLM runtime did not publish an endpoint")
            if not await self._publish_loading(claim, handle.endpoint_url):
                await self._cleanup_runtime_handle(handle)
                return "stale"
            managed = _ManagedReplica(
                claim=claim,
                handle=handle,
                startup_deadline=asyncio.get_running_loop().time() + self.ready_timeout_seconds,
            )
            self._handles[claim.replica_id] = managed
            if await self._probe_ready(managed):
                return await self._publish_ready(managed)
            return "launched"
        except asyncio.CancelledError:
            if handle is not None:
                await asyncio.shield(self._cleanup_unpublished(claim, handle, "startup cancelled"))
            raise
        except Exception as exc:
            if handle is not None:
                await self._cleanup_runtime_handle(handle)
            self._handles.pop(claim.replica_id, None)
            accepted = await self._mark_terminal(
                claim,
                status=ReplicaStatus.FAILED,
                error_code=ErrorCode.MODEL_LOAD_FAILED.value,
                error_message=f"failed to launch vLLM container: {type(exc).__name__}",
            )
            self.logger.error(
                "vLLM replica launch failed",
                replica_id=str(claim.replica_id),
                execution_id=str(claim.execution_id),
                error_type=type(exc).__name__,
            )
            return "failed" if accepted else "stale"

    async def _advance_starting_replicas(self) -> tuple[int, int, int]:
        starting = [item for item in self._handles.values() if not item.published]
        ready = failed = stale = 0
        for item in starting:
            if await self._probe_ready(item):
                outcome = await self._publish_ready(item)
                ready += int(outcome == "ready")
                stale += int(outcome == "stale")
                continue
            if asyncio.get_running_loop().time() < item.startup_deadline:
                continue
            outcome = await self._terminate_handle(
                item,
                status=ReplicaStatus.FAILED,
                error_code=ErrorCode.MODEL_LOAD_TIMEOUT.value,
                error_message="vLLM container did not become ready before the startup deadline",
            )
            failed += int(outcome == "failed")
            stale += int(outcome == "stale")
        return ready, failed, stale

    async def _probe_ready(self, item: _ManagedReplica) -> bool:
        assert item.handle.endpoint_url is not None
        try:
            response = await self.http_client.get(
                f"{item.handle.endpoint_url.rstrip('/')}/health",
                timeout=self.probe_timeout,
            )
            return 200 <= response.status_code < 300
        except (httpx.TimeoutException, httpx.RequestError):
            return False

    async def _publish_loading(self, claim: VLLMReplicaClaim, endpoint_url: str) -> bool:
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

    async def _publish_ready(self, item: _ManagedReplica) -> str:
        claim = item.claim
        assert item.handle.endpoint_url is not None
        async with self.database.session() as session, session.begin():
            accepted = await ServiceRepository.mark_replica_running(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                endpoint_url=item.handle.endpoint_url,
                model_revision=claim.model_revision,
                image_digest=item.handle.image_digest,
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
        if not accepted:
            await self._terminate_handle(
                item,
                status=ReplicaStatus.LOST,
                error_message="vLLM replica ownership became stale during startup",
            )
            return "stale"
        item.published = True
        return "ready"

    async def _fail_exited_replicas(self) -> int:
        failed = 0
        for item in list(self._handles.values()):
            state = await self.runtime.inspect(item.handle)
            if state.running:
                continue
            if state.oom_killed:
                message = "vLLM container was terminated by the out-of-memory killer"
            else:
                message = f"vLLM container exited unexpectedly with code {state.exit_code}"
            outcome = await self._terminate_handle(
                item,
                status=ReplicaStatus.FAILED,
                error_code=(
                    ErrorCode.OOM_KILLED.value
                    if state.oom_killed
                    else ErrorCode.REPLICA_UNHEALTHY.value
                ),
                error_message=message,
                stop_first=False,
            )
            failed += int(outcome == "failed")
        return failed

    async def _renew_active_leases(self) -> int:
        if not self._handles:
            return 0
        stale: list[_ManagedReplica] = []
        async with self.database.session() as session, session.begin():
            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            for item in self._handles.values():
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
                    stale.append(item)
        for item in stale:
            await self._terminate_handle(
                item,
                status=ReplicaStatus.LOST,
                error_message="vLLM replica lease ownership became stale",
            )
        return len(stale)

    async def _stop_requested_replicas(self) -> int:
        async with self.database.session() as session:
            now = await database_utcnow(session)
            replicas = list(
                await session.scalars(
                    select(ServiceReplica).where(
                        ServiceReplica.worker_id == self.worker_id,
                        ServiceReplica.status.in_({ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}),
                        ServiceReplica.execution_id.is_not(None),
                        or_(
                            ServiceReplica.status == ReplicaStatus.STOPPING,
                            ServiceReplica.active_requests == 0,
                            ServiceReplica.drain_deadline.is_(None),
                            ServiceReplica.drain_deadline <= now,
                        ),
                    )
                )
            )
        stopped = 0
        for replica in replicas:
            assert replica.execution_id is not None
            item = self._handles.get(replica.id)
            if item is None:
                accepted = await self._mark_terminal_values(
                    replica_id=replica.id,
                    generation=replica.generation,
                    execution_id=replica.execution_id,
                    status=ReplicaStatus.STOPPED,
                    error_message=replica.error_message or "service replica stop requested",
                )
                stopped += int(accepted)
                continue
            outcome = await self._terminate_handle(
                item,
                status=ReplicaStatus.STOPPED,
                error_message=replica.error_message or "service replica stop requested",
            )
            stopped += int(outcome == "stopped")
        return stopped

    async def _terminate_handle(
        self,
        item: _ManagedReplica,
        *,
        status: ReplicaStatus,
        error_message: str,
        error_code: str | None = None,
        stop_first: bool = True,
    ) -> str:
        if stop_first:
            await self.runtime.stop(item.handle)
        await self.runtime.cleanup(item.handle)
        self._handles.pop(item.claim.replica_id, None)
        accepted = await self._mark_terminal(
            item.claim,
            status=status,
            error_code=error_code,
            error_message=error_message,
        )
        return status.value if accepted else "stale"

    async def _cleanup_unpublished(
        self,
        claim: VLLMReplicaClaim,
        handle: VLLMContainerHandle,
        reason: str,
    ) -> None:
        await self._cleanup_runtime_handle(handle)
        self._handles.pop(claim.replica_id, None)
        await self._mark_terminal(
            claim,
            status=ReplicaStatus.LOST,
            error_message=f"vLLM {reason}",
        )

    async def _cleanup_runtime_handle(self, handle: VLLMContainerHandle) -> None:
        try:
            await self.runtime.stop(handle)
        finally:
            await self.runtime.cleanup(handle)

    async def _mark_terminal(
        self,
        claim: VLLMReplicaClaim,
        *,
        status: ReplicaStatus,
        error_message: str,
        error_code: str | None = None,
    ) -> bool:
        return await self._mark_terminal_values(
            replica_id=claim.replica_id,
            generation=claim.generation,
            execution_id=claim.execution_id,
            status=status,
            error_code=error_code,
            error_message=error_message,
        )

    async def _mark_terminal_values(
        self,
        *,
        replica_id: uuid.UUID,
        generation: int,
        execution_id: uuid.UUID,
        status: ReplicaStatus,
        error_code: str | None = None,
        error_message: str | None,
    ) -> bool:
        async with self.database.session() as session, session.begin():
            return await ServiceRepository.mark_replica_terminal(
                session,
                replica_id=replica_id,
                generation=generation,
                execution_id=execution_id,
                status=status,
                error_code=error_code,
                error_message=error_message,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )


def _record_scheduling_explain(
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


def _clear_scheduling_explain(service: ModelService, *, now: datetime) -> None:
    if service.scheduling_reason is None and not service.scheduling_details:
        return
    service.scheduling_reason = None
    service.scheduling_details = {}
    service.updated_at = now
    service.version += 1


def _label_uuid(handle: VLLMContainerHandle, key: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(handle.labels.get(key, ""))
    except ValueError:
        return None


def _labels_match_replica(handle: VLLMContainerHandle, replica: ServiceReplica) -> bool:
    return (
        handle.labels.get(SERVICE_ID_LABEL) == str(replica.service_id)
        and handle.labels.get(REPLICA_ID_LABEL) == str(replica.id)
        and handle.labels.get(GENERATION_LABEL) == str(replica.generation)
        and handle.labels.get(EXECUTION_ID_LABEL) == str(replica.execution_id)
        and handle.labels.get(WORKER_SESSION_ID_LABEL) is not None
    )
