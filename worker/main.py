import asyncio
import signal
import time
import uuid
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

from redis.exceptions import RedisError

from core.accelerators import AcceleratorDevice
from core.artifacts import build_artifact_store
from core.config import Settings, get_settings
from core.database import Database
from core.enums import ACTIVE_TASK_STATUSES, WorkerStatus
from core.logging import configure_logging, get_logger
from core.redis import RedisQueue
from core.runtime_profiles import RuntimeProfileCatalog
from repositories.tasks import RecoverableRuntimeExecution, TaskRepository
from repositories.workers import WorkerRepository
from scheduler import Scheduler, TaskAssignment
from worker.artifact_workspace import ArtifactWorkspaceManager
from worker.capabilities import detect_capabilities
from worker.docker_runtime import DockerRuntime
from worker.executor import TaskExecutor
from worker.fake_runtime import FakeComputeRuntime
from worker.gpu_inventory import (
    InventoryProviderResult,
    InventorySnapshot,
    InventoryStatus,
    NoGPUInventoryProvider,
    bind_kubernetes_runtime_profiles,
    build_accelerator_inventory_registry,
)
from worker.heartbeat import ActiveExecution, Heartbeat
from worker.kubernetes_runtime import (
    EXECUTION_ID_LABEL,
    TASK_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    KubernetesRuntime,
    KubernetesRuntimeError,
)
from worker.runtime import ComputeRuntime, RuntimeHandle
from worker.runtime_registry import RuntimeRegistry


def _inventory_payload(devices: Iterable[AcceleratorDevice]) -> list[dict[str, object]]:
    return [
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
            "health": item.health,
            "fake": item.fake,
        }
        for item in devices
    ]


class WorkerService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = Database(settings.database_url)
        self.queue = RedisQueue(
            settings.redis_url,
            log_stream_maxlen=settings.log_stream_maxlen,
            log_stream_ttl_seconds=settings.log_stream_ttl_seconds,
            ready_stream_maxlen=settings.ready_stream_maxlen,
            socket_timeout=settings.redis_socket_timeout,
        )
        self.logger = get_logger("worker")
        self.capabilities = detect_capabilities(NoGPUInventoryProvider())
        self.worker_id = settings.worker_id or (
            f"{self.capabilities.hostname}-{uuid.uuid4().hex[:12]}"
        )
        self.worker_session_id = uuid.uuid4()
        runtime_types = tuple(
            item.strip() for item in settings.worker_runtime_types.split(",") if item.strip()
        )
        runtime_profile_catalog = (
            RuntimeProfileCatalog.from_path(Path(settings.runtime_profile_manifest_path))
            if "kubernetes" in runtime_types
            else None
        )
        self.inventory_registry = build_accelerator_inventory_registry(
            settings, worker_id=self.worker_id
        )
        self.runtime_profile_catalog = runtime_profile_catalog
        self.inventory_refresh_enabled = any(
            provider.name == "kubernetes-node" for provider in self.inventory_registry.providers
        )
        self.inventory_provider_results: tuple[InventoryProviderResult, ...] = ()
        self.gpu_devices: tuple[AcceleratorDevice, ...] = ()
        if settings.fake_gpu_count and "fake" not in runtime_types:
            raise ValueError("fake GPU inventory requires the fake compute runtime")
        runtimes: dict[str, ComputeRuntime] = {}
        self.docker_runtime: DockerRuntime | None = None
        self.kubernetes_runtime: KubernetesRuntime | None = None
        if "docker" in runtime_types:
            self.docker_runtime = DockerRuntime(
                pids_limit=settings.docker_pids_limit,
                tmpfs_size_mb=settings.docker_tmpfs_size_mb,
                stop_timeout=settings.docker_stop_timeout,
                always_pull=settings.docker_always_pull,
                cluster_id=settings.cluster_id,
            )
            runtimes["docker"] = self.docker_runtime
        if "kubernetes" in runtime_types:
            assert runtime_profile_catalog is not None
            self.kubernetes_runtime = KubernetesRuntime(
                namespace=settings.kubernetes_namespace,
                cluster_id=settings.cluster_id,
                app_env=settings.app_env,
                node_name=settings.worker_node_name or self.capabilities.hostname,
                cleanup_grace_seconds=settings.kubernetes_cleanup_grace_seconds,
                kubeconfig=settings.kubernetes_kubeconfig,
                in_cluster=settings.kubernetes_in_cluster,
                service_account_name=settings.kubernetes_serving_service_account_name,
                image_pull_secrets=settings.kubernetes_serving_image_pull_secrets,
                worker_pod_namespace=settings.kubernetes_worker_pod_namespace,
                worker_statefulset_name=settings.kubernetes_worker_statefulset_name,
                runtime_profile_catalog=runtime_profile_catalog,
            )
            runtimes["kubernetes"] = self.kubernetes_runtime
        if "fake" in runtime_types:
            runtimes["fake"] = FakeComputeRuntime()
        self.runtime = RuntimeRegistry(runtimes)
        self.artifact_store = build_artifact_store(settings)
        self.artifact_workspace = ArtifactWorkspaceManager(
            self.database,
            settings,
            self.artifact_store,
        )
        self.scheduler = Scheduler(
            self.database.session,
            lease_seconds=settings.task_lease_seconds,
            fallback_limit=settings.batch_size,
            mode=settings.scheduler_mode,
            worker_session_id=self.worker_session_id,
            cpu_price_per_hour=settings.cpu_price_per_hour,
            memory_price_per_gb_hour=settings.memory_price_per_gb_hour,
            gpu_price_per_hour=settings.gpu_price_per_hour,
        )
        self.executor = TaskExecutor(
            self.database,
            self.queue,
            self.runtime,
            worker_id=self.worker_id,
            settings=settings,
            artifact_workspace=self.artifact_workspace,
            worker_session_id=self.worker_session_id,
        )
        self.active: dict[uuid.UUID, ActiveExecution] = {}
        self.inflight: set[asyncio.Task[None]] = set()
        self.stop_requested = asyncio.Event()
        self.heartbeat_stop = asyncio.Event()
        self.next_orphan_reconcile = 0.0
        self.recovery_kubernetes_handles: list[RuntimeHandle] = []
        self.recovered_kubernetes_executions: list[
            tuple[RecoverableRuntimeExecution, RuntimeHandle]
        ] = []

    async def run(self) -> None:
        await self._discover_recoverable_kubernetes_executions()
        inventory_snapshot, devices = await self._take_accelerator_inventory_snapshot()
        self._apply_accelerator_inventory(inventory_snapshot, devices)
        for result in self.inventory_provider_results:
            log = (
                self.logger.info
                if result.status == InventoryStatus.AVAILABLE
                else self.logger.warning
            )
            log(
                "accelerator inventory discovery",
                provider=result.provider,
                status=result.status.value,
                device_count=len(result.devices),
                rejected_rows=result.rejected_rows,
                reason=result.message,
            )
        docker_version = (
            await self.docker_runtime.version() if self.docker_runtime is not None else None
        )
        await self._register(docker_version)
        await self._transfer_recoverable_kubernetes_executions()
        for recovered, handle in self.recovered_kubernetes_executions:
            self._start_recovered_execution(recovered, handle)
        try:
            await self.queue.ensure_ready_group()
        except RedisError as exc:
            self.logger.warning(
                "Redis unavailable at startup; PostgreSQL claim fallback remains active",
                worker_id=self.worker_id,
                error=str(exc),
            )

        heartbeat = Heartbeat(
            self.database,
            worker_id=self.worker_id,
            active=self.active,
            settings=self.settings,
            worker_session_id=self.worker_session_id,
            refresh_inventory=(
                self._refresh_accelerator_inventory if self.inventory_refresh_enabled else None
            ),
        )
        heartbeat_task = asyncio.create_task(heartbeat.run(self.heartbeat_stop))
        self.logger.info(
            "worker online",
            worker_id=self.worker_id,
            concurrency=self.settings.worker_concurrency,
            docker_version=docker_version,
            gpu_count=self.capabilities.gpu_count,
        )
        try:
            await self._consume()
        finally:
            await self._shutdown(heartbeat_task)

    async def _discover_recoverable_kubernetes_executions(self) -> None:
        if self.kubernetes_runtime is None:
            return
        self.recovery_kubernetes_handles = list(
            await self.kubernetes_runtime.list_managed(worker_id=self.worker_id)
        )

    async def _transfer_recoverable_kubernetes_executions(self) -> None:
        if self.kubernetes_runtime is None or not self.recovery_kubernetes_handles:
            return
        controller_handles: list[RuntimeHandle] = []
        for handle in self.recovery_kubernetes_handles:
            try:
                controller_handles.append(
                    await self.kubernetes_runtime.transfer_controller(
                        handle,
                        controller_session_id=self.worker_session_id,
                    )
                )
            except KubernetesRuntimeError as exc:
                self.logger.warning(
                    "Kubernetes Job controller CAS transfer quarantined",
                    worker_id=self.worker_id,
                    runtime_object_id=handle.object_id,
                    error=str(exc),
                )
        observations = [
            {
                "task_id": handle.labels.get(TASK_ID_LABEL),
                "execution_id": handle.labels.get(EXECUTION_ID_LABEL),
                "worker_session_id": handle.labels.get(WORKER_SESSION_ID_LABEL),
                "controller_session_id": str(handle.controller_session_id),
                "namespace": handle.namespace,
                "resource_name": handle.object_id,
                "resource_uid": handle.resource_uid,
                "spec_hash": handle.spec_hash,
                "observed_pod_name": handle.observation.pod_name,
                "observed_pod_uid": handle.observation.pod_uid,
            }
            for handle in controller_handles
        ]
        if not observations:
            return
        async with self.database.session() as session, session.begin():
            matched = await TaskRepository.transfer_recoverable_kubernetes_executions(
                session,
                worker_id=self.worker_id,
                new_worker_session_id=self.worker_session_id,
                lease_seconds=self.settings.task_lease_seconds,
                observations=observations,
                kubernetes_cleanup_grace_seconds=self.settings.kubernetes_cleanup_grace_seconds,
            )
        by_execution_id = {
            uuid.UUID(handle.labels[EXECUTION_ID_LABEL]): handle for handle in controller_handles
        }
        self.recovered_kubernetes_executions = [
            (item, by_execution_id[item.execution_id]) for item in matched
        ]
        self.logger.info(
            "recoverable Kubernetes Job executions discovered",
            worker_id=self.worker_id,
            execution_count=len(matched),
            quarantined_count=(
                len(self.recovery_kubernetes_handles)
                - len(matched)
                + len(self.kubernetes_runtime.recovery_conflicts)
            ),
        )

    def request_stop(self) -> None:
        self.stop_requested.set()

    async def _consume(self) -> None:
        while not self.stop_requested.is_set():
            try:
                await self._maybe_reconcile_orphans()
            except Exception as exc:
                self.logger.exception(
                    "orphan reconciliation failed; task claims are paused",
                    worker_id=self.worker_id,
                    error=str(exc),
                )
                await asyncio.sleep(self.settings.scheduler_poll_interval)
                continue
            available = self.settings.worker_concurrency - len(self.active)
            if available <= 0:
                await self._wait_for_capacity()
                continue

            messages: list[tuple[str, uuid.UUID]] = []
            try:
                messages = await self.queue.reclaim_ready(
                    consumer=self.worker_id,
                    min_idle_ms=max(1000, int(self.settings.task_lease_seconds * 1000)),
                    count=available,
                )
                remaining = available - len(messages)
                if remaining > 0:
                    messages.extend(
                        await self.queue.read_ready(
                            consumer=self.worker_id,
                            count=remaining,
                            block_ms=max(1, int(self.settings.scheduler_poll_interval * 1000)),
                        )
                    )
            except RedisError as exc:
                self.logger.warning(
                    "Redis queue read failed; using PostgreSQL fallback",
                    worker_id=self.worker_id,
                    error=str(exc),
                )

            claimed = 0
            for message_id, task_id in messages:
                if len(self.active) >= self.settings.worker_concurrency:
                    break
                assignment = await self.scheduler.claim_for_worker(
                    worker_id=self.worker_id, message_task_id=task_id
                )
                if assignment is not None:
                    self._start_assignment(assignment)
                    claimed += 1
                try:
                    await self.queue.acknowledge_ready(message_id)
                except RedisError as exc:
                    self.logger.warning(
                        "Redis acknowledgement failed; duplicate delivery is safe",
                        task_id=str(task_id),
                        worker_id=self.worker_id,
                        error=str(exc),
                    )

            while (
                not self.stop_requested.is_set()
                and len(self.active) < self.settings.worker_concurrency
            ):
                assignment = await self.scheduler.claim_for_worker(worker_id=self.worker_id)
                if assignment is None:
                    break
                self._start_assignment(assignment)
                claimed += 1
            if claimed == 0 and not messages:
                await asyncio.sleep(self.settings.scheduler_poll_interval)

    def _start_assignment(self, assignment: TaskAssignment) -> None:
        execution = ActiveExecution(
            task_id=assignment.task_id,
            execution_id=assignment.execution_id,
            runtime_type=assignment.runtime_type,
        )
        self.active[assignment.task_id] = execution
        task = asyncio.create_task(self._run_assignment(execution))
        self.inflight.add(task)
        task.add_done_callback(self._assignment_done)
        self.logger.info(
            "task assigned",
            task_id=str(assignment.task_id),
            worker_id=self.worker_id,
            execution_id=str(assignment.execution_id),
            source=assignment.source.value,
        )

    def _start_recovered_execution(
        self,
        recovered: RecoverableRuntimeExecution,
        handle: RuntimeHandle,
    ) -> None:
        handle.observation.log_cursor_bytes = recovered.runtime_log_cursor_bytes
        execution = ActiveExecution(
            task_id=recovered.task_id,
            execution_id=recovered.execution_id,
            runtime_type=handle.runtime_type,
        )
        execution.runtime_handle_durable.set()
        self.active[recovered.task_id] = execution
        task = asyncio.create_task(self._run_assignment(execution, recovered_handle=handle))
        self.inflight.add(task)
        task.add_done_callback(self._assignment_done)
        self.logger.info(
            "Kubernetes Job execution adopted after worker restart",
            task_id=str(recovered.task_id),
            worker_id=self.worker_id,
            execution_id=str(recovered.execution_id),
            worker_session_id=str(recovered.worker_session_id),
        )

    def _assignment_done(self, task: asyncio.Task[None]) -> None:
        self.inflight.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self.logger.error(
                "assignment coroutine failed; lease recovery will reconcile database state",
                worker_id=self.worker_id,
                error=str(error),
                exc_info=error,
            )

    async def _run_assignment(
        self,
        execution: ActiveExecution,
        *,
        recovered_handle: RuntimeHandle | None = None,
    ) -> None:
        try:
            result = await self.executor.execute(
                execution,
                recovered_handle=recovered_handle,
            )
            self.logger.info(
                "task execution finished",
                task_id=str(execution.task_id),
                worker_id=self.worker_id,
                execution_id=str(execution.execution_id),
                accepted=result.accepted,
                status=result.status.value if result.status is not None else None,
                retry_scheduled=result.retry_scheduled,
            )
        finally:
            self.active.pop(execution.task_id, None)

    async def _wait_for_capacity(self) -> None:
        if not self.inflight:
            await asyncio.sleep(0.1)
            return
        await asyncio.wait(self.inflight, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)

    async def _register(self, docker_version: str | None) -> None:
        async with self.database.session() as session, session.begin():
            worker = await WorkerRepository.register(
                session,
                worker_id=self.worker_id,
                hostname=self.capabilities.hostname,
                concurrency=self.settings.worker_concurrency,
                cpu_count=self.capabilities.cpu_count,
                memory_total_mb=self.capabilities.memory_total_mb,
                docker_version=docker_version,
                labels=self.settings.worker_labels,
                gpu_count=self.capabilities.gpu_count,
                gpu_model=self.capabilities.gpu_model,
                gpu_memory_mb=self.capabilities.gpu_memory_mb,
                worker_session_id=self.worker_session_id,
                node_name=self.settings.worker_node_name or self.capabilities.hostname,
                runtime_types=list(self.runtime.runtime_types),
            )
            await WorkerRepository.replace_gpu_inventory(
                session,
                worker_id=self.worker_id,
                worker_session_id=worker.worker_session_id,
                devices=_inventory_payload(self.gpu_devices),
            )

    async def _refresh_accelerator_inventory(self) -> None:
        snapshot, devices = await self._take_accelerator_inventory_snapshot()
        async with self.database.session() as session, session.begin():
            await WorkerRepository.replace_gpu_inventory(
                session,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
                devices=_inventory_payload(devices),
            )
        self._apply_accelerator_inventory(snapshot, devices)
        for result in snapshot.provider_results:
            if result.status != InventoryStatus.AVAILABLE:
                self.logger.warning(
                    "accelerator inventory refresh degraded",
                    worker_id=self.worker_id,
                    provider=result.provider,
                    status=result.status.value,
                    rejected_rows=result.rejected_rows,
                    reason=result.message,
                )

    async def _take_accelerator_inventory_snapshot(
        self,
    ) -> tuple[InventorySnapshot, tuple[AcceleratorDevice, ...]]:
        try:
            snapshot = await self.inventory_registry.snapshot_async()
            devices = (
                bind_kubernetes_runtime_profiles(snapshot.devices, self.runtime_profile_catalog)
                if self.runtime_profile_catalog is not None
                else snapshot.devices
            )
        except Exception as exc:
            self.logger.exception(
                "accelerator inventory refresh failed; stale capacity is invalidated",
                worker_id=self.worker_id,
                error=str(exc),
            )
            snapshot = InventorySnapshot(
                devices=(),
                provider_results=(
                    InventoryProviderResult(
                        provider="inventory-refresh",
                        status=InventoryStatus.UNAVAILABLE,
                        message="refresh_failed",
                    ),
                ),
            )
            devices = ()
        return snapshot, devices

    def _apply_accelerator_inventory(
        self,
        snapshot: InventorySnapshot,
        devices: tuple[AcceleratorDevice, ...],
    ) -> None:
        self.inventory_provider_results = snapshot.provider_results
        self.gpu_devices = devices
        self.capabilities = replace(
            self.capabilities,
            gpu_count=len(devices),
            gpu_model=", ".join(dict.fromkeys(item.model for item in devices)) or None,
            gpu_memory_mb=sum(item.memory_total_mb for item in devices),
        )

    async def _maybe_reconcile_orphans(self) -> None:
        if self.docker_runtime is None:
            return
        now = time.monotonic()
        if now < self.next_orphan_reconcile:
            return
        containers = await self.docker_runtime.list_worker_managed_containers()
        cleanup_failures: list[str] = []
        for container in containers:
            labels = container.labels or {}
            raw_task_id = labels.get("mini-ai-cloud.task_id")
            raw_execution_id = labels.get("mini-ai-cloud.execution_id")
            container_worker_id = labels.get("mini-ai-cloud.worker_id")
            stale = True
            try:
                task_id = uuid.UUID(raw_task_id or "")
                execution_id = uuid.UUID(raw_execution_id or "")
            except ValueError:
                task_id = None
                execution_id = None

            if task_id is not None and execution_id is not None:
                async with self.database.session() as session:
                    task = await TaskRepository.get(session, task_id)
                stale = not (
                    task is not None
                    and task.status in ACTIVE_TASK_STATUSES
                    and task.execution_id == execution_id
                    and task.worker_id == container_worker_id
                )
                if container_worker_id == self.worker_id:
                    active = self.active.get(task_id)
                    stale = stale or active is None or active.execution_id != execution_id

            if not stale:
                continue
            try:
                if container.status in {"running", "restarting", "paused"}:
                    await self.docker_runtime.stop_container(container)
                await self.docker_runtime.remove_container(container)
                self.logger.warning(
                    "stale managed container removed",
                    container_id=container.id,
                    task_id=raw_task_id,
                    execution_id=raw_execution_id,
                )
            except Exception as exc:
                cleanup_failures.append(f"{container.id}: {exc}")

        self.next_orphan_reconcile = now + self.settings.orphan_reconcile_interval
        if cleanup_failures:
            raise RuntimeError("; ".join(cleanup_failures))

    async def _shutdown(self, heartbeat_task: asyncio.Task[None]) -> None:
        self.logger.info("worker draining", worker_id=self.worker_id)
        await self._best_effort_worker_status(WorkerStatus.DRAINING)

        if self.inflight:
            _done, pending = await asyncio.wait(
                self.inflight, timeout=self.settings.worker_shutdown_timeout
            )
            if pending:
                relinquish_kubernetes = await self._replacement_worker_expected()
                relinquished_task_ids: set[uuid.UUID] = set()
                if relinquish_kubernetes:
                    try:
                        relinquished_task_ids = await self._preserve_kubernetes_handoff_leases()
                    except Exception as exc:
                        self.logger.warning(
                            "Kubernetes Job handoff leases could not be fenced; "
                            "Jobs will be stopped",
                            worker_id=self.worker_id,
                            error=str(exc),
                        )
                self.logger.warning(
                    "shutdown deadline reached; stopping local runtimes and relinquishing "
                    "durable Kubernetes Jobs",
                    worker_id=self.worker_id,
                    active_tasks=[str(task_id) for task_id in self.active],
                )
                for execution in self.active.values():
                    if execution.task_id in relinquished_task_ids:
                        execution.relinquish_requested.set()
                    else:
                        execution.ownership_lost.set()
                _done, pending = await asyncio.wait(
                    pending, timeout=self.settings.docker_stop_timeout + 5
                )
                if pending:
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)

        self.heartbeat_stop.set()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        await self._best_effort_worker_status(WorkerStatus.OFFLINE)
        for name, close in (
            ("runtimes", self.runtime.close),
            ("redis", self.queue.close),
            ("database", self.database.dispose),
        ):
            try:
                await close()
            except Exception as exc:
                self.logger.error(
                    "worker resource cleanup failed",
                    worker_id=self.worker_id,
                    resource=name,
                    error=str(exc),
                )
        self.logger.info("worker offline", worker_id=self.worker_id)

    async def _replacement_worker_expected(self) -> bool:
        if self.kubernetes_runtime is None or not any(
            execution.runtime_type == "kubernetes" and execution.runtime_handle_durable.is_set()
            for execution in self.active.values()
        ):
            return False
        try:
            return await asyncio.wait_for(
                self.kubernetes_runtime.replacement_worker_expected(worker_id=self.worker_id),
                timeout=5.0,
            )
        except Exception as exc:
            self.logger.error(
                "worker replacement could not be proven; Kubernetes Jobs will be stopped",
                worker_id=self.worker_id,
                error=str(exc),
            )
            return False

    async def _preserve_kubernetes_handoff_leases(self) -> set[uuid.UUID]:
        """Fence reaper retries before preserving Jobs for a rolling restart."""

        preserved: set[uuid.UUID] = set()
        for execution in self.active.values():
            if (
                execution.runtime_type != "kubernetes"
                or not execution.runtime_handle_durable.is_set()
            ):
                continue
            try:
                async with self.database.session() as session, session.begin():
                    await TaskRepository.preserve_kubernetes_handoff_lease(
                        session,
                        task_id=execution.task_id,
                        worker_id=self.worker_id,
                        execution_id=execution.execution_id,
                        worker_session_id=self.worker_session_id,
                        cleanup_grace_seconds=self.settings.kubernetes_cleanup_grace_seconds,
                    )
            except Exception as exc:
                self.logger.warning(
                    "Kubernetes Job handoff lease could not be fenced; Job will be stopped",
                    task_id=str(execution.task_id),
                    execution_id=str(execution.execution_id),
                    error=str(exc),
                )
            else:
                preserved.add(execution.task_id)
        return preserved

    async def _best_effort_worker_status(self, status: WorkerStatus) -> None:
        try:
            async with self.database.session() as session, session.begin():
                await WorkerRepository.set_status(
                    session,
                    self.worker_id,
                    status,
                    worker_session_id=self.worker_session_id,
                )
        except Exception as exc:
            self.logger.error(
                "worker status update failed",
                worker_id=self.worker_id,
                status=status.value,
                error=str(exc),
            )


async def async_main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    service = WorkerService(settings)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, service.request_stop)
        except NotImplementedError:
            signal.signal(signum, lambda *_args: service.request_stop())
    await service.run()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
