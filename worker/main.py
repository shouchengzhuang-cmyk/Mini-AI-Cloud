import asyncio
import signal
import time
import uuid
from dataclasses import replace
from pathlib import Path

from redis.exceptions import RedisError

from core.artifacts import build_artifact_store
from core.config import Settings, get_settings
from core.database import Database
from core.enums import ACTIVE_TASK_STATUSES, WorkerStatus
from core.logging import configure_logging, get_logger
from core.redis import RedisQueue
from core.runtime_profiles import RuntimeProfileCatalog
from repositories.tasks import TaskRepository
from repositories.workers import WorkerRepository
from scheduler import Scheduler, TaskAssignment
from worker.artifact_workspace import ArtifactWorkspaceManager
from worker.capabilities import detect_capabilities
from worker.docker_runtime import DockerRuntime
from worker.executor import TaskExecutor
from worker.fake_runtime import FakeComputeRuntime
from worker.gpu_inventory import (
    InventoryStatus,
    NoGPUInventoryProvider,
    bind_kubernetes_runtime_profiles,
    build_accelerator_inventory_registry,
)
from worker.heartbeat import ActiveExecution, Heartbeat
from worker.kubernetes_runtime import KubernetesRuntime
from worker.runtime import ComputeRuntime
from worker.runtime_registry import RuntimeRegistry


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
        inventory_registry = build_accelerator_inventory_registry(
            settings, worker_id=self.worker_id
        )
        inventory_snapshot = inventory_registry.snapshot()
        self.inventory_provider_results = inventory_snapshot.provider_results
        self.gpu_devices = (
            bind_kubernetes_runtime_profiles(
                inventory_snapshot.devices,
                runtime_profile_catalog,
            )
            if runtime_profile_catalog is not None
            else inventory_snapshot.devices
        )
        self.capabilities = replace(
            self.capabilities,
            gpu_count=len(self.gpu_devices),
            gpu_model=", ".join(dict.fromkeys(item.model for item in self.gpu_devices)) or None,
            gpu_memory_mb=sum(item.memory_total_mb for item in self.gpu_devices),
        )
        if settings.fake_gpu_count and "fake" not in runtime_types:
            raise ValueError("fake GPU inventory requires the fake compute runtime")
        runtimes: dict[str, ComputeRuntime] = {}
        self.docker_runtime: DockerRuntime | None = None
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
            runtimes["kubernetes"] = KubernetesRuntime(
                namespace=settings.kubernetes_namespace,
                node_name=settings.worker_node_name or self.capabilities.hostname,
                cleanup_grace_seconds=settings.kubernetes_cleanup_grace_seconds,
                kubeconfig=settings.kubernetes_kubeconfig,
                in_cluster=settings.kubernetes_in_cluster,
                runtime_profile_catalog=runtime_profile_catalog,
            )
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

    async def run(self) -> None:
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
            task_id=assignment.task_id, execution_id=assignment.execution_id
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

    async def _run_assignment(self, execution: ActiveExecution) -> None:
        try:
            result = await self.executor.execute(execution)
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
                        "health": item.health,
                        "fake": item.fake,
                    }
                    for item in self.gpu_devices
                ],
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
                self.logger.warning(
                    "shutdown deadline reached; stopping active task containers",
                    worker_id=self.worker_id,
                    active_tasks=[str(task_id) for task_id in self.active],
                )
                for execution in self.active.values():
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
