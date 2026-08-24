from __future__ import annotations

import asyncio
import os
import socket
import sys
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import psutil
from sqlalchemy import Select, func, or_, select

from core.database import Database
from core.enums import RuntimeType, WorkerStatus
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


@dataclass(frozen=True, slots=True)
class FakeReplicaClaim:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    model: str


@dataclass(frozen=True, slots=True)
class FakeRuntimeRunResult:
    claimed: int = 0
    started: int = 0
    stopped: int = 0
    failed: int = 0
    stale: int = 0


@dataclass(slots=True)
class _ManagedReplica:
    claim: FakeReplicaClaim
    endpoint_url: str
    process: asyncio.subprocess.Process
    monitor_task: asyncio.Task[None] | None = None
    expected_stop: bool = False


class FakeReplicaRuntimeController:
    """Run fake model replicas as fenced local subprocesses in development and tests.

    This controller is intentionally not a general Worker implementation. It only
    consumes services whose serving and execution runtimes are both ``fake`` and
    publishes loopback endpoints after their readiness probe succeeds.
    """

    def __init__(
        self,
        database: Database,
        *,
        app_env: str,
        http_client: httpx.AsyncClient | None = None,
        worker_id: str | None = None,
        python_executable: str = sys.executable,
        script_path: str | Path | None = None,
        batch_size: int = 10,
        ready_timeout_seconds: float = 10.0,
        stop_timeout_seconds: float = 5.0,
        probe_interval_seconds: float = 0.1,
        inference_delay_seconds: float = 0.0,
        lease_seconds: float = 3600.0,
    ) -> None:
        if app_env not in {"development", "test"}:
            raise ValueError("fake replica runtime is prohibited outside development and test")
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if ready_timeout_seconds <= 0 or stop_timeout_seconds <= 0:
            raise ValueError("ready and stop timeouts must be positive")
        if probe_interval_seconds <= 0:
            raise ValueError("probe_interval_seconds must be positive")
        if inference_delay_seconds < 0:
            raise ValueError("inference_delay_seconds must not be negative")
        if lease_seconds <= ready_timeout_seconds:
            raise ValueError("lease_seconds must be greater than ready_timeout_seconds")
        if not python_executable.strip():
            raise ValueError("python_executable must not be blank")
        resolved_worker_id = worker_id or f"fake-local-{socket.gethostname()}"
        if not resolved_worker_id.strip() or len(resolved_worker_id) > 255:
            raise ValueError("worker_id must contain 1 to 255 characters")

        resolved_script = (
            Path(script_path)
            if script_path is not None
            else Path(__file__).resolve().parents[2] / "scripts" / "fake_inference.py"
        ).resolve()
        if not resolved_script.is_file():
            raise ValueError(f"fake inference script does not exist: {resolved_script}")

        self.database = database
        self.worker_id = resolved_worker_id.strip()
        self.worker_session_id = uuid.uuid4()
        self.python_executable = python_executable
        self.script_path = resolved_script
        self.batch_size = batch_size
        self.ready_timeout_seconds = ready_timeout_seconds
        self.stop_timeout_seconds = stop_timeout_seconds
        self.probe_interval_seconds = probe_interval_seconds
        self.inference_delay_seconds = inference_delay_seconds
        self.lease_seconds = lease_seconds
        self.http_client = http_client or httpx.AsyncClient()
        self._owns_http_client = http_client is None
        self._handles: dict[uuid.UUID, _ManagedReplica] = {}
        self._lock = asyncio.Lock()
        self._cycle_lock = asyncio.Lock()
        self._registered = False
        self._closed = False
        self.logger = get_logger("fake_replica_runtime")

    @property
    def active_process_count(self) -> int:
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
                ModelService.runtime == ServingRuntime.FAKE,
                ModelService.runtime_type == RuntimeType.FAKE,
                ModelService.desired_replicas > 0,
                pending_exists,
            )
            .order_by(ModelService.updated_at, ModelService.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def run_once(self) -> FakeRuntimeRunResult:
        async with self._cycle_lock:
            if self._closed:
                raise RuntimeError("fake replica runtime is closed")
            await self._ensure_worker()
            stopped = await self._stop_requested_replicas()
            stale = await self._renew_active_leases()
            claims = await self._claim_pending_replicas()
            if not claims:
                return FakeRuntimeRunResult(stopped=stopped, stale=stale)

            outcomes = await asyncio.gather(*(self._launch(claim) for claim in claims))
            result = FakeRuntimeRunResult(
                claimed=len(claims),
                started=sum(outcome == "started" for outcome in outcomes),
                stopped=stopped + sum(outcome == "stopped" for outcome in outcomes),
                failed=sum(outcome == "failed" for outcome in outcomes),
                stale=stale + sum(outcome == "stale" for outcome in outcomes),
            )
            self.logger.info(
                "fake model replica runtime cycle completed",
                worker_id=self.worker_id,
                claimed=result.claimed,
                started=result.started,
                stopped=result.stopped,
                failed=result.failed,
                stale=result.stale,
            )
            return result

    async def close(self) -> None:
        async with self._cycle_lock:
            async with self._lock:
                if self._closed:
                    return
                self._closed = True
                handles = list(self._handles.values())
            await asyncio.gather(
                *(
                    self._stop_handle(
                        handle,
                        status=ReplicaStatus.STOPPED,
                        error_message="fake runtime controller closed",
                    )
                    for handle in handles
                )
            )
            monitor_tasks = [
                handle.monitor_task for handle in handles if handle.monitor_task is not None
            ]
            if monitor_tasks:
                await asyncio.gather(*monitor_tasks, return_exceptions=True)
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

    async def _ensure_worker(self) -> None:
        if not self._registered:
            async with self.database.session() as session, session.begin():
                await WorkerRepository.register(
                    session,
                    worker_id=self.worker_id,
                    worker_session_id=self.worker_session_id,
                    hostname="localhost",
                    node_name="localhost",
                    concurrency=self.batch_size,
                    cpu_count=max(1, os.cpu_count() or 1),
                    memory_total_mb=1024,
                    docker_version=None,
                    labels={
                        "managed-by": "fake-replica-runtime",
                        "runtime": RuntimeType.FAKE.value,
                    },
                    runtime_types=[RuntimeType.FAKE.value],
                    gpu_count=0,
                    gpu_model=None,
                    gpu_memory_mb=0,
                )
            self._registered = True
            await self._recover_owned_orphans()
            return

        async with self.database.session() as session, session.begin():
            worker = await WorkerRepository.heartbeat(
                session,
                self.worker_id,
                self.active_process_count,
                worker_session_id=self.worker_session_id,
            )
        if worker is None:
            raise RuntimeError("fake runtime virtual worker session is stale")

    async def _recover_owned_orphans(self) -> None:
        async with self.database.session() as session:
            rows = list(
                await session.execute(
                    select(ServiceReplica, ModelService.model)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ModelService.runtime == ServingRuntime.FAKE,
                        ModelService.runtime_type == RuntimeType.FAKE,
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
        for replica, model in rows:
            assert replica.execution_id is not None
            await asyncio.to_thread(
                self._terminate_owned_orphan_process,
                replica.endpoint_url,
                model,
                replica.id,
                replica.execution_id,
            )
            status = (
                ReplicaStatus.STOPPED
                if replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                else ReplicaStatus.LOST
            )
            async with self.database.session() as session, session.begin():
                await ServiceRepository.mark_replica_terminal(
                    session,
                    replica_id=replica.id,
                    generation=replica.generation,
                    execution_id=replica.execution_id,
                    status=status,
                    error_message="fake runtime restarted without a local process handle",
                    worker_id=self.worker_id,
                    worker_session_id=self.worker_session_id,
                )

    def _terminate_owned_orphan_process(
        self,
        endpoint_url: str | None,
        model: str,
        replica_id: uuid.UUID,
        execution_id: uuid.UUID,
    ) -> bool:
        """Best-effort cleanup of a fenced fake server from an older controller.

        New processes are matched by the exact script, model and fenced
        replica/execution identity, so recovery also works before an endpoint is
        persisted. Legacy processes without identity arguments additionally
        require their persisted loopback port. PID reuse must never make recovery
        terminate an unrelated Python process.
        """

        port: int | None = None
        if endpoint_url is not None:
            parsed = urlsplit(endpoint_url)
            try:
                port = parsed.port
            except ValueError:
                return False
            if parsed.hostname not in {"127.0.0.1", "::1", "localhost"} or port is None:
                return False
        matched = False
        for process in psutil.process_iter(["cmdline"]):
            try:
                command = process.info.get("cmdline") or []
                if not _fake_process_matches(
                    command,
                    script_path=self.script_path,
                    port=port,
                    model=model,
                    replica_id=replica_id,
                    execution_id=execution_id,
                ):
                    continue
                matched = True
                process.terminate()
                try:
                    process.wait(timeout=self.stop_timeout_seconds)
                except psutil.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=self.stop_timeout_seconds)
            except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
        return matched

    async def _claim_pending_replicas(self) -> list[FakeReplicaClaim]:
        async with self.database.session() as session, session.begin():
            services = list(await session.scalars(self.claim_candidates_query(self.batch_size)))
            if not services:
                return []

            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            claims: list[FakeReplicaClaim] = []
            for service in services:
                remaining = self.batch_size - len(claims)
                if remaining == 0:
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
                claimable = min(remaining, max(0, service.desired_replicas - already_started))
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
                        claims.append(
                            FakeReplicaClaim(
                                service_id=service.id,
                                replica_id=replica.id,
                                generation=service.generation,
                                execution_id=execution_id,
                                model=service.model,
                            )
                        )
            return claims

    async def _renew_active_leases(self) -> int:
        async with self._lock:
            handles = [handle for handle in self._handles.values() if not handle.expected_stop]
        if not handles:
            return 0

        stale: list[_ManagedReplica] = []
        async with self.database.session() as session, session.begin():
            now = await database_utcnow(session)
            lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            for handle in handles:
                claim = handle.claim
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
                    stale.append(handle)

        if stale:
            await asyncio.gather(
                *(
                    self._stop_handle(
                        handle,
                        status=ReplicaStatus.LOST,
                        error_message="fake replica lease ownership became stale",
                    )
                    for handle in stale
                )
            )
        return len(stale)

    async def _launch(self, claim: FakeReplicaClaim) -> str:
        port = _allocate_loopback_port()
        try:
            process = await asyncio.create_subprocess_exec(
                self.python_executable,
                str(self.script_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--model",
                claim.model,
                "--replica-id",
                str(claim.replica_id),
                "--execution-id",
                str(claim.execution_id),
                "--delay-seconds",
                str(self.inference_delay_seconds),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except (OSError, RuntimeError) as exc:
            accepted, status = await self._mark_terminal(
                claim,
                default_status=ReplicaStatus.FAILED,
                error_code="MODEL_LOAD_FAILED",
                error_message=f"failed to start fake inference process: {type(exc).__name__}",
            )
            return status.value if accepted else "stale"

        handle = _ManagedReplica(
            claim=claim,
            endpoint_url=f"http://127.0.0.1:{port}",
            process=process,
        )
        async with self._lock:
            if self._closed:
                handle.expected_stop = True
            else:
                self._handles[claim.replica_id] = handle
        try:
            return await self._publish_when_ready(handle)
        except asyncio.CancelledError:
            await asyncio.shield(
                self._stop_handle(
                    handle,
                    status=ReplicaStatus.STOPPED,
                    error_message="fake runtime startup was cancelled",
                )
            )
            raise
        except Exception as exc:
            self.logger.exception(
                "fake replica startup failed",
                replica_id=str(claim.replica_id),
                execution_id=str(claim.execution_id),
                error_type=type(exc).__name__,
            )
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.FAILED,
                error_code="MODEL_LOAD_FAILED",
                error_message=f"fake replica startup failed: {type(exc).__name__}",
            )

    async def _publish_when_ready(self, handle: _ManagedReplica) -> str:
        claim = handle.claim
        if handle.expected_stop:
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.STOPPED,
                error_message="fake runtime controller closed during process startup",
            )

        async with self.database.session() as session, session.begin():
            accepted = await ServiceRepository.mark_replica_loading(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                endpoint_url=handle.endpoint_url,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )
        if not accepted:
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.STOPPED,
                error_message="fake replica ownership became stale after process start",
            )

        ready = await self._wait_until_ready(handle)
        if not ready:
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.FAILED,
                error_code="MODEL_LOAD_TIMEOUT",
                error_message="fake inference process did not become ready",
            )

        async with self.database.session() as session, session.begin():
            accepted = await ServiceRepository.mark_replica_running(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                endpoint_url=handle.endpoint_url,
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
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.STOPPED,
                error_message="fake replica ownership became stale during startup",
            )
        async with self._lock:
            if self._closed:
                handle.expected_stop = True
            else:
                handle.monitor_task = asyncio.create_task(
                    self._monitor(handle),
                    name=f"fake-replica-{claim.replica_id}",
                )
        if handle.expected_stop:
            return await self._stop_handle(
                handle,
                status=ReplicaStatus.STOPPED,
                error_message="fake runtime controller closed during process startup",
            )
        return "started"

    async def _wait_until_ready(self, handle: _ManagedReplica) -> bool:
        deadline = asyncio.get_running_loop().time() + self.ready_timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if handle.process.returncode is not None or self._closed:
                return False
            try:
                response = await self.http_client.get(
                    f"{handle.endpoint_url}/health",
                    timeout=min(1.0, self.ready_timeout_seconds),
                )
                if 200 <= response.status_code < 300:
                    return True
            except httpx.RequestError:
                pass
            await asyncio.sleep(self.probe_interval_seconds)
        return False

    async def _monitor(self, handle: _ManagedReplica) -> None:
        return_code = await handle.process.wait()
        while not handle.expected_stop:
            try:
                accepted, status = await self._mark_terminal(
                    handle.claim,
                    default_status=ReplicaStatus.FAILED,
                    error_code="REPLICA_UNHEALTHY",
                    error_message=f"fake inference process exited with code {return_code}",
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.exception(
                    "failed to persist fake replica process exit",
                    replica_id=str(handle.claim.replica_id),
                    execution_id=str(handle.claim.execution_id),
                    error_type=type(exc).__name__,
                )
                await asyncio.sleep(max(0.1, self.probe_interval_seconds))
                continue
            async with self._lock:
                if self._handles.get(handle.claim.replica_id) is handle:
                    self._handles.pop(handle.claim.replica_id, None)
            if accepted:
                self.logger.warning(
                    "fake inference process exited",
                    replica_id=str(handle.claim.replica_id),
                    execution_id=str(handle.claim.execution_id),
                    status=status.value,
                    return_code=return_code,
                )
            return

    async def _stop_requested_replicas(self) -> int:
        async with self.database.session() as session:
            now = await database_utcnow(session)
            requested = list(
                await session.scalars(
                    select(ServiceReplica.id)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ModelService.runtime == ServingRuntime.FAKE,
                        ModelService.runtime_type == RuntimeType.FAKE,
                        ServiceReplica.worker_id == self.worker_id,
                        ServiceReplica.status.in_({ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}),
                        or_(
                            ServiceReplica.status == ReplicaStatus.STOPPING,
                            ServiceReplica.active_requests == 0,
                            ServiceReplica.drain_deadline.is_(None),
                            ServiceReplica.drain_deadline <= now,
                        ),
                    )
                )
            )
        async with self._lock:
            handles = [self._handles[item] for item in requested if item in self._handles]
        outcomes = await asyncio.gather(
            *(
                self._stop_handle(
                    handle,
                    status=ReplicaStatus.STOPPED,
                    error_message="fake replica stop requested",
                )
                for handle in handles
            )
        )
        return sum(outcome == "stopped" for outcome in outcomes)

    async def _stop_handle(
        self,
        handle: _ManagedReplica,
        *,
        status: ReplicaStatus,
        error_code: str | None = None,
        error_message: str,
    ) -> str:
        handle.expected_stop = True
        await self._terminate_process(handle.process)
        try:
            accepted, terminal_status = await self._mark_terminal(
                handle.claim,
                default_status=status,
                error_code=error_code,
                error_message=error_message,
            )
        finally:
            async with self._lock:
                if self._handles.get(handle.claim.replica_id) is handle:
                    self._handles.pop(handle.claim.replica_id, None)
        return terminal_status.value if accepted else "stale"

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            process.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=self.stop_timeout_seconds)
            return
        except TimeoutError:
            pass
        try:
            process.kill()
        except ProcessLookupError:
            return
        await process.wait()

    async def _mark_terminal(
        self,
        claim: FakeReplicaClaim,
        *,
        default_status: ReplicaStatus,
        error_code: str | None = None,
        error_message: str,
    ) -> tuple[bool, ReplicaStatus]:
        async with self.database.session() as session, session.begin():
            current_status = await session.scalar(
                select(ServiceReplica.status).where(ServiceReplica.id == claim.replica_id)
            )
            status = (
                ReplicaStatus.STOPPED
                if current_status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
                else default_status
            )
            accepted = await ServiceRepository.mark_replica_terminal(
                session,
                replica_id=claim.replica_id,
                generation=claim.generation,
                execution_id=claim.execution_id,
                status=status,
                error_code=error_code,
                error_message=error_message,
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
            )
        return accepted, status


def _allocate_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return int(port)


def _fake_process_matches(
    command: list[str],
    *,
    script_path: Path,
    port: int | None,
    model: str,
    replica_id: uuid.UUID,
    execution_id: uuid.UUID,
) -> bool:
    resolved_script = script_path.resolve()
    script_matches = False
    for argument in command:
        try:
            if Path(argument).resolve() == resolved_script:
                script_matches = True
                break
        except (OSError, ValueError):
            continue
    if not script_matches or _command_option(command, "--model") != model:
        return False

    process_replica_id = _command_option(command, "--replica-id")
    process_execution_id = _command_option(command, "--execution-id")
    if process_replica_id is None and process_execution_id is None:
        # Compatibility with Fake processes created before identity arguments
        # were added. A persisted loopback port is still required for safety.
        return port is not None and _command_option(command, "--port") == str(port)
    return process_replica_id == str(replica_id) and process_execution_id == str(execution_id)


def _command_option(command: list[str], name: str) -> str | None:
    try:
        index = command.index(name)
    except ValueError:
        return None
    return command[index + 1] if index + 1 < len(command) else None
