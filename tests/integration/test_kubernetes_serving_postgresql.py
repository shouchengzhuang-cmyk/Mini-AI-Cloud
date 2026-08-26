from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text

from api.services.kubernetes_replica_runtime import KubernetesReplicaRuntimeController
from core.database import Database
from core.enums import RuntimeType, WorkerStatus
from models.identity import Project
from models.outbox import OutboxEvent
from models.service import ModelService, ReplicaStatus, ServiceReplica, ServingRuntime
from models.worker import Worker
from repositories.quotas import QuotaRepository
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository
from worker.kubernetes_serving_runtime import (
    KubernetesServingHandle,
    KubernetesServingLaunchSpec,
    KubernetesServingRecoveryConflict,
    KubernetesServingState,
)

pytestmark = [pytest.mark.integration, pytest.mark.live]

DEFAULT_LIVE_DATABASE_URL = "postgresql+asyncpg://task:local-dev-only@127.0.0.1:5432/task_platform"


@pytest_asyncio.fixture
async def live_database() -> AsyncIterator[Database]:
    configured_url = os.getenv("LIVE_DATABASE_URL")
    database = Database(configured_url or DEFAULT_LIVE_DATABASE_URL)
    try:
        async with asyncio.timeout(2):
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
    except Exception as exc:
        await database.dispose()
        if configured_url:
            pytest.fail(f"configured live PostgreSQL is unavailable ({type(exc).__name__})")
        pytest.skip(f"live PostgreSQL is unavailable ({type(exc).__name__})")
    try:
        yield database
    finally:
        await database.dispose()


class _Runtime:
    def __init__(self, *, block_cleanup: bool = False) -> None:
        self.block_cleanup = block_cleanup
        self.cleanup_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_cancelled = asyncio.Event()
        self.force_cleaned: list[str] = []
        self.closed = False

    async def version(self) -> str:
        return "live-postgresql-test"

    async def prepare(
        self,
        spec: KubernetesServingLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
    ) -> KubernetesServingHandle:
        del spec, worker_id, worker_session_id
        raise AssertionError("prepare is not used by this integration test")

    async def start(self, handle: KubernetesServingHandle) -> KubernetesServingHandle:
        raise AssertionError(f"start is not used by this integration test: {handle.object_id}")

    async def inspect(self, handle: KubernetesServingHandle) -> KubernetesServingState:
        raise AssertionError(f"inspect is not used by this integration test: {handle.object_id}")

    async def request_stop(self, handle: KubernetesServingHandle) -> None:
        raise AssertionError(
            f"request_stop is not used by this integration test: {handle.object_id}"
        )

    async def force_cleanup(self, handle: KubernetesServingHandle) -> None:
        self.cleanup_started.set()
        if self.block_cleanup:
            try:
                await self.cleanup_release.wait()
            except asyncio.CancelledError:
                self.cleanup_cancelled.set()
                raise
        self.force_cleaned.append(handle.object_id)

    async def list_managed(self, *, worker_id: str) -> Sequence[KubernetesServingHandle]:
        del worker_id
        return ()

    @property
    def recovery_conflicts(self) -> Sequence[KubernetesServingRecoveryConflict]:
        return ()

    async def close(self) -> None:
        self.closed = True


def _controller(
    database: Database,
    runtime: _Runtime,
    *,
    worker_id: str,
) -> KubernetesReplicaRuntimeController:
    return KubernetesReplicaRuntimeController(
        database,
        runtime,
        app_env="test",
        cluster_id="live-postgresql",
        image="mini-ai-cloud:postgresql-test",
        fake_enabled=True,
        worker_id=worker_id,
        batch_size=1,
        startup_timeout_seconds=1,
        drain_timeout_seconds=1,
        poll_interval_seconds=0.05,
        lease_seconds=5,
        failure_backoff_seconds=1,
        termination_grace_seconds=1,
    )


async def _create_service(database: Database) -> tuple[uuid.UUID, uuid.UUID]:
    project_id = uuid.uuid4()
    service_id: uuid.UUID
    run_id = uuid.uuid4().hex
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=f"Kubernetes PostgreSQL {run_id}",
                slug=f"kubernetes-postgresql-{run_id}",
            )
        )
        await session.flush()
        await QuotaRepository.initialize(session, project_id=project_id)
        service = await ServiceRepository.create(
            session,
            project_id=project_id,
            name="postgresql-controller",
            model="fake/postgresql-controller",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.KUBERNETES,
            image="mini-ai-cloud:postgresql-test",
            cpu_millicores=100,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=1,
        )
        await ServiceRepository.reconcile_locked(session, service)
        service_id = service.id
    return project_id, service_id


async def _register_worker(
    database: Database,
    *,
    worker_id: str,
    worker_session_id: uuid.UUID,
) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
            hostname=f"{worker_id}.invalid",
            node_name="live-postgresql",
            concurrency=1,
            cpu_count=1,
            memory_total_mb=128,
            docker_version="live-postgresql-test",
            labels={"live-test": "kubernetes-controller"},
            runtime_types=[RuntimeType.KUBERNETES.value],
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def _cleanup(
    database: Database,
    *,
    project_ids: Sequence[uuid.UUID] = (),
    service_ids: Sequence[uuid.UUID] = (),
    worker_ids: Sequence[str] = (),
) -> None:
    aggregate_ids: list[uuid.UUID] = list(service_ids)
    async with database.session() as session, session.begin():
        if service_ids:
            aggregate_ids.extend(
                await session.scalars(
                    select(ServiceReplica.id).where(ServiceReplica.service_id.in_(service_ids))
                )
            )
        if aggregate_ids:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(aggregate_ids))
            )
        if service_ids:
            await session.execute(delete(ModelService).where(ModelService.id.in_(service_ids)))
        if project_ids:
            await session.execute(delete(Project).where(Project.id.in_(project_ids)))
        if worker_ids:
            await session.execute(delete(Worker).where(Worker.id.in_(worker_ids)))


async def test_live_postgresql_concurrent_controllers_claim_one_replica(
    live_database: Database,
) -> None:
    project_id, service_id = await _create_service(live_database)
    worker_ids = (
        f"k8s-claim-a-{uuid.uuid4().hex[:12]}",
        f"k8s-claim-b-{uuid.uuid4().hex[:12]}",
    )
    controllers = [
        _controller(live_database, _Runtime(), worker_id=worker_id) for worker_id in worker_ids
    ]
    try:
        await asyncio.gather(*(controller._register_worker() for controller in controllers))
        start = asyncio.Event()

        async def contend(
            controller: KubernetesReplicaRuntimeController,
        ) -> int:
            await start.wait()
            claims, _waiting_backoff = await controller._claim_pending_replicas()
            return len(claims)

        contenders = [asyncio.create_task(contend(controller)) for controller in controllers]
        start.set()
        claim_counts = await asyncio.gather(*contenders)

        async with live_database.session() as session:
            replicas = await ServiceRepository.list_replicas(session, service_id)
        assert sum(claim_counts) == 1
        assert len(replicas) == 1
        assert replicas[0].status == ReplicaStatus.STARTING
        assert replicas[0].worker_id in worker_ids
        assert replicas[0].execution_id is not None
    finally:
        await _cleanup(
            live_database,
            project_ids=[project_id],
            service_ids=[service_id],
            worker_ids=worker_ids,
        )


async def test_live_postgresql_session_takeover_rejects_stale_replica_result(
    live_database: Database,
) -> None:
    project_id, service_id = await _create_service(live_database)
    worker_id = f"k8s-takeover-{uuid.uuid4().hex[:12]}"
    session_a = uuid.uuid4()
    session_b = uuid.uuid4()
    execution_id = uuid.uuid4()
    try:
        await _register_worker(
            live_database,
            worker_id=worker_id,
            worker_session_id=session_a,
        )
        async with live_database.session() as session, session.begin():
            replica = (await ServiceRepository.list_replicas(session, service_id))[0]
            replica_id = replica.id
            generation = replica.generation
            assert await ServiceRepository.bind_replica_execution(
                session,
                replica_id=replica_id,
                generation=generation,
                worker_id=worker_id,
                worker_session_id=session_a,
                execution_id=execution_id,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
            )

        await _register_worker(
            live_database,
            worker_id=worker_id,
            worker_session_id=session_b,
        )
        async with live_database.session() as session, session.begin():
            assert not await ServiceRepository.mark_replica_loading(
                session,
                replica_id=replica_id,
                generation=generation,
                execution_id=execution_id,
                endpoint_url="http://stale-controller.invalid:8000",
                worker_id=worker_id,
                worker_session_id=session_a,
            )
        async with live_database.session() as session, session.begin():
            assert await ServiceRepository.mark_replica_loading(
                session,
                replica_id=replica_id,
                generation=generation,
                execution_id=execution_id,
                endpoint_url="http://current-controller.invalid:8000",
                worker_id=worker_id,
                worker_session_id=session_b,
            )

        async with live_database.session() as session:
            worker = await WorkerRepository.get(session, worker_id)
            persisted_replica = await session.get(ServiceReplica, replica_id)
        assert worker is not None and worker.worker_session_id == session_b
        assert persisted_replica is not None
        assert persisted_replica.status == ReplicaStatus.LOADING
        assert persisted_replica.endpoint_url == "http://current-controller.invalid:8000"
        assert persisted_replica.execution_id == execution_id
    finally:
        await _cleanup(
            live_database,
            project_ids=[project_id],
            service_ids=[service_id],
            worker_ids=[worker_id],
        )


async def test_live_postgresql_cancelled_kubernetes_io_releases_fence_and_blocks_stale_delete(
    live_database: Database,
) -> None:
    worker_id = f"k8s-io-fence-{uuid.uuid4().hex[:12]}"
    runtime_a = _Runtime(block_cleanup=True)
    runtime_b = _Runtime()
    controller_a = _controller(live_database, runtime_a, worker_id=worker_id)
    controller_b = _controller(live_database, runtime_b, worker_id=worker_id)
    handle = KubernetesServingHandle(
        object_id="blocked-cleanup",
        display_id="blocked-cleanup",
        endpoint_url="http://blocked-cleanup.invalid:8000",
    )
    cleanup_task: asyncio.Task[bool] | None = None
    takeover_task: asyncio.Task[None] | None = None
    try:
        await controller_a._register_worker()
        cleanup_task = asyncio.create_task(controller_a._force_cleanup_if_current(handle))
        await asyncio.wait_for(runtime_a.cleanup_started.wait(), timeout=2)

        takeover_task = asyncio.create_task(controller_b._register_worker())
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(takeover_task), timeout=0.1)

        cleanup_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup_task
        await asyncio.wait_for(runtime_a.cleanup_cancelled.wait(), timeout=2)
        await asyncio.wait_for(takeover_task, timeout=2)

        assert not await controller_a._force_cleanup_if_current(handle)
        assert runtime_a.force_cleaned == []
        async with live_database.session() as session:
            worker = await WorkerRepository.get(session, worker_id)
        assert worker is not None
        assert worker.worker_session_id == controller_b.worker_session_id
    finally:
        for task in (cleanup_task, takeover_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await _cleanup(live_database, worker_ids=[worker_id])


async def test_live_postgresql_skip_locked_scan_bypasses_controller_fence(
    live_database: Database,
) -> None:
    run_id = uuid.uuid4().hex[:12]
    locked_worker_id = f"k8s-skip-locked-a-{run_id}"
    available_worker_id = f"k8s-skip-locked-b-{run_id}"
    worker_ids = [locked_worker_id, available_worker_id]
    row_locked = asyncio.Event()
    release_row = asyncio.Event()

    async def hold_worker_row() -> None:
        async with live_database.session() as session, session.begin():
            worker = await session.get(Worker, locked_worker_id, with_for_update=True)
            assert worker is not None
            row_locked.set()
            await release_row.wait()

    lock_task: asyncio.Task[None] | None = None
    try:
        for worker_id in worker_ids:
            await _register_worker(
                live_database,
                worker_id=worker_id,
                worker_session_id=uuid.uuid4(),
            )
        async with live_database.session() as session, session.begin():
            workers = list(await session.scalars(select(Worker).where(Worker.id.in_(worker_ids))))
            for worker in workers:
                worker.status = WorkerStatus.ONLINE
                worker.last_heartbeat_at = datetime.now(UTC) - timedelta(minutes=10)

        lock_task = asyncio.create_task(hold_worker_row())
        await asyncio.wait_for(row_locked.wait(), timeout=2)
        async with asyncio.timeout(2):
            async with live_database.session() as session, session.begin():
                marked = await WorkerRepository.mark_stale_offline(
                    session,
                    offline_timeout_seconds=60,
                    limit=1,
                )
        assert marked == [available_worker_id]
    finally:
        release_row.set()
        if lock_task is not None:
            await asyncio.gather(lock_task, return_exceptions=True)
        await _cleanup(live_database, worker_ids=worker_ids)
