from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import Table, event
from sqlalchemy.dialects import postgresql

from api.services.fake_replica_runtime import FakeReplicaRuntimeController
from core.database import Database
from core.enums import RuntimeType, WorkerStatus
from models.base import Base
from models.identity import Project, User
from models.outbox import OutboxEvent
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServingRuntime,
)
from models.usage import ProjectQuota, ProjectQuotaState
from models.worker import Worker
from repositories.services import ServiceRepository

PROJECT_ID = uuid.UUID("c0000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def fake_runtime_database(tmp_path: Path) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'fake-runtime.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                cast(Table, User.__table__),
                cast(Table, Project.__table__),
                cast(Table, ProjectQuota.__table__),
                cast(Table, ProjectQuotaState.__table__),
                cast(Table, Worker.__table__),
                cast(Table, ModelService.__table__),
                cast(Table, ServiceReplica.__table__),
                cast(Table, OutboxEvent.__table__),
            ],
        )
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_ID, name="Fake Runtime Tests", slug="fake-runtime-tests"))
    try:
        yield database
    finally:
        await database.dispose()


async def _create_service(
    database: Database,
    *,
    runtime: ServingRuntime = ServingRuntime.FAKE,
    runtime_type: RuntimeType = RuntimeType.FAKE,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name=f"fake-{uuid.uuid4().hex[:8]}",
            model="fake/local-model",
            runtime=runtime,
            runtime_type=runtime_type,
            image=None,
            cpu_millicores=100,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=1,
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id


async def _replica(database: Database, service_id: uuid.UUID) -> ServiceReplica:
    async with database.session() as session:
        replicas = await ServiceRepository.list_replicas(session, service_id)
    assert len(replicas) == 1
    return replicas[0]


def test_fake_runtime_claims_use_postgresql_skip_locked() -> None:
    compiled = str(
        FakeReplicaRuntimeController.claim_candidates_query(10).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "model_services.runtime" in compiled
    assert "model_services.runtime_type" in compiled


async def test_fake_runtime_starts_ready_replica_and_stops_scale_down(
    fake_runtime_database: Database,
) -> None:
    service_id = await _create_service(fake_runtime_database)
    controller = FakeReplicaRuntimeController(
        fake_runtime_database,
        app_env="test",
        ready_timeout_seconds=10,
        stop_timeout_seconds=2,
        probe_interval_seconds=0.05,
    )
    try:
        started = await controller.run_once()
        replica = await _replica(fake_runtime_database, service_id)

        assert started.claimed == 1
        assert started.started == 1
        assert controller.active_process_count == 1
        assert replica.status == ReplicaStatus.RUNNING
        assert replica.health == ReplicaHealth.HEALTHY
        assert replica.worker_id == controller.worker_id
        assert replica.execution_id is not None
        assert replica.endpoint_url is not None
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{replica.endpoint_url}/v1/chat/completions",
                json={
                    "model": "fake/local-model",
                    "messages": [{"role": "user", "content": "hello"}],
                },
                timeout=3,
            )
        assert response.status_code == 200
        assert response.json()["choices"][0]["message"]["content"] == "fake response: hello"

        async with fake_runtime_database.session() as session, session.begin():
            service = await ServiceRepository.set_desired_replicas(
                session,
                service_id=service_id,
                project_id=PROJECT_ID,
                desired_replicas=0,
            )
            assert service is not None
            await ServiceRepository.reconcile_locked(session, service)

        stopped = await controller.run_once()
        replica = await _replica(fake_runtime_database, service_id)
        assert stopped.stopped == 1
        assert replica.status == ReplicaStatus.STOPPED
        assert replica.endpoint_url is None
        assert controller.active_process_count == 0
    finally:
        await controller.close()

    async with fake_runtime_database.session() as session:
        worker = await session.get(Worker, controller.worker_id)
    assert worker is not None
    assert worker.runtime_types == [RuntimeType.FAKE.value]
    assert worker.status == WorkerStatus.OFFLINE


async def test_fake_runtime_monitors_unexpected_process_exit(
    fake_runtime_database: Database,
) -> None:
    service_id = await _create_service(fake_runtime_database)
    controller = FakeReplicaRuntimeController(
        fake_runtime_database,
        app_env="test",
        ready_timeout_seconds=10,
        stop_timeout_seconds=2,
        probe_interval_seconds=0.05,
    )
    try:
        result = await controller.run_once()
        assert result.started == 1
        replica = await _replica(fake_runtime_database, service_id)
        handle = controller._handles[replica.id]
        handle.process.kill()

        for _ in range(100):
            replica = await _replica(fake_runtime_database, service_id)
            if replica.status == ReplicaStatus.FAILED:
                break
            await asyncio.sleep(0.05)

        assert replica.status == ReplicaStatus.FAILED
        assert replica.endpoint_url is None
        assert replica.error_message is not None
        assert "exited with code" in replica.error_message
        assert controller.active_process_count == 0
    finally:
        await controller.close()


async def test_fake_runtime_close_terminates_active_process_and_fences_terminal_state(
    fake_runtime_database: Database,
) -> None:
    service_id = await _create_service(fake_runtime_database)
    controller = FakeReplicaRuntimeController(
        fake_runtime_database,
        app_env="test",
        ready_timeout_seconds=10,
        stop_timeout_seconds=2,
        probe_interval_seconds=0.05,
    )
    result = await controller.run_once()
    assert result.started == 1
    replica = await _replica(fake_runtime_database, service_id)
    process = controller._handles[replica.id].process

    await controller.close()
    replica = await _replica(fake_runtime_database, service_id)

    assert process.returncode is not None
    assert controller.active_process_count == 0
    assert replica.status == ReplicaStatus.STOPPED
    assert replica.endpoint_url is None
    with pytest.raises(RuntimeError, match="closed"):
        await controller.run_once()
    await controller.close()


async def test_fake_runtime_fences_process_start_failure(
    fake_runtime_database: Database,
) -> None:
    service_id = await _create_service(fake_runtime_database)
    controller = FakeReplicaRuntimeController(
        fake_runtime_database,
        app_env="test",
        python_executable="/path/that/does/not/exist/python",
        ready_timeout_seconds=1,
    )
    try:
        result = await controller.run_once()
        replica = await _replica(fake_runtime_database, service_id)

        assert result.claimed == 1
        assert result.failed == 1
        assert replica.status == ReplicaStatus.FAILED
        assert replica.execution_id is not None
        assert replica.worker_id == controller.worker_id
        assert replica.error_message is not None
        assert "failed to start" in replica.error_message
    finally:
        await controller.close()


async def test_fake_runtime_cancellation_stops_claimed_process(
    fake_runtime_database: Database,
) -> None:
    service_id = await _create_service(fake_runtime_database)

    def never_ready(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(never_ready)) as client:
        controller = FakeReplicaRuntimeController(
            fake_runtime_database,
            app_env="test",
            http_client=client,
            ready_timeout_seconds=10,
            stop_timeout_seconds=2,
            probe_interval_seconds=0.05,
        )
        run = asyncio.create_task(controller.run_once())
        for _ in range(100):
            if controller.active_process_count == 1:
                break
            await asyncio.sleep(0.02)
        assert controller.active_process_count == 1
        process = next(iter(controller._handles.values())).process

        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

        replica = await _replica(fake_runtime_database, service_id)
        assert process.returncode is not None
        assert replica.status == ReplicaStatus.STOPPED
        assert replica.endpoint_url is None
        assert controller.active_process_count == 0
        await controller.close()


async def test_fake_runtime_ignores_services_without_both_fake_runtime_types(
    fake_runtime_database: Database,
) -> None:
    service_ids = [
        await _create_service(
            fake_runtime_database,
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.FAKE,
        ),
        await _create_service(
            fake_runtime_database,
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.DOCKER,
        ),
    ]
    controller = FakeReplicaRuntimeController(fake_runtime_database, app_env="development")
    try:
        result = await controller.run_once()
        assert result.claimed == 0
        assert controller.active_process_count == 0
        assert [
            (await _replica(fake_runtime_database, service_id)).status for service_id in service_ids
        ] == [ReplicaStatus.PENDING, ReplicaStatus.PENDING]
    finally:
        await controller.close()


def test_fake_runtime_is_prohibited_in_production() -> None:
    with pytest.raises(ValueError, match="prohibited"):
        FakeReplicaRuntimeController(cast(Database, object()), app_env="production")
