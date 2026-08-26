import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event, select
from sqlalchemy.dialects import postgresql

from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import RuntimeType
from models.base import Base
from models.identity import Project, User
from models.outbox import OutboxEvent
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServiceStatus,
    ServingRuntime,
)
from models.usage import ProjectQuota, ProjectQuotaState
from models.worker import Worker
from repositories.quotas import QuotaExceededError, QuotaRepository
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository

PROJECT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def service_database(tmp_path: Any) -> AsyncIterator[Database]:
    path = (tmp_path / "services.sqlite3").as_posix()
    database = Database(f"sqlite+aiosqlite:///{path}?timeout=30")

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
        session.add(Project(id=PROJECT_ID, name="Service Tests", slug="service-tests"))

    try:
        yield database
    finally:
        await database.dispose()


async def _create_service(database: Database, *, replicas: int = 2) -> uuid.UUID:
    async with database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name=f"service-{uuid.uuid4().hex[:8]}",
            model="org/model-v1",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="example/vllm:test",
            cpu_millicores=1000,
            memory_mb=1024,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=replicas,
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id


async def _register_worker(database: Database, worker_id: str) -> None:
    async with database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=worker_id,
            hostname=f"{worker_id}.test",
            concurrency=4,
            cpu_count=4,
            memory_total_mb=8192,
            docker_version="test",
            labels={"runtime": "docker"},
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )


async def test_service_create_scale_stop_accounts_desired_commitment_once(
    service_database: Database,
) -> None:
    async with service_database.session() as session, session.begin():
        await QuotaRepository.initialize(session, project_id=PROJECT_ID)
        await QuotaRepository.replace(
            session,
            project_id=PROJECT_ID,
            max_queued_tasks=None,
            max_running_tasks=2,
            max_cpu_millicores=2_000,
            max_memory_mb=2_048,
            max_gpus=2,
            max_services=1,
            max_service_replicas=2,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="quota-service",
            model="org/model-v1",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="example/vllm:test",
            cpu_millicores=1_000,
            memory_mb=1_024,
            gpu_count=1,
            gpu_memory_mb=16_384,
            desired_replicas=1,
        )
        state = await session.get(ProjectQuotaState, PROJECT_ID)
        assert state is not None
        assert (state.service_count, state.service_replicas) == (1, 1)
        quota_version = state.version
        await ServiceRepository.reconcile_locked(session, service)
        await ServiceRepository.reconcile_locked(session, service)
        assert state.version == quota_version

        scaled = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service.id,
            project_id=PROJECT_ID,
            desired_replicas=2,
        )
        assert scaled is not None
        assert state.service_replicas == 2
        assert state.service_reserved_cpu_millicores == 2_000
        repeated_version = state.version
        await ServiceRepository.set_desired_replicas(
            session,
            service_id=service.id,
            project_id=PROJECT_ID,
            desired_replicas=2,
        )
        assert state.version == repeated_version

        with pytest.raises(QuotaExceededError) as services_exceeded:
            await ServiceRepository.create(
                session,
                project_id=PROJECT_ID,
                name="second-active-service",
                model="org/model-v2",
                runtime=ServingRuntime.VLLM,
                runtime_type=RuntimeType.DOCKER,
                image="example/vllm:test",
                cpu_millicores=1,
                memory_mb=16,
                gpu_count=0,
                gpu_memory_mb=0,
                desired_replicas=1,
            )
        assert services_exceeded.value.resource == "services"

        stopped = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service.id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert stopped is not None
        assert (state.service_count, state.service_replicas) == (0, 0)
        assert state.service_reserved_cpu_millicores == 0
        assert state.service_reserved_memory_mb == 0
        assert state.service_reserved_gpus == 0


def test_reconcile_candidates_use_postgresql_skip_locked() -> None:
    compiled = str(
        ServiceRepository.reconcile_candidates_query(25).compile(dialect=postgresql.dialect())
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "LIMIT" in compiled


def test_expired_lease_candidates_use_postgresql_skip_locked() -> None:
    compiled = str(
        ServiceRepository.expired_lease_services_query(25, now=datetime.now(UTC)).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "lease_expires_at" in compiled
    assert "LIMIT" in compiled


async def test_reconciliation_is_idempotent_and_records_typed_intent(
    service_database: Database,
) -> None:
    service_id = await _create_service(service_database)
    reconciler = ServiceReconciler(service_database)

    result = await reconciler.run_once()

    assert result.services_seen == 0
    assert result.replicas_created == 0
    async with service_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
        replicas = await ServiceRepository.list_replicas(session, service_id)
        events = list(
            await session.scalars(
                select(OutboxEvent).order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )

    assert service is not None
    assert service.status == ServiceStatus.DEPLOYING
    assert [(replica.generation, replica.ordinal) for replica in replicas] == [(1, 0), (1, 1)]
    assert [(item.aggregate_type, item.event_type) for item in events] == [
        ("service", "service.reconcile"),
        ("service_replica", "service.replica.created"),
        ("service_replica", "service.replica.created"),
    ]
    replica_table = cast(Table, ServiceReplica.__table__)
    constraint_names = {constraint.name for constraint in replica_table.constraints}
    assert "uq_service_replicas_generation_ordinal" in constraint_names


async def test_generation_fencing_health_and_round_robin_selection(
    service_database: Database,
) -> None:
    service_id = await _create_service(service_database)
    await _register_worker(service_database, "worker-a")
    await _register_worker(service_database, "worker-b")
    execution_ids = [uuid.uuid4(), uuid.uuid4()]
    endpoints = ["http://worker-a:8000", "http://worker-b:8000"]

    async with service_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(session, service_id, generation=1)
        for index, replica in enumerate(replicas):
            if index == 0:
                assert not await ServiceRepository.bind_replica_execution(
                    session,
                    replica_id=replica.id,
                    generation=1,
                    worker_id="worker-a",
                    execution_id=execution_ids[index],
                    lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
                )
            assert await ServiceRepository.bind_replica_execution(
                session,
                replica_id=replica.id,
                generation=1,
                worker_id=f"worker-{'ab'[index]}",
                execution_id=execution_ids[index],
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
            )
            if index == 0:
                assert not await ServiceRepository.bind_replica_execution(
                    session,
                    replica_id=replica.id,
                    generation=1,
                    worker_id="worker-b",
                    execution_id=execution_ids[index],
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
                )
                with pytest.raises(ValueError, match="absolute HTTP"):
                    await ServiceRepository.mark_replica_running(
                        session,
                        replica_id=replica.id,
                        generation=1,
                        execution_id=execution_ids[index],
                        endpoint_url="worker-a:8000",
                    )
            assert await ServiceRepository.mark_replica_running(
                session,
                replica_id=replica.id,
                generation=1,
                execution_id=execution_ids[index],
                endpoint_url=endpoints[index],
            )
            if index == 0:
                with pytest.raises(ValueError, match="healthy or unhealthy"):
                    await ServiceRepository.record_replica_health(
                        session,
                        replica_id=replica.id,
                        generation=1,
                        execution_id=execution_ids[index],
                        health=ReplicaHealth.UNKNOWN,
                    )
            assert await ServiceRepository.record_replica_health(
                session,
                replica_id=replica.id,
                generation=1,
                execution_id=execution_ids[index],
                health=ReplicaHealth.HEALTHY,
            )
        old_replica_ids = [replica.id for replica in replicas]

    selected_endpoints: list[str] = []
    for _ in range(3):
        async with service_database.session() as session, session.begin():
            selection = await ServiceRepository.choose_healthy_endpoint(
                session,
                service_id=service_id,
                project_id=PROJECT_ID,
            )
            assert selection is not None
            selected_endpoints.append(selection.endpoint_url)

    assert selected_endpoints == [endpoints[0], endpoints[1], endpoints[0]]
    async with service_database.session() as session, session.begin():
        assert await ServiceRepository.record_replica_health(
            session,
            replica_id=old_replica_ids[0],
            generation=1,
            execution_id=execution_ids[0],
            health=ReplicaHealth.UNHEALTHY,
            error_message="probe failed",
        )
        service = await ServiceRepository.get(session, service_id)
        selection = await ServiceRepository.choose_healthy_endpoint(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
        )
        assert service is not None and service.status == ServiceStatus.DEGRADED
        assert selection is not None and selection.endpoint_url == endpoints[1]
        assert await ServiceRepository.record_replica_health(
            session,
            replica_id=old_replica_ids[0],
            generation=1,
            execution_id=execution_ids[0],
            health=ReplicaHealth.HEALTHY,
        )

    async with service_database.session() as session:
        counts = (await ServiceRepository.counts_for_service_ids(session, [service_id]))[service_id]
        service = await ServiceRepository.get(session, service_id)
    assert counts.actual_replicas == 2
    assert counts.healthy_replicas == 2
    assert service is not None and service.status == ServiceStatus.RUNNING

    async with service_database.session() as session, session.begin():
        service = await ServiceRepository.get(session, service_id, for_update=True)
        assert service is not None
        service.generation += 1
        service.status = ServiceStatus.PENDING
        service.version += 1
        result = await ServiceRepository.reconcile_locked(session, service)

    assert result.replicas_created == 2
    assert result.replicas_stopping == 2
    async with service_database.session() as session, session.begin():
        assert not await ServiceRepository.record_replica_health(
            session,
            replica_id=old_replica_ids[0],
            generation=1,
            execution_id=execution_ids[0],
            health=ReplicaHealth.HEALTHY,
        )
        assert await ServiceRepository.mark_replica_terminal(
            session,
            replica_id=old_replica_ids[0],
            generation=1,
            execution_id=execution_ids[0],
            status=ReplicaStatus.STOPPED,
        )
        assert (
            await ServiceRepository.choose_healthy_endpoint(
                session,
                service_id=service_id,
                project_id=PROJECT_ID,
            )
            is None
        )

    async with service_database.session() as session:
        replicas = await ServiceRepository.list_replicas(session, service_id)
    current = [replica for replica in replicas if replica.generation == 2]
    old = [replica for replica in replicas if replica.generation == 1]
    assert [replica.status for replica in current] == [
        ReplicaStatus.PENDING,
        ReplicaStatus.PENDING,
    ]
    assert [replica.status for replica in old] == [
        ReplicaStatus.STOPPED,
        ReplicaStatus.STOPPING,
    ]


async def test_expired_replica_lease_is_fenced_and_replaced(
    service_database: Database,
) -> None:
    service_id = await _create_service(service_database, replicas=1)
    await _register_worker(service_database, "worker-lease")
    execution_id = uuid.uuid4()

    async with service_database.session() as session, session.begin():
        replica = (await ServiceRepository.list_replicas(session, service_id))[0]
        assert await ServiceRepository.bind_replica_execution(
            session,
            replica_id=replica.id,
            generation=replica.generation,
            worker_id="worker-lease",
            execution_id=execution_id,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        assert not await ServiceRepository.renew_replica_lease(
            session,
            replica_id=replica.id,
            generation=replica.generation,
            execution_id=uuid.uuid4(),
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=2),
        )
        assert await ServiceRepository.renew_replica_lease(
            session,
            replica_id=replica.id,
            generation=replica.generation,
            execution_id=execution_id,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=2),
        )
        assert await ServiceRepository.mark_replica_running(
            session,
            replica_id=replica.id,
            generation=replica.generation,
            execution_id=execution_id,
            endpoint_url="http://worker-lease:8000",
        )
        replica.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        expired_replica_id = replica.id

    result = await ServiceReconciler(service_database).run_once()

    assert result.replicas_created == 1
    async with service_database.session() as session:
        replicas = await ServiceRepository.list_replicas(session, service_id)
        events = list(
            await session.scalars(
                select(OutboxEvent).where(OutboxEvent.event_type == "service.replica.lease_expired")
            )
        )

    expired = next(item for item in replicas if item.id == expired_replica_id)
    replacement = next(item for item in replicas if item.id != expired_replica_id)
    assert expired.status == ReplicaStatus.LOST
    assert expired.lease_expires_at is None
    assert expired.endpoint_url is None
    assert replacement.status == ReplicaStatus.PENDING
    assert len(events) == 1
    assert events[0].payload["execution_id"] == str(execution_id)
