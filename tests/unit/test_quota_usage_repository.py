import uuid
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event

from core.database import Database
from core.enums import RuntimeType, TaskStatus
from core.rbac import ProjectStatus
from models import Base
from models.identity import Project
from models.service import ServingRuntime
from models.task import Task
from models.usage import ProjectQuotaState, TaskExecution
from repositories.quotas import (
    QuotaExceededError,
    QuotaInvariantViolation,
    QuotaRepository,
)
from repositories.services import ServiceRepository
from repositories.usage import UsageInvariantViolation, UsageRepository


@pytest_asyncio.fixture
async def accounting_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'accounting.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield database
    finally:
        await database.dispose()


async def _project(database: Database, slug: str) -> uuid.UUID:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=slug,
                slug=slug,
                status=ProjectStatus.ACTIVE,
            )
        )
        await session.flush()
        await QuotaRepository.initialize(session, project_id=project_id)
    return project_id


async def test_quota_admission_reservation_and_release_preserve_strict_invariants(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "quota-project")
    async with accounting_database.session() as session, session.begin():
        await QuotaRepository.replace(
            session,
            project_id=project_id,
            max_queued_tasks=1,
            max_running_tasks=1,
            max_cpu_millicores=1_000,
            max_memory_mb=2_048,
            max_gpus=1,
            max_services=None,
            max_service_replicas=None,
            max_artifact_bytes=None,
            daily_cost_limit=Decimal("1.0"),
        )
        state = await QuotaRepository.admit_queued(session, project_id=project_id)
        assert state.queued_tasks == 1
        with pytest.raises(QuotaExceededError) as exceeded:
            await QuotaRepository.admit_queued(session, project_id=project_id)
        assert exceeded.value.resource == "queued_tasks"

        state = await QuotaRepository.reserve_execution(
            session,
            project_id=project_id,
            cpu_millicores=1_000,
            memory_mb=1_024,
            gpu_count=1,
            estimated_cost=Decimal("0.40"),
        )
        assert (state.queued_tasks, state.running_tasks) == (0, 1)
        assert state.reserved_cpu_millicores == 1_000
        with pytest.raises(QuotaInvariantViolation):
            await QuotaRepository.reserve_execution(
                session,
                project_id=project_id,
                cpu_millicores=1,
                memory_mb=1,
                gpu_count=0,
            )

        state = await QuotaRepository.release(
            session,
            project_id=project_id,
            cpu_millicores=1_000,
            memory_mb=1_024,
            gpu_count=1,
            reserved_cost=Decimal("0.40"),
            settled_cost=Decimal("0.25"),
        )
        assert state.running_tasks == 0
        assert state.reserved_cpu_millicores == 0
        assert state.reserved_memory_mb == 0
        assert state.reserved_gpus == 0
        assert state.daily_reserved_cost == Decimal("0")
        assert state.daily_settled_cost == Decimal("0.25")
        with pytest.raises(QuotaInvariantViolation):
            await QuotaRepository.release(
                session,
                project_id=project_id,
                cpu_millicores=1_000,
                memory_mb=1_024,
                gpu_count=1,
            )


async def test_quota_cannot_be_lowered_below_current_usage_and_daily_state_rolls_over(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "quota-update")
    async with accounting_database.session() as session, session.begin():
        await QuotaRepository.admit_queued(session, project_id=project_id)
        with pytest.raises(QuotaExceededError):
            await QuotaRepository.replace(
                session,
                project_id=project_id,
                max_queued_tasks=0,
                max_running_tasks=None,
                max_cpu_millicores=None,
                max_memory_mb=None,
                max_gpus=None,
                max_services=None,
                max_service_replicas=None,
                max_artifact_bytes=None,
                daily_cost_limit=None,
            )
        state = await session.get(ProjectQuotaState, project_id, with_for_update=True)
        assert state is not None
        state.accounting_date = date(2000, 1, 1)
        state.daily_reserved_cost = Decimal("2")
        state.daily_settled_cost = Decimal("3")

    async with accounting_database.session() as session, session.begin():
        snapshot = await QuotaRepository.get_locked(session, project_id=project_id)
        assert snapshot.state.accounting_date == datetime.now(UTC).date()
        assert snapshot.state.daily_reserved_cost == 0
        assert snapshot.state.daily_settled_cost == 0


async def test_service_commitments_share_hard_resources_without_mutating_task_counters(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "service-quota")
    async with accounting_database.session() as session, session.begin():
        await QuotaRepository.replace(
            session,
            project_id=project_id,
            max_queued_tasks=2,
            max_running_tasks=2,
            max_cpu_millicores=3_000,
            max_memory_mb=3_072,
            max_gpus=2,
            max_services=1,
            max_service_replicas=2,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )
        state = await QuotaRepository.replace_service_commitment(
            session,
            project_id=project_id,
            current_replicas=0,
            desired_replicas=1,
            cpu_millicores=2_000,
            memory_mb=1_024,
            gpu_count=1,
        )
        assert (state.running_tasks, state.reserved_cpu_millicores) == (0, 0)
        assert (state.service_count, state.service_replicas) == (1, 1)
        assert state.service_reserved_cpu_millicores == 2_000
        assert state.service_reserved_memory_mb == 1_024
        assert state.service_reserved_gpus == 1
        with pytest.raises(QuotaExceededError) as lowered_below_service:
            await QuotaRepository.replace(
                session,
                project_id=project_id,
                max_queued_tasks=2,
                max_running_tasks=0,
                max_cpu_millicores=3_000,
                max_memory_mb=3_072,
                max_gpus=2,
                max_services=1,
                max_service_replicas=2,
                max_artifact_bytes=None,
                daily_cost_limit=None,
            )
        assert lowered_below_service.value.resource == "running_tasks"

        await QuotaRepository.admit_queued(session, project_id=project_id)
        state = await QuotaRepository.reserve_execution(
            session,
            project_id=project_id,
            cpu_millicores=1_000,
            memory_mb=1_024,
            gpu_count=1,
        )
        assert (state.running_tasks, state.service_replicas) == (1, 1)
        assert (state.reserved_cpu_millicores, state.service_reserved_cpu_millicores) == (
            1_000,
            2_000,
        )

        await QuotaRepository.admit_queued(session, project_id=project_id)
        with pytest.raises(QuotaExceededError) as running_exceeded:
            await QuotaRepository.reserve_execution(
                session,
                project_id=project_id,
                cpu_millicores=1,
                memory_mb=16,
                gpu_count=0,
            )
        assert running_exceeded.value.resource == "running_tasks"

        await QuotaRepository.release(
            session,
            project_id=project_id,
            cpu_millicores=1_000,
            memory_mb=1_024,
            gpu_count=1,
        )
        with pytest.raises(QuotaExceededError) as cpu_exceeded:
            await QuotaRepository.replace_service_commitment(
                session,
                project_id=project_id,
                current_replicas=1,
                desired_replicas=2,
                cpu_millicores=2_000,
                memory_mb=1_024,
                gpu_count=1,
            )
        assert cpu_exceeded.value.resource == "cpu_millicores"

        state = await QuotaRepository.replace_service_commitment(
            session,
            project_id=project_id,
            current_replicas=1,
            desired_replicas=0,
            cpu_millicores=2_000,
            memory_mb=1_024,
            gpu_count=1,
        )
        assert (state.service_count, state.service_replicas) == (0, 0)
        assert state.service_reserved_cpu_millicores == 0
        assert state.service_reserved_memory_mb == 0
        assert state.service_reserved_gpus == 0
        version = state.version
        repeated = await QuotaRepository.replace_service_commitment(
            session,
            project_id=project_id,
            current_replicas=0,
            desired_replicas=0,
            cpu_millicores=2_000,
            memory_mb=1_024,
            gpu_count=1,
        )
        assert repeated.version == version


async def _execution(
    database: Database,
    *,
    project_id: uuid.UUID,
    attempt: int,
    assigned_at: datetime,
) -> tuple[uuid.UUID, uuid.UUID]:
    task_id = uuid.uuid4()
    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Task(
                id=task_id,
                project_id=project_id,
                image="python:3.12",
                command=["python", "-V"],
                status=TaskStatus.RUNNING,
            )
        )
        session.add(
            TaskExecution(
                id=execution_id,
                task_id=task_id,
                project_id=project_id,
                worker_id=None,
                worker_session_id=None,
                attempt=attempt,
                status="running",
                cpu_millicores=1_000,
                memory_mb=1_024,
                gpu_count=1,
                gpu_model="A100",
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1.0"),
                assigned_at=assigned_at,
                runtime_type=RuntimeType.DOCKER.value,
            )
        )
    return task_id, execution_id


async def test_usage_ledger_is_idempotent_and_aggregates_by_settlement_window(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "usage-project")
    other_project_id = await _project(accounting_database, "usage-isolated")
    base = datetime(2026, 1, 1, tzinfo=UTC)
    task_id, execution_id = await _execution(
        accounting_database,
        project_id=project_id,
        attempt=1,
        assigned_at=base,
    )
    async with accounting_database.session() as session, session.begin():
        first, created = await UsageRepository.record_execution(
            session,
            execution_id=execution_id,
            project_id=project_id,
            task_id=task_id,
            started_at=base,
            finished_at=base + timedelta(minutes=10),
            cpu_seconds=Decimal("600"),
            memory_gb_seconds=Decimal("600"),
            gpu_seconds=Decimal("600"),
            gpu_model="A100",
            cost=Decimal("0.20"),
        )
        repeated, created_again = await UsageRepository.record_execution(
            session,
            execution_id=execution_id,
            project_id=project_id,
            task_id=task_id,
            started_at=base,
            finished_at=base + timedelta(minutes=10),
            cpu_seconds=Decimal("600"),
            memory_gb_seconds=Decimal("600"),
            gpu_seconds=Decimal("600"),
            gpu_model="A100",
            cost=Decimal("0.20"),
        )
        assert created is True
        assert created_again is False
        assert repeated.id == first.id
        with pytest.raises(UsageInvariantViolation, match="conflicting usage"):
            await UsageRepository.record_execution(
                session,
                execution_id=execution_id,
                project_id=project_id,
                task_id=task_id,
                started_at=base,
                finished_at=base + timedelta(minutes=10),
                cpu_seconds=Decimal("999"),
                memory_gb_seconds=Decimal("999"),
                gpu_seconds=Decimal("999"),
                cost=Decimal("999"),
            )

    async with accounting_database.session() as session:
        aggregate = await UsageRepository.aggregate_settled(
            session,
            project_id=project_id,
            from_time=base,
            to_time=base + timedelta(hours=1),
        )
        isolated = await UsageRepository.aggregate_settled(
            session,
            project_id=other_project_id,
            from_time=base,
            to_time=base + timedelta(hours=1),
        )

    assert aggregate.execution_count == 1
    assert aggregate.cpu_seconds == Decimal("600")
    assert aggregate.gpu_seconds == Decimal("600")
    assert aggregate.costs[0].currency == "USD"
    assert aggregate.costs[0].cost == Decimal("0.20")
    assert aggregate.gpu_breakdown[0].gpu_model == "A100"
    assert isolated.execution_count == 0
    assert isolated.costs == ()


async def test_usage_identity_must_match_the_locked_execution(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "usage-invariant")
    task_id, execution_id = await _execution(
        accounting_database,
        project_id=project_id,
        attempt=1,
        assigned_at=datetime.now(UTC),
    )
    async with accounting_database.session() as session, session.begin():
        with pytest.raises(UsageInvariantViolation):
            await UsageRepository.record_execution(
                session,
                execution_id=execution_id,
                project_id=uuid.uuid4(),
                task_id=task_id,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                cpu_seconds=Decimal("0"),
                memory_gb_seconds=Decimal("0"),
                gpu_seconds=Decimal("0"),
                cost=Decimal("0"),
            )


async def test_serving_usage_is_idempotent_and_aggregates_reported_tokens(
    accounting_database: Database,
) -> None:
    project_id = await _project(accounting_database, "serving-usage")
    other_project_id = await _project(accounting_database, "serving-usage-other")
    base = datetime(2026, 1, 1, tzinfo=UTC)
    request_id = uuid.uuid4()
    async with accounting_database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=project_id,
            name="serving-usage-model",
            model="org/serving-usage-model",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="example/vllm:test",
            cpu_millicores=1_000,
            memory_mb=2_048,
            gpu_count=2,
            gpu_memory_mb=16_384,
            desired_replicas=1,
        )
        await ServiceRepository.reconcile_locked(session, service)
        replica = (await ServiceRepository.list_replicas(session, service.id))[0]
        replica.container_started_at = base + timedelta(seconds=10)
        replica.stopped_at = base + timedelta(seconds=40)
        usage, created = await UsageRepository.record_serving_request(
            session,
            request_id=request_id,
            project_id=project_id,
            service_id=service.id,
            replica_id=replica.id,
            path="/v1/chat/completions",
            outcome="success",
            error_code=None,
            streamed=True,
            started_at=base,
            finished_at=base + timedelta(seconds=1, microseconds=250_000),
            request_duration_seconds=Decimal("1.25"),
            time_to_first_token_seconds=Decimal("0.15"),
            allocated_gpu_seconds=Decimal("2.50"),
            prompt_tokens=7,
            completion_tokens=3,
            total_tokens=10,
        )
        repeated, created_again = await UsageRepository.record_serving_request(
            session,
            request_id=request_id,
            project_id=project_id,
            service_id=service.id,
            replica_id=replica.id,
            path="/v1/chat/completions",
            outcome="success",
            error_code=None,
            streamed=True,
            started_at=base,
            finished_at=base + timedelta(seconds=1, microseconds=250_000),
            request_duration_seconds=Decimal("1.25"),
            time_to_first_token_seconds=Decimal("0.15"),
            allocated_gpu_seconds=Decimal("2.50"),
            prompt_tokens=7,
            completion_tokens=3,
            total_tokens=10,
        )
        assert created is True
        assert created_again is False
        assert repeated.id == usage.id

        with pytest.raises(ValueError, match="entirely available"):
            await UsageRepository.record_serving_request(
                session,
                request_id=uuid.uuid4(),
                project_id=project_id,
                service_id=service.id,
                replica_id=replica.id,
                path="/v1/completions",
                outcome="success",
                error_code=None,
                streamed=False,
                started_at=base,
                finished_at=base,
                request_duration_seconds=Decimal("0"),
                time_to_first_token_seconds=None,
                allocated_gpu_seconds=Decimal("0"),
                prompt_tokens=1,
                completion_tokens=None,
                total_tokens=None,
            )

    async with accounting_database.session() as session:
        aggregate = await UsageRepository.aggregate_settled(
            session,
            project_id=project_id,
            from_time=base,
            to_time=base + timedelta(hours=1),
        )
        isolated = await UsageRepository.aggregate_settled(
            session,
            project_id=other_project_id,
            from_time=base,
            to_time=base + timedelta(hours=1),
        )

    assert aggregate.serving_request_count == 1
    assert aggregate.serving_requests_with_token_usage == 1
    assert aggregate.input_tokens == 7
    assert aggregate.output_tokens == 3
    assert aggregate.total_tokens == 10
    assert aggregate.serving_allocated_gpu_seconds == Decimal("2.500000")
    assert aggregate.serving_replica_gpu_seconds == Decimal("60.000000")
    assert isolated.serving_request_count == 0
    assert isolated.input_tokens == 0
    assert isolated.serving_allocated_gpu_seconds == Decimal("0.000000")
    assert isolated.serving_replica_gpu_seconds == Decimal("0.000000")
