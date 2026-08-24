import uuid
from datetime import UTC, datetime, timedelta

import pytest

from api.services.reaper import Reaper
from core.config import Settings
from core.database import Database
from core.enums import RuntimeType
from core.metrics import render_metrics
from models.outbox import OutboxEvent
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServiceStatus,
    ServingRuntime,
)
from repositories.workers import WorkerRepository

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def test_reaper_exports_cluster_capacity_allocation_and_service_gauges(
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.register(
            session,
            worker_id="metrics-worker",
            hostname="metrics-worker",
            concurrency=4,
            cpu_count=4,
            memory_total_mb=8192,
            docker_version="test",
            labels={},
            gpu_count=2,
            gpu_model="FAKE-A100",
            gpu_memory_mb=81_920,
        )
        worker.reserved_cpu = 1.5
        worker.reserved_memory_mb = 2048
        worker.reserved_gpus = 1

        service = ModelService(
            project_id=PROJECT_ID,
            name="metrics-service",
            model="fake/model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.FAKE,
            image=None,
            status=ServiceStatus.RUNNING,
        )
        session.add(service)
        await session.flush()
        replica = ServiceReplica(
            service_id=service.id,
            runtime=ServingRuntime.FAKE,
            generation=1,
            ordinal=0,
            status=ReplicaStatus.RUNNING,
            health=ReplicaHealth.HEALTHY,
        )
        session.add(replica)
        old = datetime.now(UTC) - timedelta(minutes=5)
        session.add_all(
            [
                OutboxEvent(
                    aggregate_id=uuid.uuid4(),
                    event_type="metrics.available",
                    payload={},
                    created_at=old,
                    available_at=old,
                ),
                OutboxEvent(
                    aggregate_id=uuid.uuid4(),
                    event_type="metrics.backoff",
                    payload={},
                    available_at=datetime.now(UTC) + timedelta(hours=1),
                ),
                OutboxEvent(
                    aggregate_id=uuid.uuid4(),
                    event_type="metrics.processed",
                    payload={},
                    created_at=old,
                    available_at=old,
                    processed_at=old,
                ),
            ]
        )

    settings = Settings(database_url=str(database.engine.url), control_plane_enabled=False)
    await Reaper(database, settings).refresh_gauges()
    metrics = render_metrics().decode("utf-8")

    assert "worker_capacity_cpu 4000.0" in metrics
    assert "worker_capacity_memory 8192.0" in metrics
    assert "worker_capacity_gpu 2.0" in metrics
    assert "worker_allocated_cpu 1500.0" in metrics
    assert "worker_allocated_memory 2048.0" in metrics
    assert "worker_allocated_gpu 1.0" in metrics
    assert 'worker_allocated_resources{resource="cpu_millicores"} 1500.0' in metrics
    assert "services_ready 1.0" in metrics
    assert 'service_replicas{state="running"} 1.0' in metrics
    replica_health = next(
        line for line in metrics.splitlines() if line.startswith("replica_health{")
    )
    assert f'service_id="{service.id}"' in replica_health
    assert f'replica_id="{replica.id}"' in replica_health
    assert 'health="healthy"' in replica_health
    assert replica_health.endswith(" 1.0")
    assert "outbox_pending 2.0" in metrics
    oldest_age = next(
        float(line.split()[1])
        for line in metrics.splitlines()
        if line.startswith("outbox_oldest_age_seconds ")
    )
    assert oldest_age >= 240
