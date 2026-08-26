from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from itertools import pairwise

import httpx
import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from api.services.fake_replica_runtime import FakeReplicaRuntimeController
from api.services.gateway import GatewayForwardResult, GatewayMetrics, GatewayService
from api.services.service_health import ServiceHealthController
from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import RuntimeType
from models.identity import Project
from models.service import ReplicaHealth, ReplicaStatus, ServiceReplica, ServingRuntime
from repositories.quotas import QuotaRepository
from repositories.services import ReconcileResult, ServiceRepository

pytestmark = pytest.mark.integration


class RecordingGatewayMetrics(GatewayMetrics):
    def __init__(self) -> None:
        super().__init__()
        self.selected_replicas: list[uuid.UUID] = []

    async def request_started(
        self,
        service_id: uuid.UUID,
        replica_id: uuid.UUID | None = None,
    ) -> None:
        await super().request_started(service_id, replica_id)
        if replica_id is not None:
            self.selected_replicas.append(replica_id)


async def _connected() -> bool:
    return False


async def _create_service(
    database: Database,
    *,
    desired_replicas: int,
) -> tuple[uuid.UUID, uuid.UUID]:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=f"Fake serving E2E {project_id.hex[:8]}",
                slug=f"fake-serving-e2e-{project_id.hex}",
            )
        )
        await session.flush()
        await QuotaRepository.initialize(session, project_id=project_id)
        service = await ServiceRepository.create(
            session,
            project_id=project_id,
            name="chat-main",
            model="fake/e2e-model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.FAKE,
            image=None,
            cpu_millicores=100,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=desired_replicas,
        )
    return project_id, service.id


async def _replicas(database: Database, service_id: uuid.UUID) -> list[ServiceReplica]:
    async with database.session() as session:
        return await ServiceRepository.list_replicas(session, service_id)


async def _ready_replicas(database: Database, service_id: uuid.UUID) -> list[ServiceReplica]:
    return [
        replica
        for replica in await _replicas(database, service_id)
        if replica.status == ReplicaStatus.RUNNING
        and replica.health == ReplicaHealth.HEALTHY
        and replica.endpoint_url is not None
    ]


async def _wait_for_replica_status(
    database: Database,
    replica_id: uuid.UUID,
    status: ReplicaStatus,
    *,
    timeout_seconds: float = 5.0,
) -> ServiceReplica:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        async with database.session() as session:
            replica = await session.get(ServiceReplica, replica_id)
        if replica is not None and replica.status == status:
            return replica
        await asyncio.sleep(0.02)
    raise AssertionError(f"replica {replica_id} did not reach {status.value}")


async def _scale_service(
    database: Database,
    *,
    project_id: uuid.UUID,
    service_id: uuid.UUID,
    desired_replicas: int,
) -> None:
    async with database.session() as session, session.begin():
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=project_id,
            desired_replicas=desired_replicas,
        )
    assert service is not None


async def _chat(
    gateway: GatewayService,
    *,
    project_id: uuid.UUID,
    prompt: str,
    stream: bool,
) -> GatewayForwardResult:
    return await gateway.forward(
        project_id=project_id,
        public_model="chat-main",
        path="/v1/chat/completions",
        payload={
            "model": "chat-main",
            "messages": [{"role": "user", "content": prompt}],
            "stream": stream,
        },
        request_headers={"authorization": "Bearer must-not-reach-upstream"},
        stream_requested=stream,
        client_disconnected=_connected,
    )


async def _consume_stream(stream: AsyncIterator[bytes]) -> tuple[list[bytes], list[float]]:
    chunks: list[bytes] = []
    observed_at: list[float] = []
    async for chunk in stream:
        if chunk:
            chunks.append(chunk)
            observed_at.append(time.monotonic())
    return chunks, observed_at


@pytest.mark.e2e
async def test_fake_serving_real_tcp_gateway_recovery_scaling_and_drain(
    database: Database,
) -> None:
    project_id, service_id = await _create_service(database, desired_replicas=2)
    reconciler = ServiceReconciler(database, drain_timeout_seconds=5)
    reconciled = await reconciler.reconcile_service(service_id, project_id=project_id)
    assert reconciled is not None and reconciled.replicas_created == 2

    metrics = RecordingGatewayMetrics()
    draining_stream: AsyncIterator[bytes] | None = None
    async with (
        httpx.AsyncClient(trust_env=False) as runtime_http,
        httpx.AsyncClient(trust_env=False) as gateway_http,
        httpx.AsyncClient(trust_env=False) as health_http,
    ):
        controller = FakeReplicaRuntimeController(
            database,
            app_env="test",
            http_client=runtime_http,
            ready_timeout_seconds=10,
            stop_timeout_seconds=2,
            probe_interval_seconds=0.02,
            inference_delay_seconds=0.06,
        )
        gateway = GatewayService(
            database,
            gateway_http,
            metrics,
            request_timeout=10,
            connect_timeout=2,
            first_token_timeout=2,
        )
        health = ServiceHealthController(
            database,
            health_http,
            timeout_seconds=1,
            interval_seconds=0.01,
            failure_threshold=1,
        )
        try:
            started = await controller.run_once()
            assert started.claimed == 2
            assert started.started == 2
            ready = await _ready_replicas(database, service_id)
            assert len(ready) == 2
            assert len({replica.endpoint_url for replica in ready}) == 2
            assert all(replica.container_started_at is not None for replica in ready)
            assert all(replica.ready_at is not None for replica in ready)

            for prompt in ("first request", "second request"):
                response = await _chat(
                    gateway,
                    project_id=project_id,
                    prompt=prompt,
                    stream=False,
                )
                assert response.status_code == 200
                assert response.stream is None
                assert response.body is not None
                payload = json.loads(response.body)
                assert payload["model"] == "fake/e2e-model"
                assert payload["choices"][0]["message"]["content"] == (f"fake response: {prompt}")

            first_two = metrics.selected_replicas[:2]
            assert len(first_two) == 2
            assert first_two[0] != first_two[1]
            assert set(first_two) == {replica.id for replica in ready}

            streamed = await _chat(
                gateway,
                project_id=project_id,
                prompt=(
                    "show several independently observable server-sent event chunks "
                    "over a real loopback TCP connection"
                ),
                stream=True,
            )
            assert streamed.status_code == 200
            assert streamed.stream is not None
            chunks, observed_at = await _consume_stream(streamed.stream)
            body = b"".join(chunks)
            assert len(chunks) >= 3
            assert body.count(b"data:") >= 5
            assert body.endswith(b"data: [DONE]\n\n")
            intervals = [later - earlier for earlier, later in pairwise(observed_at)]
            assert any(interval >= 0.03 for interval in intervals)

            victim = ready[0]
            victim_handle = controller._handles[victim.id]
            assert victim_handle.monitor_task is not None
            # Suppress the primary subprocess watcher to exercise the independent
            # health-controller and reconciler recovery path after a real process kill.
            victim_handle.monitor_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await victim_handle.monitor_task
            victim_handle.monitor_task = None
            victim_handle.process.kill()
            await asyncio.wait_for(victim_handle.process.wait(), timeout=2)

            async with database.session() as session, session.begin():
                due = await ServiceRepository.list_replicas(
                    session,
                    service_id,
                    for_update=True,
                )
                for replica in due:
                    if replica.status == ReplicaStatus.RUNNING:
                        replica.last_health_at = datetime(2000, 1, 1, tzinfo=UTC)
            health_result = await health.run_once()
            assert health_result.claimed == 2
            assert health_result.healthy == 1
            assert health_result.failed == 1
            async with database.session() as session:
                unhealthy = await session.get(ServiceReplica, victim.id)
            assert unhealthy is not None
            assert unhealthy.status == ReplicaStatus.RUNNING
            assert unhealthy.health == ReplicaHealth.UNHEALTHY
            assert unhealthy.error_code == "REPLICA_UNHEALTHY"

            replaced = await reconciler.reconcile_service(service_id, project_id=project_id)
            assert replaced is not None
            assert replaced.replicas_stopping == 1
            assert replaced.replicas_created == 1
            replacement_started = await controller.run_once()
            assert replacement_started.stopped == 1
            assert replacement_started.started == 1
            assert len(await _ready_replicas(database, service_id)) == 2

            await _scale_service(
                database,
                project_id=project_id,
                service_id=service_id,
                desired_replicas=4,
            )
            scaled_up = await reconciler.reconcile_service(service_id, project_id=project_id)
            assert scaled_up is not None and scaled_up.replicas_created == 2
            additional_started = await controller.run_once()
            assert additional_started.started == 2
            ready = await _ready_replicas(database, service_id)
            assert len(ready) == 4

            async with database.session() as session, session.begin():
                service = await ServiceRepository.get(
                    session,
                    service_id,
                    project_id=project_id,
                    for_update=True,
                )
                assert service is not None
                service.round_robin_cursor = len(ready) - 1

            in_flight = await _chat(
                gateway,
                project_id=project_id,
                prompt=(
                    "keep this streamed request active while three replicas enter "
                    "their graceful drain lifecycle"
                ),
                stream=True,
            )
            assert in_flight.stream is not None
            draining_stream = in_flight.stream
            selected_id = metrics.selected_replicas[-1]
            assert selected_id == ready[-1].id
            async with database.session() as session:
                selected = await session.get(ServiceReplica, selected_id)
            assert selected is not None and selected.active_requests == 1

            await _scale_service(
                database,
                project_id=project_id,
                service_id=service_id,
                desired_replicas=1,
            )
            scaled_down = await reconciler.reconcile_service(service_id, project_id=project_id)
            assert scaled_down is not None and scaled_down.replicas_stopping == 3
            replicas = await _replicas(database, service_id)
            selected = next(replica for replica in replicas if replica.id == selected_id)
            assert selected.status == ReplicaStatus.DRAINING
            assert selected.active_requests == 1
            draining = [replica for replica in replicas if replica.status == ReplicaStatus.DRAINING]
            assert len(draining) == 3

            partial_stop = await controller.run_once()
            assert partial_stop.stopped == 2
            assert selected_id in controller._handles
            assert controller._handles[selected_id].process.returncode is None

            drained_chunks, _ = await _consume_stream(draining_stream)
            draining_stream = None
            assert b"data: [DONE]\n\n" in b"".join(drained_chunks)
            async with database.session() as session:
                selected = await session.get(ServiceReplica, selected_id)
            assert selected is not None and selected.active_requests == 0

            final_stop = await controller.run_once()
            assert final_stop.stopped == 1
            selected = await _wait_for_replica_status(
                database,
                selected_id,
                ReplicaStatus.STOPPED,
            )
            assert selected.active_requests == 0
            final_ready = await _ready_replicas(database, service_id)
            assert len(final_ready) == 1
            assert controller.active_process_count == 1
        finally:
            if draining_stream is not None:
                close_stream = getattr(draining_stream, "aclose", None)
                if close_stream is not None:
                    await close_stream()
            await controller.close()


async def test_two_sqlite_reconcilers_never_persist_more_than_desired(
    database: Database,
) -> None:
    project_id, service_id = await _create_service(database, desired_replicas=3)
    first = ServiceReconciler(database)
    second = ServiceReconciler(database)
    start = asyncio.Event()

    async def contend(controller: ServiceReconciler) -> ReconcileResult:
        await start.wait()
        result = await controller.reconcile_service(service_id, project_id=project_id)
        assert result is not None
        return result

    contenders = [
        asyncio.create_task(contend(first)),
        asyncio.create_task(contend(second)),
    ]
    start.set()
    outcomes = await asyncio.gather(*contenders, return_exceptions=True)
    assert any(isinstance(outcome, ReconcileResult) for outcome in outcomes)
    assert all(
        isinstance(outcome, (ReconcileResult, IntegrityError, OperationalError))
        for outcome in outcomes
    )

    replicas = await _replicas(database, service_id)
    assert len(replicas) <= 3

    converged = await first.reconcile_service(service_id, project_id=project_id)
    assert converged is not None
    replicas = await _replicas(database, service_id)
    assert len(replicas) == 3
    assert {(replica.generation, replica.ordinal) for replica in replicas} == {
        (1, 0),
        (1, 1),
        (1, 2),
    }
