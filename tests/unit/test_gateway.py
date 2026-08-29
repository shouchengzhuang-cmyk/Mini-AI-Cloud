import asyncio
import json
import uuid
from collections.abc import AsyncIterator, MutableMapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Table, event, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import Message, Scope

from api.dependencies import get_principal
from api.errors import APIError, register_exception_handlers
from api.routes import gateway as gateway_routes
from api.routes import services as service_routes
from api.services import gateway as gateway_service_module
from api.services.autoscaler import ServiceAutoscaler
from api.services.gateway import GatewayMetrics, GatewayService, ServiceLoad
from api.services.service_health import ServiceHealthController
from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorVendor,
    GatewayRoutingPolicy,
    ModelAvailabilityStatus,
    ProjectRole,
    RuntimeType,
)
from core.rbac import Principal, PrincipalKind
from models.base import Base
from models.identity import ApiKey, Project, User
from models.model_variant import LogicalModel, ModelVariant
from models.outbox import OutboxEvent
from models.registry import RegisteredModel
from models.routing import VendorCircuitState
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServiceStatus,
    ServingRuntime,
)
from models.usage import AuditEvent, ProjectQuota, ProjectQuotaState, ServingRequestUsage
from models.worker import Worker
from repositories.gateway_routing import GatewayRoute, GatewayRoutingRepository
from repositories.quotas import QuotaRepository
from repositories.services import EndpointSelection, ServiceRepository
from repositories.workers import WorkerRepository

PROJECT_ID = uuid.UUID("20000000-0000-0000-0000-000000000001")


async def test_scale_locks_logical_model_before_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id = uuid.uuid4()
    logical_model_id = uuid.uuid4()
    preview = cast(
        ModelService,
        SimpleNamespace(
            runtime_type=RuntimeType.KUBERNETES,
            logical_model_id=logical_model_id,
        ),
    )
    locked = cast(ModelService, object())
    calls: list[str] = []

    async def get_service(
        _session: AsyncSession,
        requested_service_id: uuid.UUID,
        *,
        project_id: uuid.UUID | None = None,
        for_update: bool = False,
    ) -> ModelService | None:
        assert requested_service_id == service_id
        assert project_id == PROJECT_ID
        calls.append(f"service:{for_update}")
        return locked if for_update else preview

    async def get_logical_model(
        _session: AsyncSession,
        *,
        project_id: uuid.UUID,
        logical_model_id: uuid.UUID,
        for_update: bool = False,
    ) -> object:
        assert project_id == PROJECT_ID
        assert logical_model_id == preview.logical_model_id
        calls.append(f"logical:{for_update}")
        return object()

    monkeypatch.setattr(
        service_routes.ServiceRepository,
        "get",
        staticmethod(get_service),
    )
    monkeypatch.setattr(
        service_routes.LogicalModelRepository,
        "get",
        staticmethod(get_logical_model),
    )

    result = await service_routes._lock_service_for_scale(
        cast(AsyncSession, object()),
        service_id=service_id,
        project_id=PROJECT_ID,
        desired_replicas=2,
    )

    assert result is locked
    assert calls == ["service:False", "logical:True", "service:True"]


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False
        self.yielded = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class DelayedChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[tuple[float, bytes]]) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for delay, chunk in self.chunks:
            if delay:
                await asyncio.sleep(delay)
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class DisconnectChunkStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.waiting_for_next_chunk = asyncio.Event()
        self.closed = False
        self._never = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'data: {"choices":[]}\n\n'
        self.waiting_for_next_chunk.set()
        await self._never.wait()

    async def aclose(self) -> None:
        await asyncio.sleep(0.01)
        self.closed = True


class FaultAfterFirstChunkStream(httpx.AsyncByteStream):
    def __init__(self, request: httpx.Request) -> None:
        self.request = request
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'data: {"choices":[{"delta":{"content":"pinned"}}]}\n\n'
        raise httpx.ReadError("fault after stream start", request=self.request)

    async def aclose(self) -> None:
        self.closed = True


class GatedCompletionStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.allow_completion = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'data: {"choices":[]}\n\n'
        await self.allow_completion.wait()
        yield b"data: [DONE]\n\n"

    async def aclose(self) -> None:
        self.closed = True


@pytest_asyncio.fixture
async def gateway_database(tmp_path: Any) -> AsyncIterator[Database]:
    path = (tmp_path / "gateway.sqlite3").as_posix()
    database = Database(f"sqlite+aiosqlite:///{path}?timeout=30")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                cast(Table, User.__table__),
                cast(Table, ApiKey.__table__),
                cast(Table, Project.__table__),
                cast(Table, ProjectQuota.__table__),
                cast(Table, ProjectQuotaState.__table__),
                cast(Table, Worker.__table__),
                cast(Table, RegisteredModel.__table__),
                cast(Table, LogicalModel.__table__),
                cast(Table, ModelVariant.__table__),
                cast(Table, ModelService.__table__),
                cast(Table, ServiceReplica.__table__),
                cast(Table, ServingRequestUsage.__table__),
                cast(Table, VendorCircuitState.__table__),
                cast(Table, AuditEvent.__table__),
                cast(Table, OutboxEvent.__table__),
            ],
        )
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_ID, name="Gateway Tests", slug="gateway-tests"))
    try:
        yield database
    finally:
        await database.dispose()


async def _ready_service(database: Database) -> tuple[uuid.UUID, list[ServiceReplica]]:
    for worker_id in ("worker-a", "worker-b"):
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
    async with database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="chat-main",
            model="org/upstream-model",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="example/vllm:test",
            cpu_millicores=1000,
            memory_mb=1024,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=2,
        )
        await ServiceRepository.reconcile_locked(session, service)
        replicas = await ServiceRepository.list_replicas(session, service.id)
        for index, replica in enumerate(replicas):
            execution_id = uuid.uuid4()
            assert await ServiceRepository.bind_replica_execution(
                session,
                replica_id=replica.id,
                generation=1,
                worker_id=f"worker-{'ab'[index]}",
                execution_id=execution_id,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
            )
            assert await ServiceRepository.mark_replica_running(
                session,
                replica_id=replica.id,
                generation=1,
                execution_id=execution_id,
                endpoint_url=f"http://worker-{'ab'[index]}.test:8000",
            )
            assert await ServiceRepository.record_replica_health(
                session,
                replica_id=replica.id,
                generation=1,
                execution_id=execution_id,
                health=ReplicaHealth.HEALTHY,
            )
        return service.id, replicas


async def _ready_dual_vendor_services(
    database: Database,
    *,
    routing_policy: GatewayRoutingPolicy = GatewayRoutingPolicy.BALANCED,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    nvidia_service_id, _ = await _ready_service(database)
    logical_model_id = uuid.uuid4()
    nvidia_variant_id = uuid.uuid4()
    ascend_variant_id = uuid.uuid4()
    profile_digest = "sha256:" + "1" * 64
    artifact_digest = "sha256:" + "2" * 64
    async with database.session() as session, session.begin():
        session.add(
            LogicalModel(
                id=logical_model_id,
                project_id=PROJECT_ID,
                name="logical-chat",
                public_name="logical-chat",
                status=ModelAvailabilityStatus.READY,
                routing_policy=routing_policy,
            )
        )
        session.add_all(
            [
                ModelVariant(
                    id=nvidia_variant_id,
                    logical_model_id=logical_model_id,
                    name="nvidia-variant",
                    vendor=AcceleratorVendor.NVIDIA,
                    kind=AcceleratorKind.GPU,
                    runtime_profile_id="nvidia-vllm",
                    runtime_profile_version="1.0.0",
                    runtime_profile_digest=profile_digest,
                    artifact_source="physical/nvidia",
                    artifact_revision="revision-1",
                    artifact_digest=artifact_digest,
                    architecture="test",
                    dtype="bfloat16",
                    status=ModelAvailabilityStatus.READY,
                ),
                ModelVariant(
                    id=ascend_variant_id,
                    logical_model_id=logical_model_id,
                    name="ascend-variant",
                    vendor=AcceleratorVendor.HUAWEI_ASCEND,
                    kind=AcceleratorKind.NPU,
                    runtime_profile_id="ascend-vllm",
                    runtime_profile_version="1.0.0",
                    runtime_profile_digest=profile_digest,
                    artifact_source="physical/ascend",
                    artifact_revision="revision-1",
                    artifact_digest=artifact_digest,
                    architecture="test",
                    dtype="bfloat16",
                    status=ModelAvailabilityStatus.READY,
                ),
            ]
        )
        await session.flush()
        nvidia_service = await session.get(ModelService, nvidia_service_id)
        assert nvidia_service is not None
        nvidia_replicas = await ServiceRepository.list_replicas(
            session, nvidia_service_id, for_update=True
        )
        nvidia_service.name = "logical-chat"
        nvidia_service.model = "physical/nvidia"
        nvidia_service.logical_model_id = logical_model_id
        nvidia_service.model_variant_id = nvidia_variant_id
        nvidia_service.selected_vendor = "nvidia"
        nvidia_service.selected_kind = "gpu"
        nvidia_service.selected_model = "test-gpu"
        nvidia_service.runtime_profile_id = "nvidia-vllm"
        nvidia_service.runtime_profile_version = "1.0.0"
        nvidia_service.runtime_profile_digest = profile_digest
        nvidia_service.allocation_authority = "control_plane_exact_device"
        nvidia_service.selection_policy = "prefer-nvidia"
        nvidia_service.eligible_node_names = ["worker-a.test", "worker-b.test"]
        for replica in nvidia_replicas:
            replica.logical_model_id = logical_model_id
            replica.model_variant_id = nvidia_variant_id
            replica.selected_vendor = "nvidia"
            replica.selected_kind = "gpu"
            replica.selected_model = "test-gpu"
            replica.runtime_profile_id = "nvidia-vllm"
            replica.runtime_profile_version = "1.0.0"
            replica.runtime_profile_digest = profile_digest
            replica.allocation_authority = "control_plane_exact_device"
            replica.selection_policy = "prefer-nvidia"
            replica.eligible_node_names = ["worker-a.test", "worker-b.test"]

        await QuotaRepository.replace(
            session,
            project_id=PROJECT_ID,
            max_queued_tasks=None,
            max_running_tasks=None,
            max_cpu_millicores=None,
            max_memory_mb=None,
            max_gpus=None,
            max_nvidia_gpus=None,
            max_ascend_npus=None,
            max_services=None,
            max_service_replicas=None,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )
        ascend_service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="logical-chat-ascend",
            model="physical/ascend",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.DOCKER,
            image="example/vllm:test",
            cpu_millicores=1000,
            memory_mb=1024,
            gpu_count=1,
            gpu_memory_mb=0,
            desired_replicas=1,
            logical_model_id=logical_model_id,
            model_variant_id=ascend_variant_id,
            selected_vendor="huawei-ascend",
            selected_kind="npu",
            selected_model="Atlas A2",
            runtime_profile_id="ascend-vllm",
            runtime_profile_version="1.0.0",
            runtime_profile_digest=profile_digest,
            allocation_authority="control_plane_exact_device",
            selection_policy="prefer-nvidia",
            eligible_node_names=["ascend.test"],
        )
        await ServiceRepository.reconcile_locked(session, ascend_service)
        ascend_replica = (await ServiceRepository.list_replicas(session, ascend_service.id))[0]
        execution_id = uuid.uuid4()
        assert await ServiceRepository.bind_replica_execution(
            session,
            replica_id=ascend_replica.id,
            generation=1,
            worker_id="worker-a",
            execution_id=execution_id,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        assert await ServiceRepository.mark_replica_running(
            session,
            replica_id=ascend_replica.id,
            generation=1,
            execution_id=execution_id,
            endpoint_url="http://ascend.test:8000",
        )
        assert await ServiceRepository.record_replica_health(
            session,
            replica_id=ascend_replica.id,
            generation=1,
            execution_id=execution_id,
            health=ReplicaHealth.HEALTHY,
        )
    return logical_model_id, nvidia_variant_id, ascend_variant_id


def _api_key_principal() -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=PROJECT_ID,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=ProjectRole.VIEWER,
        key_prefix="mkc_gateway_test",
    )


async def test_gateway_routes_round_robin_filter_headers_and_stream(
    gateway_database: Database,
) -> None:
    service_id, replicas = await _ready_service(gateway_database)
    seen: list[httpx.Request] = []
    streams: list[ChunkStream] = []
    routed_replica_ids: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        payload = json.loads(request.content)
        if payload.get("stream") is True:
            stream = ChunkStream([b'data: {"choices":[]}\n\n', b"data: [DONE]\n\n"])
            streams.append(stream)
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream", "Connection": "X-Hop"},
                stream=stream,
            )
        return httpx.Response(
            200,
            headers={
                "Content-Type": "application/json",
                "Connection": "X-Hop",
                "X-Hop": "remove-me",
                "X-Upstream": "kept",
            },
            json={"model": payload["model"], "choices": []},
        )

    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    metrics = GatewayMetrics()
    gateway = GatewayService(
        gateway_database,
        upstream_client,
        metrics,
        request_timeout=5,
        endpoint_host_allowlist="*.test",
    )
    app = FastAPI()
    app.state.gateway_service = gateway
    register_exception_handlers(app)
    app.include_router(gateway_routes.router)
    app.dependency_overrides[get_principal] = _api_key_principal
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://gateway.test"
    ) as client:
        for _ in range(2):
            response = await client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer platform-secret",
                    "Host": "attacker-controlled.example",
                    "Connection": "X-Remove",
                    "X-Remove": "remove-me",
                    "X-Custom": "kept",
                    "X-Mini-AI-Replica-Id": "spoofed",
                },
                json={"model": "chat-main", "messages": [{"role": "user", "content": "hi"}]},
            )
            assert response.status_code == 200
            assert response.json()["model"] == "org/upstream-model"
            assert response.headers["x-upstream"] == "kept"
            assert "x-hop" not in response.headers
            routed_replica_ids.append(response.headers["x-mini-ai-replica-id"])

        listed = await client.get("/v1/models")
        assert listed.status_code == 200
        assert [item["id"] for item in listed.json()["data"]] == ["chat-main"]

        streamed = await client.post(
            "/v1/completions",
            json={"model": "chat-main", "prompt": "hi", "stream": True},
        )
        assert streamed.status_code == 200
        assert streamed.content.endswith(b"data: [DONE]\n\n")
        routed_replica_ids.append(streamed.headers["x-mini-ai-replica-id"])

        disconnected = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "disconnect", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _true(),
        )
        assert disconnected.stream is not None
        assert [chunk async for chunk in disconnected.stream] == []

    await upstream_client.aclose()
    assert [request.url.host for request in seen] == [
        "worker-a.test",
        "worker-b.test",
        "worker-a.test",
        "worker-b.test",
    ]
    assert all(request.headers.get("authorization") is None for request in seen)
    assert all(request.headers.get("x-api-key") is None for request in seen)
    assert [request.headers["host"] for request in seen] == [
        "worker-a.test:8000",
        "worker-b.test:8000",
        "worker-a.test:8000",
        "worker-b.test:8000",
    ]
    assert all(request.headers.get("x-remove") is None for request in seen)
    assert all(request.headers.get("x-mini-ai-replica-id") is None for request in seen)
    assert all(request.headers["x-custom"] == "kept" for request in seen[:2])
    assert routed_replica_ids[:2] == [str(replica.id) for replica in replicas]
    assert routed_replica_ids[2] == routed_replica_ids[0]
    assert streams and streams[0].closed is True
    assert streams[1].closed is True
    load = await metrics.snapshot(service_id)
    assert load is not None and load.active_requests == 0
    async with gateway_database.session() as session:
        usages = list(await session.scalars(select(ServingRequestUsage)))
        persisted_replicas = await ServiceRepository.list_replicas(session, service_id)
    cancelled = [usage for usage in usages if usage.outcome == "client_disconnect"]
    assert len(cancelled) == 1
    assert cancelled[0].error_code == "CLIENT_DISCONNECTED"
    assert cancelled[0].allocated_gpu_seconds is None
    assert all(replica.active_requests == 0 for replica in persisted_replicas)


async def test_gateway_bounds_buffered_responses_but_keeps_sse_streaming(
    gateway_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    streams: dict[str, ChunkStream] = {}

    async def upstream(request: httpx.Request) -> httpx.Response:
        prompt = str(json.loads(request.content)["prompt"])
        if prompt == "exact":
            stream = ChunkStream([b"123", b"456"])
            streams[prompt] = stream
            return httpx.Response(200, headers={"Content-Type": "application/json"}, stream=stream)
        if prompt == "oversized":
            stream = ChunkStream([b"1234", b"5678", b"must-not-be-read"])
            streams[prompt] = stream
            return httpx.Response(200, headers={"Content-Type": "application/json"}, stream=stream)
        if prompt == "declared-oversized":
            stream = ChunkStream([b"must-not-be-read"])
            streams[prompt] = stream
            return httpx.Response(
                200,
                headers={"Content-Type": "application/json", "Content-Length": "7"},
                stream=stream,
            )
        stream = ChunkStream([b"data: 123456789\n\n", b"data: [DONE]\n\n"])
        streams[prompt] = stream
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

    outcomes: list[str] = []
    monkeypatch.setattr(
        gateway_service_module,
        "_observe_gateway",
        lambda outcome, _started_at: outcomes.append(outcome),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        metrics = GatewayMetrics()
        gateway = GatewayService(
            gateway_database,
            upstream_client,
            metrics,
            request_timeout=5,
            max_response_bytes=6,
            endpoint_host_allowlist="*.test",
        )

        exact = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "exact"},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )
        assert exact.body == b"123456"
        assert streams["exact"].closed is True

        app = FastAPI()
        app.state.gateway_service = gateway
        register_exception_handlers(app)
        app.include_router(gateway_routes.router)
        app.dependency_overrides[get_principal] = _api_key_principal
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://gateway.test"
        ) as client:
            oversized = await client.post(
                "/v1/completions",
                json={"model": "chat-main", "prompt": "oversized"},
            )
        assert oversized.status_code == 502
        assert oversized.json()["error"]["code"] == "UPSTREAM_RESPONSE_TOO_LARGE"
        assert streams["oversized"].yielded == 2
        assert streams["oversized"].closed is True

        with pytest.raises(APIError) as declared_error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="chat-main",
                path="/v1/completions",
                payload={"model": "chat-main", "prompt": "declared-oversized"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )
        assert declared_error.value.status_code == 502
        assert declared_error.value.code == "UPSTREAM_RESPONSE_TOO_LARGE"
        assert streams["declared-oversized"].yielded == 0
        assert streams["declared-oversized"].closed is True

        sse = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "sse", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        assert sse.stream is not None
        assert b"".join([chunk async for chunk in sse.stream]).endswith(b"data: [DONE]\n\n")
        assert streams["sse"].closed is True

    load = await metrics.snapshot(service_id)
    assert load is not None and load.active_requests == 0
    assert outcomes == [
        "success",
        "response_too_large",
        "response_too_large",
        "success",
    ]


async def test_gateway_downstream_send_failure_closes_upstream_and_releases_request(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    stream = ChunkStream([b'data: {"choices":[]}\n\n', b"data: [DONE]\n\n"])

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

    metrics = GatewayMetrics()
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            metrics,
            request_timeout=1,
            endpoint_host_allowlist="*.test",
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "disconnect", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        response = gateway_routes._gateway_response(result)
        assert isinstance(response, gateway_routes._GatewayStreamingResponse)

        async def broken_send(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.body":
                raise ConnectionError("downstream closed")

        with pytest.raises(ConnectionError, match="downstream closed"):
            await response.stream_response(broken_send)

    assert stream.closed is True
    load = await metrics.snapshot(service_id)
    assert load is not None and load.active_requests == 0
    async with gateway_database.session() as session:
        usage = await session.scalar(select(ServingRequestUsage))
        replicas = await ServiceRepository.list_replicas(session, service_id)
    assert usage is not None
    assert usage.outcome == "client_disconnect"
    assert usage.error_code == "CLIENT_DISCONNECTED"
    assert all(replica.active_requests == 0 for replica in replicas)


async def test_gateway_asgi_23_disconnect_completes_cancel_safe_cleanup(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    stream = DisconnectChunkStream()

    async def upstream(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

    metrics = GatewayMetrics()
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            metrics,
            request_timeout=1,
            endpoint_host_allowlist="*.test",
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "disconnect", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        response = gateway_routes._gateway_response(result)
        assert isinstance(response, gateway_routes._GatewayStreamingResponse)

        messages: list[Message] = []

        async def receive() -> Message:
            await stream.waiting_for_next_chunk.wait()
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            messages.append(message)

        scope: Scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
        }
        await response(scope, receive, send)

    assert any(message["type"] == "http.response.body" for message in messages)
    assert stream.closed is True
    load = await metrics.snapshot(service_id)
    assert load is not None and load.active_requests == 0
    async with gateway_database.session() as session:
        usage = await session.scalar(select(ServingRequestUsage))
        replicas = await ServiceRepository.list_replicas(session, service_id)
    assert usage is not None
    assert usage.outcome == "client_disconnect"
    assert usage.error_code == "CLIENT_DISCONNECTED"
    assert all(replica.active_requests == 0 for replica in replicas)


async def test_cancel_safe_cleanup_survives_direct_asyncio_cancellation() -> None:
    started = asyncio.Event()
    completed = asyncio.Event()

    async def cleanup() -> None:
        started.set()
        await asyncio.sleep(0.01)
        completed.set()

    task = asyncio.create_task(gateway_service_module._await_cancel_safe(cleanup()))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed.is_set()


async def test_gateway_records_only_reported_upstream_usage_and_releases_replicas(
    gateway_database: Database,
) -> None:
    service_id, replicas = await _ready_service(gateway_database)

    async def upstream(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        prompt = str(payload["prompt"])
        if payload.get("stream") is True:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=ChunkStream(
                    [
                        b'data: {"choices":[]}\n\n',
                        b'data: {"choices":[],"us',
                        b'age":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}\n\n',
                        b"data: [DONE]\n\n",
                    ]
                ),
            )
        if prompt == "reported":
            usage = {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}
        elif prompt == "error-reported":
            usage = {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5}
        else:
            usage = {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 99}
        return httpx.Response(
            429 if prompt == "error-reported" else 200,
            headers={"Content-Type": "application/json; charset=utf-8"},
            json={"choices": [], "usage": usage},
        )

    metrics = GatewayMetrics()
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        gateway = GatewayService(
            gateway_database,
            upstream_client,
            metrics,
            request_timeout=1,
            endpoint_host_allowlist="*.test",
        )
        for prompt, expected_status in (
            ("reported", 200),
            ("malformed", 200),
            ("error-reported", 429),
        ):
            result = await gateway.forward(
                project_id=PROJECT_ID,
                public_model="chat-main",
                path="/v1/completions",
                payload={"model": "chat-main", "prompt": prompt},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )
            assert result.status_code == expected_status

        streamed = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "streamed", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        assert streamed.stream is not None
        assert b"".join([chunk async for chunk in streamed.stream]).endswith(b"data: [DONE]\n\n")

    async with gateway_database.session() as session:
        rows = list(
            await session.scalars(
                select(ServingRequestUsage).order_by(ServingRequestUsage.started_at)
            )
        )
        persisted_replicas = await ServiceRepository.list_replicas(session, service_id)

    assert len(rows) == 4
    reported = next(row for row in rows if row.total_tokens == 3)
    error_reported = next(row for row in rows if row.total_tokens == 5)
    malformed = next(row for row in rows if not row.streamed and row.total_tokens is None)
    streamed_usage = next(row for row in rows if row.streamed)
    assert (reported.prompt_tokens, reported.completion_tokens) == (2, 1)
    assert error_reported.outcome == "upstream_error"
    assert error_reported.error_code == "UPSTREAM_HTTP_ERROR"
    assert malformed.prompt_tokens is None
    assert (
        streamed_usage.prompt_tokens,
        streamed_usage.completion_tokens,
        streamed_usage.total_tokens,
    ) == (4, 2, 6)
    assert streamed_usage.time_to_first_token_seconds is not None
    assert all(row.allocated_gpu_seconds == Decimal("0.000000") for row in rows)
    assert all(
        row.outcome == "success" and row.error_code is None
        for row in rows
        if row is not error_reported
    )
    assert all(replica.active_requests == 0 for replica in persisted_replicas)
    for replica in replicas:
        load = await metrics.replica_snapshot(service_id, replica.id)
        assert load is not None and load.active_requests == 0


async def test_gateway_enforces_connect_first_token_and_overall_deadlines(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)

    async def upstream(request: httpx.Request) -> httpx.Response:
        prompt = str(json.loads(request.content)["prompt"])
        if prompt == "connect":
            raise httpx.ConnectTimeout("connect timeout", request=request)
        if prompt == "first-token":
            stream = DelayedChunkStream([(0.2, b"late")])
        else:
            stream = DelayedChunkStream(
                [
                    (0, b'data: {"choices":[]}\n\n'),
                    (0.3, b"data: [DONE]\n\n"),
                ]
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

    metrics = GatewayMetrics()
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        gateway = GatewayService(
            gateway_database,
            upstream_client,
            metrics,
            request_timeout=0.2,
            connect_timeout=0.05,
            first_token_timeout=0.1,
            endpoint_host_allowlist="*.test",
        )
        with pytest.raises(APIError) as connect_error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="chat-main",
                path="/v1/completions",
                payload={"model": "chat-main", "prompt": "connect"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )
        assert connect_error.value.code == "UPSTREAM_CONNECT_TIMEOUT"

        with pytest.raises(APIError) as first_token_error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="chat-main",
                path="/v1/completions",
                payload={"model": "chat-main", "prompt": "first-token", "stream": True},
                request_headers={},
                stream_requested=True,
                client_disconnected=lambda: _false(),
            )
        assert first_token_error.value.code == "INFERENCE_REQUEST_TIMEOUT"

        overall = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "overall", "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        assert overall.stream is not None
        chunks = [chunk async for chunk in overall.stream]
        assert chunks == [b'data: {"choices":[]}\n\n']

    async with gateway_database.session() as session:
        rows = list(await session.scalars(select(ServingRequestUsage)))
        persisted_replicas = await ServiceRepository.list_replicas(session, service_id)

    assert {row.outcome for row in rows} == {
        "connect_timeout",
        "first_token_timeout",
        "inference_timeout",
    }
    assert {row.error_code for row in rows} == {
        "UPSTREAM_CONNECT_TIMEOUT",
        "INFERENCE_REQUEST_TIMEOUT",
    }
    assert all(row.allocated_gpu_seconds is None for row in rows)
    assert all(replica.active_requests == 0 for replica in persisted_replicas)
    service_load = await metrics.snapshot(service_id)
    assert service_load is not None and service_load.active_requests == 0


async def test_gateway_returns_stable_no_healthy_replica_error(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    async with gateway_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(session, service_id, for_update=True)
        for replica in replicas:
            replica.health = ReplicaHealth.UNHEALTHY

    calls = 0

    async def must_not_call(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"choices": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(must_not_call)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=1,
            endpoint_host_allowlist="*.test",
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="chat-main",
                path="/v1/completions",
                payload={"model": "chat-main", "prompt": "hello"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )

    assert error.value.status_code == 503
    assert error.value.code == "NO_HEALTHY_REPLICA"
    assert calls == 0
    async with gateway_database.session() as session:
        usage = await session.scalar(select(ServingRequestUsage))
    assert usage is not None
    assert usage.outcome == "no_healthy_replica"
    assert usage.error_code == "NO_HEALTHY_REPLICA"
    assert usage.replica_id is None


async def test_gateway_returns_stable_unavailable_and_requires_api_key(
    gateway_database: Database,
) -> None:
    await _ready_service(gateway_database)

    async def unavailable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(unavailable))
    gateway = GatewayService(
        gateway_database,
        upstream_client,
        GatewayMetrics(),
        request_timeout=5,
        endpoint_host_allowlist="*.test",
    )
    with pytest.raises(APIError) as unavailable_error:
        await gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "hello"},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )
    await upstream_client.aclose()
    assert unavailable_error.value.status_code == 503
    assert unavailable_error.value.code == "UPSTREAM_DISCONNECTED"

    async def timed_out(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow upstream", request=request)

    timeout_client = httpx.AsyncClient(transport=httpx.MockTransport(timed_out))
    timeout_gateway = GatewayService(
        gateway_database,
        timeout_client,
        GatewayMetrics(),
        request_timeout=1,
        endpoint_host_allowlist="*.test",
    )
    with pytest.raises(APIError) as timeout_error:
        await timeout_gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "hello"},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )
    await timeout_client.aclose()
    assert timeout_error.value.status_code == 503
    assert timeout_error.value.code == "INFERENCE_REQUEST_TIMEOUT"

    legacy = Principal(kind=PrincipalKind.LEGACY, project_id=PROJECT_ID)
    with pytest.raises(APIError) as auth_error:
        await gateway_routes.require_gateway_principal(legacy)
    assert auth_error.value.status_code == 401
    assert auth_error.value.code == "API_KEY_REQUIRED"


async def test_gateway_rejects_unsafe_database_endpoint_and_never_follows_redirects(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    async with gateway_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(session, service_id)
        for replica in replicas:
            replica.endpoint_url = "http://8.8.8.8:8000"

    calls: list[str] = []

    async def must_not_be_called(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"choices": []})

    blocked_client = httpx.AsyncClient(transport=httpx.MockTransport(must_not_be_called))
    blocked_gateway = GatewayService(
        gateway_database,
        blocked_client,
        GatewayMetrics(),
        request_timeout=5,
    )
    with pytest.raises(APIError) as blocked:
        await blocked_gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "hello"},
            request_headers={"Host": "attacker.example"},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )
    await blocked_client.aclose()
    assert blocked.value.code == "UNSAFE_REPLICA_ENDPOINT"
    assert calls == []

    async with gateway_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(session, service_id)
        for index, replica in enumerate(replicas):
            replica.endpoint_url = f"http://worker-{'ab'[index]}.test:8000"

    async def redirect(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(
            307,
            headers={"Location": "http://169.254.169.254/latest/meta-data"},
        )

    redirect_client = httpx.AsyncClient(
        transport=httpx.MockTransport(redirect),
        follow_redirects=True,
    )
    redirect_gateway = GatewayService(
        gateway_database,
        redirect_client,
        GatewayMetrics(),
        request_timeout=5,
        endpoint_host_allowlist="*.test",
    )
    with pytest.raises(APIError) as redirected:
        await redirect_gateway.forward(
            project_id=PROJECT_ID,
            public_model="chat-main",
            path="/v1/completions",
            payload={"model": "chat-main", "prompt": "hello"},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )
    await redirect_client.aclose()
    assert redirected.value.code == "UPSTREAM_REDIRECT_REJECTED"
    assert len(calls) == 1
    assert calls[0].startswith("http://worker-")


async def _false() -> bool:
    return False


async def _true() -> bool:
    return True


async def test_health_probe_claim_is_exclusive_across_controllers(
    gateway_database: Database,
) -> None:
    service_id, _replicas = await _ready_service(gateway_database)
    await _make_health_due(gateway_database, service_id)
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_health(_request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(200, json={"status": "ok"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(slow_health))
    first = ServiceHealthController(
        gateway_database,
        client,
        timeout_seconds=5,
        interval_seconds=5,
        failure_threshold=2,
    )
    second = ServiceHealthController(
        gateway_database,
        client,
        timeout_seconds=5,
        interval_seconds=5,
        failure_threshold=2,
    )
    first_task = asyncio.create_task(first.run_once())
    await asyncio.wait_for(started.wait(), timeout=2)
    second_result = await second.run_once()
    release.set()
    first_result = await first_task
    await client.aclose()

    assert first_result.claimed == 2
    assert first_result.healthy == 2
    assert second_result.claimed == 0


async def test_health_threshold_marks_unhealthy_then_reconciles_replacements(
    gateway_database: Database,
) -> None:
    service_id, original_replicas = await _ready_service(gateway_database)
    await _make_health_due(gateway_database, service_id)

    async def failing_health(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(failing_health))
    controller = ServiceHealthController(
        gateway_database,
        client,
        timeout_seconds=2,
        interval_seconds=1,
        failure_threshold=2,
    )
    first = await controller.run_once()
    assert first.failed == 2
    async with gateway_database.session() as session:
        persisted = await ServiceRepository.list_replicas(session, service_id)
    assert all(replica.health == ReplicaHealth.HEALTHY for replica in persisted)
    assert all(replica.health_failure_count == 1 for replica in persisted)

    async with gateway_database.session() as session, session.begin():
        persisted = await ServiceRepository.list_replicas(session, service_id, for_update=True)
        for replica in persisted:
            replica.last_health_at = datetime(2000, 1, 1, tzinfo=UTC)

    second = await controller.run_once()
    await client.aclose()
    assert second.failed == 2
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
        persisted = await ServiceRepository.list_replicas(session, service_id)
    assert service is not None and service.status == ServiceStatus.DEGRADED
    assert all(replica.health == ReplicaHealth.UNHEALTHY for replica in persisted)

    reconciled = await ServiceReconciler(gateway_database).run_once()
    assert reconciled.replicas_stopping == 2
    assert reconciled.replicas_created == 2
    async with gateway_database.session() as session:
        all_replicas = await ServiceRepository.list_replicas(session, service_id)
    original_ids = {replica.id for replica in original_replicas}
    original = [replica for replica in all_replicas if replica.id in original_ids]
    replacements = [replica for replica in all_replicas if replica.id not in original_ids]
    assert all(replica.status == ReplicaStatus.DRAINING for replica in original)
    assert all(replica.status == ReplicaStatus.PENDING for replica in replacements)


class StaticMetrics:
    def __init__(self) -> None:
        self.loads: dict[uuid.UUID, ServiceLoad] = {}

    async def snapshot(self, service_id: uuid.UUID) -> ServiceLoad | None:
        return self.loads.get(service_id)


class MutableKubernetesAdmission:
    def __init__(self, *, ready: bool) -> None:
        self.admission_ready = ready


async def _make_health_due(database: Database, service_id: uuid.UUID) -> None:
    async with database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(session, service_id, for_update=True)
        for replica in replicas:
            replica.last_health_at = datetime(2000, 1, 1, tzinfo=UTC)


async def test_autoscaler_changes_only_desired_and_holds_on_missing_or_cooldown(
    gateway_database: Database,
) -> None:
    async with gateway_database.session() as session, session.begin():
        created_service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="autoscaled",
            model="org/model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.FAKE,
            image=None,
            cpu_millicores=500,
            memory_mb=512,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=1,
            autoscaling_enabled=True,
            autoscaling_min_replicas=1,
            autoscaling_max_replicas=4,
            autoscaling_target_concurrency=2,
            autoscaling_cooldown_seconds=60,
        )
        await ServiceRepository.reconcile_locked(session, created_service)
        service_id = created_service.id

    metrics = StaticMetrics()
    autoscaler = ServiceAutoscaler(
        gateway_database,
        metrics,
        metric_max_age_seconds=30,
    )
    metrics.loads[service_id] = ServiceLoad(
        active_requests=5,
        observed_at=datetime.now(UTC),
    )
    first = await autoscaler.run_once()
    assert first.scaled == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
        replica_count = len(await ServiceRepository.list_replicas(session, service_id))
    assert service is not None and service.desired_replicas == 3
    assert replica_count == 1

    metrics.loads[service_id] = ServiceLoad(
        active_requests=8,
        observed_at=datetime.now(UTC),
    )
    cooling = await autoscaler.run_once()
    assert cooling.cooling_down == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
    assert service is not None and service.desired_replicas == 3

    metrics.loads.clear()
    missing = await autoscaler.run_once()
    assert missing.missing_metrics == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
    assert service is not None and service.desired_replicas == 3

    async with gateway_database.session() as session, session.begin():
        service = await ServiceRepository.get(session, service_id, for_update=True)
        assert service is not None
        service.last_scaled_at = datetime(2000, 1, 1, tzinfo=UTC)
    metrics.loads[service_id] = ServiceLoad(
        active_requests=0,
        observed_at=datetime.now(UTC),
    )
    scaled_down = await autoscaler.run_once()
    assert scaled_down.scaled == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
        replica_count = len(await ServiceRepository.list_replicas(session, service_id))
        reconcile_events = list(
            await session.scalars(
                select(OutboxEvent).where(
                    OutboxEvent.aggregate_id == service_id,
                    OutboxEvent.event_type == "service.reconcile",
                )
            )
        )
    assert service is not None and service.desired_replicas == 1
    assert replica_count == 1
    assert len(reconcile_events) == 3


async def test_kubernetes_autoscaler_gates_only_scale_up_on_runtime_admission(
    gateway_database: Database,
) -> None:
    async with gateway_database.session() as session, session.begin():
        created_service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="kubernetes-autoscaled",
            model="fake/kind-model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.KUBERNETES,
            image="mini-ai-cloud:kind-serving-v4a",
            cpu_millicores=250,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=1,
            autoscaling_enabled=True,
            autoscaling_min_replicas=1,
            autoscaling_max_replicas=4,
            autoscaling_target_concurrency=2,
            autoscaling_cooldown_seconds=0,
        )
        await ServiceRepository.reconcile_locked(session, created_service)
        service_id = created_service.id

    metrics = StaticMetrics()
    metrics.loads[service_id] = ServiceLoad(
        active_requests=5,
        observed_at=datetime.now(UTC),
    )
    unavailable = await ServiceAutoscaler(gateway_database, metrics).run_once()
    assert unavailable.scaled == 0
    assert unavailable.held == 1

    admission = MutableKubernetesAdmission(ready=False)
    autoscaler = ServiceAutoscaler(
        gateway_database,
        metrics,
        kubernetes_runtime=admission,
    )
    still_unavailable = await autoscaler.run_once()
    assert still_unavailable.scaled == 0
    assert still_unavailable.held == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
        replicas = await ServiceRepository.list_replicas(session, service_id)
        quota_state = await session.get(ProjectQuotaState, PROJECT_ID)
    assert service is not None and service.desired_replicas == 1
    assert len(replicas) == 1
    assert quota_state is not None and quota_state.service_replicas == 1

    admission.admission_ready = True
    recovered = await autoscaler.run_once()
    assert recovered.scaled == 1
    async with gateway_database.session() as session, session.begin():
        service = await ServiceRepository.get(session, service_id, for_update=True)
        assert service is not None and service.desired_replicas == 3
        service.last_scaled_at = datetime(2000, 1, 1, tzinfo=UTC)

    admission.admission_ready = False
    metrics.loads[service_id] = ServiceLoad(
        active_requests=0,
        observed_at=datetime.now(UTC),
    )
    scaled_down = await autoscaler.run_once()
    assert scaled_down.scaled == 1
    async with gateway_database.session() as session:
        service = await ServiceRepository.get(session, service_id)
    assert service is not None and service.desired_replicas == 1


async def test_autoscaler_holds_when_scale_up_would_exceed_project_quota(
    gateway_database: Database,
) -> None:
    async with gateway_database.session() as session, session.begin():
        await QuotaRepository.initialize(session, project_id=PROJECT_ID)
        await QuotaRepository.replace(
            session,
            project_id=PROJECT_ID,
            max_queued_tasks=None,
            max_running_tasks=1,
            max_cpu_millicores=500,
            max_memory_mb=512,
            max_gpus=0,
            max_services=1,
            max_service_replicas=1,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="quota-autoscaled",
            model="org/model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.FAKE,
            image=None,
            cpu_millicores=500,
            memory_mb=512,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=1,
            autoscaling_enabled=True,
            autoscaling_min_replicas=1,
            autoscaling_max_replicas=4,
            autoscaling_target_concurrency=1,
            autoscaling_cooldown_seconds=0,
        )
        service_id = service.id

    metrics = StaticMetrics()
    metrics.loads[service_id] = ServiceLoad(
        active_requests=3,
        observed_at=datetime.now(UTC),
    )
    result = await ServiceAutoscaler(gateway_database, metrics).run_once()

    assert result.examined == 1
    assert result.scaled == 0
    assert result.held == 1
    async with gateway_database.session() as session:
        persisted = await ServiceRepository.get(session, service_id)
        state = await session.get(ProjectQuotaState, PROJECT_ID)
    assert persisted is not None and persisted.desired_replicas == 1
    assert state is not None and state.service_replicas == 1
    assert state.service_reserved_cpu_millicores == 500


async def test_logical_model_fallback_records_circuit_audit_and_physical_usage(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    hosts: list[str] = []
    logical_started_at = datetime.now(UTC)
    ascend_dispatched_at: datetime | None = None

    async def upstream(request: httpx.Request) -> httpx.Response:
        nonlocal ascend_dispatched_at
        assert request.url.host is not None
        hosts.append(request.url.host)
        if request.url.host.startswith("worker-"):
            await asyncio.sleep(0.1)
            raise httpx.ConnectError("injected nvidia connect fault", request=request)
        ascend_dispatched_at = datetime.now(UTC)
        payload = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "model": payload["model"],
                "choices": [],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
            circuit_failure_threshold=1,
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="logical-chat",
            path="/v1/chat/completions",
            payload={"model": "logical-chat", "messages": []},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )

    assert result.status_code == 200
    assert result.headers["x-mini-ai-accelerator-vendor"] == "huawei-ascend"
    assert result.headers["x-mini-ai-model-variant-id"] == str(ascend_variant_id)
    assert json.loads(result.body or b"{}")["model"] == "physical/ascend"
    assert len(hosts) == 2
    assert hosts[0].startswith("worker-")
    assert hosts[1] == "ascend.test"

    async with gateway_database.session() as session:
        circuits = list(
            await session.scalars(
                select(VendorCircuitState).where(
                    VendorCircuitState.logical_model_id == logical_model_id
                )
            )
        )
        audit = await session.scalar(
            select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
        )
        usage = list(await session.scalars(select(ServingRequestUsage)))
    by_vendor = {item.vendor: item for item in circuits}
    assert by_vendor["nvidia"].state == "open"
    assert by_vendor["huawei-ascend"].state == "closed"
    assert audit is not None
    assert audit.outcome == "success"
    assert audit.details["from_variant_id"] == str(nvidia_variant_id)
    assert audit.details["to_variant_id"] == str(ascend_variant_id)
    assert len(usage) == 1
    physical_usage = usage[0]
    assert (physical_usage.selected_vendor, physical_usage.model_variant_id) == (
        "huawei-ascend",
        ascend_variant_id,
    )
    assert physical_usage.total_tokens == 5
    assert physical_usage.allocated_gpu_seconds == physical_usage.request_duration_seconds
    assert audit.request_id == str(physical_usage.request_id)
    assert ascend_dispatched_at is not None
    usage_started_at = physical_usage.started_at.replace(tzinfo=UTC)
    assert usage_started_at >= logical_started_at + timedelta(seconds=0.08)
    assert usage_started_at <= ascend_dispatched_at


@pytest.mark.parametrize(
    ("public_model", "routing_policy", "strict_vendor"),
    [
        ("strict-nvidia-alias", GatewayRoutingPolicy.STRICT_NVIDIA, "nvidia"),
        ("strict-ascend-alias", GatewayRoutingPolicy.STRICT_ASCEND, "huawei-ascend"),
    ],
)
async def test_strict_vendor_policy_never_routes_an_alternate(
    gateway_database: Database,
    public_model: str,
    routing_policy: GatewayRoutingPolicy,
    strict_vendor: str,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.public_name = public_model
        logical_model.routing_policy = routing_policy

    async with gateway_database.session() as session, session.begin():
        anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model=public_model,
        )
        assert anchor is not None
        assert route is not None
        assert route.selected_vendor == strict_vendor

        retry_anchor, retry_route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model=public_model,
            excluded_vendors=frozenset({strict_vendor}),
        )
        assert retry_anchor is not None
        assert retry_route is None


@pytest.mark.parametrize(
    ("public_model", "routing_policy", "expected_host"),
    [
        ("strict-nvidia-alias", GatewayRoutingPolicy.STRICT_NVIDIA, "worker-"),
        ("strict-ascend-alias", GatewayRoutingPolicy.STRICT_ASCEND, "ascend.test"),
    ],
)
async def test_strict_vendor_failure_does_not_fallback_across_vendors(
    gateway_database: Database,
    public_model: str,
    routing_policy: GatewayRoutingPolicy,
    expected_host: str,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.public_name = public_model
        logical_model.routing_policy = routing_policy

    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        raise httpx.ConnectError("injected strict-backend fault", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model=public_model,
                path="/v1/completions",
                payload={"model": public_model, "prompt": "strict"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )

    assert error.value.code == "UPSTREAM_DISCONNECTED"
    assert len(hosts) == 1
    if expected_host.endswith("-"):
        assert hosts[0].startswith(expected_host)
    else:
        assert hosts[0] == expected_host


async def test_ready_logical_model_public_name_routes_to_physical_variant(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.public_name = "Public Logical Chat"

    async with gateway_database.session() as session, session.begin():
        anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="Public Logical Chat",
        )

    assert anchor is not None
    assert anchor.name == "logical-chat"
    assert route is not None
    assert route.model_variant_id == nvidia_variant_id
    assert route.selected_vendor == "nvidia"
    assert route.upstream_model == "physical/nvidia"


async def test_logical_model_remains_routable_during_scale_reconciliation(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database,
        routing_policy=GatewayRoutingPolicy.STRICT_NVIDIA,
    )
    async with gateway_database.session() as session, session.begin():
        service = await session.scalar(
            select(ModelService).where(
                ModelService.logical_model_id == logical_model_id,
                ModelService.model_variant_id == nvidia_variant_id,
            )
        )
        assert service is not None
        scaled = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service.id,
            project_id=PROJECT_ID,
            desired_replicas=3,
            eligible_node_names=["worker-a.test", "worker-b.test"],
        )
        assert scaled is not None and scaled.status == ServiceStatus.PENDING

    async with gateway_database.session() as session, session.begin():
        catalog = await GatewayRoutingRepository.list_available_models(
            session,
            project_id=PROJECT_ID,
        )
        _anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )

    assert [entry.model_id for entry in catalog] == ["logical-chat"]
    assert route is not None
    assert route.model_variant_id == nvidia_variant_id
    assert route.selected_vendor == "nvidia"


@pytest.mark.parametrize("public_name", ["Public Logical Chat", "m" * 255])
async def test_listed_logical_model_id_is_accepted_by_http_gateway(
    gateway_database: Database,
    public_name: str,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.public_name = public_name

    dispatched_models: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        dispatched_models.append(payload["model"])
        return httpx.Response(200, json={"model": payload["model"], "choices": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        gateway = GatewayService(
            gateway_database,
            upstream_client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
        )
        app = FastAPI()
        app.state.gateway_service = gateway
        register_exception_handlers(app)
        app.include_router(gateway_routes.router)
        app.dependency_overrides[get_principal] = _api_key_principal
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            listed = await client.get("/v1/models")
            assert listed.status_code == 200
            assert [item["id"] for item in listed.json()["data"]] == [public_name]

            completion = await client.post(
                "/v1/completions",
                json={"model": public_name, "prompt": "roundtrip"},
            )

    assert completion.status_code == 200
    assert completion.json()["model"] == "physical/nvidia"
    assert dispatched_models == ["physical/nvidia"]


async def test_gateway_model_catalog_aggregates_logical_identity_and_filters_policy(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with httpx.AsyncClient() as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
        )
        assert [model.id for model in (await gateway.list_models(project_id=PROJECT_ID)).data] == [
            "logical-chat"
        ]

        async with gateway_database.session() as session, session.begin():
            logical_model = await session.get(LogicalModel, logical_model_id)
            assert logical_model is not None
            logical_model.public_name = "Public Logical Chat"
        listed = await gateway.list_models(project_id=PROJECT_ID)
        assert [model.id for model in listed.data] == ["Public Logical Chat"]

        async with gateway_database.session() as session, session.begin():
            logical_model = await session.get(LogicalModel, logical_model_id)
            nvidia_variant = await session.get(ModelVariant, nvidia_variant_id)
            assert logical_model is not None and nvidia_variant is not None
            logical_model.routing_policy = GatewayRoutingPolicy.STRICT_NVIDIA
            nvidia_variant.status = ModelAvailabilityStatus.DEGRADED
        assert (await gateway.list_models(project_id=PROJECT_ID)).data == []

        async with gateway_database.session() as session, session.begin():
            logical_model = await session.get(LogicalModel, logical_model_id)
            assert logical_model is not None
            logical_model.routing_policy = GatewayRoutingPolicy.BALANCED
        assert [model.id for model in (await gateway.list_models(project_id=PROJECT_ID)).data] == [
            "Public Logical Chat"
        ]

        ascend_replica_id: uuid.UUID
        ascend_execution_id: uuid.UUID
        async with gateway_database.session() as session, session.begin():
            ascend_service = await session.scalar(
                select(ModelService).where(
                    ModelService.logical_model_id == logical_model_id,
                    ModelService.selected_vendor == "huawei-ascend",
                )
            )
            assert ascend_service is not None
            ascend_replicas = await ServiceRepository.list_replicas(
                session, ascend_service.id, for_update=True
            )
            assert ascend_replicas
            ascend_replica_id = ascend_replicas[0].id
            assert ascend_replicas[0].execution_id is not None
            ascend_execution_id = ascend_replicas[0].execution_id
            ascend_replicas[0].execution_id = None
        assert (await gateway.list_models(project_id=PROJECT_ID)).data == []

        async with gateway_database.session() as session, session.begin():
            ascend_replica = await session.get(ServiceReplica, ascend_replica_id)
            assert ascend_replica is not None
            ascend_replica.execution_id = ascend_execution_id
        assert [model.id for model in (await gateway.list_models(project_id=PROJECT_ID)).data] == [
            "Public Logical Chat"
        ]

        async with gateway_database.session() as session, session.begin():
            ascend_service = await session.scalar(
                select(ModelService).where(
                    ModelService.logical_model_id == logical_model_id,
                    ModelService.selected_vendor == "huawei-ascend",
                )
            )
            assert ascend_service is not None
            ascend_replicas = await ServiceRepository.list_replicas(
                session, ascend_service.id, for_update=True
            )
            for replica in ascend_replicas:
                replica.health = ReplicaHealth.UNHEALTHY
        assert (await gateway.list_models(project_id=PROJECT_ID)).data == []

        async with gateway_database.session() as session, session.begin():
            logical_model = await session.get(LogicalModel, logical_model_id)
            assert logical_model is not None
            logical_model.status = ModelAvailabilityStatus.DEGRADED
        assert (await gateway.list_models(project_id=PROJECT_ID)).data == []


async def test_gateway_fails_closed_on_historical_cross_namespace_collision(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name="legacy-shadow",
            model="unrelated/direct-model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.DOCKER,
            image=None,
            cpu_millicores=100,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            desired_replicas=0,
        )
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.public_name = "legacy-shadow"

    upstream_calls = 0

    async def upstream(_request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(200, json={"choices": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="legacy-shadow",
                path="/v1/completions",
                payload={"model": "legacy-shadow", "prompt": "must not dispatch"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )
        assert error.value.status_code == 409
        assert error.value.code == "GATEWAY_MODEL_NAME_CONFLICT"
        assert upstream_calls == 0
        assert (await gateway.list_models(project_id=PROJECT_ID)).data == []


@pytest.mark.parametrize(
    ("routing_policy", "expected_vendor"),
    [
        (GatewayRoutingPolicy.PREFER_NVIDIA, "nvidia"),
        (GatewayRoutingPolicy.PREFER_ASCEND, "huawei-ascend"),
    ],
)
async def test_prefer_routing_policy_is_deterministic(
    gateway_database: Database,
    routing_policy: GatewayRoutingPolicy,
    expected_vendor: str,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database,
        routing_policy=routing_policy,
    )

    selected_vendors: list[str | None] = []
    for _ in range(3):
        async with gateway_database.session() as session, session.begin():
            _anchor, route = await GatewayRoutingRepository.choose_route(
                session,
                project_id=PROJECT_ID,
                public_model="logical-chat",
            )
            assert route is not None
            selected_vendors.append(route.selected_vendor)

    async with gateway_database.session() as session:
        logical_model = await session.get(LogicalModel, logical_model_id)
    assert selected_vendors == [expected_vendor] * 3
    assert logical_model is not None
    assert logical_model.routing_cursor == 0


async def test_balanced_policy_round_robins_without_mutating_logical_metadata(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database,
        routing_policy=GatewayRoutingPolicy.BALANCED,
    )
    async with gateway_database.session() as session:
        before = await session.get(LogicalModel, logical_model_id)
        assert before is not None
        before_status = before.status
        before_version = before.version
        before_updated_at = before.updated_at

    selected_vendors: list[str | None] = []
    for _ in range(4):
        async with gateway_database.session() as session, session.begin():
            _anchor, route = await GatewayRoutingRepository.choose_route(
                session,
                project_id=PROJECT_ID,
                public_model="logical-chat",
            )
            assert route is not None
            selected_vendors.append(route.selected_vendor)

    async with gateway_database.session() as session:
        after = await session.get(LogicalModel, logical_model_id)
    assert selected_vendors == ["nvidia", "huawei-ascend", "nvidia", "huawei-ascend"]
    assert after is not None
    assert after.routing_cursor == 4
    assert after.status is before_status
    assert after.version == before_version
    assert after.updated_at == before_updated_at


async def test_balanced_policy_skips_unavailable_candidate_and_advances_past_selection(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        session.add(
            VendorCircuitState(
                project_id=PROJECT_ID,
                logical_model_id=logical_model_id,
                vendor="nvidia",
                state="open",
                failure_count=2,
                opened_until=datetime.now(UTC) + timedelta(minutes=1),
                last_error_code="UPSTREAM_DISCONNECTED",
                version=2,
                updated_at=datetime.now(UTC),
            )
        )

    for expected_cursor in (2, 4):
        async with gateway_database.session() as session, session.begin():
            _anchor, route = await GatewayRoutingRepository.choose_route(
                session,
                project_id=PROJECT_ID,
                public_model="logical-chat",
            )
            assert route is not None
            assert route.model_variant_id == ascend_variant_id
            assert [(skip.selected_vendor, skip.reason) for skip in route.preflight_skips] == [
                ("nvidia", "circuit_open")
            ]
        async with gateway_database.session() as session:
            logical_model = await session.get(LogicalModel, logical_model_id)
            assert logical_model is not None
            assert logical_model.routing_cursor == expected_cursor

    async with gateway_database.session() as session, session.begin():
        circuit = await session.scalar(
            select(VendorCircuitState).where(
                VendorCircuitState.project_id == PROJECT_ID,
                VendorCircuitState.logical_model_id == logical_model_id,
                VendorCircuitState.vendor == "nvidia",
            )
        )
        assert circuit is not None
        circuit.state = "closed"
        circuit.opened_until = None

    async with gateway_database.session() as session, session.begin():
        _anchor, recovered_route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )
    assert recovered_route is not None
    assert recovered_route.model_variant_id == nvidia_variant_id


async def test_balanced_routing_cursor_wraps_without_bigint_overflow(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.routing_cursor = 2**63 - 1

    async with gateway_database.session() as session:
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        original_version = logical_model.version
        original_updated_at = logical_model.updated_at

    async with gateway_database.session() as session, session.begin():
        _anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )
        assert route is not None
        assert route.model_variant_id == ascend_variant_id

    async with gateway_database.session() as session:
        logical_model = await session.get(LogicalModel, logical_model_id)
    assert logical_model is not None
    assert logical_model.routing_cursor == 0
    assert logical_model.version == original_version
    assert logical_model.updated_at == original_updated_at


async def test_concurrent_balanced_routes_do_not_lose_cursor_updates(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    start = asyncio.Event()

    async def choose() -> str | None:
        await start.wait()
        async with gateway_database.session() as session, session.begin():
            _anchor, route = await GatewayRoutingRepository.choose_route(
                session,
                project_id=PROJECT_ID,
                public_model="logical-chat",
            )
            assert route is not None
            return route.selected_vendor

    contenders = [asyncio.create_task(choose()) for _ in range(8)]
    await asyncio.sleep(0)
    start.set()
    selected_vendors = await asyncio.gather(*contenders)

    async with gateway_database.session() as session:
        logical_model = await session.get(LogicalModel, logical_model_id)
    assert logical_model is not None
    assert logical_model.routing_cursor == 8
    assert selected_vendors.count("nvidia") == 4
    assert selected_vendors.count("huawei-ascend") == 4


@pytest.mark.parametrize(
    "status",
    [ModelAvailabilityStatus.DEGRADED, ModelAvailabilityStatus.DISABLED],
)
async def test_non_ready_logical_model_is_not_routable(
    gateway_database: Database,
    status: ModelAvailabilityStatus,
) -> None:
    logical_model_id, _nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        logical_model = await session.get(LogicalModel, logical_model_id)
        assert logical_model is not None
        logical_model.status = status

    async with gateway_database.session() as session, session.begin():
        anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )

    assert anchor is not None
    assert route is None


@pytest.mark.parametrize(
    "status",
    [ModelAvailabilityStatus.DEGRADED, ModelAvailabilityStatus.DISABLED],
)
async def test_non_ready_preferred_variant_is_skipped_with_reason(
    gateway_database: Database,
    status: ModelAvailabilityStatus,
) -> None:
    _logical_model_id, nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        nvidia_variant = await session.get(ModelVariant, nvidia_variant_id)
        assert nvidia_variant is not None
        nvidia_variant.status = status

    async with gateway_database.session() as session, session.begin():
        _anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )

    assert route is not None
    assert route.model_variant_id == ascend_variant_id
    assert route.selected_vendor == "huawei-ascend"
    assert [(skip.selected_vendor, skip.reason) for skip in route.preflight_skips] == [
        ("nvidia", "variant_not_ready")
    ]


async def test_route_metadata_identifies_preferred_vendor_open_circuit_skip(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        session.add(
            VendorCircuitState(
                project_id=PROJECT_ID,
                logical_model_id=logical_model_id,
                vendor="nvidia",
                state="open",
                failure_count=2,
                opened_until=datetime.now(UTC) + timedelta(minutes=1),
                last_error_code="UPSTREAM_DISCONNECTED",
                version=2,
                updated_at=datetime.now(UTC),
            )
        )

    async with gateway_database.session() as session, session.begin():
        _anchor, route = await GatewayRoutingRepository.choose_route(
            session,
            project_id=PROJECT_ID,
            public_model="logical-chat",
        )

    assert route is not None
    assert route.model_variant_id == ascend_variant_id
    assert route.selected_vendor == "huawei-ascend"
    assert [(skip.selected_vendor, skip.reason) for skip in route.preflight_skips] == [
        ("nvidia", "circuit_open")
    ]


async def test_concurrent_first_circuit_outcomes_create_one_state_row(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    route = GatewayRoute(
        service_id=uuid.uuid4(),
        logical_model_id=logical_model_id,
        model_variant_id=nvidia_variant_id,
        selected_vendor="nvidia",
        upstream_model="physical/nvidia",
        gpu_count=1,
        selection=EndpointSelection(
            service_id=uuid.uuid4(),
            replica_id=uuid.uuid4(),
            generation=1,
            execution_id=uuid.uuid4(),
            endpoint_url="http://worker-a.test:8000",
        ),
    )
    start = asyncio.Event()

    async def record_failure() -> None:
        await start.wait()
        async with gateway_database.session() as session, session.begin():
            await GatewayRoutingRepository.record_outcome(
                session,
                route=route,
                project_id=PROJECT_ID,
                success=False,
                error_code="UPSTREAM_DISCONNECTED",
                failure_threshold=100,
                cooldown_seconds=30,
            )

    tasks = [asyncio.create_task(record_failure()) for _ in range(8)]
    await asyncio.sleep(0)
    start.set()
    await asyncio.gather(*tasks)

    async with gateway_database.session() as session:
        states = list(
            await session.scalars(
                select(VendorCircuitState).where(
                    VendorCircuitState.project_id == PROJECT_ID,
                    VendorCircuitState.logical_model_id == logical_model_id,
                    VendorCircuitState.vendor == "nvidia",
                )
            )
        )
    assert len(states) == 1
    assert states[0].state == "closed"
    assert states[0].failure_count == 8
    assert states[0].version == 8


async def test_vendor_fallback_is_bounded_to_one_alternate(
    gateway_database: Database,
) -> None:
    _logical_model_id, nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    calls = 0

    async def upstream(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("injected backend fault", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="logical-chat",
                path="/v1/completions",
                payload={"model": "logical-chat", "prompt": "fault"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )
    assert error.value.code == "UPSTREAM_DISCONNECTED"
    assert calls == 2

    async with gateway_database.session() as session:
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert len(audits) == 1
    assert audits[0].outcome == "failure"
    assert audits[0].details["from_variant_id"] == str(nvidia_variant_id)
    assert audits[0].details["from_vendor"] == "nvidia"
    assert audits[0].details["to_variant_id"] == str(ascend_variant_id)
    assert audits[0].details["to_vendor"] == "huawei-ascend"
    assert audits[0].details["from_service_id"] != audits[0].details["to_service_id"]
    assert audits[0].details["reason"] == "UPSTREAM_DISCONNECTED"
    assert audits[0].details["failure_reason"] == "UPSTREAM_DISCONNECTED"
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "huawei-ascend",
        ascend_variant_id,
    )
    assert usages[0].outcome == "upstream_disconnected"
    assert usages[0].allocated_gpu_seconds is None
    assert audits[0].request_id == str(usages[0].request_id)


async def test_upstream_5xx_is_returned_without_cross_vendor_replay(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        return httpx.Response(503, json={"error": "backend unavailable"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
            circuit_failure_threshold=1,
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="logical-chat",
            path="/v1/chat/completions",
            payload={"model": "logical-chat", "messages": []},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )

    assert result.status_code == 503
    assert len(hosts) == 1
    assert hosts[0].startswith("worker-")
    async with gateway_database.session() as session:
        circuit = await session.scalar(
            select(VendorCircuitState).where(
                VendorCircuitState.logical_model_id == logical_model_id,
                VendorCircuitState.vendor == "nvidia",
            )
        )
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert circuit is not None and circuit.state == "open"
    assert audits == []
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "nvidia",
        nvidia_variant_id,
    )
    assert usages[0].outcome == "upstream_error"


@pytest.mark.parametrize(
    ("failure_type", "expected_code"),
    [
        ("read_timeout", "INFERENCE_REQUEST_TIMEOUT"),
        ("write_timeout", "INFERENCE_REQUEST_TIMEOUT"),
        ("pool_timeout", "INFERENCE_REQUEST_TIMEOUT"),
        ("read_error", "UPSTREAM_DISCONNECTED"),
    ],
)
async def test_ambiguous_transport_failure_never_cross_vendor_replays_post(
    gateway_database: Database,
    failure_type: str,
    expected_code: str,
) -> None:
    _logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    calls = 0

    async def upstream(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        failure_class = {
            "read_timeout": httpx.ReadTimeout,
            "write_timeout": httpx.WriteTimeout,
            "pool_timeout": httpx.PoolTimeout,
            "read_error": httpx.ReadError,
        }[failure_type]
        raise failure_class("ambiguous transport failure", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="logical-chat",
                path="/v1/completions",
                payload={"model": "logical-chat", "prompt": failure_type},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )

    assert error.value.code == expected_code
    assert calls == 1
    async with gateway_database.session() as session:
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert audits == []
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "nvidia",
        nvidia_variant_id,
    )


async def test_buffered_response_read_failure_never_cross_vendor_replays_post(
    gateway_database: Database,
) -> None:
    _logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=FaultAfterFirstChunkStream(request),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
        )
        with pytest.raises(APIError) as error:
            await gateway.forward(
                project_id=PROJECT_ID,
                public_model="logical-chat",
                path="/v1/completions",
                payload={"model": "logical-chat", "prompt": "read-fault"},
                request_headers={},
                stream_requested=False,
                client_disconnected=lambda: _false(),
            )

    assert error.value.code == "UPSTREAM_DISCONNECTED"
    assert len(hosts) == 1
    assert hosts[0].startswith("worker-")
    async with gateway_database.session() as session:
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert audits == []
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "nvidia",
        nvidia_variant_id,
    )
    assert usages[0].outcome == "upstream_disconnected"


async def test_unsafe_endpoint_preflight_can_fallback_before_post_dispatch(
    gateway_database: Database,
) -> None:
    _logical_model_id, _nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    async with gateway_database.session() as session, session.begin():
        nvidia_service = await session.scalar(
            select(ModelService).where(ModelService.selected_vendor == "nvidia")
        )
        assert nvidia_service is not None
        nvidia_replicas = await ServiceRepository.list_replicas(
            session,
            nvidia_service.id,
            for_update=True,
        )
        for replica in nvidia_replicas:
            replica.endpoint_url = "http://public.example:8000"

    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        return httpx.Response(200, json={"choices": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="ascend.test",
            fallback_attempts=1,
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="logical-chat",
            path="/v1/completions",
            payload={"model": "logical-chat", "prompt": "preflight"},
            request_headers={},
            stream_requested=False,
            client_disconnected=lambda: _false(),
        )

    assert result.status_code == 200
    assert hosts == ["ascend.test"]
    async with gateway_database.session() as session:
        audit = await session.scalar(
            select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert audit is not None and audit.outcome == "success"
    assert audit.details["reason"] == "UNSAFE_REPLICA_ENDPOINT"
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "huawei-ascend",
        ascend_variant_id,
    )


async def test_sse_stream_start_pins_vendor_and_does_not_fallback(
    gateway_database: Database,
) -> None:
    logical_model_id, nvidia_variant_id, _ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        if request.url.host == "ascend.test":
            return httpx.Response(200, json={"choices": []})
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=FaultAfterFirstChunkStream(request),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
            circuit_failure_threshold=1,
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="logical-chat",
            path="/v1/chat/completions",
            payload={"model": "logical-chat", "messages": [], "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )
        assert result.stream is not None
        chunks = [chunk async for chunk in result.stream]

    assert chunks == [b'data: {"choices":[{"delta":{"content":"pinned"}}]}\n\n']
    assert len(hosts) == 1
    assert hosts[0].startswith("worker-")
    assert result.headers["x-mini-ai-model-variant-id"] == str(nvidia_variant_id)
    async with gateway_database.session() as session:
        circuit = await session.scalar(
            select(VendorCircuitState).where(
                VendorCircuitState.logical_model_id == logical_model_id,
                VendorCircuitState.vendor == "nvidia",
            )
        )
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert circuit is not None and circuit.state == "open"
    assert circuit.last_error_code == "UPSTREAM_DISCONNECTED"
    assert audits == []
    assert len(usages) == 1
    assert usages[0].outcome == "upstream_disconnected"
    assert usages[0].error_code == "UPSTREAM_DISCONNECTED"


async def test_fallback_stream_defers_success_circuit_audit_and_usage_until_completion(
    gateway_database: Database,
) -> None:
    logical_model_id, _nvidia_variant_id, ascend_variant_id = await _ready_dual_vendor_services(
        gateway_database
    )
    stream = GatedCompletionStream()
    hosts: list[str] = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        hosts.append(request.url.host)
        if request.url.host.startswith("worker-"):
            raise httpx.ConnectError("injected nvidia connect fault", request=request)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=stream,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        gateway = GatewayService(
            gateway_database,
            client,
            GatewayMetrics(),
            request_timeout=2,
            endpoint_host_allowlist="*.test",
            fallback_attempts=1,
            circuit_failure_threshold=1,
        )
        result = await gateway.forward(
            project_id=PROJECT_ID,
            public_model="logical-chat",
            path="/v1/chat/completions",
            payload={"model": "logical-chat", "messages": [], "stream": True},
            request_headers={},
            stream_requested=True,
            client_disconnected=lambda: _false(),
        )

        async with gateway_database.session() as session:
            ascend_circuit_before = await session.scalar(
                select(VendorCircuitState).where(
                    VendorCircuitState.logical_model_id == logical_model_id,
                    VendorCircuitState.vendor == "huawei-ascend",
                )
            )
            audits_before = list(
                await session.scalars(
                    select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
                )
            )
            usages_before = list(await session.scalars(select(ServingRequestUsage)))
        assert ascend_circuit_before is None
        assert audits_before == []
        assert usages_before == []

        stream.allow_completion.set()
        assert result.stream is not None
        chunks = [chunk async for chunk in result.stream]

    assert chunks == [b'data: {"choices":[]}\n\n', b"data: [DONE]\n\n"]
    assert stream.closed is True
    assert len(hosts) == 2
    async with gateway_database.session() as session:
        ascend_circuit = await session.scalar(
            select(VendorCircuitState).where(
                VendorCircuitState.logical_model_id == logical_model_id,
                VendorCircuitState.vendor == "huawei-ascend",
            )
        )
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "gateway.vendor_fallback")
            )
        )
        usages = list(await session.scalars(select(ServingRequestUsage)))
    assert ascend_circuit is not None and ascend_circuit.state == "closed"
    assert len(audits) == 1 and audits[0].outcome == "success"
    assert len(usages) == 1
    assert (usages[0].selected_vendor, usages[0].model_variant_id) == (
        "huawei-ascend",
        ascend_variant_id,
    )
