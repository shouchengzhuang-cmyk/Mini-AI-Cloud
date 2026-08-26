import asyncio
import json
import uuid
from collections.abc import AsyncIterator, MutableMapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Table, event, select
from starlette.types import Message, Scope

from api.dependencies import get_principal
from api.errors import APIError, register_exception_handlers
from api.routes import gateway as gateway_routes
from api.services import gateway as gateway_service_module
from api.services.autoscaler import ServiceAutoscaler
from api.services.gateway import GatewayMetrics, GatewayService, ServiceLoad
from api.services.service_health import ServiceHealthController
from api.services.service_reconciler import ServiceReconciler
from core.database import Database
from core.enums import ProjectRole, RuntimeType
from core.rbac import Principal, PrincipalKind
from models.base import Base
from models.identity import Project, User
from models.outbox import OutboxEvent
from models.registry import RegisteredModel
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServiceStatus,
    ServingRuntime,
)
from models.usage import ProjectQuota, ProjectQuotaState, ServingRequestUsage
from models.worker import Worker
from repositories.quotas import QuotaRepository
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository

PROJECT_ID = uuid.UUID("20000000-0000-0000-0000-000000000001")


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
                cast(Table, Project.__table__),
                cast(Table, ProjectQuota.__table__),
                cast(Table, ProjectQuotaState.__table__),
                cast(Table, Worker.__table__),
                cast(Table, RegisteredModel.__table__),
                cast(Table, ModelService.__table__),
                cast(Table, ServiceReplica.__table__),
                cast(Table, ServingRequestUsage.__table__),
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
