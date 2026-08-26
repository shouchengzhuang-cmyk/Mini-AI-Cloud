import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from starlette.types import Message, Receive, Scope, Send
from starlette.websockets import WebSocketState

from api.main import create_app
from api.middleware import RequestBodyLimitMiddleware
from api.pagination import CursorKey
from api.routes.events import _send_available, project_event_stream
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus
from core.rbac import Principal, PrincipalKind, ProjectRole, ProjectStatus
from core.redis import RedisQueue
from models.identity import Project
from models.outbox import OutboxEvent
from models.task import Task
from models.usage import AuditEvent
from repositories.audit import AuditRepository
from repositories.events import ProjectEventRepository

pytestmark = pytest.mark.integration


async def _chunked_body(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


async def test_body_limit_replay_preserves_live_disconnect_channel() -> None:
    received: list[Message] = []

    async def downstream(scope: Scope, receive: Receive, send: Send) -> None:
        del scope, send
        received.append(await receive())
        received.append(await receive())

    messages: list[Message] = [
        {"type": "http.request", "body": b"payload", "more_body": False},
        {"type": "http.disconnect"},
    ]
    message_iterator = iter(messages)

    async def receive() -> Message:
        return next(message_iterator)

    async def send(_message: Message) -> None:
        return None

    middleware = RequestBodyLimitMiddleware(downstream, max_bytes=1024)
    await middleware(
        cast(
            Scope,
            {
                "type": "http",
                "method": "POST",
                "path": "/api/v1/tasks",
                "headers": [],
            },
        ),
        receive,
        send,
    )

    assert received == [
        {"type": "http.request", "body": b"payload", "more_body": False},
        {"type": "http.disconnect"},
    ]


def _bootstrap_payload() -> dict[str, object]:
    return {
        "user": {
            "username": "audit-owner",
            "email": "audit-owner@example.com",
            "password": "correct horse battery staple",
        },
        "project": {"name": "Audit Primary", "slug": "audit-primary"},
        "api_key_name": "audit-bootstrap",
    }


async def test_chunked_body_without_content_length_is_rejected(
    database: Database,
    redis_queue: RedisQueue,
) -> None:
    settings = Settings(
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        api_request_max_bytes=1024,
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.post(
            "/api/v1/bootstrap",
            content=_chunked_body(b"a" * 700, b"b" * 700),
            headers={"Content-Type": "application/json", "X-Request-ID": "chunked-limit"},
        )

    assert response.request.headers.get("content-length") is None
    assert response.request.headers.get("transfer-encoding") == "chunked"
    assert response.status_code == 413
    assert response.headers["X-Request-ID"] == "chunked-limit"
    assert response.json()["error"]["code"] == "REQUEST_TOO_LARGE"


async def test_request_metrics_middleware_preserves_unhandled_exception(
    database: Database,
    redis_queue: RedisQueue,
) -> None:
    settings = Settings(
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )

    @app.get("/_test/unhandled")
    async def unhandled() -> None:
        raise RuntimeError("sentinel route failure")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        with pytest.raises(RuntimeError, match="sentinel route failure"):
            await client.get("/_test/unhandled")


async def test_identity_writes_are_audited_and_cursor_paginated(
    api_client: AsyncClient,
    database: Database,
) -> None:
    bootstrapped = await api_client.post("/api/v1/bootstrap", json=_bootstrap_payload())
    assert bootstrapped.status_code == 201
    project_id = uuid.UUID(bootstrapped.json()["project"]["id"])
    auth = {"Authorization": f"Bearer {bootstrapped.json()['api_key']['api_key']}"}

    first = await api_client.post(
        "/api/v1/users",
        json={
            "username": "audited-user",
            "email": "audited-user@example.com",
            "password": "another correct horse battery staple",
        },
        headers={**auth, "X-Request-ID": "audit-success"},
    )
    assert first.status_code == 201
    duplicate = await api_client.post(
        "/api/v1/users",
        json={
            "username": "audited-user",
            "email": "other@example.com",
            "password": "another correct horse battery staple",
        },
        headers={**auth, "X-Request-ID": "audit-failure"},
    )
    assert duplicate.status_code == 409

    first_page = await api_client.get("/api/v1/audit-events", params={"limit": 1}, headers=auth)
    assert first_page.status_code == 200
    first_body = first_page.json()
    assert first_body["pagination"]["total"] == 2
    assert first_body["pagination"]["next_cursor"]
    assert first_body["items"][0]["project_id"] == str(project_id)
    assert first_body["items"][0]["action"] == "user.create"
    assert first_body["items"][0]["source_ip"] is not None

    second_page = await api_client.get(
        "/api/v1/audit-events",
        params={"limit": 1, "cursor": first_body["pagination"]["next_cursor"]},
        headers=auth,
    )
    assert second_page.status_code == 200
    cursor_events = first_body["items"] + second_page.json()["items"]
    assert {item["request_id"]: item["outcome"] for item in cursor_events} == {
        "audit-success": "success",
        "audit-failure": "failure",
    }

    offset_page = await api_client.get(
        "/api/v1/audit-events", params={"limit": 1, "offset": 1}, headers=auth
    )
    assert offset_page.status_code == 200
    assert offset_page.json()["items"] == second_page.json()["items"]

    async with database.session() as session:
        global_bootstrap = await session.scalar(
            select(AuditEvent).where(AuditEvent.action == "identity.bootstrap")
        )
    assert global_bootstrap is not None and global_bootstrap.project_id is None


async def test_authenticated_quota_write_uses_generic_audit_middleware(
    api_client: AsyncClient,
    database: Database,
) -> None:
    bootstrapped = await api_client.post("/api/v1/bootstrap", json=_bootstrap_payload())
    assert bootstrapped.status_code == 201
    project_id = uuid.UUID(bootstrapped.json()["project"]["id"])
    api_key_id = uuid.UUID(bootstrapped.json()["api_key"]["id"])
    auth = {"Authorization": f"Bearer {bootstrapped.json()['api_key']['api_key']}"}
    updated = await api_client.put(
        f"/api/v1/projects/{project_id}/quota",
        json={"max_queued_tasks": 50},
        headers={**auth, "X-Request-ID": "quota-audit"},
    )
    assert updated.status_code == 200

    async with database.session() as session:
        event = await session.scalar(
            select(AuditEvent).where(AuditEvent.request_id == "quota-audit")
        )
    assert event is not None
    assert event.project_id == project_id
    assert event.api_key_id == api_key_id
    assert event.actor_type == "api_key"
    assert event.action == "project.replace_project_quota"
    assert event.resource_type == "project"
    assert event.resource_id == str(project_id)
    assert event.outcome == "success"


async def test_audit_and_outbox_event_reads_never_cross_projects(database: Database) -> None:
    first_project_id = uuid.UUID("30000000-0000-0000-0000-000000000001")
    second_project_id = uuid.UUID("30000000-0000-0000-0000-000000000002")
    first_task_id = uuid.UUID("31000000-0000-0000-0000-000000000001")
    second_task_id = uuid.UUID("31000000-0000-0000-0000-000000000002")
    first_event_id = uuid.UUID("32000000-0000-0000-0000-000000000001")
    second_event_id = uuid.UUID("32000000-0000-0000-0000-000000000002")
    occurred_at = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)

    async with database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=first_project_id,
                    name="First tenant",
                    slug="event-first",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=second_project_id,
                    name="Second tenant",
                    slug="event-second",
                    status=ProjectStatus.ACTIVE,
                ),
                Task(
                    id=first_task_id,
                    project_id=first_project_id,
                    image="example/test:latest",
                    command=["true"],
                    status=TaskStatus.QUEUED,
                ),
                Task(
                    id=second_task_id,
                    project_id=second_project_id,
                    image="example/test:latest",
                    command=["true"],
                    status=TaskStatus.QUEUED,
                ),
            ]
        )
        session.add_all(
            [
                OutboxEvent(
                    id=first_event_id,
                    aggregate_id=first_task_id,
                    aggregate_type="task",
                    event_type="task.ready",
                    payload={"task_id": str(first_task_id)},
                    created_at=occurred_at,
                    available_at=occurred_at,
                ),
                OutboxEvent(
                    id=second_event_id,
                    aggregate_id=second_task_id,
                    aggregate_type="task",
                    event_type="task.ready",
                    payload={"task_id": str(second_task_id)},
                    created_at=occurred_at + timedelta(seconds=1),
                    available_at=occurred_at,
                ),
            ]
        )
        first_audit = await AuditRepository.record(
            session,
            project_id=first_project_id,
            actor_type="system",
            actor_user_id=None,
            api_key_id=None,
            action="test.first",
            resource_type="test",
            resource_id=None,
            outcome="success",
            request_id=None,
            source_ip=None,
            occurred_at=occurred_at,
        )
        await AuditRepository.record(
            session,
            project_id=second_project_id,
            actor_type="system",
            actor_user_id=None,
            api_key_id=None,
            action="test.second",
            resource_type="test",
            resource_id=None,
            outcome="success",
            request_id=None,
            source_ip=None,
            occurred_at=occurred_at,
        )

    async with database.session() as session:
        first_events = await ProjectEventRepository.list_for_project(
            session, project_id=first_project_id, limit=100
        )
        first_audits = await AuditRepository.list_for_project(
            session, project_id=first_project_id, limit=100
        )
        hidden = await AuditRepository.get_for_project(
            session,
            project_id=second_project_id,
            event_id=first_audit.id,
        )
        resumed = await ProjectEventRepository.list_for_project(
            session,
            project_id=first_project_id,
            limit=100,
            after=CursorKey(created_at=occurred_at, item_id=first_event_id),
        )

    assert [event.id for event in first_events] == [first_event_id]
    assert [event.action for event in first_audits] == ["test.first"]
    assert hidden is None
    assert resumed == []


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []
        self.closed: tuple[int, str | None] | None = None
        self.client_state = WebSocketState.DISCONNECTED

    async def send_json(self, payload: dict[str, object]) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed = (code, reason)


async def test_websocket_batch_cursor_resumes_without_replaying_or_crossing_tenants(
    database: Database,
) -> None:
    first_project_id = uuid.UUID("40000000-0000-0000-0000-000000000001")
    second_project_id = uuid.UUID("40000000-0000-0000-0000-000000000002")
    first_event_id = uuid.UUID("42000000-0000-0000-0000-000000000001")
    second_event_id = uuid.UUID("42000000-0000-0000-0000-000000000002")
    third_event_id = uuid.UUID("42000000-0000-0000-0000-000000000003")
    occurred_at = datetime(2026, 8, 23, 13, 0, tzinfo=UTC)
    async with database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=first_project_id,
                    name="WebSocket first",
                    slug="websocket-first",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=second_project_id,
                    name="WebSocket second",
                    slug="websocket-second",
                    status=ProjectStatus.ACTIVE,
                ),
                OutboxEvent(
                    id=first_event_id,
                    aggregate_id=uuid.uuid4(),
                    aggregate_type="project",
                    event_type="service.ready",
                    payload={"project_id": str(first_project_id)},
                    created_at=occurred_at,
                    available_at=occurred_at,
                ),
                OutboxEvent(
                    id=second_event_id,
                    aggregate_id=uuid.uuid4(),
                    aggregate_type="project",
                    event_type="service.ready",
                    payload={"project_id": str(second_project_id)},
                    created_at=occurred_at + timedelta(seconds=1),
                    available_at=occurred_at,
                ),
            ]
        )

    settings = Settings(control_plane_enabled=False)
    first_socket = _FakeWebSocket()
    cursor = await _send_available(
        first_socket,  # type: ignore[arg-type]
        database,
        first_project_id,
        None,
        settings,
    )
    assert [item["id"] for item in first_socket.sent] == [str(first_event_id)]
    assert cursor is not None

    async with database.session() as session, session.begin():
        session.add(
            OutboxEvent(
                id=third_event_id,
                aggregate_id=uuid.uuid4(),
                aggregate_type="project",
                event_type="service.scaled",
                payload={"project_id": str(first_project_id)},
                created_at=occurred_at + timedelta(seconds=2),
                available_at=occurred_at,
            )
        )
    resumed_socket = _FakeWebSocket()
    resumed_cursor = await _send_available(
        resumed_socket,  # type: ignore[arg-type]
        database,
        first_project_id,
        cursor,
        settings,
    )
    assert [item["id"] for item in resumed_socket.sent] == [str(third_event_id)]
    assert resumed_cursor is not None and resumed_cursor.item_id == third_event_id

    denied_socket = _FakeWebSocket()
    cross_project_principal = Principal(
        kind=PrincipalKind.API_KEY,
        project_id=second_project_id,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=ProjectRole.VIEWER,
        key_prefix="mkc_websocket_test",
    )
    await project_event_stream(
        denied_socket,  # type: ignore[arg-type]
        first_project_id,
        cross_project_principal,
        None,
    )
    assert denied_socket.closed == (4403, "PROJECT_ACCESS_DENIED")
