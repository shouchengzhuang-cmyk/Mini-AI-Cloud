import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from api.dependencies import get_websocket_principal
from api.main import create_app
from core.config import Settings
from core.database import Database
from core.enums import ProjectRole
from core.rbac import Principal, PrincipalKind, ProjectStatus
from models import Base
from models.identity import Project
from models.outbox import OutboxEvent

FIRST_PROJECT_ID = uuid.UUID("50000000-0000-0000-0000-000000000001")
SECOND_PROJECT_ID = uuid.UUID("50000000-0000-0000-0000-000000000002")


async def _seed_database(url: str) -> None:
    database = Database(url)
    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    now = datetime(2026, 8, 23, 14, 0, tzinfo=UTC)
    async with database.session() as session, session.begin():
        session.add_all(
            [
                Project(
                    id=FIRST_PROJECT_ID,
                    name="WebSocket first",
                    slug="ws-route-first",
                    status=ProjectStatus.ACTIVE,
                ),
                Project(
                    id=SECOND_PROJECT_ID,
                    name="WebSocket second",
                    slug="ws-route-second",
                    status=ProjectStatus.ACTIVE,
                ),
                _event(FIRST_PROJECT_ID, "first", now),
                _event(SECOND_PROJECT_ID, "hidden", now + timedelta(seconds=1)),
                _event(FIRST_PROJECT_ID, "second", now + timedelta(seconds=2)),
            ]
        )
    await database.dispose()


def _event(project_id: uuid.UUID, marker: str, created_at: datetime) -> OutboxEvent:
    return OutboxEvent(
        aggregate_id=uuid.uuid4(),
        aggregate_type="project",
        event_type="service.test",
        payload={"project_id": str(project_id), "marker": marker},
        created_at=created_at,
        available_at=created_at,
    )


def _principal() -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=FIRST_PROJECT_ID,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=ProjectRole.VIEWER,
        key_prefix="mkc_ws_route_test",
    )


def test_websocket_route_is_project_scoped_and_reconnects_from_cursor(tmp_path: Any) -> None:
    url = f"sqlite+aiosqlite:///{(tmp_path / 'websocket.sqlite3').as_posix()}"
    asyncio.run(_seed_database(url))
    database = Database(url)
    settings = Settings(
        database_url=url,
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        legacy_anonymous_enabled=False,
        outbox_poll_interval=0.05,
    )
    app = create_app(settings=settings, database=database, start_control_plane=False)
    app.dependency_overrides[get_websocket_principal] = _principal

    with TestClient(app) as client:
        with client.websocket_connect(
            f"/api/v1/projects/{FIRST_PROJECT_ID}/events/ws"
        ) as websocket:
            first = websocket.receive_json()
        assert first["payload"]["marker"] == "first"

        with client.websocket_connect(
            f"/api/v1/projects/{FIRST_PROJECT_ID}/events/ws",
            params={"cursor": first["cursor"]},
        ) as websocket:
            second = websocket.receive_json()
        assert second["payload"]["marker"] == "second"

        with pytest.raises(WebSocketDisconnect) as denied:
            with client.websocket_connect(f"/api/v1/projects/{SECOND_PROJECT_ID}/events/ws"):
                pass
        assert denied.value.code == 4403
