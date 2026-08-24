import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import event

from core.config import Settings
from core.database import Database
from core.rbac import ProjectStatus
from core.redis import RedisQueue
from models import Base
from models.identity import Project


@pytest_asyncio.fixture
async def database(tmp_path: Any) -> AsyncIterator[Database]:
    path = (tmp_path / "integration.sqlite3").as_posix()
    database = Database(f"sqlite+aiosqlite:///{path}?timeout=30")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
                name="Legacy local project",
                slug="legacy-local",
                status=ProjectStatus.ACTIVE,
            )
        )

    try:
        yield database
    finally:
        await database.dispose()


@pytest_asyncio.fixture
async def redis_queue() -> AsyncIterator[RedisQueue]:
    queue = RedisQueue("redis://unused.invalid/0", log_stream_maxlen=100)
    original_client = queue.client
    queue.client = cast(Redis, FakeRedis(decode_responses=True))
    await original_client.aclose()
    try:
        yield queue
    finally:
        await queue.close()


@pytest_asyncio.fixture
async def api_client(
    database: Database, redis_queue: RedisQueue, tmp_path: Any
) -> AsyncIterator[AsyncClient]:
    from api.main import create_app

    settings = Settings(
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        artifact_local_root=str(tmp_path / "artifacts"),
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
