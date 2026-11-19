from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import Select, text, update
from sqlalchemy.engine import ScalarResult
from sqlalchemy.schema import CreateSchema, DropSchema

from core.database import Database
from core.scheduler_lab_export import export_v2_from_session
from models import Base, GPUDevice, Project, Task, Worker

pytestmark = [pytest.mark.integration, pytest.mark.live]


@pytest_asyncio.fixture
async def export_database() -> AsyncIterator[Database]:
    configured_url = os.getenv("LIVE_DATABASE_URL")
    if not configured_url:
        pytest.skip("set LIVE_DATABASE_URL to a PostgreSQL test database")
    database = Database(configured_url)
    schema = f"x0_export_{uuid.uuid4().hex}"
    try:
        assert database.engine.dialect.name == "postgresql"
        database.engine.update_execution_options(schema_translate_map={None: schema})
        async with database.engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
            await connection.run_sync(Base.metadata.create_all)
        try:
            yield database
        finally:
            async with database.engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
    finally:
        await database.dispose()


async def test_export_reads_one_read_only_snapshot_across_concurrent_commit(
    export_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_id = uuid.uuid4()
    producer = "mini-ai-cloud/postgresql-test"
    async with export_database.session() as session, session.begin():
        session.add(Project(id=project_id, name="Export snapshot", slug="export-snapshot"))
        session.add(
            Worker(
                id="export-worker",
                hostname="worker.invalid",
                cpu_count=1,
                memory_total_mb=1024,
                labels={"revision": "before"},
            )
        )
        await session.flush()
        session.add_all(
            [
                GPUDevice(
                    worker_id="export-worker",
                    device_uuid="test-device",
                    device_index=0,
                    model="snapshot-fixture",
                    memory_total_mb=1024,
                    memory_free_mb=1024,
                ),
                Task(
                    project_id=project_id,
                    image="example.invalid/workload",
                    command=["test"],
                    priority=10,
                ),
            ]
        )
    async with export_database.session() as session:
        before = await export_v2_from_session(session, producer=producer)

    async with export_database.session() as reader:
        scalars = reader.scalars
        transaction_settings: list[str | None] = []

        async def first_select(statement: Select[tuple[Worker]]) -> ScalarResult[Worker]:
            assert statement.column_descriptions[0]["entity"] is Worker
            workers = await scalars(statement)
            monkeypatch.setattr(reader, "scalars", scalars)
            transaction_settings.append(await reader.scalar(text("SHOW transaction_isolation")))
            transaction_settings.append(await reader.scalar(text("SHOW transaction_read_only")))
            # Publish a new generation after the reader has seen the old workers.
            async with export_database.session() as writer, writer.begin():
                await writer.execute(update(Worker).values(labels={"revision": "after"}))
                await writer.execute(update(GPUDevice).values(health="unhealthy"))
                await writer.execute(update(Task).values(priority=90))
            return workers

        monkeypatch.setattr(reader, "scalars", first_select)
        during = await export_v2_from_session(reader, producer=producer)
        assert during == before
        assert transaction_settings == ["repeatable read", "on"]

    async with export_database.session() as session:
        after = await export_v2_from_session(session, producer=producer)
    assert after["workers"] != before["workers"]
    assert after["tasks"] != before["tasks"]
