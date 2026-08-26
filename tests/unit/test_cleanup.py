from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest_asyncio
from sqlalchemy import Table, event

from core.database import Database
from models.artifact import Artifact, Dataset, DatasetVersion, TaskArtifact
from models.base import Base
from models.identity import Project, User
from repositories.cleanup import CleanupRepository


@pytest_asyncio.fixture
async def cleanup_database(tmp_path: Path) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'cleanup.sqlite3').as_posix()}")

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
                cast(Table, Artifact.__table__),
                cast(Table, TaskArtifact.__table__),
                cast(Table, Dataset.__table__),
                cast(Table, DatasetVersion.__table__),
            ],
        )
    try:
        yield database
    finally:
        await database.dispose()


async def test_artifact_retention_selects_old_unreferenced_objects_for_configured_backend(
    cleanup_database: Database,
) -> None:
    project_id = uuid.uuid4()
    old = datetime.now(UTC) - timedelta(days=60)
    recent = datetime.now(UTC) - timedelta(days=1)
    candidate_id = uuid.uuid4()
    referenced_id = uuid.uuid4()
    ready_id = uuid.uuid4()
    recently_verified_id = uuid.uuid4()
    other_backend_id = uuid.uuid4()
    recent_id = uuid.uuid4()
    dataset_id = uuid.uuid4()

    async with cleanup_database.session() as session, session.begin():
        session.add(Project(id=project_id, name="Retention", slug="retention"))
        await session.flush()
        session.add_all(
            [
                Artifact(
                    id=candidate_id,
                    project_id=project_id,
                    name="candidate.bin",
                    state="pending",
                    backend="local",
                    object_key=f"objects/{candidate_id.hex}",
                    created_at=old,
                ),
                Artifact(
                    id=referenced_id,
                    project_id=project_id,
                    name="dataset.bin",
                    state="failed",
                    backend="local",
                    object_key=f"objects/{referenced_id.hex}",
                    created_at=old,
                ),
                Artifact(
                    id=ready_id,
                    project_id=project_id,
                    name="ready.bin",
                    state="ready",
                    backend="local",
                    object_key=f"objects/{ready_id.hex}",
                    created_at=old,
                    verified_at=old,
                ),
                Artifact(
                    id=recently_verified_id,
                    project_id=project_id,
                    name="recently-verified.bin",
                    state="ready",
                    backend="local",
                    object_key=f"objects/{recently_verified_id.hex}",
                    created_at=old,
                    verified_at=recent,
                ),
                Artifact(
                    id=other_backend_id,
                    project_id=project_id,
                    name="other-backend.bin",
                    state="failed",
                    backend="s3",
                    object_key=f"objects/{other_backend_id.hex}",
                    created_at=old,
                ),
                Artifact(
                    id=recent_id,
                    project_id=project_id,
                    name="recent.bin",
                    state="pending",
                    backend="local",
                    object_key=f"objects/{recent_id.hex}",
                    created_at=recent,
                ),
            ]
        )
        await session.flush()
        session.add(
            Dataset(
                id=dataset_id,
                project_id=project_id,
                name="training-data",
                current_version=1,
                created_at=old,
            )
        )
        await session.flush()
        session.add(
            DatasetVersion(
                dataset_id=dataset_id,
                version=1,
                artifact_id=referenced_id,
                metadata_json={},
                created_at=old,
            )
        )

    async with cleanup_database.session() as session:
        candidates = await CleanupRepository.artifact_candidates(
            session,
            retention_days=30,
            backend="local",
            limit=100,
        )

    assert set(candidates) == {(project_id, candidate_id), (project_id, ready_id)}
