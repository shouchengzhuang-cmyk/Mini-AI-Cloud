import hashlib
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Table, event

from api.errors import register_exception_handlers
from api.routes.artifacts import router
from core.artifacts import LocalArtifactStore
from core.config import Settings
from core.database import Database
from models.artifact import Artifact, Dataset, DatasetVersion, TaskArtifact
from models.base import Base
from models.identity import Project, User
from models.usage import ProjectQuota, ProjectQuotaState

LEGACY_PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def artifact_client(tmp_path: Path) -> AsyncIterator[AsyncClient]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'api.sqlite3').as_posix()}")

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
                cast(Table, Artifact.__table__),
                cast(Table, TaskArtifact.__table__),
                cast(Table, Dataset.__table__),
                cast(Table, DatasetVersion.__table__),
            ],
        )
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=LEGACY_PROJECT_ID,
                name="Legacy artifacts",
                slug="legacy-artifacts",
            )
        )
        session.add(ProjectQuota(project_id=LEGACY_PROJECT_ID, max_artifact_bytes=4096))
        session.add(ProjectQuotaState(project_id=LEGACY_PROJECT_ID))

    settings = Settings(
        _env_file=None,
        legacy_project_id=str(LEGACY_PROJECT_ID),
        artifact_backend="local",
        artifact_local_root=str(tmp_path / "objects"),
        artifact_max_bytes=1024,
    )
    app = FastAPI()
    app.state.database = database
    app.state.settings = settings
    app.state.artifact_store = LocalArtifactStore(
        settings.artifact_local_root,
        max_bytes=settings.artifact_max_bytes,
    )
    register_exception_handlers(app)
    app.include_router(router)
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
    finally:
        await database.dispose()


async def test_local_artifact_api_uses_authorized_streaming_and_hides_object_key(
    artifact_client: AsyncClient,
) -> None:
    content = b"api artifact"
    checksum = hashlib.sha256(content).hexdigest()

    created = await artifact_client.post(
        "/api/v1/artifacts",
        json={
            "name": "result.bin",
            "content_type": "application/octet-stream",
            "size_bytes": len(content),
            "sha256": checksum,
        },
    )
    assert created.status_code == 201
    artifact = created.json()
    artifact_id = artifact["id"]
    assert "object_key" not in artifact
    assert artifact["state"] == "pending"

    grant = await artifact_client.post(
        f"/api/v1/artifacts/{artifact_id}/upload-url",
        headers={"Host": "attacker.example"},
    )
    assert grant.status_code == 200
    assert grant.json()["authorization"] == "api"
    assert grant.json()["method"] == "PUT"
    assert grant.json()["url"] == f"/api/v1/artifacts/{artifact_id}/content"

    uploaded = await artifact_client.put(
        f"/api/v1/artifacts/{artifact_id}/content",
        content=content,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Content-SHA256": checksum,
        },
    )
    assert uploaded.status_code == 200

    finalized = await artifact_client.post(
        f"/api/v1/artifacts/{artifact_id}/finalize",
        json={"size_bytes": len(content), "sha256": checksum},
    )
    assert finalized.status_code == 200
    assert finalized.json()["state"] == "ready"

    download_grant = await artifact_client.get(
        f"/api/v1/artifacts/{artifact_id}/download-url",
        headers={"Host": "attacker.example"},
    )
    assert download_grant.status_code == 200
    assert download_grant.json()["authorization"] == "api"
    assert download_grant.json()["url"] == f"/api/v1/artifacts/{artifact_id}/content"

    downloaded = await artifact_client.get(f"/api/v1/artifacts/{artifact_id}/content")
    assert downloaded.status_code == 200
    assert downloaded.content == content
    assert downloaded.headers["etag"] == f'"{checksum}"'


async def test_artifact_api_enforces_dynamic_size_limit_and_checksum(
    artifact_client: AsyncClient,
) -> None:
    too_large = await artifact_client.post(
        "/api/v1/artifacts",
        json={
            "name": "large.bin",
            "size_bytes": 1025,
            "sha256": hashlib.sha256(b"x" * 1025).hexdigest(),
        },
    )
    assert too_large.status_code == 413
    assert too_large.json()["error"]["code"] == "ARTIFACT_TOO_LARGE"

    content = b"correct"
    checksum = hashlib.sha256(content).hexdigest()
    created = await artifact_client.post(
        "/api/v1/artifacts",
        json={
            "name": "checksum.bin",
            "size_bytes": len(content),
            "sha256": checksum,
        },
    )
    artifact_id = created.json()["id"]
    rejected = await artifact_client.put(
        f"/api/v1/artifacts/{artifact_id}/content",
        content=content,
        headers={"X-Content-SHA256": "f" * 64},
    )
    assert rejected.status_code == 422
    assert rejected.json()["error"]["code"] == "ARTIFACT_INTEGRITY_MISMATCH"
