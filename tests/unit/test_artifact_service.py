import hashlib
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event

from api.schemas.artifacts import ArtifactCreate, ArtifactFinalize
from api.services.artifacts import ArtifactService
from core.artifacts import ArtifactIntegrityError, ArtifactState, LocalArtifactStore
from core.config import Settings
from core.database import Database
from core.rbac import Principal, PrincipalKind
from models.artifact import Artifact, Dataset, DatasetVersion, TaskArtifact
from models.base import Base
from models.identity import Project, User
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.artifacts import ArtifactQuotaExceededError


@pytest_asyncio.fixture
async def artifact_database(tmp_path: Path) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'artifacts.sqlite3').as_posix()}")

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
    try:
        yield database
    finally:
        await database.dispose()


async def _project(
    database: Database,
    *,
    quota_bytes: int,
) -> uuid.UUID:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=f"Project {project_id.hex[:8]}",
                slug=f"project-{project_id.hex[:8]}",
            )
        )
        session.add(ProjectQuota(project_id=project_id, max_artifact_bytes=quota_bytes))
        session.add(ProjectQuotaState(project_id=project_id))
    return project_id


def _principal(project_id: uuid.UUID) -> Principal:
    return Principal(kind=PrincipalKind.LEGACY, project_id=project_id)


def _service(database: Database, root: Path) -> ArtifactService:
    settings = Settings(
        _env_file=None,
        artifact_backend="local",
        artifact_local_root=str(root),
        artifact_max_bytes=1024,
    )
    return ArtifactService(
        database,
        settings,
        LocalArtifactStore(root, max_bytes=settings.artifact_max_bytes),
    )


async def _body(content: bytes) -> AsyncIterator[bytes]:
    yield content


async def test_artifact_service_is_project_scoped_and_finalization_is_idempotent(
    artifact_database: Database,
    tmp_path: Path,
) -> None:
    project_id = await _project(artifact_database, quota_bytes=100)
    other_project_id = await _project(artifact_database, quota_bytes=100)
    service = _service(artifact_database, tmp_path / "objects")
    content = b"verified model artifact"
    checksum = hashlib.sha256(content).hexdigest()
    payload = ArtifactCreate(
        name="model.bin",
        size_bytes=len(content),
        sha256=checksum,
    )

    artifact = await service.create(payload, principal=_principal(project_id))
    assert artifact.state == ArtifactState.PENDING.value
    assert "model.bin" not in artifact.object_key
    assert str(project_id) not in artifact.object_key
    assert await service.get(artifact.id, principal=_principal(other_project_id)) is None

    await service.upload(
        artifact.id,
        _body(content),
        principal=_principal(project_id),
        content_length=len(content),
        content_sha256=checksum,
    )
    finalized = await service.finalize(
        artifact.id,
        ArtifactFinalize(size_bytes=len(content), sha256=checksum),
        principal=_principal(project_id),
    )
    finalized_again = await service.finalize(
        artifact.id,
        ArtifactFinalize(size_bytes=len(content), sha256=checksum),
        principal=_principal(project_id),
    )
    stored, stream = await service.download(artifact.id, principal=_principal(project_id))

    assert finalized.state == ArtifactState.READY.value
    assert finalized_again.id == finalized.id
    assert stored.verified_at is not None
    assert b"".join([chunk async for chunk in stream]) == content

    async with artifact_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, project_id)
        assert quota_state is not None
        assert quota_state.artifact_bytes == len(content)

    deleted = await service.delete(artifact.id, principal=_principal(project_id))
    deleted_again = await service.delete(artifact.id, principal=_principal(project_id))
    assert deleted.state == ArtifactState.DELETED.value
    assert deleted_again.state == ArtifactState.DELETED.value
    async with artifact_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, project_id)
        assert quota_state is not None
        assert quota_state.artifact_bytes == 0


async def test_failed_upload_releases_reserved_quota_and_cannot_be_downloaded(
    artifact_database: Database,
    tmp_path: Path,
) -> None:
    project_id = await _project(artifact_database, quota_bytes=20)
    service = _service(artifact_database, tmp_path / "objects")
    expected = b"correct"
    artifact = await service.create(
        ArtifactCreate(
            name="result.bin",
            size_bytes=len(expected),
            sha256=hashlib.sha256(expected).hexdigest(),
        ),
        principal=_principal(project_id),
    )

    with pytest.raises(ArtifactIntegrityError):
        await service.upload(
            artifact.id,
            _body(b"invalid"),
            principal=_principal(project_id),
            content_length=len(expected),
            content_sha256=None,
        )

    failed = await service.get(artifact.id, principal=_principal(project_id))
    assert failed is not None
    assert failed.state == ArtifactState.FAILED.value
    assert failed.failure_reason is not None
    async with artifact_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, project_id)
        assert quota_state is not None
        assert quota_state.artifact_bytes == 0


async def test_project_artifact_quota_is_reserved_when_pending(
    artifact_database: Database,
    tmp_path: Path,
) -> None:
    project_id = await _project(artifact_database, quota_bytes=5)
    service = _service(artifact_database, tmp_path / "objects")

    with pytest.raises(ArtifactQuotaExceededError):
        await service.create(
            ArtifactCreate(
                name="too-large.bin",
                size_bytes=6,
                sha256=hashlib.sha256(b"123456").hexdigest(),
            ),
            principal=_principal(project_id),
        )

    async with artifact_database.session() as session:
        quota_state = await session.get(ProjectQuotaState, project_id)
        assert quota_state is not None
        assert quota_state.artifact_bytes == 0
