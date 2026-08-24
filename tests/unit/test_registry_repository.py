import base64
import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event, select

from core.database import Database
from core.image_policy import ImagePolicyAction, ImageRule
from core.rbac import ProjectStatus
from core.secrets import SecretCipher, SecretKeyRing
from models.base import Base
from models.identity import Project, User
from models.registry import (
    ImagePolicy,
    ImagePolicyRule,
    RegisteredModel,
    Secret,
    SecretVersion,
)
from repositories.registry import ImagePolicyRepository, RegisteredModelRepository
from repositories.secrets import SecretNotFoundError, SecretRepository, SecretRevokedError


@pytest_asyncio.fixture
async def registry_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'registry.sqlite3').as_posix()}")

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
                cast(Table, RegisteredModel.__table__),
                cast(Table, Secret.__table__),
                cast(Table, SecretVersion.__table__),
                cast(Table, ImagePolicy.__table__),
                cast(Table, ImagePolicyRule.__table__),
            ],
        )
    try:
        yield database
    finally:
        await database.dispose()


def _cipher(key_id: str = "v1", key: bytes = b"k" * 32) -> SecretCipher:
    encoded = base64.urlsafe_b64encode(key).decode().rstrip("=")
    return SecretCipher(SecretKeyRing.from_encoded(f"{key_id}:{encoded}"))


async def _create_project(database: Database, slug: str) -> uuid.UUID:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=slug,
                slug=slug,
                status=ProjectStatus.ACTIVE,
            )
        )
    return project_id


async def test_secret_versions_are_encrypted_rotatable_and_project_scoped(
    registry_database: Database,
) -> None:
    project_id = await _create_project(registry_database, "secret-project")
    other_project_id = await _create_project(registry_database, "other-project")
    cipher = _cipher()
    async with registry_database.session() as session, session.begin():
        secret = await SecretRepository.create(
            session,
            project_id=project_id,
            name="DATABASE_PASSWORD",
            value="first-secret-value",
            cipher=cipher,
        )
        secret_id = secret.id

    async with registry_database.session() as session:
        stored = await session.scalar(
            select(SecretVersion).where(
                SecretVersion.secret_id == secret_id,
                SecretVersion.version == 1,
            )
        )
        assert stored is not None
        assert b"first-secret-value" not in stored.ciphertext
        assert (
            await SecretRepository.decrypt(
                session,
                project_id=project_id,
                secret_id=secret_id,
                cipher=cipher,
            )
        ).value == "first-secret-value"
        assert (
            await SecretRepository.get(
                session,
                project_id=other_project_id,
                secret_id=secret_id,
            )
            is None
        )
        with pytest.raises(SecretNotFoundError):
            await SecretRepository.decrypt(
                session,
                project_id=other_project_id,
                secret_id=secret_id,
                cipher=cipher,
            )

    async with registry_database.session() as session, session.begin():
        rotated = await SecretRepository.rotate(
            session,
            project_id=project_id,
            secret_id=secret_id,
            value="second-secret-value",
            cipher=cipher,
        )
        assert rotated.current_version == 2

    async with registry_database.session() as session:
        assert (
            await SecretRepository.decrypt(
                session,
                project_id=project_id,
                secret_id=secret_id,
                cipher=cipher,
                version=1,
            )
        ).value == "first-secret-value"
        assert (
            await SecretRepository.decrypt(
                session,
                project_id=project_id,
                secret_id=secret_id,
                cipher=cipher,
            )
        ).value == "second-secret-value"

    async with registry_database.session() as session, session.begin():
        await SecretRepository.revoke(
            session,
            project_id=project_id,
            secret_id=secret_id,
        )
    async with registry_database.session() as session:
        with pytest.raises(SecretRevokedError):
            await SecretRepository.decrypt(
                session,
                project_id=project_id,
                secret_id=secret_id,
                cipher=cipher,
            )


async def test_registered_models_and_image_policies_are_project_scoped(
    registry_database: Database,
) -> None:
    project_id = await _create_project(registry_database, "registry-project")
    other_project_id = await _create_project(registry_database, "isolated-project")
    digest = "sha256:" + "a" * 64
    async with registry_database.session() as session, session.begin():
        model = await RegisteredModelRepository.create(
            session,
            project_id=project_id,
            name="qwen-small",
            provider="huggingface",
            source="Qwen/Qwen2.5-0.5B-Instruct",
            revision="main",
            size_bytes=100,
            gpu_memory_mb=2048,
            architecture="qwen2",
            metadata={"format": "safetensors"},
            created_by_user_id=None,
        )
        model_id = model.id
        policy = await ImagePolicyRepository.replace(
            session,
            project_id=project_id,
            default_action=ImagePolicyAction.DENY,
            require_digest=True,
            rules=[
                ImageRule(
                    action=ImagePolicyAction.ALLOW,
                    registry="ghcr.io",
                    repository_glob="example/*",
                    digest=digest,
                )
            ],
        )
        assert len(policy.rules) == 1

    async with registry_database.session() as session:
        assert (
            await RegisteredModelRepository.get(
                session,
                project_id=other_project_id,
                model_id=model_id,
            )
            is None
        )
        allowed = await ImagePolicyRepository.evaluate(
            session,
            project_id=project_id,
            image=f"ghcr.io/example/model@{digest}",
        )
        denied = await ImagePolicyRepository.evaluate(
            session,
            project_id=project_id,
            image=f"ghcr.io/other/model@{digest}",
        )
        unconfigured = await ImagePolicyRepository.evaluate(
            session,
            project_id=other_project_id,
            image=f"ghcr.io/example/model@{digest}",
        )

    assert allowed.allowed is True
    assert allowed.canonical_image == f"ghcr.io/example/model@{digest}"
    assert denied.allowed is False
    assert unconfigured.allowed is False
