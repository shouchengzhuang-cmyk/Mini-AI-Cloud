import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event

from core.database import Database
from core.rbac import MembershipStatus, PrincipalKind, ProjectRole
from core.security import hash_password
from models.base import Base
from models.identity import ApiKey, Project, ProjectMembership, User
from repositories.identity import (
    ApiKeyRepository,
    LastProjectOwnerError,
    MembershipRepository,
    ProjectRepository,
    UserRepository,
)


@pytest_asyncio.fixture
async def identity_database(tmp_path: Any) -> AsyncIterator[Database]:
    path = (tmp_path / "identity.sqlite3").as_posix()
    database = Database(f"sqlite+aiosqlite:///{path}")

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
                cast(Table, ProjectMembership.__table__),
                cast(Table, ApiKey.__table__),
            ],
        )
    try:
        yield database
    finally:
        await database.dispose()


async def _create_user(database: Database, *, suffix: str) -> User:
    async with database.session() as session, session.begin():
        return await UserRepository.create(
            session,
            username=f"user-{suffix}",
            email=f"user-{suffix}@example.com",
            password_hash=hash_password(f"sufficient password for {suffix}"),
        )


async def test_create_project_issues_and_authenticates_one_time_api_key(
    identity_database: Database,
) -> None:
    owner = await _create_user(identity_database, suffix="owner")
    hmac_key = b"k" * 32
    async with identity_database.session() as session, session.begin():
        project, membership = await ProjectRepository.create_with_owner(
            session,
            name="Example Project",
            slug="example-project",
            owner_user_id=owner.id,
        )
        issued = await ApiKeyRepository.issue(
            session,
            project_id=project.id,
            user_id=owner.id,
            name="automation",
            hmac_key=hmac_key,
            hash_key_id="primary-v1",
            created_by_user_id=owner.id,
        )
        project_id = project.id
        api_key_id = issued.api_key.id
        token = issued.token

    assert membership.role == ProjectRole.OWNER
    assert token not in repr(issued)
    assert token.encode() != issued.api_key.secret_hash

    async with identity_database.session() as session:
        principal = await ApiKeyRepository.authenticate(
            session,
            token,
            resolve_hmac_key=lambda key_id: hmac_key if key_id == "primary-v1" else None,
        )

    assert principal is not None
    assert principal.kind == PrincipalKind.API_KEY
    assert principal.project_id == project_id
    assert principal.user_id == owner.id
    assert principal.api_key_id == api_key_id
    assert principal.role == ProjectRole.OWNER

    async with identity_database.session() as session, session.begin():
        revoked = await ApiKeyRepository.revoke(session, api_key_id)
    assert revoked is not None and revoked.revoked_at is not None

    async with identity_database.session() as session:
        assert (
            await ApiKeyRepository.authenticate(
                session,
                token,
                resolve_hmac_key=lambda _key_id: hmac_key,
            )
            is None
        )


async def test_membership_changes_preserve_last_owner_invariant(
    identity_database: Database,
) -> None:
    owner = await _create_user(identity_database, suffix="first-owner")
    second_owner = await _create_user(identity_database, suffix="second-owner")
    async with identity_database.session() as session, session.begin():
        project, _membership = await ProjectRepository.create_with_owner(
            session,
            name="Owner Safety",
            slug="owner-safety",
            owner_user_id=owner.id,
        )
        project_id = project.id

    async with identity_database.session() as session, session.begin():
        with pytest.raises(LastProjectOwnerError):
            await MembershipRepository.remove(
                session,
                project_id=project_id,
                user_id=owner.id,
            )

    async with identity_database.session() as session, session.begin():
        await MembershipRepository.add_or_restore(
            session,
            project_id=project_id,
            user_id=second_owner.id,
            role=ProjectRole.OWNER,
            created_by_user_id=owner.id,
        )
        removed = await MembershipRepository.remove(
            session,
            project_id=project_id,
            user_id=owner.id,
        )

    assert removed.status == MembershipStatus.REMOVED
    assert removed.removed_at is not None


async def test_user_repository_rejects_plaintext_password(
    identity_database: Database,
) -> None:
    async with identity_database.session() as session, session.begin():
        with pytest.raises(ValueError, match="Argon2id"):
            await UserRepository.create(
                session,
                username=f"plaintext-{uuid.uuid4().hex[:8]}",
                email=f"plaintext-{uuid.uuid4().hex[:8]}@example.com",
                password_hash="this is plaintext",
            )
