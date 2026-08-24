import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.rbac import (
    MembershipStatus,
    Principal,
    PrincipalKind,
    ProjectRole,
    ProjectStatus,
    UserStatus,
)
from core.security import (
    generate_api_key,
    is_argon2id_password_hash,
    normalize_email,
    normalize_project_slug,
    normalize_username,
    parse_api_key_prefix,
    verify_api_key,
)
from models.identity import ApiKey, Project, ProjectMembership, User
from repositories.clock import database_utcnow

ApiKeyResolver = Callable[[str], bytes | None]


class IdentityNotFoundError(LookupError):
    pass


class MembershipNotActiveError(RuntimeError):
    pass


class LastProjectOwnerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class IssuedApiKey:
    api_key: ApiKey
    token: str = field(repr=False)


class UserRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        username: str,
        email: str,
        password_hash: str,
    ) -> User:
        if not is_argon2id_password_hash(password_hash):
            raise ValueError("password_hash must be an encoded Argon2id hash")
        now = await database_utcnow(session)
        user = User(
            username=username.strip(),
            username_normalized=normalize_username(username),
            email=email.strip(),
            email_normalized=normalize_email(email),
            password_hash=password_hash,
            status=UserStatus.ACTIVE,
            created_at=now,
            updated_at=now,
            password_changed_at=now,
        )
        session.add(user)
        await session.flush()
        return user

    @staticmethod
    async def get(
        session: AsyncSession,
        user_id: uuid.UUID,
        *,
        for_update: bool = False,
    ) -> User | None:
        query = select(User).where(User.id == user_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_by_username(session: AsyncSession, username: str) -> User | None:
        normalized = normalize_username(username)
        return await session.scalar(select(User).where(User.username_normalized == normalized))

    @staticmethod
    async def get_by_email(session: AsyncSession, email: str) -> User | None:
        normalized = normalize_email(email)
        return await session.scalar(select(User).where(User.email_normalized == normalized))

    @staticmethod
    async def disable(session: AsyncSession, user_id: uuid.UUID) -> User | None:
        user = await UserRepository.get(session, user_id, for_update=True)
        if user is None:
            return None
        user.status = UserStatus.DISABLED
        user.updated_at = await database_utcnow(session)
        user.version += 1
        return user


class ProjectRepository:
    @staticmethod
    async def create_with_owner(
        session: AsyncSession,
        *,
        name: str,
        slug: str,
        owner_user_id: uuid.UUID,
    ) -> tuple[Project, ProjectMembership]:
        owner = await UserRepository.get(session, owner_user_id, for_update=True)
        if owner is None or owner.status != UserStatus.ACTIVE:
            raise IdentityNotFoundError("active project owner does not exist")

        normalized_name = name.strip()
        if not normalized_name or len(normalized_name) > 128:
            raise ValueError("project name must contain 1-128 characters")
        now = await database_utcnow(session)
        project = Project(
            name=normalized_name,
            slug=normalize_project_slug(slug),
            status=ProjectStatus.ACTIVE,
            created_by_user_id=owner.id,
            created_at=now,
            updated_at=now,
        )
        session.add(project)
        await session.flush()
        membership = ProjectMembership(
            project_id=project.id,
            user_id=owner.id,
            role=ProjectRole.OWNER,
            status=MembershipStatus.ACTIVE,
            created_by_user_id=owner.id,
            created_at=now,
            updated_at=now,
        )
        session.add(membership)
        await session.flush()
        return project, membership

    @staticmethod
    async def get(
        session: AsyncSession,
        project_id: uuid.UUID,
        *,
        for_update: bool = False,
    ) -> Project | None:
        query = select(Project).where(Project.id == project_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list_for_user(
        session: AsyncSession,
        user_id: uuid.UUID,
        *,
        limit: int,
        offset: int,
    ) -> list[Project]:
        return list(
            await session.scalars(
                select(Project)
                .join(
                    ProjectMembership,
                    ProjectMembership.project_id == Project.id,
                )
                .where(
                    ProjectMembership.user_id == user_id,
                    ProjectMembership.status == MembershipStatus.ACTIVE,
                    Project.status == ProjectStatus.ACTIVE,
                )
                .order_by(Project.created_at.desc(), Project.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def count_for_user(session: AsyncSession, user_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(Project.id))
                .join(
                    ProjectMembership,
                    ProjectMembership.project_id == Project.id,
                )
                .where(
                    ProjectMembership.user_id == user_id,
                    ProjectMembership.status == MembershipStatus.ACTIVE,
                    Project.status == ProjectStatus.ACTIVE,
                )
            )
            or 0
        )


class MembershipRepository:
    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        for_update: bool = False,
    ) -> ProjectMembership | None:
        query = select(ProjectMembership).where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == user_id,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_active(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> ProjectMembership | None:
        return await session.scalar(
            select(ProjectMembership).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == user_id,
                ProjectMembership.status == MembershipStatus.ACTIVE,
            )
        )

    @staticmethod
    async def add_or_restore(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        role: ProjectRole,
        created_by_user_id: uuid.UUID,
    ) -> ProjectMembership:
        await _lock_active_project(session, project_id)
        user = await UserRepository.get(session, user_id, for_update=True)
        if user is None or user.status != UserStatus.ACTIVE:
            raise IdentityNotFoundError("active membership user does not exist")
        now = await database_utcnow(session)
        membership = await MembershipRepository.get(
            session,
            project_id=project_id,
            user_id=user_id,
            for_update=True,
        )
        if membership is None:
            membership = ProjectMembership(
                project_id=project_id,
                user_id=user_id,
                role=role,
                status=MembershipStatus.ACTIVE,
                created_by_user_id=created_by_user_id,
                created_at=now,
                updated_at=now,
            )
            session.add(membership)
        else:
            membership.role = role
            membership.status = MembershipStatus.ACTIVE
            membership.removed_at = None
            membership.updated_at = now
            membership.version += 1
        await session.flush()
        return membership

    @staticmethod
    async def change_role(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        role: ProjectRole,
    ) -> ProjectMembership:
        await _lock_active_project(session, project_id)
        membership = await MembershipRepository.get(
            session,
            project_id=project_id,
            user_id=user_id,
            for_update=True,
        )
        if membership is None or membership.status != MembershipStatus.ACTIVE:
            raise MembershipNotActiveError("active project membership does not exist")
        if membership.role == ProjectRole.OWNER and role != ProjectRole.OWNER:
            await _ensure_another_owner(session, project_id)
        membership.role = role
        membership.updated_at = await database_utcnow(session)
        membership.version += 1
        return membership

    @staticmethod
    async def remove(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> ProjectMembership:
        await _lock_active_project(session, project_id)
        membership = await MembershipRepository.get(
            session,
            project_id=project_id,
            user_id=user_id,
            for_update=True,
        )
        if membership is None or membership.status != MembershipStatus.ACTIVE:
            raise MembershipNotActiveError("active project membership does not exist")
        if membership.role == ProjectRole.OWNER:
            await _ensure_another_owner(session, project_id)
        now = await database_utcnow(session)
        membership.status = MembershipStatus.REMOVED
        membership.removed_at = now
        membership.updated_at = now
        membership.version += 1
        return membership


class ApiKeyRepository:
    @staticmethod
    async def issue(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        name: str,
        hmac_key: bytes,
        hash_key_id: str,
        created_by_user_id: uuid.UUID,
        expires_at: datetime | None = None,
    ) -> IssuedApiKey:
        await _lock_active_project(session, project_id)
        membership = await MembershipRepository.get(
            session,
            project_id=project_id,
            user_id=user_id,
            for_update=True,
        )
        if membership is None or membership.status != MembershipStatus.ACTIVE:
            raise MembershipNotActiveError("API key user is not an active project member")
        normalized_name = name.strip()
        if not normalized_name or len(normalized_name) > 128:
            raise ValueError("API key name must contain 1-128 characters")
        if expires_at is not None:
            if expires_at.tzinfo is None or expires_at.utcoffset() is None:
                raise ValueError("API key expiration must include a timezone")
            expires_at = expires_at.astimezone(UTC)
        now = await database_utcnow(session)
        if expires_at is not None and expires_at <= now:
            raise ValueError("API key expiration must be in the future")
        material = generate_api_key(hmac_key, hash_key_id=hash_key_id)
        api_key = ApiKey(
            project_id=project_id,
            user_id=user_id,
            name=normalized_name,
            key_prefix=material.prefix,
            secret_hash=material.secret_hash,
            hash_key_id=material.hash_key_id,
            created_by_user_id=created_by_user_id,
            created_at=now,
            expires_at=expires_at,
        )
        session.add(api_key)
        await session.flush()
        return IssuedApiKey(api_key=api_key, token=material.token)

    @staticmethod
    async def authenticate(
        session: AsyncSession,
        token: str,
        *,
        resolve_hmac_key: ApiKeyResolver,
    ) -> Principal | None:
        try:
            prefix = parse_api_key_prefix(token)
        except ValueError:
            return None

        row = (
            await session.execute(
                select(ApiKey, ProjectMembership.role)
                .join(
                    ProjectMembership,
                    and_(
                        ProjectMembership.project_id == ApiKey.project_id,
                        ProjectMembership.user_id == ApiKey.user_id,
                    ),
                )
                .join(User, User.id == ApiKey.user_id)
                .join(Project, Project.id == ApiKey.project_id)
                .where(
                    ApiKey.key_prefix == prefix,
                    ApiKey.revoked_at.is_(None),
                    or_(ApiKey.expires_at.is_(None), ApiKey.expires_at > func.current_timestamp()),
                    User.status == UserStatus.ACTIVE,
                    Project.status == ProjectStatus.ACTIVE,
                    ProjectMembership.status == MembershipStatus.ACTIVE,
                )
            )
        ).first()
        if row is None:
            return None
        api_key, role = row
        hmac_key = resolve_hmac_key(api_key.hash_key_id)
        if hmac_key is None or not verify_api_key(token, api_key.secret_hash, hmac_key):
            return None
        return Principal(
            kind=PrincipalKind.API_KEY,
            project_id=api_key.project_id,
            user_id=api_key.user_id,
            api_key_id=api_key.id,
            role=role,
            key_prefix=api_key.key_prefix,
        )

    @staticmethod
    async def get(
        session: AsyncSession,
        api_key_id: uuid.UUID,
        *,
        for_update: bool = False,
    ) -> ApiKey | None:
        query = select(ApiKey).where(ApiKey.id == api_key_id)
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list_for_project(
        session: AsyncSession,
        project_id: uuid.UUID,
        *,
        limit: int,
        offset: int,
    ) -> list[ApiKey]:
        return list(
            await session.scalars(
                select(ApiKey)
                .where(ApiKey.project_id == project_id)
                .order_by(ApiKey.created_at.desc(), ApiKey.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def revoke(session: AsyncSession, api_key_id: uuid.UUID) -> ApiKey | None:
        api_key = await ApiKeyRepository.get(session, api_key_id, for_update=True)
        if api_key is None:
            return None
        if api_key.revoked_at is None:
            api_key.revoked_at = await database_utcnow(session)
            api_key.version += 1
        return api_key

    @staticmethod
    async def touch_last_used(session: AsyncSession, api_key_id: uuid.UUID) -> bool:
        api_key = await ApiKeyRepository.get(session, api_key_id, for_update=True)
        if api_key is None or api_key.revoked_at is not None:
            return False
        api_key.last_used_at = await database_utcnow(session)
        api_key.version += 1
        return True


async def _lock_active_project(session: AsyncSession, project_id: uuid.UUID) -> Project:
    project = await ProjectRepository.get(session, project_id, for_update=True)
    if project is None or project.status != ProjectStatus.ACTIVE:
        raise IdentityNotFoundError("active project does not exist")
    return project


async def _ensure_another_owner(session: AsyncSession, project_id: uuid.UUID) -> None:
    owners = int(
        await session.scalar(
            select(func.count(ProjectMembership.user_id)).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.status == MembershipStatus.ACTIVE,
                ProjectMembership.role == ProjectRole.OWNER,
            )
        )
        or 0
    )
    if owners <= 1:
        raise LastProjectOwnerError("a project must retain at least one active owner")
