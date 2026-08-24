import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import ACTIVE_TASK_STATUSES
from core.secrets import EncryptedSecret, SecretCipher, SecretValue
from models.registry import Secret, SecretVersion, TaskSecretBinding
from models.scheduling import ResourceReservation
from models.task import Task
from models.worker import Worker
from repositories.clock import database_utcnow

_SECRET_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")


class SecretNotFoundError(LookupError):
    pass


class SecretRevokedError(RuntimeError):
    pass


class SecretBindingError(ValueError):
    """A task references a missing, revoked, cross-project, or conflicting secret."""


class SecretResolutionError(RuntimeError):
    """A worker cannot safely resolve a task's pinned secret bindings."""


@dataclass(frozen=True, slots=True)
class TaskSecretReference:
    secret_id: uuid.UUID
    version: int
    env_name: str


@dataclass(slots=True)
class ResolvedTaskSecrets:
    _environment: dict[str, str] = field(repr=False)

    @property
    def environment(self) -> Mapping[str, str]:
        return MappingProxyType(self._environment)

    @property
    def values(self) -> tuple[str, ...]:
        return tuple(self._environment.values())

    def clear(self) -> None:
        self._environment.clear()


class SecretRepository:
    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        value: str,
        cipher: SecretCipher,
        description: str | None = None,
        created_by_user_id: uuid.UUID | None = None,
    ) -> Secret:
        normalized_name = _normalize_name(name)
        normalized_description = _normalize_description(description)
        now = await database_utcnow(session)
        secret = Secret(
            project_id=project_id,
            name=normalized_name,
            description=normalized_description,
            current_version=1,
            created_by_user_id=created_by_user_id,
            created_at=now,
            updated_at=now,
        )
        session.add(secret)
        await session.flush()
        encrypted = cipher.encrypt(
            value,
            project_id=project_id,
            secret_id=secret.id,
            version=1,
        )
        session.add(_secret_version(secret.id, 1, encrypted, now))
        await session.flush()
        return secret

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
        for_update: bool = False,
    ) -> Secret | None:
        query = select(Secret).where(
            Secret.id == secret_id,
            Secret.project_id == project_id,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def get_by_name(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        for_update: bool = False,
    ) -> Secret | None:
        query = select(Secret).where(
            Secret.project_id == project_id,
            Secret.name == _normalize_name(name),
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[Secret]:
        return list(
            await session.scalars(
                select(Secret)
                .where(Secret.project_id == project_id)
                .order_by(Secret.created_at.desc(), Secret.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def count(session: AsyncSession, *, project_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(Secret.id)).where(Secret.project_id == project_id)
            )
            or 0
        )

    @staticmethod
    async def rotate(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
        value: str,
        cipher: SecretCipher,
    ) -> Secret:
        secret = await SecretRepository.get(
            session,
            project_id=project_id,
            secret_id=secret_id,
            for_update=True,
        )
        if secret is None:
            raise SecretNotFoundError("secret does not exist in the project")
        if secret.revoked_at is not None:
            raise SecretRevokedError("a revoked secret cannot be rotated")
        version = secret.current_version + 1
        now = await database_utcnow(session)
        encrypted = cipher.encrypt(
            value,
            project_id=project_id,
            secret_id=secret.id,
            version=version,
        )
        session.add(_secret_version(secret.id, version, encrypted, now))
        secret.current_version = version
        secret.updated_at = now
        await session.flush()
        return secret

    @staticmethod
    async def revoke(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
    ) -> Secret | None:
        secret = await SecretRepository.get(
            session,
            project_id=project_id,
            secret_id=secret_id,
            for_update=True,
        )
        if secret is None:
            return None
        if secret.revoked_at is None:
            now = await database_utcnow(session)
            secret.revoked_at = now
            secret.updated_at = now
        return secret

    @staticmethod
    async def decrypt(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        secret_id: uuid.UUID,
        cipher: SecretCipher,
        version: int | None = None,
    ) -> SecretValue:
        secret = await SecretRepository.get(
            session,
            project_id=project_id,
            secret_id=secret_id,
        )
        if secret is None:
            raise SecretNotFoundError("secret does not exist in the project")
        if secret.revoked_at is not None:
            raise SecretRevokedError("a revoked secret cannot be decrypted")
        selected_version = secret.current_version if version is None else version
        if selected_version < 1:
            raise SecretNotFoundError("secret version does not exist")
        stored = await session.scalar(
            select(SecretVersion).where(
                SecretVersion.secret_id == secret.id,
                SecretVersion.version == selected_version,
            )
        )
        if stored is None:
            raise SecretNotFoundError("secret version does not exist")
        return cipher.decrypt(
            EncryptedSecret(
                ciphertext=stored.ciphertext,
                nonce=stored.nonce,
                key_id=stored.key_id,
            ),
            project_id=project_id,
            secret_id=secret.id,
            version=selected_version,
        )


class TaskSecretBindingRepository:
    @staticmethod
    async def bind(
        session: AsyncSession,
        *,
        task: Task,
        project_id: uuid.UUID,
        references: list[TaskSecretReference],
        public_environment_names: set[str],
    ) -> None:
        if task.project_id != project_id:
            raise SecretBindingError("task and secret bindings must belong to the same project")
        env_names = [reference.env_name for reference in references]
        if len(env_names) != len(set(env_names)):
            raise SecretBindingError("secret binding environment names must be unique")
        if public_environment_names.intersection(env_names):
            raise SecretBindingError("secret bindings must not overwrite public environment values")

        for reference in references:
            if reference.version < 1:
                raise SecretBindingError("secret version must be positive")
            secret = await session.scalar(
                select(Secret)
                .join(
                    SecretVersion,
                    (SecretVersion.secret_id == Secret.id)
                    & (SecretVersion.version == reference.version),
                )
                .where(
                    Secret.id == reference.secret_id,
                    Secret.project_id == project_id,
                )
                .with_for_update()
            )
            if secret is None or secret.revoked_at is not None:
                # Intentionally collapse missing, cross-project, missing-version, and
                # revoked cases so the task API cannot be used as a secret oracle.
                raise SecretBindingError("one or more secret bindings are unavailable")
            session.add(
                TaskSecretBinding(
                    task_id=task.id,
                    env_name=reference.env_name,
                    secret_id=reference.secret_id,
                    secret_version=reference.version,
                )
            )
        if references:
            await session.flush()

    @staticmethod
    async def resolve_for_execution(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        cipher: SecretCipher | None,
        worker_session_id: uuid.UUID | None = None,
    ) -> ResolvedTaskSecrets:
        task = await session.scalar(
            select(Task)
            .where(
                Task.id == task_id,
                Task.project_id == project_id,
                Task.worker_id == worker_id,
                Task.execution_id == execution_id,
                Task.status.in_(ACTIVE_TASK_STATUSES),
            )
            .with_for_update()
        )
        if task is None:
            raise SecretResolutionError("task execution ownership is no longer valid")
        if worker_session_id is not None:
            worker = await session.get(Worker, worker_id, with_for_update=True)
            reservation = await session.scalar(
                select(ResourceReservation)
                .where(
                    ResourceReservation.execution_id == execution_id,
                    ResourceReservation.worker_id == worker_id,
                    ResourceReservation.worker_session_id == worker_session_id,
                    ResourceReservation.released_at.is_(None),
                )
                .with_for_update()
            )
            if (
                worker is None
                or worker.worker_session_id != worker_session_id
                or reservation is None
            ):
                raise SecretResolutionError("task execution ownership is no longer valid")

        rows = list(
            (
                await session.execute(
                    select(TaskSecretBinding, Secret, SecretVersion)
                    .join(Secret, Secret.id == TaskSecretBinding.secret_id)
                    .join(
                        SecretVersion,
                        (SecretVersion.secret_id == TaskSecretBinding.secret_id)
                        & (SecretVersion.version == TaskSecretBinding.secret_version),
                    )
                    .where(
                        TaskSecretBinding.task_id == task_id,
                        Secret.project_id == project_id,
                    )
                    .order_by(TaskSecretBinding.env_name)
                    .with_for_update()
                )
            ).all()
        )
        binding_count = int(
            await session.scalar(
                select(func.count(TaskSecretBinding.env_name)).where(
                    TaskSecretBinding.task_id == task_id
                )
            )
            or 0
        )
        if binding_count != len(rows):
            raise SecretResolutionError("task secret bindings are unavailable")

        environment: dict[str, str] = {}
        for binding, secret, stored in rows:
            if secret.revoked_at is not None:
                raise SecretResolutionError("task secret bindings are unavailable")
            if cipher is None:
                raise SecretResolutionError("task secret decryption is not configured")
            try:
                value = cipher.decrypt(
                    EncryptedSecret(
                        ciphertext=stored.ciphertext,
                        nonce=stored.nonce,
                        key_id=stored.key_id,
                    ),
                    project_id=project_id,
                    secret_id=secret.id,
                    version=binding.secret_version,
                )
            except ValueError as exc:
                raise SecretResolutionError("task secret bindings cannot be decrypted") from exc
            environment[binding.env_name] = value.value
        return ResolvedTaskSecrets(environment)


def _secret_version(
    secret_id: uuid.UUID,
    version: int,
    encrypted: EncryptedSecret,
    created_at: datetime,
) -> SecretVersion:
    return SecretVersion(
        secret_id=secret_id,
        version=version,
        ciphertext=encrypted.ciphertext,
        nonce=encrypted.nonce,
        key_id=encrypted.key_id,
        created_at=created_at,
    )


def _normalize_name(value: str) -> str:
    name = value.strip()
    if not _SECRET_NAME.fullmatch(name):
        raise ValueError(
            "secret name must start with a letter and contain only letters, digits, '.', '_' or '-'"
        )
    return name


def _normalize_description(value: str | None) -> str | None:
    if value is None:
        return None
    description = value.strip()
    if not description:
        return None
    if len(description) > 2_000 or "\x00" in description:
        raise ValueError("secret description must be at most 2000 characters without NUL")
    return description
