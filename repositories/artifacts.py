import hmac
import uuid

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.pagination import CursorKey
from core.artifacts import ArtifactObjectInfo, ArtifactState, normalize_sha256
from core.rbac import ProjectStatus
from models.artifact import Artifact, DatasetVersion, TaskArtifact
from models.identity import Project
from models.usage import ProjectQuota, ProjectQuotaState
from repositories.clock import database_utcnow


class ArtifactRecordNotFoundError(LookupError):
    pass


class ArtifactProjectUnavailableError(LookupError):
    pass


class ArtifactStateConflictError(RuntimeError):
    pass


class ArtifactReferencedError(ArtifactStateConflictError):
    pass


class ArtifactQuotaExceededError(RuntimeError):
    def __init__(self, *, limit_bytes: int, used_bytes: int, requested_bytes: int) -> None:
        super().__init__(
            "project artifact quota exceeded: "
            f"limit={limit_bytes} used={used_bytes} requested={requested_bytes}"
        )
        self.limit_bytes = limit_bytes
        self.used_bytes = used_bytes
        self.requested_bytes = requested_bytes


class ArtifactAccountingInvariantError(RuntimeError):
    pass


class ArtifactRepository:
    @staticmethod
    async def create_pending(
        session: AsyncSession,
        *,
        artifact_id: uuid.UUID,
        project_id: uuid.UUID,
        name: str,
        backend: str,
        object_key: str,
        content_type: str | None,
        size_bytes: int,
        sha256: str,
        created_by_user_id: uuid.UUID | None,
    ) -> Artifact:
        if size_bytes < 0:
            raise ValueError("size_bytes must not be negative")
        sha256 = normalize_sha256(sha256)
        project = await session.get(Project, project_id, with_for_update=True)
        if project is None or project.status != ProjectStatus.ACTIVE:
            raise ArtifactProjectUnavailableError("active project does not exist")
        quota = await session.get(ProjectQuota, project_id, with_for_update=True)
        quota_state = await session.get(ProjectQuotaState, project_id, with_for_update=True)
        if quota is None or quota_state is None:
            raise ArtifactAccountingInvariantError(
                f"project {project_id} is missing artifact quota accounting rows"
            )
        if (
            quota.max_artifact_bytes is not None
            and quota_state.artifact_bytes + size_bytes > quota.max_artifact_bytes
        ):
            raise ArtifactQuotaExceededError(
                limit_bytes=quota.max_artifact_bytes,
                used_bytes=quota_state.artifact_bytes,
                requested_bytes=size_bytes,
            )
        now = await database_utcnow(session)
        artifact = Artifact(
            id=artifact_id,
            project_id=project_id,
            name=name,
            state=ArtifactState.PENDING.value,
            backend=backend,
            object_key=object_key,
            content_type=content_type,
            size_bytes=size_bytes,
            sha256=sha256,
            created_by_user_id=created_by_user_id,
            created_at=now,
        )
        session.add(artifact)
        quota_state.artifact_bytes += size_bytes
        quota_state.version += 1
        quota_state.updated_at = now
        await session.flush()
        return artifact

    @staticmethod
    async def get(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
        for_update: bool = False,
        include_deleted: bool = False,
    ) -> Artifact | None:
        query = select(Artifact).where(
            Artifact.id == artifact_id,
            Artifact.project_id == project_id,
        )
        if not include_deleted:
            query = query.where(Artifact.deleted_at.is_(None))
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        state: ArtifactState | None,
        limit: int,
        offset: int,
        after: CursorKey | None = None,
    ) -> list[Artifact]:
        query = select(Artifact).where(
            Artifact.project_id == project_id,
            Artifact.deleted_at.is_(None),
        )
        if state is not None:
            query = query.where(Artifact.state == state.value)
        if after is not None:
            query = query.where(
                or_(
                    Artifact.created_at < after.created_at,
                    (Artifact.created_at == after.created_at) & (Artifact.id < after.item_id),
                )
            )
        return list(
            await session.scalars(
                query.order_by(Artifact.created_at.desc(), Artifact.id.desc())
                .limit(limit)
                .offset(0 if after is not None else offset)
            )
        )

    @staticmethod
    async def count(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        state: ArtifactState | None,
    ) -> int:
        query = select(func.count(Artifact.id)).where(
            Artifact.project_id == project_id,
            Artifact.deleted_at.is_(None),
        )
        if state is not None:
            query = query.where(Artifact.state == state.value)
        return int(await session.scalar(query) or 0)

    @staticmethod
    async def mark_ready(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
        final_object_key: str,
        info: ArtifactObjectInfo,
    ) -> Artifact:
        artifact = await ArtifactRepository._required_locked(
            session, project_id=project_id, artifact_id=artifact_id
        )
        if artifact.state == ArtifactState.READY.value:
            _require_same_final_artifact(artifact, final_object_key, info)
            return artifact
        if artifact.state != ArtifactState.PENDING.value:
            raise ArtifactStateConflictError(
                f"artifact in state {artifact.state!r} cannot be finalized"
            )
        _require_expected_artifact(artifact, info)
        artifact.state = ArtifactState.READY.value
        artifact.object_key = final_object_key
        artifact.size_bytes = info.size_bytes
        artifact.sha256 = info.sha256
        artifact.verified_at = await database_utcnow(session)
        artifact.failure_reason = None
        await session.flush()
        return artifact

    @staticmethod
    async def mark_failed(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
        reason: str,
    ) -> Artifact:
        artifact = await ArtifactRepository._required_locked(
            session, project_id=project_id, artifact_id=artifact_id
        )
        if artifact.state == ArtifactState.FAILED.value:
            return artifact
        if artifact.state != ArtifactState.PENDING.value:
            raise ArtifactStateConflictError(
                f"artifact in state {artifact.state!r} cannot be marked failed"
            )
        await _release_quota_bytes(session, artifact)
        artifact.state = ArtifactState.FAILED.value
        artifact.failure_reason = reason[:4_096]
        await session.flush()
        return artifact

    @staticmethod
    async def mark_deleted(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> Artifact:
        artifact = await ArtifactRepository._required_locked(
            session,
            project_id=project_id,
            artifact_id=artifact_id,
            include_deleted=True,
        )
        if artifact.state == ArtifactState.DELETED.value:
            return artifact
        if artifact.state in {ArtifactState.PENDING.value, ArtifactState.READY.value}:
            await _release_quota_bytes(session, artifact)
        artifact.state = ArtifactState.DELETED.value
        artifact.deleted_at = await database_utcnow(session)
        await session.flush()
        return artifact

    @staticmethod
    async def begin_delete(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> Artifact:
        """Fence new references before deleting the backing object.

        Reference writers lock the artifact and require ``ready`` state. Once
        this transaction commits, a task or dataset can no longer attach the
        object while deletion is in progress.
        """

        project = await session.get(Project, project_id, with_for_update=True)
        if project is None:
            raise ArtifactRecordNotFoundError("artifact project does not exist")
        quota_state = await session.get(ProjectQuotaState, project_id, with_for_update=True)
        if quota_state is None:
            raise ArtifactAccountingInvariantError(
                f"project {project_id} is missing artifact quota accounting rows"
            )
        artifact = await ArtifactRepository._required_locked(
            session,
            project_id=project_id,
            artifact_id=artifact_id,
            include_deleted=True,
        )
        if artifact.state in {ArtifactState.DELETING.value, ArtifactState.DELETED.value}:
            return artifact
        referenced = await session.scalar(
            select(
                select(TaskArtifact.task_id).where(TaskArtifact.artifact_id == artifact.id).exists()
                | select(DatasetVersion.dataset_id)
                .where(DatasetVersion.artifact_id == artifact.id)
                .exists()
            )
        )
        if referenced:
            raise ArtifactReferencedError("artifact is referenced by a task or dataset")
        if artifact.state in {ArtifactState.PENDING.value, ArtifactState.READY.value}:
            await _release_quota_bytes(session, artifact)
        artifact.state = ArtifactState.DELETING.value
        await session.flush()
        return artifact

    @staticmethod
    async def _required_locked(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
        include_deleted: bool = False,
    ) -> Artifact:
        artifact = await ArtifactRepository.get(
            session,
            project_id=project_id,
            artifact_id=artifact_id,
            for_update=True,
            include_deleted=include_deleted,
        )
        if artifact is None:
            raise ArtifactRecordNotFoundError("artifact does not exist in project")
        return artifact


def _require_expected_artifact(artifact: Artifact, info: ArtifactObjectInfo) -> None:
    if artifact.size_bytes != info.size_bytes or artifact.sha256 is None:
        raise ArtifactAccountingInvariantError(
            "verified artifact metadata does not match its pending reservation"
        )
    if not hmac.compare_digest(artifact.sha256, info.sha256):
        raise ArtifactAccountingInvariantError(
            "verified artifact checksum does not match its pending reservation"
        )


def _require_same_final_artifact(
    artifact: Artifact,
    final_object_key: str,
    info: ArtifactObjectInfo,
) -> None:
    _require_expected_artifact(artifact, info)
    if artifact.object_key != final_object_key:
        raise ArtifactAccountingInvariantError(
            "ready artifact points at an unexpected storage object"
        )


async def _release_quota_bytes(session: AsyncSession, artifact: Artifact) -> None:
    reserved_bytes = artifact.size_bytes or 0
    quota_state = await session.get(ProjectQuotaState, artifact.project_id, with_for_update=True)
    if quota_state is None or quota_state.artifact_bytes < reserved_bytes:
        raise ArtifactAccountingInvariantError(
            f"project {artifact.project_id} artifact usage is below the reserved object size"
        )
    now = await database_utcnow(session)
    quota_state.artifact_bytes -= reserved_bytes
    quota_state.version += 1
    quota_state.updated_at = now
