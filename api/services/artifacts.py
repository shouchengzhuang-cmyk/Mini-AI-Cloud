import hmac
import uuid
from collections.abc import AsyncIterable, AsyncIterator

from api.pagination import CursorKey
from api.schemas.artifacts import ArtifactCreate, ArtifactFinalize
from core.artifacts import (
    ArtifactIntegrityError,
    ArtifactObjectChangedError,
    ArtifactState,
    ArtifactStore,
    ArtifactTooLargeError,
    SignedArtifactURL,
)
from core.config import Settings
from core.database import Database
from core.rbac import Principal
from models.artifact import Artifact
from repositories.artifacts import (
    ArtifactRecordNotFoundError,
    ArtifactRepository,
    ArtifactStateConflictError,
)


class ArtifactBackendMismatchError(RuntimeError):
    pass


class ArtifactService:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        store: ArtifactStore,
    ) -> None:
        self.database = database
        self.settings = settings
        self.store = store

    async def create(self, payload: ArtifactCreate, *, principal: Principal) -> Artifact:
        project_id = _principal_project_id(principal)
        if payload.size_bytes > min(self.settings.artifact_max_bytes, self.store.max_bytes):
            raise ArtifactTooLargeError(
                payload.size_bytes,
                min(self.settings.artifact_max_bytes, self.store.max_bytes),
            )
        artifact_id = uuid.uuid4()
        staging_key = _staging_key(project_id, artifact_id)
        async with self.database.session() as session, session.begin():
            return await ArtifactRepository.create_pending(
                session,
                artifact_id=artifact_id,
                project_id=project_id,
                name=payload.name,
                backend=self.store.backend,
                object_key=staging_key,
                content_type=payload.content_type,
                size_bytes=payload.size_bytes,
                sha256=payload.sha256,
                created_by_user_id=principal.user_id,
            )

    async def get(self, artifact_id: uuid.UUID, *, principal: Principal) -> Artifact | None:
        project_id = _principal_project_id(principal)
        async with self.database.session() as session:
            return await ArtifactRepository.get(
                session,
                project_id=project_id,
                artifact_id=artifact_id,
            )

    async def list(
        self,
        *,
        principal: Principal,
        state: ArtifactState | None,
        limit: int,
        offset: int,
        after: CursorKey | None = None,
    ) -> tuple[list[Artifact], int]:
        project_id = _principal_project_id(principal)
        async with self.database.session() as session:
            artifacts = await ArtifactRepository.list(
                session,
                project_id=project_id,
                state=state,
                limit=limit,
                offset=offset,
                after=after,
            )
            total = await ArtifactRepository.count(
                session,
                project_id=project_id,
                state=state,
            )
        return artifacts, total

    async def upload_access(
        self,
        artifact_id: uuid.UUID,
        *,
        principal: Principal,
    ) -> tuple[Artifact, SignedArtifactURL | None]:
        artifact = await self._required(artifact_id, principal=principal)
        _require_pending(artifact)
        self._require_backend(artifact)
        size_bytes, sha256, content_type = _expected_metadata(artifact)
        signed = await self.store.signed_upload_url(
            artifact.object_key,
            content_type=content_type,
            expected_size_bytes=size_bytes,
            expected_sha256=sha256,
            expires_seconds=self.settings.artifact_signed_url_ttl_seconds,
        )
        return artifact, signed

    async def upload(
        self,
        artifact_id: uuid.UUID,
        chunks: AsyncIterable[bytes],
        *,
        principal: Principal,
        content_length: int | None,
        content_sha256: str | None,
    ) -> Artifact:
        artifact = await self._required(artifact_id, principal=principal)
        _require_pending(artifact)
        self._require_backend(artifact)
        size_bytes, sha256, content_type = _expected_metadata(artifact)
        if content_length is not None and content_length != size_bytes:
            raise ArtifactIntegrityError(
                f"Content-Length mismatch: expected {size_bytes}, got {content_length}"
            )
        if content_sha256 is not None and not hmac.compare_digest(
            content_sha256.casefold(), sha256
        ):
            raise ArtifactIntegrityError("X-Content-SHA256 does not match artifact metadata")
        try:
            await self.store.put(
                artifact.object_key,
                chunks,
                content_type=content_type,
                expected_size_bytes=size_bytes,
                expected_sha256=sha256,
            )
        except (ArtifactIntegrityError, ArtifactTooLargeError) as exc:
            await self._mark_failed(artifact_id, principal=principal, reason=str(exc))
            raise
        return artifact

    async def finalize(
        self,
        artifact_id: uuid.UUID,
        payload: ArtifactFinalize,
        *,
        principal: Principal,
    ) -> Artifact:
        artifact = await self._required(artifact_id, principal=principal)
        self._require_backend(artifact)
        size_bytes, sha256, _content_type = _expected_metadata(artifact)
        if payload.size_bytes != size_bytes or not hmac.compare_digest(payload.sha256, sha256):
            raise ArtifactIntegrityError(
                "finalize size and checksum must match the pending artifact metadata"
            )
        final_key = _final_key(artifact.project_id, artifact.id)
        if artifact.state == ArtifactState.READY.value:
            if artifact.object_key != final_key:
                raise ArtifactStateConflictError(
                    "ready artifact points at an unexpected storage object"
                )
            return artifact
        _require_pending(artifact)
        try:
            info = await self.store.finalize(
                artifact.object_key,
                final_key,
                expected_size_bytes=size_bytes,
                expected_sha256=sha256,
            )
        except (
            ArtifactIntegrityError,
            ArtifactObjectChangedError,
            ArtifactTooLargeError,
        ) as exc:
            await self._mark_failed(artifact_id, principal=principal, reason=str(exc))
            await self.store.delete(artifact.object_key)
            raise
        async with self.database.session() as session, session.begin():
            return await ArtifactRepository.mark_ready(
                session,
                project_id=artifact.project_id,
                artifact_id=artifact.id,
                final_object_key=final_key,
                info=info,
            )

    async def download_access(
        self,
        artifact_id: uuid.UUID,
        *,
        principal: Principal,
    ) -> tuple[Artifact, SignedArtifactURL | None]:
        artifact = await self._required(artifact_id, principal=principal)
        _require_ready(artifact)
        self._require_backend(artifact)
        signed = await self.store.signed_download_url(
            artifact.object_key,
            download_name=artifact.name,
            expires_seconds=self.settings.artifact_signed_url_ttl_seconds,
        )
        return artifact, signed

    async def download(
        self,
        artifact_id: uuid.UUID,
        *,
        principal: Principal,
    ) -> tuple[Artifact, AsyncIterator[bytes]]:
        artifact = await self._required(artifact_id, principal=principal)
        _require_ready(artifact)
        self._require_backend(artifact)
        return artifact, self.store.read(artifact.object_key)

    async def delete(self, artifact_id: uuid.UUID, *, principal: Principal) -> Artifact:
        project_id = _principal_project_id(principal)
        async with self.database.session() as session, session.begin():
            artifact = await ArtifactRepository.begin_delete(
                session,
                project_id=project_id,
                artifact_id=artifact_id,
            )
            if artifact.state != ArtifactState.DELETED.value:
                self._require_backend(artifact)
        if artifact.state == ArtifactState.DELETED.value:
            return artifact
        # Delete both deterministic locations because an unexpired staging PUT
        # grant may have recreated the staging object after finalization.
        await self.store.delete(_staging_key(project_id, artifact.id))
        await self.store.delete(_final_key(project_id, artifact.id))
        async with self.database.session() as session, session.begin():
            return await ArtifactRepository.mark_deleted(
                session,
                project_id=project_id,
                artifact_id=artifact_id,
            )

    async def _required(self, artifact_id: uuid.UUID, *, principal: Principal) -> Artifact:
        artifact = await self.get(artifact_id, principal=principal)
        if artifact is None:
            raise ArtifactRecordNotFoundError("artifact does not exist in project")
        return artifact

    async def _mark_failed(
        self,
        artifact_id: uuid.UUID,
        *,
        principal: Principal,
        reason: str,
    ) -> Artifact:
        project_id = _principal_project_id(principal)
        async with self.database.session() as session, session.begin():
            return await ArtifactRepository.mark_failed(
                session,
                project_id=project_id,
                artifact_id=artifact_id,
                reason=reason,
            )

    def _require_backend(self, artifact: Artifact) -> None:
        if artifact.backend != self.store.backend:
            raise ArtifactBackendMismatchError(
                f"artifact backend {artifact.backend!r} is not available from this API instance"
            )


def _principal_project_id(principal: Principal) -> uuid.UUID:
    if principal.project_id is None:
        raise ArtifactRecordNotFoundError("artifact project is not available")
    return principal.project_id


def _expected_metadata(artifact: Artifact) -> tuple[int, str, str]:
    if artifact.size_bytes is None or artifact.sha256 is None:
        raise ArtifactStateConflictError("pending artifact is missing size or checksum metadata")
    return artifact.size_bytes, artifact.sha256, artifact.content_type or "application/octet-stream"


def _require_pending(artifact: Artifact) -> None:
    if artifact.state != ArtifactState.PENDING.value:
        raise ArtifactStateConflictError(
            f"artifact in state {artifact.state!r} does not accept uploads"
        )


def _require_ready(artifact: Artifact) -> None:
    if artifact.state != ArtifactState.READY.value:
        raise ArtifactStateConflictError(
            f"artifact in state {artifact.state!r} is not downloadable"
        )


def _staging_key(project_id: uuid.UUID, artifact_id: uuid.UUID) -> str:
    return f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/staging"


def _final_key(project_id: uuid.UUID, artifact_id: uuid.UUID) -> str:
    return f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/content"
