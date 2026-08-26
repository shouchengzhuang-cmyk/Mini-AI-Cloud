from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import shutil
import tempfile
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select

from core.artifacts import ArtifactIntegrityError, ArtifactState, ArtifactStore
from core.config import Settings
from core.database import Database
from models.artifact import Artifact, TaskArtifact
from repositories.artifacts import ArtifactRepository
from repositories.task_artifacts import TaskArtifactRepository

_CHUNK_BYTES = 1024 * 1024
_OUTPUT_PLACEHOLDER_BYTES = 32


class ArtifactWorkspaceError(RuntimeError):
    pass


class ArtifactOutputNotProducedError(ArtifactWorkspaceError):
    """A declared output still contains the Worker's private placeholder."""


@dataclass(frozen=True, slots=True)
class PreparedArtifactMount:
    binding_id: uuid.UUID
    host_path: Path
    container_path: str
    read_only: bool
    volume_name: str | None = None
    volume_subpath: str | None = None
    output_placeholder_size_bytes: int | None = None
    output_placeholder_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class PreparedArtifactWorkspace:
    task_id: uuid.UUID
    project_id: uuid.UUID
    worker_id: str
    execution_id: uuid.UUID
    root: Path | None
    mounts: tuple[PreparedArtifactMount, ...]
    outputs: tuple[TaskArtifact, ...]
    worker_session_id: uuid.UUID | None = None


class ArtifactWorkspaceManager:
    """Materialize task artifacts in a private Worker directory.

    Host paths are generated from binding UUIDs and are never influenced by a
    user-supplied path. Only the validated container target is carried into the
    runtime mount specification.
    """

    def __init__(
        self,
        database: Database,
        settings: Settings,
        store: ArtifactStore,
    ) -> None:
        self.database = database
        self.settings = settings
        self.store = store
        self.root = Path(settings.artifact_workspace_root).expanduser().resolve()

    async def prepare(
        self,
        *,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID | None = None,
    ) -> PreparedArtifactWorkspace:
        async with self.database.session() as session, session.begin():
            bindings = await TaskArtifactRepository.list_for_execution(
                session,
                task_id=task_id,
                project_id=project_id,
                worker_id=worker_id,
                execution_id=execution_id,
                worker_session_id=worker_session_id,
            )
            input_ids = [
                binding.artifact_id
                for binding in bindings
                if binding.direction == "input" and binding.artifact_id is not None
            ]
            artifacts = {
                artifact.id: artifact
                for artifact in await session.scalars(
                    select(Artifact).where(Artifact.id.in_(input_ids))
                )
            }
        if not bindings:
            return PreparedArtifactWorkspace(
                task_id=task_id,
                project_id=project_id,
                worker_id=worker_id,
                execution_id=execution_id,
                root=None,
                mounts=(),
                outputs=(),
                worker_session_id=worker_session_id,
            )

        workspace = await self._create_workspace(task_id, execution_id)
        mounts: list[PreparedArtifactMount] = []
        outputs: list[TaskArtifact] = []
        try:
            for binding in bindings:
                host_path = _binding_host_path(workspace, binding)
                placeholder_size: int | None = None
                placeholder_sha256: str | None = None
                if binding.direction == "input":
                    if binding.artifact_id is None:
                        raise ArtifactWorkspaceError("input artifact binding is unbound")
                    artifact = artifacts.get(binding.artifact_id)
                    if not isinstance(artifact, Artifact):
                        raise ArtifactWorkspaceError("input artifact record is missing")
                    await self._materialize_input(artifact, host_path)
                    read_only = True
                elif binding.direction == "output":
                    await asyncio.to_thread(host_path.parent.mkdir, parents=True, exist_ok=True)
                    placeholder_size, placeholder_sha256 = await asyncio.to_thread(
                        _create_output_placeholder,
                        host_path,
                    )
                    read_only = False
                    outputs.append(binding)
                else:
                    raise ArtifactWorkspaceError("task artifact direction is invalid")
                mounts.append(
                    PreparedArtifactMount(
                        binding_id=binding.id,
                        host_path=host_path,
                        container_path=binding.mount_path,
                        read_only=read_only,
                        volume_name=self.settings.docker_artifact_workspace_volume or None,
                        volume_subpath=(
                            host_path.relative_to(self.root).as_posix()
                            if self.settings.docker_artifact_workspace_volume
                            else None
                        ),
                        output_placeholder_size_bytes=placeholder_size,
                        output_placeholder_sha256=placeholder_sha256,
                    )
                )
        except BaseException:
            await self.cleanup_path(workspace)
            raise
        return PreparedArtifactWorkspace(
            task_id=task_id,
            project_id=project_id,
            worker_id=worker_id,
            execution_id=execution_id,
            root=workspace,
            mounts=tuple(mounts),
            outputs=tuple(outputs),
            worker_session_id=worker_session_id,
        )

    async def publish_outputs(self, workspace: PreparedArtifactWorkspace) -> list[Artifact]:
        if workspace.root is None or not workspace.outputs:
            return []
        async with self.database.session() as session, session.begin():
            current = await TaskArtifactRepository.list_for_execution(
                session,
                task_id=workspace.task_id,
                project_id=workspace.project_id,
                worker_id=workspace.worker_id,
                execution_id=workspace.execution_id,
                worker_session_id=workspace.worker_session_id,
            )
        current_by_id = {binding.id: binding for binding in current}
        mounts_by_binding_id = {mount.binding_id: mount for mount in workspace.mounts}
        published: list[Artifact] = []
        for declared in workspace.outputs:
            binding = current_by_id.get(declared.id)
            if binding is None or binding.direction != "output":
                raise ArtifactWorkspaceError("output artifact declaration became stale")
            mount = mounts_by_binding_id.get(binding.id)
            if (
                mount is None
                or mount.output_placeholder_size_bytes is None
                or mount.output_placeholder_sha256 is None
            ):
                raise ArtifactWorkspaceError("output artifact placeholder metadata is missing")
            try:
                artifact = await self._publish_file(
                    workspace,
                    binding,
                    mount.host_path,
                    placeholder_size_bytes=mount.output_placeholder_size_bytes,
                    placeholder_sha256=mount.output_placeholder_sha256,
                )
            except ArtifactOutputNotProducedError:
                if binding.required:
                    raise
                continue
            except (ArtifactIntegrityError, ArtifactWorkspaceError, OSError):
                if binding.required:
                    raise
                continue
            published.append(artifact)
        return published

    async def cleanup(self, workspace: PreparedArtifactWorkspace) -> None:
        if workspace.root is not None:
            await self.cleanup_path(workspace.root)

    async def cleanup_path(self, path: Path) -> None:
        resolved = await asyncio.to_thread(path.resolve)
        root = self.root
        if resolved == root or root not in resolved.parents:
            raise ArtifactWorkspaceError("refusing to clean a path outside artifact workspace")
        await asyncio.to_thread(shutil.rmtree, resolved, True)

    async def _create_workspace(self, task_id: uuid.UUID, execution_id: uuid.UUID) -> Path:
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True, mode=0o700)
        path = await asyncio.to_thread(
            tempfile.mkdtemp,
            prefix=f"task-{task_id.hex[:12]}-{execution_id.hex[:12]}-",
            dir=self.root,
        )
        workspace = await asyncio.to_thread(Path(path).resolve)
        if self.root not in workspace.parents:
            raise ArtifactWorkspaceError("temporary artifact workspace escaped its root")
        await asyncio.to_thread(workspace.chmod, 0o700)
        return workspace

    async def _materialize_input(self, artifact: Artifact, destination: Path) -> None:
        if (
            artifact.project_id is None
            or artifact.state != ArtifactState.READY.value
            or artifact.deleted_at is not None
            or artifact.size_bytes is None
            or artifact.sha256 is None
            or artifact.backend != self.store.backend
        ):
            raise ArtifactWorkspaceError("input artifact is not readable by this Worker")
        if artifact.size_bytes > self.settings.artifact_max_bytes:
            raise ArtifactWorkspaceError("input artifact exceeds Worker size limit")
        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
        source = self.store.read(artifact.object_key)
        output = await asyncio.to_thread(destination.open, "xb")
        digest = hashlib.sha256()
        size = 0
        try:
            async for chunk in source:
                size += len(chunk)
                if size > min(self.settings.artifact_max_bytes, self.store.max_bytes):
                    raise ArtifactWorkspaceError("input artifact exceeded Worker size limit")
                digest.update(chunk)
                await asyncio.to_thread(output.write, chunk)
            await asyncio.to_thread(output.flush)
        except BaseException:
            await asyncio.to_thread(output.close)
            await asyncio.to_thread(destination.unlink, missing_ok=True)
            raise
        await asyncio.to_thread(output.close)
        if size != artifact.size_bytes or not hmac.compare_digest(
            digest.hexdigest(), artifact.sha256
        ):
            await asyncio.to_thread(destination.unlink, missing_ok=True)
            raise ArtifactIntegrityError("downloaded input artifact failed integrity validation")
        await asyncio.to_thread(destination.chmod, 0o444)

    async def _publish_file(
        self,
        workspace: PreparedArtifactWorkspace,
        binding: TaskArtifact,
        source: Path,
        *,
        placeholder_size_bytes: int,
        placeholder_sha256: str,
    ) -> Artifact:
        if not await asyncio.to_thread(source.exists):
            raise ArtifactOutputNotProducedError(
                f"declared output artifact '{binding.name}' was not produced"
            )
        if not await asyncio.to_thread(_is_regular_file, source):
            raise ArtifactWorkspaceError("declared artifact output is not a regular file")
        maximum = min(self.settings.artifact_max_bytes, self.store.max_bytes)
        size, checksum = await asyncio.to_thread(
            _inspect_output_file,
            source,
            maximum_bytes=maximum,
        )
        if size == placeholder_size_bytes and hmac.compare_digest(
            checksum,
            placeholder_sha256,
        ):
            raise ArtifactOutputNotProducedError(
                f"declared output artifact '{binding.name}' was not produced"
            )
        artifact_id = uuid.uuid4()
        staging_key = _staging_key(workspace.project_id, artifact_id)
        final_key = _final_key(workspace.project_id, artifact_id)
        async with self.database.session() as session, session.begin():
            await TaskArtifactRepository.list_for_execution(
                session,
                task_id=workspace.task_id,
                project_id=workspace.project_id,
                worker_id=workspace.worker_id,
                execution_id=workspace.execution_id,
                worker_session_id=workspace.worker_session_id,
            )
            artifact = await ArtifactRepository.create_pending(
                session,
                artifact_id=artifact_id,
                project_id=workspace.project_id,
                name=binding.name,
                backend=self.store.backend,
                object_key=staging_key,
                content_type="application/octet-stream",
                size_bytes=size,
                sha256=checksum,
                created_by_user_id=None,
            )
        try:
            await self.store.put(
                staging_key,
                _file_chunks(source),
                content_type="application/octet-stream",
                expected_size_bytes=size,
                expected_sha256=checksum,
            )
            info = await self.store.finalize(
                staging_key,
                final_key,
                expected_size_bytes=size,
                expected_sha256=checksum,
            )
        except Exception as exc:
            await self.store.delete(staging_key)
            async with self.database.session() as session, session.begin():
                await ArtifactRepository.mark_failed(
                    session,
                    project_id=workspace.project_id,
                    artifact_id=artifact_id,
                    reason=f"Worker output upload failed: {type(exc).__name__}",
                )
            raise
        try:
            async with self.database.session() as session, session.begin():
                await TaskArtifactRepository.list_for_execution(
                    session,
                    task_id=workspace.task_id,
                    project_id=workspace.project_id,
                    worker_id=workspace.worker_id,
                    execution_id=workspace.execution_id,
                    worker_session_id=workspace.worker_session_id,
                )
                artifact = await ArtifactRepository.mark_ready(
                    session,
                    project_id=workspace.project_id,
                    artifact_id=artifact_id,
                    final_object_key=final_key,
                    info=info,
                )
                await TaskArtifactRepository.bind_output(
                    session,
                    binding_id=binding.id,
                    task_id=workspace.task_id,
                    project_id=workspace.project_id,
                    artifact_id=artifact.id,
                )
                return artifact
        except Exception as exc:
            await self.store.delete(final_key)
            async with self.database.session() as session, session.begin():
                await ArtifactRepository.mark_failed(
                    session,
                    project_id=workspace.project_id,
                    artifact_id=artifact_id,
                    reason=f"Worker output commit failed: {type(exc).__name__}",
                )
            raise


def _binding_host_path(workspace: Path, binding: TaskArtifact) -> Path:
    direction = "inputs" if binding.direction == "input" else "outputs"
    candidate = (workspace / direction / binding.id.hex).resolve()
    if workspace not in candidate.parents:
        raise ArtifactWorkspaceError("artifact host path escaped its workspace")
    return candidate


def _inspect_output_file(
    path: Path,
    *,
    maximum_bytes: int | None = None,
) -> tuple[int, str]:
    declared_size = path.stat().st_size
    if maximum_bytes is not None and declared_size > maximum_bytes:
        raise ArtifactWorkspaceError("declared artifact output exceeds size limit")
    size = 0
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(_CHUNK_BYTES):
            size += len(chunk)
            if maximum_bytes is not None and size > maximum_bytes:
                raise ArtifactWorkspaceError("declared artifact output exceeds size limit")
            digest.update(chunk)
    if size != declared_size:
        raise ArtifactWorkspaceError("declared artifact output changed during inspection")
    return size, digest.hexdigest()


def _create_output_placeholder(path: Path) -> tuple[int, str]:
    """Create a file mount target that cannot be mistaken for a produced empty file.

    Docker volume-subpath mounts require the file to exist before container
    creation. A random marker lets the Worker distinguish that prerequisite
    from a workload deliberately truncating the output to a valid empty file.
    """

    marker = secrets.token_bytes(_OUTPUT_PLACEHOLDER_BYTES)
    with path.open("xb") as output:
        output.write(marker)
        output.flush()
    path.chmod(0o666)
    return len(marker), hashlib.sha256(marker).hexdigest()


def _is_regular_file(path: Path) -> bool:
    return path.is_file() and not path.is_symlink()


async def _file_chunks(path: Path) -> AsyncIterator[bytes]:
    source = await asyncio.to_thread(path.open, "rb")
    try:
        while chunk := await asyncio.to_thread(source.read, _CHUNK_BYTES):
            yield chunk
    finally:
        await asyncio.to_thread(source.close)


def _staging_key(project_id: uuid.UUID, artifact_id: uuid.UUID) -> str:
    return f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/staging"


def _final_key(project_id: uuid.UUID, artifact_id: uuid.UUID) -> str:
    return f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/content"
