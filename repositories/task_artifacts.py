from __future__ import annotations

import posixpath
import re
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.artifacts import ArtifactState
from core.enums import ACTIVE_TASK_STATUSES
from models.artifact import Artifact, TaskArtifact
from models.scheduling import ResourceReservation
from models.task import Task
from models.worker import Worker
from repositories.clock import database_utcnow

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$")
_ALLOWED_OUTPUT_ROOTS = ("/output/", "/workspace/outputs/")


class TaskArtifactNotFoundError(LookupError):
    pass


class TaskArtifactConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OutputArtifactSpec:
    name: str
    path: str
    required: bool = True


class TaskArtifactRepository:
    @staticmethod
    async def create_bindings(
        session: AsyncSession,
        *,
        task: Task,
        project_id: uuid.UUID,
        input_artifact_ids: list[uuid.UUID],
        outputs: list[OutputArtifactSpec],
    ) -> list[TaskArtifact]:
        if task.project_id != project_id:
            raise TaskArtifactNotFoundError("task does not belong to artifact project")
        if len(input_artifact_ids) > 100 or len(outputs) > 100:
            raise ValueError("a task may declare at most 100 input and 100 output artifacts")
        if len(input_artifact_ids) != len(set(input_artifact_ids)):
            raise ValueError("task input artifacts must be unique")

        artifacts: list[Artifact] = []
        if input_artifact_ids:
            artifacts = list(
                await session.scalars(
                    select(Artifact)
                    .where(
                        Artifact.id.in_(input_artifact_ids),
                        Artifact.project_id == project_id,
                    )
                    .order_by(Artifact.id)
                    .with_for_update()
                )
            )
            by_id = {artifact.id: artifact for artifact in artifacts}
            if set(by_id) != set(input_artifact_ids):
                raise TaskArtifactNotFoundError("input artifact does not exist in the project")
            if any(
                artifact.state != ArtifactState.READY.value or artifact.deleted_at is not None
                for artifact in artifacts
            ):
                raise TaskArtifactConflictError("input artifacts must be ready")

        output_specs = [
            OutputArtifactSpec(
                name=validate_artifact_name(item.name),
                path=validate_output_path(item.path),
                required=item.required,
            )
            for item in outputs
        ]
        if len({item.name for item in output_specs}) != len(output_specs):
            raise ValueError("task output artifact names must be unique")
        if len({item.path for item in output_specs}) != len(output_specs):
            raise ValueError("task output artifact paths must be unique")

        by_id = {artifact.id: artifact for artifact in artifacts}
        now = await database_utcnow(session)
        bindings: list[TaskArtifact] = []
        for index, artifact_id in enumerate(input_artifact_ids):
            artifact = by_id[artifact_id]
            safe_name = _safe_input_filename(artifact.name)
            binding = TaskArtifact(
                task_id=task.id,
                artifact_id=artifact.id,
                direction="input",
                name=f"input-{index:03d}-{artifact.id.hex}",
                mount_path=(f"/workspace/inputs/{index:03d}-{artifact.id.hex[:12]}-{safe_name}"),
                required=True,
                created_at=now,
            )
            session.add(binding)
            bindings.append(binding)
        for output in output_specs:
            binding = TaskArtifact(
                task_id=task.id,
                artifact_id=None,
                direction="output",
                name=output.name,
                mount_path=output.path,
                required=output.required,
                created_at=now,
            )
            session.add(binding)
            bindings.append(binding)
        await session.flush()
        return bindings

    @staticmethod
    async def list_for_task(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
    ) -> list[TaskArtifact]:
        task_exists = await session.scalar(
            select(Task.id).where(Task.id == task_id, Task.project_id == project_id)
        )
        if task_exists is None:
            raise TaskArtifactNotFoundError("task does not exist in the project")
        return list(
            await session.scalars(
                select(TaskArtifact)
                .where(TaskArtifact.task_id == task_id)
                .order_by(TaskArtifact.direction, TaskArtifact.created_at, TaskArtifact.id)
            )
        )

    @staticmethod
    async def list_for_execution(
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
        worker_id: str,
        execution_id: uuid.UUID,
        worker_session_id: uuid.UUID | None = None,
    ) -> list[TaskArtifact]:
        query = select(Task).where(
            Task.id == task_id,
            Task.project_id == project_id,
            Task.worker_id == worker_id,
            Task.execution_id == execution_id,
            Task.status.in_(ACTIVE_TASK_STATUSES),
        )
        if worker_session_id is not None:
            query = (
                query.join(Worker, Worker.id == Task.worker_id)
                .join(
                    ResourceReservation,
                    ResourceReservation.execution_id == Task.execution_id,
                )
                .where(
                    Worker.worker_session_id == worker_session_id,
                    ResourceReservation.worker_id == worker_id,
                    ResourceReservation.worker_session_id == worker_session_id,
                    ResourceReservation.released_at.is_(None),
                )
            )
        task = await session.scalar(query.with_for_update())
        if task is None:
            raise TaskArtifactConflictError("task artifact execution ownership is stale")
        return list(
            await session.scalars(
                select(TaskArtifact)
                .where(TaskArtifact.task_id == task_id)
                .order_by(TaskArtifact.direction, TaskArtifact.created_at, TaskArtifact.id)
            )
        )

    @staticmethod
    async def bind_output(
        session: AsyncSession,
        *,
        binding_id: uuid.UUID,
        task_id: uuid.UUID,
        project_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> TaskArtifact:
        task = await session.scalar(
            select(Task).where(Task.id == task_id, Task.project_id == project_id).with_for_update()
        )
        if task is None:
            raise TaskArtifactNotFoundError("task does not exist in the project")
        binding = await session.scalar(
            select(TaskArtifact)
            .where(
                TaskArtifact.id == binding_id,
                TaskArtifact.task_id == task_id,
                TaskArtifact.direction == "output",
            )
            .with_for_update()
        )
        if binding is None:
            raise TaskArtifactNotFoundError("output declaration does not exist")
        if binding.artifact_id is not None:
            if binding.artifact_id != artifact_id:
                raise TaskArtifactConflictError("output declaration is already bound")
            return binding
        artifact = await session.scalar(
            select(Artifact)
            .where(
                Artifact.id == artifact_id,
                Artifact.project_id == project_id,
                Artifact.state == ArtifactState.READY.value,
                Artifact.deleted_at.is_(None),
            )
            .with_for_update()
        )
        if artifact is None:
            raise TaskArtifactConflictError("output artifact must be ready in the task project")
        binding.artifact_id = artifact.id
        await session.flush()
        return binding


def validate_artifact_name(value: str) -> str:
    name = value.strip()
    if not _SAFE_NAME.fullmatch(name):
        raise ValueError(
            "artifact name must start with an alphanumeric character and contain only "
            "letters, digits, dot, underscore or dash"
        )
    return name


def validate_output_path(value: str) -> str:
    if not value or len(value) > 1024 or "\x00" in value or "\\" in value:
        raise ValueError("artifact output path is invalid")
    normalized = posixpath.normpath(value)
    if (
        normalized != value
        or not value.startswith(_ALLOWED_OUTPUT_ROOTS)
        or value.endswith("/")
        or posixpath.basename(value) in {"", ".", ".."}
    ):
        raise ValueError(
            "artifact output path must be a normalized file below /output or /workspace/outputs"
        )
    return value


def _safe_input_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]", "_", value.strip())[:128]
    if not sanitized or not sanitized[0].isalnum():
        sanitized = f"artifact-{sanitized.lstrip('._-')}"
    return sanitized or "artifact"
