from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from api.errors import ConflictError
from api.pagination import CursorKey
from api.schemas.tasks import TaskCreate
from core.config import Settings
from core.database import Database
from core.enums import TaskStatus
from core.logging import get_logger
from core.metrics import TASKS_CREATED
from core.rbac import Principal, PrincipalKind, can_cancel_task
from models.task import Task, TaskLog
from repositories.quotas import QuotaExceededError
from repositories.registry import ImagePolicyRepository
from repositories.secrets import (
    SecretBindingError,
    TaskSecretBindingRepository,
    TaskSecretReference,
)
from repositories.task_artifacts import (
    OutputArtifactSpec,
    TaskArtifactConflictError,
    TaskArtifactNotFoundError,
    TaskArtifactRepository,
)
from repositories.tasks import DependencyValidationError, TaskRepository


@dataclass(frozen=True, slots=True)
class CreateResult:
    task: Task
    created: bool


class TaskService:
    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.logger = get_logger("task_service")

    async def create(
        self,
        payload: TaskCreate,
        *,
        idempotency_key: str | None,
        principal: Principal | None = None,
    ) -> CreateResult:
        resolved_principal = principal or _legacy_principal(self.settings)
        try:
            payload.require_current_accelerator_execution_support()
        except ValueError as exc:
            raise ConflictError("ACCELERATOR_EXECUTION_NOT_READY", str(exc)) from exc
        if resolved_principal.project_id is None:
            raise RuntimeError("task principal has no project")
        if resolved_principal.kind == PrincipalKind.LEGACY and (
            payload.runtime_type.value != "docker"
            or payload.gpu_count
            or payload.network_enabled
            or payload.workload_type.value != "batch_job"
            or payload.secret_bindings
            or payload.inputs
            or payload.artifacts
        ):
            from api.errors import APIError

            raise APIError(
                403,
                "LEGACY_WORKLOAD_RESTRICTED",
                "Anonymous legacy access only permits Docker CPU batch tasks without network",
            )
        key = _validate_idempotency_key(idempotency_key)
        timeout_seconds = payload.timeout_seconds or self.settings.default_task_timeout
        normalized_payload = payload.model_copy(update={"timeout_seconds": timeout_seconds})
        request_hash = _request_hash(normalized_payload)
        retry_policy = payload.effective_retry_policy
        if timeout_seconds > self.settings.max_task_timeout:
            maximum = self.settings.max_task_timeout
            raise ConflictError(
                "TASK_TIMEOUT_LIMIT_EXCEEDED",
                f"timeout_seconds exceeds the configured maximum of {maximum}",
            )
        if retry_policy.max_attempts - 1 > self.settings.max_task_retries:
            raise ConflictError(
                "TASK_RETRY_LIMIT_EXCEEDED",
                f"max_retries exceeds the configured maximum of {self.settings.max_task_retries}",
            )

        try:
            async with self.database.session() as session, session.begin():
                if key is not None:
                    existing = await TaskRepository.get_by_idempotency_key(
                        session, key, project_id=resolved_principal.project_id
                    )
                    if existing is not None:
                        return _resolve_existing(existing, request_hash)
                image = payload.image
                if resolved_principal.kind != PrincipalKind.LEGACY:
                    try:
                        decision = await ImagePolicyRepository.evaluate(
                            session,
                            project_id=resolved_principal.project_id,
                            image=payload.image,
                        )
                    except ValueError as exc:
                        raise ConflictError("INVALID_IMAGE_REFERENCE", str(exc)) from exc
                    if not decision.allowed:
                        raise ConflictError(
                            "IMAGE_POLICY_DENIED",
                            "The project image policy rejected this task image",
                            details={"reason": decision.reason},
                        )
                    image = decision.canonical_image
                task = await TaskRepository.create_queued(
                    session,
                    image=image,
                    command=list(payload.command),
                    environment=dict(payload.environment),
                    timeout_seconds=timeout_seconds,
                    max_retries=retry_policy.max_attempts - 1,
                    retry_backoff=retry_policy.backoff.value,
                    retry_base_seconds=retry_policy.base_seconds,
                    retry_max_seconds=retry_policy.max_seconds,
                    retry_on_exit_codes=list(retry_policy.retry_on_exit_codes),
                    cpu_limit=payload.cpu_limit,
                    memory_limit_mb=payload.memory_limit_mb,
                    labels=dict(payload.labels),
                    network_enabled=payload.network_enabled,
                    gpu_count=payload.gpu_count,
                    accelerator_request_json=payload.effective_accelerator.model_dump(mode="json"),
                    idempotency_key=key,
                    request_hash=request_hash if key is not None else None,
                    project_id=resolved_principal.project_id,
                    submitted_by_user_id=resolved_principal.user_id,
                    created_by_api_key_id=resolved_principal.api_key_id,
                    runtime_type=payload.runtime_type.value,
                    workload_type=payload.workload_type.value,
                    priority=payload.priority,
                    preemptible=payload.preemptible,
                    gpu_memory_mb=payload.gpu_memory_mb,
                    gpu_model=payload.gpu_model,
                    tolerations=[dict(item) for item in payload.tolerations],
                    depends_on=list(payload.depends_on),
                    dependency_failure_policy=payload.dependency_failure_policy,
                )
                await TaskSecretBindingRepository.bind(
                    session,
                    task=task,
                    project_id=resolved_principal.project_id,
                    references=[
                        TaskSecretReference(
                            secret_id=binding.secret_id,
                            version=binding.version,
                            env_name=binding.env_name,
                        )
                        for binding in payload.secret_bindings
                    ],
                    public_environment_names=set(payload.environment),
                )
                await TaskArtifactRepository.create_bindings(
                    session,
                    task=task,
                    project_id=resolved_principal.project_id,
                    input_artifact_ids=[item.artifact_id for item in payload.inputs],
                    outputs=[
                        OutputArtifactSpec(
                            name=item.name,
                            path=item.path,
                            required=item.required,
                        )
                        for item in payload.artifacts
                    ],
                )
        except SecretBindingError as exc:
            raise ConflictError("INVALID_SECRET_BINDING", str(exc)) from exc
        except TaskArtifactNotFoundError as exc:
            raise ConflictError("INPUT_ARTIFACT_NOT_FOUND", str(exc)) from exc
        except TaskArtifactConflictError as exc:
            raise ConflictError("INVALID_TASK_ARTIFACT", str(exc)) from exc
        except QuotaExceededError as exc:
            raise ConflictError(
                "PROJECT_QUOTA_EXCEEDED",
                "The task would exceed a project quota",
                details={
                    "resource": exc.resource,
                    "limit": str(exc.limit),
                    "requested": str(exc.requested),
                },
            ) from exc
        except DependencyValidationError as exc:
            raise ConflictError("INVALID_TASK_DEPENDENCY", str(exc)) from exc
        except IntegrityError:
            if key is None:
                raise
            async with self.database.session() as session:
                existing = await TaskRepository.get_by_idempotency_key(
                    session, key, project_id=resolved_principal.project_id
                )
                if existing is None:
                    raise
                return _resolve_existing(existing, request_hash)

        TASKS_CREATED.inc()
        self.logger.info(
            "task created",
            task_id=str(task.id),
            status=task.status.value,
            idempotent=False,
        )
        return CreateResult(task=task, created=True)

    async def get(self, task_id: uuid.UUID, *, principal: Principal | None = None) -> Task | None:
        resolved_principal = principal or _legacy_principal(self.settings)
        if resolved_principal.project_id is None:
            return None
        async with self.database.session() as session:
            return await TaskRepository.get_for_project(
                session,
                project_id=resolved_principal.project_id,
                task_id=task_id,
            )

    async def list_tasks(
        self,
        *,
        status: TaskStatus | None,
        worker_id: str | None,
        limit: int,
        offset: int,
        after: CursorKey | None = None,
        principal: Principal | None = None,
    ) -> tuple[list[Task], int]:
        resolved_principal = principal or _legacy_principal(self.settings)
        if resolved_principal.project_id is None:
            return [], 0
        async with self.database.session() as session:
            items = await TaskRepository.list_for_project(
                session,
                project_id=resolved_principal.project_id,
                status=status,
                worker_id=worker_id,
                limit=limit,
                offset=offset,
                after=after,
            )
            count_query = select(func.count(Task.id)).where(
                Task.project_id == resolved_principal.project_id
            )
            if status is not None:
                count_query = count_query.where(Task.status == status)
            if worker_id is not None:
                count_query = count_query.where(Task.worker_id == worker_id)
            total = int(await session.scalar(count_query) or 0)
        return items, total

    async def cancel(
        self, task_id: uuid.UUID, *, principal: Principal | None = None
    ) -> Task | None:
        resolved_principal = principal or _legacy_principal(self.settings)
        if resolved_principal.project_id is None:
            return None
        async with self.database.session() as session, session.begin():
            existing = await TaskRepository.get_for_project(
                session,
                project_id=resolved_principal.project_id,
                task_id=task_id,
                for_update=True,
            )
            if existing is None:
                return None
            if not can_cancel_task(resolved_principal, existing.submitted_by_user_id):
                from api.errors import APIError

                raise APIError(403, "PERMISSION_DENIED", "Task cancellation is not permitted")
            task = await TaskRepository.cancel(session, task_id)
        if task is not None:
            self.logger.info(
                "task cancellation requested",
                task_id=str(task.id),
                status=task.status.value,
                cancel_requested=task.cancel_requested,
            )
        return task

    async def logs(
        self,
        task_id: uuid.UUID,
        *,
        offset: int,
        limit: int,
        principal: Principal | None = None,
    ) -> tuple[list[TaskLog], int] | None:
        resolved_principal = principal or _legacy_principal(self.settings)
        if resolved_principal.project_id is None:
            return None
        async with self.database.session() as session:
            task = await TaskRepository.get_for_project(
                session,
                project_id=resolved_principal.project_id,
                task_id=task_id,
            )
            if task is None:
                return None
            logs = await TaskRepository.list_logs_for_project(
                session,
                project_id=resolved_principal.project_id,
                task_id=task_id,
                offset=offset,
                limit=limit,
            )
            total = int(
                await session.scalar(
                    select(func.count(TaskLog.id))
                    .join(Task, Task.id == TaskLog.task_id)
                    .where(
                        Task.project_id == resolved_principal.project_id,
                        TaskLog.task_id == task_id,
                    )
                )
                or 0
            )
        return logs, total


def _request_hash(payload: TaskCreate) -> str:
    normalized = json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def _validate_idempotency_key(value: str | None) -> str | None:
    if value is None:
        return None
    if not value or len(value) > 255 or any(ord(character) < 32 for character in value):
        raise ConflictError(
            "INVALID_IDEMPOTENCY_KEY",
            "Idempotency-Key must contain 1-255 printable characters",
        )
    return value


def _resolve_existing(existing: Task, request_hash: str) -> CreateResult:
    if existing.request_hash != request_hash:
        raise ConflictError(
            "IDEMPOTENCY_KEY_REUSED",
            "Idempotency-Key was already used with a different request payload",
            details={"task_id": str(existing.id)},
        )
    return CreateResult(task=existing, created=False)


def _legacy_principal(settings: Settings) -> Principal:
    return Principal(
        kind=PrincipalKind.LEGACY,
        project_id=uuid.UUID(settings.legacy_project_id),
    )
