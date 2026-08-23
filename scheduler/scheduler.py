import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.ext.asyncio import AsyncSession

from models.task import Task
from repositories.tasks import ClaimRejected, TaskRepository
from repositories.workers import WorkerRepository
from scheduler.policies import RejectionReason, evaluate, worker_accepts_new_tasks

SessionFactory = Callable[[], AsyncSession]
TaskIdInput = uuid.UUID | str | bytes | None


class AssignmentSource(StrEnum):
    MESSAGE = "message"
    DATABASE_FALLBACK = "database_fallback"


@dataclass(frozen=True, slots=True)
class TaskAssignment:
    """Detached task data needed by a Worker to start one execution."""

    task_id: uuid.UUID
    execution_id: uuid.UUID
    worker_id: str
    source: AssignmentSource
    image: str
    command: tuple[str, ...]
    environment: dict[str, str]
    timeout_seconds: int
    cpu_limit: float
    memory_limit_mb: int
    gpu_count: int
    network_enabled: bool
    labels: dict[str, str]
    retry_count: int
    max_retries: int

    @classmethod
    def from_task(
        cls,
        task: Task,
        *,
        execution_id: uuid.UUID,
        source: AssignmentSource,
    ) -> "TaskAssignment":
        if task.worker_id is None:
            raise ValueError("claimed task has no worker_id")
        return cls(
            task_id=task.id,
            execution_id=execution_id,
            worker_id=task.worker_id,
            source=source,
            image=task.image,
            command=tuple(task.command),
            environment=dict(task.environment),
            timeout_seconds=task.timeout_seconds,
            cpu_limit=task.cpu_limit,
            memory_limit_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            network_enabled=task.network_enabled,
            labels=dict(task.labels),
            retry_count=task.retry_count,
            max_retries=task.max_retries,
        )


@dataclass(frozen=True, slots=True)
class _ClaimAttempt:
    assignment: TaskAssignment | None
    worker_available: bool
    rejection_reason: RejectionReason | None = None


class Scheduler:
    """Select and atomically claim a compatible queued task for one Worker."""

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        lease_seconds: float,
        fallback_limit: int = 100,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if fallback_limit < 1:
            raise ValueError("fallback_limit must be at least one")
        self._session_factory = session_factory
        self._lease_seconds = lease_seconds
        self._fallback_limit = fallback_limit
        self._fallback_offsets: dict[str, int] = {}

    async def claim_for_worker(
        self,
        *,
        worker_id: str,
        message_task_id: TaskIdInput = None,
    ) -> TaskAssignment | None:
        """Claim a task, preferring the ID carried by a Redis queue message.

        Invalid, stale, or incompatible messages are harmless: a bounded scan of
        PostgreSQL queued tasks is used as the recovery path. Redis acknowledgement
        remains the Worker's responsibility and must happen only after this method
        returns an assignment (or confirms the message cannot currently be claimed).
        """

        preferred_id = _parse_task_id(message_task_id)
        if preferred_id is not None:
            attempt = await self._try_claim(
                worker_id=worker_id,
                task_id=preferred_id,
                source=AssignmentSource.MESSAGE,
            )
            if attempt.assignment is not None:
                return attempt.assignment
            if not attempt.worker_available:
                return None

        candidate_ids = await self._queued_candidate_ids(worker_id)
        for task_id in candidate_ids:
            if task_id == preferred_id:
                continue
            attempt = await self._try_claim(
                worker_id=worker_id,
                task_id=task_id,
                source=AssignmentSource.DATABASE_FALLBACK,
            )
            if attempt.assignment is not None:
                return attempt.assignment
            if not attempt.worker_available:
                return None
        return None

    async def _queued_candidate_ids(self, worker_id: str) -> list[uuid.UUID]:
        async with self._session_factory() as session:
            worker = await WorkerRepository.get(session, worker_id)
            if not worker_accepts_new_tasks(worker) or worker is None:
                return []

            offset = self._fallback_offsets.get(worker_id, 0)
            wrapped = False
            for _page_number in range(20):
                candidate_ids, page_size = await TaskRepository.list_queued_candidates_for_worker(
                    session,
                    worker=worker,
                    limit=self._fallback_limit,
                    offset=offset,
                )
                if page_size == 0:
                    if offset > 0 and not wrapped:
                        offset = 0
                        wrapped = True
                        continue
                    self._fallback_offsets[worker_id] = 0
                    return []
                offset += page_size
                self._fallback_offsets[worker_id] = offset
                if candidate_ids:
                    return candidate_ids
                if page_size < self._fallback_limit:
                    self._fallback_offsets[worker_id] = 0
                    return []
            # Rotate the bounded scan window on the next poll so a long prefix
            # of label-incompatible tasks cannot starve later compatible work.
            self._fallback_offsets[worker_id] = offset
            return []

    async def _try_claim(
        self,
        *,
        worker_id: str,
        task_id: uuid.UUID,
        source: AssignmentSource,
    ) -> _ClaimAttempt:
        try:
            async with self._session_factory() as session, session.begin():
                worker = await WorkerRepository.get(session, worker_id)
                if not worker_accepts_new_tasks(worker):
                    decision = evaluate(worker, None)
                    return _ClaimAttempt(
                        assignment=None,
                        worker_available=False,
                        rejection_reason=decision.reason,
                    )

                task = await TaskRepository.get(session, task_id)
                decision = evaluate(worker, task)
                if not decision.allowed:
                    return _ClaimAttempt(
                        assignment=None,
                        worker_available=True,
                        rejection_reason=decision.reason,
                    )

                claimed_task, execution_id = await TaskRepository.claim(
                    session,
                    task_id=task_id,
                    worker_id=worker_id,
                    lease_seconds=self._lease_seconds,
                )
                assignment = TaskAssignment.from_task(
                    claimed_task,
                    execution_id=execution_id,
                    source=source,
                )
                return _ClaimAttempt(assignment=assignment, worker_available=True)
        except ClaimRejected:
            # A concurrent claim, cancellation, heartbeat, or capacity update won.
            # The transaction context has rolled back before another candidate is tried.
            return _ClaimAttempt(assignment=None, worker_available=True)


def _parse_task_id(value: TaskIdInput) -> uuid.UUID | None:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        if isinstance(value, bytes):
            value = value.decode("ascii")
        return uuid.UUID(value)
    except (UnicodeDecodeError, ValueError, AttributeError):
        return None
