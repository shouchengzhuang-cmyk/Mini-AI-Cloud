import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import ACTIVE_TASK_STATUSES, FINAL_TASK_STATUSES, TaskStatus
from core.rbac import ProjectStatus
from models.artifact import JobGroup, TaskDependency
from models.identity import Project
from models.task import Task
from repositories.clock import database_utcnow

_GROUP_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_. -]{0,254}$")


class DependencyFailurePolicy(StrEnum):
    CANCEL = "cancel"
    BLOCK = "block"


class DependencyState(StrEnum):
    READY = "ready"
    WAITING = "waiting"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class JobGroupStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DAGNotFoundError(LookupError):
    pass


class DAGConflictError(RuntimeError):
    pass


class DAGCycleError(DAGConflictError):
    pass


class DAGInvariantViolation(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DependencySpec:
    task_id: uuid.UUID
    depends_on_task_id: uuid.UUID
    failure_policy: DependencyFailurePolicy = DependencyFailurePolicy.CANCEL


@dataclass(frozen=True, slots=True)
class DependencyResolution:
    task_id: uuid.UUID
    task_status: TaskStatus
    state: DependencyState
    dependency_ids: tuple[uuid.UUID, ...]
    waiting_on_task_ids: tuple[uuid.UUID, ...]
    failed_dependency_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True, slots=True)
class JobGroupSummary:
    group: JobGroup
    status: JobGroupStatus
    task_count: int
    ready_tasks: int
    waiting_tasks: int
    blocked_tasks: int
    cancelled_tasks: int
    succeeded_tasks: int
    failed_tasks: int
    finished_at: datetime | None


class DAGRepository:
    """Manage one project-scoped DAG while locking its group row for graph writes."""

    @staticmethod
    async def create_group(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        name: str,
        retry_policy: dict[str, object],
        dependencies: list[DependencySpec],
    ) -> JobGroup:
        await _lock_active_project(session, project_id)
        normalized_name = name.strip()
        if not _GROUP_NAME.fullmatch(normalized_name):
            raise ValueError("job group name contains unsupported characters")
        _validate_retry_policy(retry_policy)
        group = JobGroup(
            project_id=project_id,
            name=normalized_name,
            status=JobGroupStatus.PENDING.value,
            retry_policy=dict(retry_policy),
            created_at=await database_utcnow(session),
        )
        session.add(group)
        await session.flush()
        if dependencies:
            await _add_dependencies_locked(
                session,
                group=group,
                dependencies=dependencies,
            )
        return group

    @staticmethod
    async def get_group(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
        for_update: bool = False,
    ) -> JobGroup | None:
        query = select(JobGroup).where(
            JobGroup.id == group_id,
            JobGroup.project_id == project_id,
        )
        if for_update:
            query = query.with_for_update()
        return await session.scalar(query)

    @staticmethod
    async def list_groups(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int,
    ) -> list[JobGroup]:
        return list(
            await session.scalars(
                select(JobGroup)
                .where(JobGroup.project_id == project_id)
                .order_by(JobGroup.created_at.desc(), JobGroup.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )

    @staticmethod
    async def summarize_groups(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        groups: list[JobGroup],
    ) -> list[JobGroupSummary]:
        """Summarize an already paginated group batch with two bounded queries."""

        if not groups:
            return []
        if any(group.project_id != project_id for group in groups):
            raise DAGInvariantViolation("job group belongs to another project")

        group_ids = [group.id for group in groups]
        edges = list(
            await session.scalars(
                select(TaskDependency).where(TaskDependency.job_group_id.in_(group_ids))
            )
        )
        task_ids = _task_ids(edges)
        tasks: list[Task] = []
        if task_ids:
            member_task_ids = select(TaskDependency.task_id).where(
                TaskDependency.job_group_id.in_(group_ids)
            )
            prerequisite_task_ids = select(TaskDependency.depends_on_task_id).where(
                TaskDependency.job_group_id.in_(group_ids)
            )
            tasks = list(
                await session.scalars(
                    select(Task).where(
                        Task.project_id == project_id,
                        or_(
                            Task.id.in_(member_task_ids),
                            Task.id.in_(prerequisite_task_ids),
                        ),
                    )
                )
            )
        tasks_by_id = {task.id: task for task in tasks}
        if len(tasks_by_id) != len(task_ids):
            raise DAGInvariantViolation("job group references missing or cross-project tasks")

        edges_by_group: dict[uuid.UUID, list[TaskDependency]] = {group.id: [] for group in groups}
        for edge in edges:
            if edge.job_group_id is None or edge.job_group_id not in edges_by_group:
                raise DAGInvariantViolation("dependency has an invalid job group")
            edges_by_group[edge.job_group_id].append(edge)

        summaries: list[JobGroupSummary] = []
        for group in groups:
            group_edges = edges_by_group[group.id]
            group_tasks = [tasks_by_id[task_id] for task_id in _task_ids(group_edges)]
            summaries.append(_summarize_loaded_group(group, group_tasks, group_edges))
        return summaries

    @staticmethod
    async def add_dependency(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
        dependency: DependencySpec,
    ) -> TaskDependency:
        await _lock_active_project(session, project_id)
        group = await DAGRepository.get_group(
            session,
            project_id=project_id,
            group_id=group_id,
            for_update=True,
        )
        if group is None:
            raise DAGNotFoundError("job group does not exist in the project")
        rows = await _add_dependencies_locked(
            session,
            group=group,
            dependencies=[dependency],
        )
        return rows[0]

    @staticmethod
    async def list_dependencies(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
    ) -> list[TaskDependency]:
        group = await DAGRepository.get_group(
            session,
            project_id=project_id,
            group_id=group_id,
        )
        if group is None:
            raise DAGNotFoundError("job group does not exist in the project")
        dependencies = list(
            await session.scalars(
                select(TaskDependency)
                .where(TaskDependency.job_group_id == group_id)
                .order_by(TaskDependency.task_id, TaskDependency.depends_on_task_id)
            )
        )
        await _task_statuses(session, project_id, _task_ids(dependencies))
        return dependencies

    @staticmethod
    async def dependency_state(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
        task_id: uuid.UUID,
    ) -> DependencyResolution:
        group = await DAGRepository.get_group(
            session,
            project_id=project_id,
            group_id=group_id,
        )
        if group is None:
            raise DAGNotFoundError("job group does not exist in the project")
        task = await session.scalar(
            select(Task).where(Task.id == task_id, Task.project_id == project_id)
        )
        if task is None:
            raise DAGNotFoundError("task does not exist in the project")
        edges = list(
            await session.scalars(
                select(TaskDependency).where(TaskDependency.job_group_id == group_id)
            )
        )
        if task_id not in _task_ids(edges):
            raise DAGNotFoundError("task is not represented in the job group")
        statuses = await _task_statuses(session, project_id, _task_ids(edges))
        return _resolve_task(task_id, statuses, edges)

    @staticmethod
    async def ready_tasks(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
    ) -> list[DependencyResolution]:
        group = await DAGRepository.get_group(
            session,
            project_id=project_id,
            group_id=group_id,
        )
        if group is None:
            raise DAGNotFoundError("job group does not exist in the project")
        edges = list(
            await session.scalars(
                select(TaskDependency).where(TaskDependency.job_group_id == group_id)
            )
        )
        task_ids = _task_ids(edges)
        statuses = await _task_statuses(session, project_id, task_ids)
        resolutions = [_resolve_task(task_id, statuses, edges) for task_id in task_ids]
        runnable_statuses = {
            TaskStatus.PENDING,
            TaskStatus.QUEUED,
            TaskStatus.RETRYING,
        }
        return sorted(
            (
                resolution
                for resolution in resolutions
                if resolution.state == DependencyState.READY
                and resolution.task_status in runnable_statuses
            ),
            key=lambda resolution: str(resolution.task_id),
        )

    @staticmethod
    async def summarize_group(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        group_id: uuid.UUID,
    ) -> JobGroupSummary:
        group = await DAGRepository.get_group(
            session,
            project_id=project_id,
            group_id=group_id,
        )
        if group is None:
            raise DAGNotFoundError("job group does not exist in the project")
        edges = list(
            await session.scalars(
                select(TaskDependency).where(TaskDependency.job_group_id == group_id)
            )
        )
        task_ids = _task_ids(edges)
        tasks = list(
            await session.scalars(
                select(Task).where(Task.project_id == project_id, Task.id.in_(task_ids))
            )
        )
        if len(tasks) != len(task_ids):
            raise DAGInvariantViolation("job group references missing or cross-project tasks")
        return _summarize_loaded_group(group, tasks, edges)


async def _add_dependencies_locked(
    session: AsyncSession,
    *,
    group: JobGroup,
    dependencies: list[DependencySpec],
) -> list[TaskDependency]:
    if len(dependencies) > 1_000:
        raise ValueError("at most 1000 dependencies may be added at once")
    for dependency in dependencies:
        if dependency.task_id == dependency.depends_on_task_id:
            raise DAGCycleError("a task cannot depend on itself")
    requested_task_ids = {
        task_id
        for dependency in dependencies
        for task_id in (dependency.task_id, dependency.depends_on_task_id)
    }
    tasks = list(
        await session.scalars(
            select(Task)
            .where(
                Task.project_id == group.project_id,
                Task.id.in_(requested_task_ids),
            )
            .order_by(Task.id)
            .with_for_update()
        )
    )
    if len(tasks) != len(requested_task_ids):
        raise DAGNotFoundError("all dependency tasks must exist in the job group's project")
    tasks_by_id = {task.id: task for task in tasks}
    membership_edges = list(
        await session.scalars(
            select(TaskDependency)
            .where(
                or_(
                    TaskDependency.task_id.in_(requested_task_ids),
                    TaskDependency.depends_on_task_id.in_(requested_task_ids),
                )
            )
            .with_for_update()
        )
    )
    if any(edge.job_group_id != group.id for edge in membership_edges):
        raise DAGConflictError("a task cannot belong to multiple job groups")
    existing_edges = list(
        await session.scalars(
            select(TaskDependency).where(TaskDependency.job_group_id == group.id).with_for_update()
        )
    )
    edge_map = {(edge.task_id, edge.depends_on_task_id): edge for edge in existing_edges}
    combined = {(edge.task_id, edge.depends_on_task_id) for edge in existing_edges}
    created: list[TaskDependency] = []
    for dependency in dependencies:
        key = (dependency.task_id, dependency.depends_on_task_id)
        existing = edge_map.get(key)
        if existing is not None:
            if existing.failure_policy != dependency.failure_policy.value:
                raise DAGConflictError("dependency already exists with another failure policy")
            created.append(existing)
            continue
        candidate = combined | {key}
        if _contains_cycle(candidate):
            raise DAGCycleError("dependency would create a cycle")
        if tasks_by_id[dependency.task_id].status not in {
            TaskStatus.PENDING,
            TaskStatus.QUEUED,
            TaskStatus.RETRYING,
        }:
            raise DAGConflictError(
                "dependencies may only be attached before the dependant task starts"
            )
        row = TaskDependency(
            task_id=dependency.task_id,
            depends_on_task_id=dependency.depends_on_task_id,
            job_group_id=group.id,
            failure_policy=dependency.failure_policy.value,
        )
        session.add(row)
        created.append(row)
        edge_map[key] = row
        combined = candidate
    await session.flush()
    return created


async def _lock_active_project(session: AsyncSession, project_id: uuid.UUID) -> Project:
    project = await session.scalar(
        select(Project).where(Project.id == project_id).with_for_update()
    )
    if project is None or project.status != ProjectStatus.ACTIVE:
        raise DAGNotFoundError("active project does not exist")
    return project


async def _task_statuses(
    session: AsyncSession,
    project_id: uuid.UUID,
    task_ids: set[uuid.UUID],
) -> dict[uuid.UUID, TaskStatus]:
    tasks = list(
        await session.scalars(
            select(Task).where(Task.project_id == project_id, Task.id.in_(task_ids))
        )
    )
    if len(tasks) != len(task_ids):
        raise DAGInvariantViolation("job group references missing or cross-project tasks")
    return {task.id: task.status for task in tasks}


def _resolve_task(
    task_id: uuid.UUID,
    statuses: dict[uuid.UUID, TaskStatus],
    edges: list[TaskDependency],
) -> DependencyResolution:
    incoming = [edge for edge in edges if edge.task_id == task_id]
    dependency_ids = tuple(sorted((edge.depends_on_task_id for edge in incoming), key=str))
    waiting: list[uuid.UUID] = []
    failed: list[uuid.UUID] = []
    failed_policies: list[DependencyFailurePolicy] = []
    for edge in incoming:
        status = statuses.get(edge.depends_on_task_id)
        if status is None:
            raise DAGInvariantViolation("dependency task status is missing")
        if status == TaskStatus.SUCCEEDED:
            continue
        if status in FINAL_TASK_STATUSES:
            failed.append(edge.depends_on_task_id)
            try:
                failed_policies.append(DependencyFailurePolicy(edge.failure_policy))
            except ValueError as exc:
                raise DAGInvariantViolation("dependency has an invalid failure policy") from exc
        else:
            waiting.append(edge.depends_on_task_id)
    if DependencyFailurePolicy.CANCEL in failed_policies:
        state = DependencyState.CANCELLED
    elif failed_policies:
        state = DependencyState.BLOCKED
    elif waiting:
        state = DependencyState.WAITING
    else:
        state = DependencyState.READY
    task_status = statuses.get(task_id)
    if task_status is None:
        raise DAGInvariantViolation("job group task status is missing")
    return DependencyResolution(
        task_id=task_id,
        task_status=task_status,
        state=state,
        dependency_ids=dependency_ids,
        waiting_on_task_ids=tuple(sorted(waiting, key=str)),
        failed_dependency_ids=tuple(sorted(failed, key=str)),
    )


def _contains_cycle(edges: set[tuple[uuid.UUID, uuid.UUID]]) -> bool:
    graph: dict[uuid.UUID, set[uuid.UUID]] = {}
    indegree: dict[uuid.UUID, int] = {}
    for task_id, dependency_id in edges:
        graph.setdefault(task_id, set()).add(dependency_id)
        graph.setdefault(dependency_id, set())
        indegree.setdefault(task_id, 0)
        indegree[dependency_id] = indegree.get(dependency_id, 0) + 1

    roots = [node for node, degree in indegree.items() if degree == 0]
    visited = 0
    while roots:
        node = roots.pop()
        visited += 1
        for neighbor in graph[node]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                roots.append(neighbor)
    return visited != len(indegree)


def _task_ids(edges: list[TaskDependency]) -> set[uuid.UUID]:
    return {task_id for edge in edges for task_id in (edge.task_id, edge.depends_on_task_id)}


def _summarize_loaded_group(
    group: JobGroup,
    tasks: list[Task],
    edges: list[TaskDependency],
) -> JobGroupSummary:
    task_ids = _task_ids(edges)
    if len(tasks) != len(task_ids) or {task.id for task in tasks} != task_ids:
        raise DAGInvariantViolation("job group references missing or cross-project tasks")
    statuses = {task.id: task.status for task in tasks}
    resolutions = [_resolve_task(task_id, statuses, edges) for task_id in task_ids]
    status = _group_status(tasks, resolutions)
    terminal_times = [task.finished_at for task in tasks if task.finished_at is not None]
    return JobGroupSummary(
        group=group,
        status=status,
        task_count=len(task_ids),
        ready_tasks=sum(
            resolution.state == DependencyState.READY
            and resolution.task_status
            in {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RETRYING}
            for resolution in resolutions
        ),
        waiting_tasks=_count_state(resolutions, DependencyState.WAITING),
        blocked_tasks=_count_state(resolutions, DependencyState.BLOCKED),
        cancelled_tasks=_count_state(resolutions, DependencyState.CANCELLED),
        succeeded_tasks=sum(task.status == TaskStatus.SUCCEEDED for task in tasks),
        failed_tasks=sum(
            task.status in FINAL_TASK_STATUSES and task.status != TaskStatus.SUCCEEDED
            for task in tasks
        ),
        finished_at=max(terminal_times)
        if terminal_times
        and status
        in {
            JobGroupStatus.SUCCEEDED,
            JobGroupStatus.FAILED,
        }
        else None,
    )


def _group_status(
    tasks: list[Task],
    resolutions: list[DependencyResolution],
) -> JobGroupStatus:
    if not tasks:
        return JobGroupStatus.PENDING
    if all(task.status == TaskStatus.SUCCEEDED for task in tasks):
        return JobGroupStatus.SUCCEEDED
    if any(
        task.status in FINAL_TASK_STATUSES and task.status != TaskStatus.SUCCEEDED for task in tasks
    ) or any(
        resolution.state in {DependencyState.BLOCKED, DependencyState.CANCELLED}
        for resolution in resolutions
    ):
        return JobGroupStatus.FAILED
    if any(task.status in ACTIVE_TASK_STATUSES for task in tasks):
        return JobGroupStatus.RUNNING
    return JobGroupStatus.PENDING


def _count_state(resolutions: list[DependencyResolution], state: DependencyState) -> int:
    return sum(resolution.state == state for resolution in resolutions)


def _validate_retry_policy(value: dict[str, object]) -> None:
    try:
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("retry_policy must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > 16_384:
        raise ValueError("retry_policy must be at most 16384 encoded bytes")
