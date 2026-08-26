import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import event

from core.database import Database
from core.enums import TaskStatus
from core.rbac import ProjectStatus
from models import Base
from models.identity import Project
from models.task import Task
from repositories.dag import (
    DAGConflictError,
    DAGCycleError,
    DAGNotFoundError,
    DAGRepository,
    DependencyFailurePolicy,
    DependencySpec,
    DependencyState,
    JobGroupStatus,
)


@pytest_asyncio.fixture
async def dag_database(tmp_path: Any) -> AsyncIterator[Database]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'dag.sqlite3').as_posix()}")

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield database
    finally:
        await database.dispose()


async def _project(database: Database, slug: str) -> uuid.UUID:
    project_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=project_id,
                name=slug,
                slug=slug,
                status=ProjectStatus.ACTIVE,
            )
        )
    return project_id


async def _task(database: Database, project_id: uuid.UUID) -> uuid.UUID:
    task_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            Task(
                id=task_id,
                project_id=project_id,
                image="python:3.12",
                command=["python", "-V"],
                status=TaskStatus.QUEUED,
            )
        )
    return task_id


async def _tasks(database: Database, project_id: uuid.UUID, count: int) -> list[uuid.UUID]:
    task_ids = [uuid.uuid4() for _ in range(count)]
    async with database.session() as session, session.begin():
        session.add_all(
            [
                Task(
                    id=task_id,
                    project_id=project_id,
                    image="python:3.12",
                    command=["python", "-V"],
                    status=TaskStatus.QUEUED,
                )
                for task_id in task_ids
            ]
        )
    return task_ids


async def _set_status(
    database: Database,
    task_id: uuid.UUID,
    status: TaskStatus,
) -> None:
    async with database.session() as session, session.begin():
        task = await session.get(Task, task_id, with_for_update=True)
        assert task is not None
        task.status = status
        if status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
            task.finished_at = datetime.now(UTC)


async def test_cycle_detection_rejects_self_two_node_and_long_cycles(
    dag_database: Database,
) -> None:
    project_id = await _project(dag_database, "cycles")
    task_a = await _task(dag_database, project_id)
    task_b = await _task(dag_database, project_id)
    task_c = await _task(dag_database, project_id)
    task_d = await _task(dag_database, project_id)

    with pytest.raises(DAGCycleError, match="itself"):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.create_group(
                session,
                project_id=project_id,
                name="self-cycle",
                retry_policy={},
                dependencies=[DependencySpec(task_a, task_a)],
            )

    async with dag_database.session() as session, session.begin():
        group = await DAGRepository.create_group(
            session,
            project_id=project_id,
            name="valid-chain",
            retry_policy={},
            dependencies=[
                DependencySpec(task_a, task_b),
                DependencySpec(task_b, task_c),
            ],
        )
        group_id = group.id
        valid = await DAGRepository.add_dependency(
            session,
            project_id=project_id,
            group_id=group_id,
            dependency=DependencySpec(task_d, task_a),
        )
        assert valid.task_id == task_d

    with pytest.raises(DAGCycleError):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.add_dependency(
                session,
                project_id=project_id,
                group_id=group_id,
                dependency=DependencySpec(task_c, task_a),
            )

    project_two = await _project(dag_database, "two-node")
    first = await _task(dag_database, project_two)
    second = await _task(dag_database, project_two)
    async with dag_database.session() as session, session.begin():
        two_group = await DAGRepository.create_group(
            session,
            project_id=project_two,
            name="two-node-cycle",
            retry_policy={},
            dependencies=[DependencySpec(first, second)],
        )
        two_group_id = two_group.id
    with pytest.raises(DAGCycleError):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.add_dependency(
                session,
                project_id=project_two,
                group_id=two_group_id,
                dependency=DependencySpec(second, first),
            )

    project_long = await _project(dag_database, "long-cycle")
    chain = await _tasks(dag_database, project_long, 64)
    async with dag_database.session() as session, session.begin():
        long_group = await DAGRepository.create_group(
            session,
            project_id=project_long,
            name="long-cycle",
            retry_policy={},
            dependencies=[
                DependencySpec(task_id, dependency_id)
                for task_id, dependency_id in zip(chain[1:], chain[:-1], strict=True)
            ],
        )
        long_group_id = long_group.id
    with pytest.raises(DAGCycleError):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.add_dependency(
                session,
                project_id=project_long,
                group_id=long_group_id,
                dependency=DependencySpec(chain[0], chain[-1]),
            )


@pytest.mark.parametrize(
    ("failure_policy", "expected"),
    [
        (DependencyFailurePolicy.CANCEL, DependencyState.CANCELLED),
        (DependencyFailurePolicy.BLOCK, DependencyState.BLOCKED),
    ],
)
async def test_dependencies_become_ready_only_after_success_and_apply_failure_policy(
    dag_database: Database,
    failure_policy: DependencyFailurePolicy,
    expected: DependencyState,
) -> None:
    project_id = await _project(dag_database, f"readiness-{failure_policy.value}")
    dependent = await _task(dag_database, project_id)
    prerequisite = await _task(dag_database, project_id)
    async with dag_database.session() as session, session.begin():
        group = await DAGRepository.create_group(
            session,
            project_id=project_id,
            name=f"readiness-{failure_policy.value}",
            retry_policy={},
            dependencies=[
                DependencySpec(
                    dependent,
                    prerequisite,
                    failure_policy=failure_policy,
                )
            ],
        )
        group_id = group.id

    async with dag_database.session() as session:
        waiting = await DAGRepository.dependency_state(
            session,
            project_id=project_id,
            group_id=group_id,
            task_id=dependent,
        )
        roots = await DAGRepository.ready_tasks(
            session,
            project_id=project_id,
            group_id=group_id,
        )
    assert waiting.state == DependencyState.WAITING
    assert waiting.waiting_on_task_ids == (prerequisite,)
    assert [item.task_id for item in roots] == [prerequisite]

    await _set_status(dag_database, prerequisite, TaskStatus.SUCCEEDED)
    async with dag_database.session() as session:
        ready = await DAGRepository.dependency_state(
            session,
            project_id=project_id,
            group_id=group_id,
            task_id=dependent,
        )
        runnable = await DAGRepository.ready_tasks(
            session,
            project_id=project_id,
            group_id=group_id,
        )
    assert ready.state == DependencyState.READY
    assert [item.task_id for item in runnable] == [dependent]

    failed_dependent = await _task(dag_database, project_id)
    failed_prerequisite = await _task(dag_database, project_id)
    async with dag_database.session() as session, session.begin():
        failed_group = await DAGRepository.create_group(
            session,
            project_id=project_id,
            name=f"failure-{failure_policy.value}",
            retry_policy={},
            dependencies=[
                DependencySpec(
                    failed_dependent,
                    failed_prerequisite,
                    failure_policy=failure_policy,
                )
            ],
        )
        failed_group_id = failed_group.id
    await _set_status(dag_database, failed_prerequisite, TaskStatus.FAILED)
    async with dag_database.session() as session:
        failed = await DAGRepository.dependency_state(
            session,
            project_id=project_id,
            group_id=failed_group_id,
            task_id=failed_dependent,
        )
        summary = await DAGRepository.summarize_group(
            session,
            project_id=project_id,
            group_id=failed_group_id,
        )
        unchanged = await session.get(Task, failed_dependent)
    assert failed.state == expected
    assert failed.failed_dependency_ids == (failed_prerequisite,)
    assert summary.status == JobGroupStatus.FAILED
    assert unchanged is not None and unchanged.status == TaskStatus.QUEUED


async def test_job_group_rejects_cross_project_tasks_and_multi_group_membership(
    dag_database: Database,
) -> None:
    project_id = await _project(dag_database, "isolated")
    other_project_id = await _project(dag_database, "isolated-other")
    first = await _task(dag_database, project_id)
    second = await _task(dag_database, project_id)
    outsider = await _task(dag_database, other_project_id)

    with pytest.raises(DAGNotFoundError):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.create_group(
                session,
                project_id=project_id,
                name="cross-project",
                retry_policy={},
                dependencies=[DependencySpec(first, outsider)],
            )

    async with dag_database.session() as session, session.begin():
        await DAGRepository.create_group(
            session,
            project_id=project_id,
            name="first-group",
            retry_policy={},
            dependencies=[DependencySpec(first, second)],
        )
    third = await _task(dag_database, project_id)
    with pytest.raises(DAGConflictError, match="multiple"):
        async with dag_database.session() as session, session.begin():
            await DAGRepository.create_group(
                session,
                project_id=project_id,
                name="second-group",
                retry_policy={},
                dependencies=[DependencySpec(first, third)],
            )


async def test_group_succeeds_only_when_every_represented_task_succeeds(
    dag_database: Database,
) -> None:
    project_id = await _project(dag_database, "group-success")
    dependent = await _task(dag_database, project_id)
    prerequisite = await _task(dag_database, project_id)
    async with dag_database.session() as session, session.begin():
        group = await DAGRepository.create_group(
            session,
            project_id=project_id,
            name="successful-group",
            retry_policy={},
            dependencies=[DependencySpec(dependent, prerequisite)],
        )
        group_id = group.id
    await _set_status(dag_database, prerequisite, TaskStatus.SUCCEEDED)
    await _set_status(dag_database, dependent, TaskStatus.SUCCEEDED)

    async with dag_database.session() as session:
        summary = await DAGRepository.summarize_group(
            session,
            project_id=project_id,
            group_id=group_id,
        )
    assert summary.status == JobGroupStatus.SUCCEEDED
    assert summary.succeeded_tasks == 2
    assert summary.finished_at is not None
