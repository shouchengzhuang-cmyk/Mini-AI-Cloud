import uuid
from typing import Any

import pytest
from sqlalchemy import event, func, select

from core.database import Database
from core.enums import TaskStatus
from models.artifact import TaskDependency
from models.outbox import OutboxEvent
from models.task import Task
from repositories.tasks import TaskRepository

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def test_dependency_readiness_reads_one_batch_instead_of_one_query_per_task(
    database: Database,
) -> None:
    prerequisite = Task(
        project_id=PROJECT_ID,
        image="example/prerequisite",
        command=["true"],
        status=TaskStatus.QUEUED,
    )
    dependants = [
        Task(
            project_id=PROJECT_ID,
            image=f"example/dependant-{index}",
            command=["true"],
            status=TaskStatus.PENDING,
        )
        for index in range(20)
    ]
    async with database.session() as session, session.begin():
        session.add(prerequisite)
        session.add_all(dependants)
        await session.flush()
        session.add_all(
            TaskDependency(
                task_id=task.id,
                depends_on_task_id=prerequisite.id,
                failure_policy="cancel",
            )
            for task in dependants
        )

    statements: list[str] = []

    def capture_statement(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    async with database.session() as session, session.begin():
        await session.connection()
        event.listen(database.engine.sync_engine, "before_cursor_execute", capture_statement)
        try:
            changed = await TaskRepository.resolve_dependency_readiness(session, limit=20)
        finally:
            event.remove(database.engine.sync_engine, "before_cursor_execute", capture_statement)

    assert changed == []
    select_statements = [
        statement for statement in statements if statement.lstrip().upper().startswith("SELECT")
    ]
    assert len(select_statements) == 2
    assert "task_dependencies.task_id IN" in select_statements[1]


async def test_dependency_readiness_promotes_every_ready_task_in_the_locked_batch(
    database: Database,
) -> None:
    prerequisite = Task(
        project_id=PROJECT_ID,
        image="example/prerequisite",
        command=["true"],
        status=TaskStatus.SUCCEEDED,
    )
    dependants = [
        Task(
            project_id=PROJECT_ID,
            image=f"example/ready-dependant-{index}",
            command=["true"],
            status=TaskStatus.PENDING,
        )
        for index in range(20)
    ]
    async with database.session() as session, session.begin():
        session.add(prerequisite)
        session.add_all(dependants)
        await session.flush()
        session.add_all(
            TaskDependency(
                task_id=task.id,
                depends_on_task_id=prerequisite.id,
                failure_policy="cancel",
            )
            for task in dependants
        )

    statements: list[str] = []

    def capture_statement(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    async with database.session() as session, session.begin():
        await session.connection()
        event.listen(database.engine.sync_engine, "before_cursor_execute", capture_statement)
        try:
            changed = await TaskRepository.resolve_dependency_readiness(session, limit=20)
        finally:
            event.remove(database.engine.sync_engine, "before_cursor_execute", capture_statement)

    expected_ids = {task.id for task in dependants}
    assert set(changed) == expected_ids
    select_statements = [
        statement for statement in statements if statement.lstrip().upper().startswith("SELECT")
    ]
    assert len(select_statements) == 3
    assert "CURRENT_TIMESTAMP" in select_statements[2].upper()
    async with database.session() as session:
        promoted = set(
            await session.scalars(
                select(Task.id).where(
                    Task.id.in_(expected_ids),
                    Task.status == TaskStatus.QUEUED,
                )
            )
        )
        ready_events = int(
            await session.scalar(
                select(func.count(OutboxEvent.id)).where(
                    OutboxEvent.aggregate_id.in_(expected_ids),
                    OutboxEvent.event_type == "task.ready",
                )
            )
            or 0
        )
    assert promoted == expected_ids
    assert ready_events == len(expected_ids)
