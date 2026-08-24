import uuid

import pytest
from pydantic import ValidationError

from api.schemas.dag import JobGroupCreate, TaskDependencyCreate
from repositories.dag import DependencyFailurePolicy


def test_dependency_schema_rejects_self_and_duplicate_edges() -> None:
    task_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="itself"):
        TaskDependencyCreate(task_id=task_id, depends_on_task_id=task_id)

    prerequisite = uuid.uuid4()
    edge = {
        "task_id": task_id,
        "depends_on_task_id": prerequisite,
        "failure_policy": "block",
    }
    with pytest.raises(ValidationError, match="unique"):
        JobGroupCreate(name="duplicate", dependencies=[edge, edge])


def test_dependency_failure_policy_is_strict() -> None:
    dependency = TaskDependencyCreate(
        task_id=uuid.uuid4(),
        depends_on_task_id=uuid.uuid4(),
        failure_policy=DependencyFailurePolicy.BLOCK,
    )
    assert dependency.failure_policy == DependencyFailurePolicy.BLOCK

    with pytest.raises(ValidationError):
        TaskDependencyCreate(
            task_id=uuid.uuid4(),
            depends_on_task_id=uuid.uuid4(),
            failure_policy="continue",
        )
