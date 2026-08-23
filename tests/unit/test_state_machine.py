import pytest

from core.enums import TaskStatus
from core.state_machine import InvalidTaskTransition, can_transition, ensure_transition


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (TaskStatus.SUCCEEDED, TaskStatus.RUNNING),
        (TaskStatus.CANCELLED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.SUCCEEDED),
        (TaskStatus.PENDING, TaskStatus.RUNNING),
    ],
)
def test_illegal_task_transitions_are_rejected(current: TaskStatus, target: TaskStatus) -> None:
    assert can_transition(current, target) is False

    with pytest.raises(
        InvalidTaskTransition,
        match=f"illegal task transition: {current.value} -> {target.value}",
    ):
        ensure_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (TaskStatus.PENDING, TaskStatus.CANCELLED),
        (TaskStatus.PULLING, TaskStatus.FAILED),
        (TaskStatus.TIMED_OUT, TaskStatus.RETRYING),
        (TaskStatus.RETRYING, TaskStatus.QUEUED),
    ],
)
def test_required_cancellation_and_recovery_transitions_are_legal(
    current: TaskStatus, target: TaskStatus
) -> None:
    assert can_transition(current, target) is True
    ensure_transition(current, target)
