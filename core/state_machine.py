from collections.abc import Mapping

from core.enums import TaskStatus


class InvalidTaskTransition(ValueError):
    """Raised when a task attempts a transition outside the explicit state graph."""


ALLOWED_TRANSITIONS: Mapping[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELLED}),
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.SCHEDULING, TaskStatus.ASSIGNED, TaskStatus.CANCELLED}
    ),
    TaskStatus.SCHEDULING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.ASSIGNED, TaskStatus.CANCELLED}
    ),
    TaskStatus.ASSIGNED: frozenset(
        {
            TaskStatus.PREPARING,
            TaskStatus.PULLING,
            TaskStatus.STOPPING,
            TaskStatus.PREEMPTING,
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.PREPARING: frozenset(
        {
            TaskStatus.PULLING,
            TaskStatus.STARTING,
            TaskStatus.STOPPING,
            TaskStatus.PREEMPTING,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.PULLING: frozenset(
        {
            TaskStatus.STARTING,
            TaskStatus.RUNNING,
            TaskStatus.STOPPING,
            TaskStatus.PREEMPTING,
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.STARTING: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.STOPPING,
            TaskStatus.PREEMPTING,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.STOPPING,
            TaskStatus.PREEMPTING,
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.STOPPING: frozenset(
        {
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
        }
    ),
    TaskStatus.PREEMPTING: frozenset(
        {TaskStatus.PREEMPTED, TaskStatus.FAILED, TaskStatus.CANCELLED}
    ),
    TaskStatus.PREEMPTED: frozenset({TaskStatus.RETRYING}),
    TaskStatus.FAILED: frozenset({TaskStatus.RETRYING}),
    TaskStatus.TIMED_OUT: frozenset({TaskStatus.RETRYING}),
    TaskStatus.RETRYING: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELLED}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


def ensure_transition(current: TaskStatus, target: TaskStatus) -> None:
    if target not in ALLOWED_TRANSITIONS[current]:
        raise InvalidTaskTransition(f"illegal task transition: {current.value} -> {target.value}")


def can_transition(current: TaskStatus, target: TaskStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]
