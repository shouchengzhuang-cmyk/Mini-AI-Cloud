import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from core.enums import FINAL_TASK_STATUSES, TaskStatus, WorkerStatus
from core.state_machine import can_transition
from models.usage import ProjectQuotaState
from repositories.dag import _contains_cycle
from repositories.quotas import QuotaInvariantViolation, _assert_state_nonnegative
from scheduler.policies import (
    RejectionReason,
    TaskSnapshot,
    WorkerSnapshot,
    choose_placement,
)


@given(st.lists(st.sampled_from(list(TaskStatus)), min_size=1, max_size=200))
def test_terminal_task_states_can_only_leave_through_retrying(
    random_targets: list[TaskStatus],
) -> None:
    current = TaskStatus.PENDING
    for target in random_targets:
        previous = current
        if can_transition(current, target):
            current = target
        if previous in FINAL_TASK_STATUSES and current not in FINAL_TASK_STATUSES:
            assert previous in {
                TaskStatus.FAILED,
                TaskStatus.TIMED_OUT,
                TaskStatus.PREEMPTED,
            }
            assert current == TaskStatus.RETRYING


@given(
    cpu_capacity=st.integers(min_value=1, max_value=128_000),
    memory_capacity=st.integers(min_value=1, max_value=1_000_000),
    cpu_reserved=st.integers(min_value=0, max_value=128_000),
    memory_reserved=st.integers(min_value=0, max_value=1_000_000),
    cpu_requested=st.integers(min_value=1, max_value=128_000),
    memory_requested=st.integers(min_value=1, max_value=1_000_000),
)
def test_scheduler_never_places_beyond_cpu_or_memory_capacity(
    cpu_capacity: int,
    memory_capacity: int,
    cpu_reserved: int,
    memory_reserved: int,
    cpu_requested: int,
    memory_requested: int,
) -> None:
    worker = WorkerSnapshot(
        id="worker-1",
        status=WorkerStatus.ONLINE,
        runtime_types=frozenset({"docker"}),
        running_tasks=0,
        concurrency=1,
        cpu_allocatable_millicores=cpu_capacity,
        reserved_cpu_millicores=cpu_reserved,
        memory_allocatable_mb=memory_capacity,
        reserved_memory_mb=memory_reserved,
    )
    task = TaskSnapshot(
        id="task-1",
        project_id="project-1",
        status=TaskStatus.QUEUED,
        runtime_type="docker",
        cpu_millicores=cpu_requested,
        memory_mb=memory_requested,
        gpu_count=0,
        queued_at=datetime.now(UTC),
    )

    placement, rejected = choose_placement(task, [worker])
    fits = (
        cpu_reserved + cpu_requested <= cpu_capacity
        and memory_reserved + memory_requested <= memory_capacity
        and cpu_reserved <= cpu_capacity
        and memory_reserved <= memory_capacity
    )
    if fits:
        assert placement is not None
        assert rejected == {}
    else:
        assert placement is None
        assert rejected[worker.id] in {
            RejectionReason.INSUFFICIENT_CPU,
            RejectionReason.INSUFFICIENT_MEMORY,
        }


@given(values=st.lists(st.integers(min_value=-3, max_value=20), min_size=12, max_size=12))
def test_quota_state_rejects_every_negative_resource_counter(values: list[int]) -> None:
    state = ProjectQuotaState(
        project_id=uuid.uuid4(),
        queued_tasks=values[0],
        running_tasks=values[1],
        reserved_cpu_millicores=values[2],
        reserved_memory_mb=values[3],
        reserved_gpus=values[4],
        service_count=values[5],
        service_replicas=values[6],
        service_reserved_cpu_millicores=values[7],
        service_reserved_memory_mb=values[8],
        service_reserved_gpus=values[9],
        artifact_bytes=values[10],
        daily_reserved_cost=Decimal(values[11]),
        daily_settled_cost=Decimal("0"),
    )
    if any(value < 0 for value in values):
        with pytest.raises(QuotaInvariantViolation):
            _assert_state_nonnegative(state)
    else:
        _assert_state_nonnegative(state)


@given(
    node_count=st.integers(min_value=1, max_value=12),
    raw_edges=st.sets(
        st.tuples(
            st.integers(min_value=0, max_value=11),
            st.integers(min_value=0, max_value=11),
        ),
        max_size=50,
    ),
)
def test_dag_cycle_detector_matches_depth_first_reference(
    node_count: int, raw_edges: set[tuple[int, int]]
) -> None:
    nodes = [uuid.UUID(int=index + 1) for index in range(node_count)]
    index_edges = {(source % node_count, target % node_count) for source, target in raw_edges}
    edges = {(nodes[source], nodes[target]) for source, target in index_edges}

    assert _contains_cycle(edges) is _reference_contains_cycle(index_edges, node_count)


def _reference_contains_cycle(edges: set[tuple[int, int]], node_count: int) -> bool:
    graph: dict[int, set[int]] = {node: set() for node in range(node_count)}
    for source, target in edges:
        graph[source].add(target)
    visiting: set[int] = set()
    visited: set[int] = set()

    def visit(node: int) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        if any(visit(neighbor) for neighbor in graph[node]):
            return True
        visiting.remove(node)
        visited.add(node)
        return False

    return any(visit(node) for node in graph)
