from sqlalchemy.dialects import postgresql

from repositories.scheduling import SchedulingRepository


def _compile(query: object) -> str:
    return str(query.compile(dialect=postgresql.dialect()))


def test_global_scheduler_filters_dependencies_in_candidate_snapshot() -> None:
    compiled = _compile(SchedulingRepository.candidate_query(128))

    assert "NOT (EXISTS" in compiled
    assert "task_dependencies" in compiled
    assert "FOR UPDATE" not in compiled


def test_all_candidate_lanes_are_non_locking_snapshots() -> None:
    queries = (
        SchedulingRepository.candidate_query(128),
        SchedulingRepository.priority_candidate_query(128),
        SchedulingRepository.project_fair_candidate_query(
            128,
            cluster_cpu_millicores=8_000,
            cluster_memory_mb=16_384,
            cluster_gpus=8,
        ),
    )

    for query in queries:
        assert "FOR UPDATE" not in _compile(query)
