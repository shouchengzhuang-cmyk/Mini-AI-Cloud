from sqlalchemy.dialects import postgresql

from repositories.scheduling import SchedulingRepository


def test_global_scheduler_filters_dependencies_in_one_locked_query() -> None:
    compiled = str(SchedulingRepository.candidate_query(128).compile(dialect=postgresql.dialect()))

    assert "NOT (EXISTS" in compiled
    assert "task_dependencies" in compiled
    assert "FOR UPDATE OF tasks SKIP LOCKED" in compiled
