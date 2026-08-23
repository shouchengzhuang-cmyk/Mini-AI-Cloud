import pytest

from repositories.tasks import retry_delay_seconds


@pytest.mark.parametrize(
    ("retry_count", "expected"),
    [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0)],
)
def test_retry_delay_uses_one_based_exponential_backoff(retry_count: int, expected: float) -> None:
    assert retry_delay_seconds(retry_count) == expected


def test_retry_delay_is_capped() -> None:
    assert retry_delay_seconds(10, maximum=30.0) == 30.0


@pytest.mark.parametrize("retry_count", [0, -1])
def test_retry_delay_rejects_non_positive_retry_count(retry_count: int) -> None:
    with pytest.raises(ValueError, match="retry_count must be at least 1"):
        retry_delay_seconds(retry_count)
