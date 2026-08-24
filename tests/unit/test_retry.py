import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from core.enums import ErrorCategory, RetryBackoff
from repositories.tasks import retry_delay_seconds, should_retry_failure


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


@pytest.mark.parametrize(
    ("backoff", "expected"),
    [
        (RetryBackoff.FIXED, 2.0),
        (RetryBackoff.LINEAR, 6.0),
        (RetryBackoff.EXPONENTIAL, 8.0),
    ],
)
def test_retry_delay_supports_structured_backoff_modes(
    backoff: RetryBackoff, expected: float
) -> None:
    assert retry_delay_seconds(3, backoff=backoff, base_seconds=2.0) == expected


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    retry_count=st.integers(min_value=1, max_value=100),
    base_milliseconds=st.integers(min_value=1, max_value=100_000),
    maximum_milliseconds=st.integers(min_value=1, max_value=10_000_000),
    backoff=st.sampled_from(list(RetryBackoff)),
)
def test_retry_delay_is_positive_and_capped_for_all_valid_policies(
    retry_count: int,
    base_milliseconds: int,
    maximum_milliseconds: int,
    backoff: RetryBackoff,
) -> None:
    base_seconds = base_milliseconds / 1_000
    maximum = maximum_milliseconds / 1_000
    delay = retry_delay_seconds(
        retry_count,
        maximum,
        backoff=backoff,
        base_seconds=base_seconds,
    )

    assert 0 < delay <= maximum


@pytest.mark.parametrize(
    ("category", "exit_code", "expected"),
    [
        (ErrorCategory.INFRA_ERROR, None, True),
        (ErrorCategory.INTERNAL_ERROR, None, True),
        (ErrorCategory.TIMEOUT, None, True),
        (ErrorCategory.USER_ERROR, 1, True),
        (ErrorCategory.USER_ERROR, 2, False),
        (ErrorCategory.RESOURCE_ERROR, 137, True),
        (ErrorCategory.RESOURCE_ERROR, None, False),
        (ErrorCategory.CANCELLED, 1, False),
        (ErrorCategory.PREEMPTED, 1, False),
    ],
)
def test_retry_semantics_distinguish_failure_ownership(
    category: ErrorCategory, exit_code: int | None, expected: bool
) -> None:
    assert (
        should_retry_failure(
            error_category=category,
            exit_code=exit_code,
            retry_on_exit_codes=[1, 137],
        )
        is expected
    )
