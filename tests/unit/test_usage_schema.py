from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from api.schemas.usage import ProjectQuotaUpdate, UsageWindow


def test_quota_schema_is_strict_and_preserves_decimal_cost_limits() -> None:
    quota = ProjectQuotaUpdate(
        max_running_tasks=4,
        max_cpu_millicores=8_000,
        daily_cost_limit="12.50000000",
    )

    assert quota.daily_cost_limit == Decimal("12.50000000")
    with pytest.raises(ValidationError):
        ProjectQuotaUpdate(max_running_tasks=True)
    with pytest.raises(ValidationError):
        ProjectQuotaUpdate(max_gpus=-1)


def test_usage_window_requires_timezone_order_and_bounded_range() -> None:
    now = datetime.now(UTC)
    assert UsageWindow(from_time=now, to_time=now + timedelta(hours=1))

    with pytest.raises(ValidationError, match="timezone"):
        UsageWindow(from_time=datetime.now(), to_time=datetime.now() + timedelta(hours=1))
    with pytest.raises(ValidationError, match="after"):
        UsageWindow(from_time=now, to_time=now)
    with pytest.raises(ValidationError, match="366"):
        UsageWindow(from_time=now, to_time=now + timedelta(days=367))
