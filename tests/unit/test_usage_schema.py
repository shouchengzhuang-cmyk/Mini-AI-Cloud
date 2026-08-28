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


def test_quota_schema_maps_legacy_gpu_limit_to_nvidia_only() -> None:
    legacy = ProjectQuotaUpdate(max_gpus=4)
    explicit = ProjectQuotaUpdate(
        max_gpus=9,
        max_nvidia_gpus=3,
        max_ascend_npus=2,
    )

    assert legacy.max_nvidia_gpus == 4
    assert legacy.max_ascend_npus == 0
    assert explicit.max_nvidia_gpus == 3
    assert explicit.max_ascend_npus == 2


def test_usage_window_requires_timezone_order_and_bounded_range() -> None:
    now = datetime.now(UTC)
    assert UsageWindow(from_time=now, to_time=now + timedelta(hours=1))

    with pytest.raises(ValidationError, match="timezone"):
        UsageWindow(from_time=datetime.now(), to_time=datetime.now() + timedelta(hours=1))
    with pytest.raises(ValidationError, match="after"):
        UsageWindow(from_time=now, to_time=now)
    with pytest.raises(ValidationError, match="366"):
        UsageWindow(from_time=now, to_time=now + timedelta(days=367))
