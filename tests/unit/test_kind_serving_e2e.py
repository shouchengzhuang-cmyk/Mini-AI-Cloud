from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from scripts.kind_serving_e2e import KindServingE2EError, _bounded_backoff_window


def test_bad_image_backoff_uses_server_timestamps() -> None:
    persisted_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    retry_at, retry_delay = _bounded_backoff_window(
        updated_at=persisted_at.isoformat(),
        retry_not_before=(persisted_at + timedelta(seconds=5)).isoformat(),
    )

    assert retry_at == persisted_at + timedelta(seconds=5)
    assert retry_delay == timedelta(seconds=5)


@pytest.mark.parametrize("delay_seconds", [0, 11])
def test_bad_image_backoff_rejects_unbounded_server_window(delay_seconds: int) -> None:
    persisted_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    with pytest.raises(KindServingE2EError, match="bounded"):
        _bounded_backoff_window(
            updated_at=persisted_at.isoformat(),
            retry_not_before=(persisted_at + timedelta(seconds=delay_seconds)).isoformat(),
        )
