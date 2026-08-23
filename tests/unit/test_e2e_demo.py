import asyncio
import importlib
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest


class _FakeResponse:
    def __init__(self, *, continuous_heartbeat: bool) -> None:
        self.continuous_heartbeat = continuous_heartbeat

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def aiter_lines(self) -> AsyncIterator[str]:
        if not self.continuous_heartbeat:
            await asyncio.Event().wait()
            return
        while True:
            yield ": ping"
            await asyncio.sleep(0)


class _FakeAsyncClient:
    continuous_heartbeat = False

    def __init__(self, **_kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def stream(self, *_args: object, **_kwargs: object) -> _FakeResponse:
        return _FakeResponse(continuous_heartbeat=self.continuous_heartbeat)


@pytest.mark.parametrize("continuous_heartbeat", [False, True])
async def test_follow_sse_enforces_overall_deadline(
    monkeypatch: pytest.MonkeyPatch, continuous_heartbeat: bool
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).parents[2] / "scripts"))
    e2e_demo = importlib.import_module("e2e_demo")
    _FakeAsyncClient.continuous_heartbeat = continuous_heartbeat
    monkeypatch.setattr(e2e_demo.httpx, "AsyncClient", _FakeAsyncClient)

    with pytest.raises(RuntimeError, match="overall E2E deadline"):
        await e2e_demo._follow_sse(
            "http://testserver",
            "task-deadline-test",
            deadline=time.monotonic() + 0.02,
            request_timeout=30,
        )
