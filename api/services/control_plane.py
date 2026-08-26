import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from api.services.outbox import OutboxDispatcher
from api.services.reaper import Reaper
from core.config import Settings
from core.logging import get_logger
from core.metrics import SCHEDULER_FAILURES, SCHEDULER_LATENCY


@dataclass(frozen=True, slots=True)
class ControllerSpec:
    name: str
    operation: Callable[[], Awaitable[object]]
    interval: float
    startup: Callable[[], Awaitable[object]] | None = None


@dataclass(slots=True)
class ControllerState:
    runs: int = 0
    failures: int = 0
    last_started_at: datetime | None = None
    last_succeeded_at: datetime | None = None
    last_error: str | None = None


class ControlPlane:
    """Run independently fenced reconciliation controllers with observable state."""

    def __init__(
        self,
        dispatcher: OutboxDispatcher,
        reaper: Reaper,
        settings: Settings,
        *,
        controllers: Iterable[ControllerSpec] = (),
    ) -> None:
        self.dispatcher = dispatcher
        self.reaper = reaper
        self.settings = settings
        self.logger = get_logger("control_plane")
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        builtins = (
            ControllerSpec("outbox", dispatcher.dispatch_once, settings.outbox_poll_interval),
            ControllerSpec(
                "reaper", reaper.run_once, settings.reaper_interval, reaper.recover_startup
            ),
        )
        specs = (*builtins, *controllers)
        names = [spec.name for spec in specs]
        if len(names) != len(set(names)):
            raise ValueError("controller names must be unique")
        if any(spec.interval <= 0 for spec in specs):
            raise ValueError("controller intervals must be greater than zero")
        self._controllers = specs
        self.states = {spec.name: ControllerState() for spec in specs}

    async def start(self) -> None:
        self._stop.clear()
        for spec in self._controllers:
            if spec.startup is not None:
                await asyncio.wait_for(
                    spec.startup(), timeout=self.settings.control_operation_timeout
                )
        self._tasks = [
            asyncio.create_task(self._loop(spec), name=f"control:{spec.name}")
            for spec in self._controllers
        ]

    async def stop(self) -> None:
        self._stop.set()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=self.settings.control_shutdown_timeout,
                )
            except TimeoutError:
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    def snapshot(self) -> dict[str, dict[str, object]]:
        return {
            name: {
                "runs": state.runs,
                "failures": state.failures,
                "last_started_at": state.last_started_at,
                "last_succeeded_at": state.last_succeeded_at,
                "last_error": state.last_error,
            }
            for name, state in self.states.items()
        }

    async def _loop(self, spec: ControllerSpec) -> None:
        state = self.states[spec.name]
        while not self._stop.is_set():
            state.last_started_at = datetime.now(UTC)
            state.runs += 1
            started_at = time.monotonic()
            try:
                await asyncio.wait_for(
                    spec.operation(), timeout=self.settings.control_operation_timeout
                )
                state.last_succeeded_at = datetime.now(UTC)
                state.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.failures += 1
                state.last_error = str(exc)[:4096]
                if spec.name == "scheduler":
                    SCHEDULER_FAILURES.inc()
                self.logger.exception("control loop failed", loop=spec.name, error=state.last_error)
            finally:
                if spec.name == "scheduler":
                    SCHEDULER_LATENCY.observe(max(0.0, time.monotonic() - started_at))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=spec.interval)
            except TimeoutError:
                continue
