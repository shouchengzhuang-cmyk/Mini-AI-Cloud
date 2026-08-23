import asyncio
from collections.abc import Awaitable, Callable

from api.services.outbox import OutboxDispatcher
from api.services.reaper import Reaper
from core.config import Settings
from core.logging import get_logger


class ControlPlane:
    def __init__(self, dispatcher: OutboxDispatcher, reaper: Reaper, settings: Settings) -> None:
        self.dispatcher = dispatcher
        self.reaper = reaper
        self.settings = settings
        self.logger = get_logger("control_plane")
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await self.reaper.recover_startup()
        self._tasks = [
            asyncio.create_task(
                self._loop(
                    "outbox", self.dispatcher.dispatch_once, self.settings.outbox_poll_interval
                )
            ),
            asyncio.create_task(
                self._loop("reaper", self.reaper.run_once, self.settings.reaper_interval)
            ),
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

    async def _loop(
        self,
        name: str,
        operation: Callable[[], Awaitable[object]],
        interval: float,
    ) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(operation(), timeout=self.settings.control_operation_timeout)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.exception("control loop failed", loop=name, error=str(exc))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                continue
