import asyncio
from collections.abc import AsyncIterator

from worker.runtime import ExecutionSpec, RuntimeHandle, RuntimeLog


class FakeComputeRuntime:
    """Deterministic non-production runtime for scheduler and control-plane tests."""

    runtime_type = "fake"

    def __init__(self, *, delay_seconds: float = 0.01) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        self.delay_seconds = delay_seconds
        self._stopped: set[str] = set()

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        return RuntimeHandle(
            runtime_type=self.runtime_type,
            resource_kind="fake-execution",
            object_id=str(spec.execution_id),
            display_id=str(spec.execution_id),
        )

    async def start(self, handle: RuntimeHandle) -> None:
        del handle

    async def logs(
        self, handle: RuntimeHandle, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[RuntimeLog]:
        if ready is not None:
            ready.set()
        yield RuntimeLog(stream="stdout", content=b"fake runtime execution\n")
        del handle

    async def wait(self, handle: RuntimeHandle) -> int:
        await asyncio.sleep(self.delay_seconds)
        return 143 if handle.object_id in self._stopped else 0

    async def stop(self, handle: RuntimeHandle) -> None:
        self._stopped.add(handle.object_id)

    async def cleanup(self, handle: RuntimeHandle) -> None:
        self._stopped.discard(handle.object_id)

    async def close(self) -> None:
        self._stopped.clear()
