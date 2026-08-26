import asyncio
from collections.abc import AsyncIterator, Mapping

from worker.runtime import ComputeRuntime, ExecutionSpec, RuntimeHandle, RuntimeLog


class RuntimeRegistry:
    """Dispatch lifecycle calls by an immutable runtime type."""

    runtime_type = "registry"

    def __init__(self, runtimes: Mapping[str, ComputeRuntime]) -> None:
        if not runtimes:
            raise ValueError("at least one compute runtime must be configured")
        self._runtimes = dict(runtimes)
        if any(key != runtime.runtime_type for key, runtime in self._runtimes.items()):
            raise ValueError("runtime registry keys must match runtime.runtime_type")

    @property
    def runtime_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._runtimes))

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        return await self._runtime(spec.runtime_type).prepare(spec)

    async def start(self, handle: RuntimeHandle) -> None:
        await self._runtime(handle.runtime_type).start(handle)

    def logs(
        self, handle: RuntimeHandle, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[RuntimeLog]:
        return self._runtime(handle.runtime_type).logs(handle, ready=ready)

    async def wait(self, handle: RuntimeHandle) -> int:
        return await self._runtime(handle.runtime_type).wait(handle)

    async def stop(self, handle: RuntimeHandle) -> None:
        await self._runtime(handle.runtime_type).stop(handle)

    async def cleanup(self, handle: RuntimeHandle) -> None:
        await self._runtime(handle.runtime_type).cleanup(handle)

    async def close(self) -> None:
        results = []
        for runtime in self._runtimes.values():
            close = getattr(runtime, "close", None)
            if close is not None:
                results.append(close())
        if results:
            await asyncio.gather(*results)

    def _runtime(self, runtime_type: str) -> ComputeRuntime:
        try:
            return self._runtimes[runtime_type]
        except KeyError as exc:
            raise ValueError(f"runtime is not configured: {runtime_type}") from exc
