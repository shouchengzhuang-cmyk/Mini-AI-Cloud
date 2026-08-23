import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from docker.models.containers import Container

from core.config import Settings
from core.database import Database
from core.enums import LogStream, TaskStatus
from core.logging import get_logger
from core.redis import RedisQueue
from models.task import Task
from repositories.tasks import ExecutionResult, StaleExecutionError, TaskRepository
from worker.docker_runtime import DockerRuntime, RuntimeLog
from worker.heartbeat import ActiveExecution


class ExecutionTimedOut(TimeoutError):
    pass


T = TypeVar("T")

_LOG_COALESCE_FLUSH_BYTES = 16 * 1024
_LOG_COALESCE_MAX_BUFFERED_BYTES = 32 * 1024
_LOG_COALESCE_FLUSH_INTERVAL_SECONDS = 0.25


class _LogCoalescer:
    """Bound transaction volume while keeping stdout and stderr records separate."""

    def __init__(
        self,
        *,
        max_record_bytes: int,
        persist: Callable[[LogStream, str], Awaitable[None]],
    ) -> None:
        self._buffers = {
            LogStream.STDOUT: bytearray(),
            LogStream.STDERR: bytearray(),
        }
        self._pending_order: list[LogStream] = []
        self._flush_bytes = min(
            max_record_bytes,
            _LOG_COALESCE_FLUSH_BYTES,
            _LOG_COALESCE_MAX_BUFFERED_BYTES // len(self._buffers),
        )
        self._persist = persist

    @property
    def has_pending(self) -> bool:
        return any(self._buffers.values())

    async def add(self, stream: LogStream, content: bytes) -> None:
        buffer = self._buffers[stream]
        offset = 0
        while offset < len(content):
            if not buffer:
                self._pending_order.append(stream)
            size = min(self._flush_bytes - len(buffer), len(content) - offset)
            buffer.extend(content[offset : offset + size])
            offset += size
            if len(buffer) == self._flush_bytes:
                await self._flush(stream)

    async def flush_all(self) -> None:
        for stream in tuple(self._pending_order):
            await self._flush(stream)

    async def _flush(self, stream: LogStream) -> None:
        buffer = self._buffers[stream]
        if not buffer:
            return
        content = bytes(buffer)
        persist_task: asyncio.Future[None] = asyncio.ensure_future(
            self._persist(stream, content.decode("utf-8", "replace"))
        )
        try:
            await asyncio.shield(persist_task)
        except asyncio.CancelledError:
            # A drain-timeout cancellation must not interrupt the transaction
            # that makes an already-consumed log chunk durable.
            await persist_task
            buffer.clear()
            self._pending_order.remove(stream)
            raise
        buffer.clear()
        self._pending_order.remove(stream)


@dataclass(frozen=True, slots=True)
class WaitOutcome:
    reason: str
    exit_code: int | None


class TaskExecutor:
    def __init__(
        self,
        database: Database,
        queue: RedisQueue,
        runtime: DockerRuntime,
        *,
        worker_id: str,
        settings: Settings,
    ) -> None:
        self.database = database
        self.queue = queue
        self.runtime = runtime
        self.worker_id = worker_id
        self.settings = settings
        self.logger = get_logger("task_executor")

    async def execute(self, execution: ActiveExecution) -> ExecutionResult:
        container: Container | None = None
        log_task: asyncio.Task[None] | None = None
        deadline = 0.0
        try:
            task = await self._mark_pulling(execution)
            deadline = time.monotonic() + task.timeout_seconds
            await self._system_log(execution, f"pulling image {task.image}")
            await self._before_deadline(self.runtime.pull_image(task.image), deadline)
            await self._raise_if_cancelled(execution)
            container = await self._before_deadline(
                self.runtime.create_container(
                    task,
                    execution_id=execution.execution_id,
                    worker_id=self.worker_id,
                ),
                deadline,
            )
            assert container is not None
            await self._raise_if_cancelled(execution)
            await self._mark_running(execution)
            log_ready = asyncio.Event()
            log_task = asyncio.create_task(
                self._collect_logs(container, execution, ready=log_ready)
            )
            await self._before_deadline(log_ready.wait(), deadline)
            if log_task.done():
                await log_task
            await self._before_deadline(self.runtime.start_container(container), deadline)
            await self._system_log(execution, f"container {container.short_id} started")
            outcome = await self._wait_for_outcome(container, execution, deadline)
            if outcome.reason in {
                "cancelled",
                "timed_out",
                "ownership_lost",
                "log_limit_exceeded",
            }:
                await self.runtime.stop_container(container)
            if outcome.reason == "ownership_lost":
                self.logger.warning(
                    "container stopped after fencing token was revoked",
                    task_id=str(execution.task_id),
                    worker_id=self.worker_id,
                )
                return ExecutionResult(accepted=False, status=None)

            if log_task is not None:
                await self._drain_log_task(log_task, execution)
                log_task = None
            if outcome.reason == "cancelled":
                target = TaskStatus.CANCELLED
                error = "task was cancelled by user request"
            elif outcome.reason == "timed_out":
                target = TaskStatus.TIMED_OUT
                error = f"task exceeded timeout of {task.timeout_seconds} seconds"
            elif outcome.reason == "log_limit_exceeded":
                target = TaskStatus.FAILED
                error = (
                    "task log output exceeded the configured limit of "
                    f"{self.settings.max_task_log_bytes} bytes"
                )
            elif outcome.exit_code == 0:
                target = TaskStatus.SUCCEEDED
                error = None
            else:
                target = TaskStatus.FAILED
                error = f"container exited with code {outcome.exit_code}"
            await self._system_log(
                execution,
                f"container exited: status={target.value} exit_code={outcome.exit_code}",
            )
            return await self._finish(
                execution, target=target, exit_code=outcome.exit_code, error_message=error
            )
        except StaleExecutionError as exc:
            self.logger.warning(
                "stale execution discarded",
                task_id=str(execution.task_id),
                worker_id=self.worker_id,
                execution_id=str(execution.execution_id),
                error=str(exc),
            )
            return ExecutionResult(accepted=False, status=None)
        except (ExecutionTimedOut, TimeoutError):
            if container is not None:
                await self._best_effort_stop(container)
            await self._best_effort_log(execution, "task timed out")
            return await self._finish(
                execution,
                target=TaskStatus.TIMED_OUT,
                exit_code=None,
                error_message="task timed out during Docker setup or execution",
            )
        except Exception as exc:
            if container is not None:
                await self._best_effort_stop(container)
            await self._best_effort_log(execution, f"execution failed: {exc}")
            self.logger.exception(
                "task execution failed",
                task_id=str(execution.task_id),
                worker_id=self.worker_id,
                execution_id=str(execution.execution_id),
                error=str(exc),
            )
            return await self._finish(
                execution,
                target=TaskStatus.FAILED,
                exit_code=None,
                error_message=str(exc),
            )
        finally:
            if log_task is not None:
                if not log_task.done():
                    log_task.cancel()
                await asyncio.gather(log_task, return_exceptions=True)
            if container is not None:
                try:
                    await self.runtime.remove_container(container)
                except Exception as exc:
                    self.logger.error(
                        "container cleanup failed",
                        task_id=str(execution.task_id),
                        container_id=container.id,
                        error=str(exc),
                    )

    async def _mark_pulling(self, execution: ActiveExecution) -> Task:
        async with self.database.session() as session, session.begin():
            return await TaskRepository.mark_pulling(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                lease_seconds=self.settings.task_lease_seconds,
            )

    async def _mark_running(self, execution: ActiveExecution) -> None:
        async with self.database.session() as session, session.begin():
            await TaskRepository.mark_running(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                lease_seconds=self.settings.task_lease_seconds,
            )

    async def _wait_for_outcome(
        self, container: Container, execution: ActiveExecution, deadline: float
    ) -> WaitOutcome:
        wait_task = asyncio.create_task(self.runtime.wait_container(container))
        stop_task = asyncio.create_task(self._watch_for_stop(execution))
        remaining = max(0.0, deadline - time.monotonic())
        try:
            done, _pending = await asyncio.wait(
                {wait_task, stop_task}, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                return WaitOutcome("timed_out", None)
            if stop_task in done:
                return WaitOutcome(stop_task.result(), None)
            return WaitOutcome("exited", wait_task.result())
        finally:
            if not stop_task.done():
                stop_task.cancel()
            if not wait_task.done():
                # The Docker wait thread exits after stop_container runs in the caller.
                wait_task.cancel()
            await asyncio.gather(stop_task, wait_task, return_exceptions=True)

    async def _watch_for_stop(self, execution: ActiveExecution) -> str:
        while True:
            if execution.ownership_lost.is_set():
                return "ownership_lost"
            if execution.log_limit_exceeded.is_set():
                return "log_limit_exceeded"
            async with self.database.session() as session:
                if await TaskRepository.cancellation_requested(
                    session,
                    task_id=execution.task_id,
                    worker_id=self.worker_id,
                    execution_id=execution.execution_id,
                ):
                    return "cancelled"
            await asyncio.sleep(0.25)

    async def _raise_if_cancelled(self, execution: ActiveExecution) -> None:
        if execution.ownership_lost.is_set():
            raise StaleExecutionError("task ownership was revoked")
        async with self.database.session() as session:
            if await TaskRepository.cancellation_requested(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
            ):
                raise StaleExecutionError("task was cancelled before container start")

    async def _collect_logs(
        self,
        container: Container,
        execution: ActiveExecution,
        *,
        ready: asyncio.Event,
    ) -> None:
        async def persist(stream: LogStream, content: str) -> None:
            await self._persist_log(execution, stream, content)

        coalescer = _LogCoalescer(
            max_record_bytes=self.settings.max_log_chunk_bytes,
            persist=persist,
        )
        total = 0
        logs = self.runtime.stream_logs(container, ready=ready).__aiter__()

        async def next_log() -> RuntimeLog:
            return await anext(logs)

        pending_item: asyncio.Task[RuntimeLog] | None = None
        flush_deadline: float | None = None
        try:
            while True:
                if pending_item is None:
                    pending_item = asyncio.create_task(next_log())
                timeout = None
                if flush_deadline is not None:
                    timeout = max(0.0, flush_deadline - time.monotonic())
                done, _pending = await asyncio.wait(
                    {pending_item},
                    timeout=timeout,
                )
                if not done:
                    await coalescer.flush_all()
                    flush_deadline = None
                    continue
                try:
                    item = pending_item.result()
                except StopAsyncIteration:
                    pending_item = None
                    break
                pending_item = None
                stream = LogStream.STDOUT if item.stream == "stdout" else LogStream.STDERR
                remaining = self.settings.max_task_log_bytes - total
                accepted = item.content[:remaining]
                if accepted:
                    await coalescer.add(stream, accepted)
                    total += len(accepted)
                    if coalescer.has_pending and flush_deadline is None:
                        flush_deadline = time.monotonic() + _LOG_COALESCE_FLUSH_INTERVAL_SECONDS
                    elif not coalescer.has_pending:
                        flush_deadline = None
                if len(accepted) < len(item.content) or total >= self.settings.max_task_log_bytes:
                    await coalescer.flush_all()
                    await self._system_log(
                        execution,
                        "task log limit reached; stopping container to protect the control plane",
                    )
                    execution.log_limit_exceeded.set()
                    return
        finally:
            if pending_item is not None:
                if not pending_item.done():
                    pending_item.cancel()
                await asyncio.gather(pending_item, return_exceptions=True)
            close = getattr(logs, "aclose", None)
            if callable(close):
                await close()
            # Async-generator completion, executor cancellation, and drain timeout
            # all pass through here, so a sub-threshold tail is still durable.
            await coalescer.flush_all()

    async def _drain_log_task(
        self, log_task: asyncio.Task[None], execution: ActiveExecution
    ) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(log_task), timeout=self.settings.log_drain_timeout
            )
        except TimeoutError:
            self.logger.warning(
                "container log stream did not close before drain deadline",
                task_id=str(execution.task_id),
                execution_id=str(execution.execution_id),
            )
            log_task.cancel()
            await asyncio.gather(log_task, return_exceptions=True)

    async def _system_log(self, execution: ActiveExecution, content: str) -> None:
        await self._persist_log(execution, LogStream.SYSTEM, content)

    async def _persist_log(
        self, execution: ActiveExecution, stream: LogStream, content: str
    ) -> None:
        async with self.database.session() as session, session.begin():
            log = await TaskRepository.append_log(
                session,
                task_id=execution.task_id,
                execution_id=execution.execution_id,
                stream=stream,
                content=content,
            )
        try:
            await self.queue.publish_log(
                task_id=execution.task_id,
                sequence=log.sequence,
            )
        except Exception as exc:
            self.logger.warning(
                "Redis log wakeup failed; PostgreSQL log remains durable",
                task_id=str(execution.task_id),
                error=str(exc),
            )

    async def _best_effort_log(self, execution: ActiveExecution, content: str) -> None:
        try:
            await self._system_log(execution, content)
        except (StaleExecutionError, LookupError):
            return

    async def _best_effort_stop(self, container: Container) -> None:
        try:
            await self.runtime.stop_container(container)
        except Exception as exc:
            self.logger.error("failed to stop container", container_id=container.id, error=str(exc))

    async def _finish(
        self,
        execution: ActiveExecution,
        *,
        target: TaskStatus,
        exit_code: int | None,
        error_message: str | None,
    ) -> ExecutionResult:
        async with self.database.session() as session, session.begin():
            result = await TaskRepository.finish_execution(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                target=target,
                exit_code=exit_code,
                error_message=error_message,
                retry_max_backoff_seconds=self.settings.retry_max_backoff_seconds,
                cpu_price_per_hour=self.settings.cpu_price_per_hour,
                gpu_price_per_hour=self.settings.gpu_price_per_hour,
            )
        if result.accepted:
            try:
                await self.queue.delete_log_stream(execution.task_id)
            except Exception as exc:
                self.logger.warning(
                    "Redis log stream cleanup failed; TTL remains the fallback",
                    task_id=str(execution.task_id),
                    error=str(exc),
                )
        return result

    @staticmethod
    async def _before_deadline(awaitable: Awaitable[T], deadline: float) -> T:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ExecutionTimedOut
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except TimeoutError as exc:
            raise ExecutionTimedOut from exc
