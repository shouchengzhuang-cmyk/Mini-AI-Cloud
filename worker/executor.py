import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from core.config import Settings
from core.database import Database
from core.enums import ErrorCategory, ErrorCode, LogStream, TaskStatus
from core.logging import get_logger
from core.redis import RedisQueue
from core.secrets import SecretCipher
from models.task import Task
from repositories.secrets import (
    ResolvedTaskSecrets,
    SecretResolutionError,
    TaskSecretBindingRepository,
)
from repositories.tasks import ExecutionResult, StaleExecutionError, TaskRepository
from worker.artifact_workspace import ArtifactWorkspaceManager, PreparedArtifactWorkspace
from worker.heartbeat import ActiveExecution
from worker.redaction import StreamingSecretRedactor, redact_text
from worker.runtime import (
    ComputeRuntime,
    ExecutionSpec,
    RuntimeFailure,
    RuntimeHandle,
    RuntimeLog,
    RuntimeMount,
)


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
        runtime: ComputeRuntime,
        *,
        worker_id: str,
        settings: Settings,
        artifact_workspace: ArtifactWorkspaceManager | None = None,
        worker_session_id: uuid.UUID | None = None,
    ) -> None:
        self.database = database
        self.queue = queue
        self.runtime = runtime
        self.worker_id = worker_id
        self.settings = settings
        self.artifact_workspace = artifact_workspace
        self.worker_session_id = worker_session_id
        self.logger = get_logger("task_executor")

    async def execute(
        self,
        execution: ActiveExecution,
        *,
        recovered_handle: RuntimeHandle | None = None,
    ) -> ExecutionResult:
        handle: RuntimeHandle | None = recovered_handle
        log_task: asyncio.Task[None] | None = None
        resolved_secrets = ResolvedTaskSecrets({})
        secret_values: tuple[str, ...] = ()
        runtime_environment: dict[str, str] = {}
        prepared_artifacts: PreparedArtifactWorkspace | None = None
        runtime_handle_durable = recovered_handle is not None
        deadline = 0.0
        if runtime_handle_durable:
            execution.runtime_handle_durable.set()
        try:
            task = await self._mark_pulling(execution)
            resolved_secrets = await self._resolve_secrets(task, execution)
            secret_values = resolved_secrets.values
            collisions = set(task.environment).intersection(resolved_secrets.environment)
            if collisions:
                raise SecretResolutionError(
                    "task secret environment conflicts with task environment"
                )
            runtime_environment.update(task.environment)
            runtime_environment.update(resolved_secrets.environment)
            deadline = time.monotonic() + task.timeout_seconds
            if self.artifact_workspace is not None and task.project_id is not None:
                prepared_artifacts = await self._before_deadline(
                    self.artifact_workspace.prepare(
                        task_id=task.id,
                        project_id=task.project_id,
                        worker_id=self.worker_id,
                        execution_id=execution.execution_id,
                        worker_session_id=self.worker_session_id,
                    ),
                    deadline,
                )
            await self._system_log(execution, f"pulling image {task.image}")
            if handle is None:
                handle = await self._before_deadline(
                    self.runtime.prepare(
                        self._execution_spec(
                            task,
                            execution,
                            environment=runtime_environment,
                            mounts=(
                                tuple(
                                    RuntimeMount(
                                        host_path=str(mount.host_path),
                                        container_path=mount.container_path,
                                        read_only=mount.read_only,
                                        volume_name=mount.volume_name,
                                        volume_subpath=mount.volume_subpath,
                                    )
                                    for mount in prepared_artifacts.mounts
                                )
                                if prepared_artifacts is not None
                                else ()
                            ),
                        )
                    ),
                    deadline,
                )
            elif handle.runtime_type != task.runtime_type.value:
                raise StaleExecutionError(
                    "recovered runtime handle does not match the task runtime"
                )
            assert handle is not None
            await self._record_runtime_handle(execution, handle)
            if handle.runtime_type == "kubernetes":
                runtime_handle_durable = True
                execution.runtime_handle_durable.set()
            pre_start_stop = await self._pre_start_stop_reason(execution)
            if pre_start_stop == "relinquish":
                return ExecutionResult(accepted=False, status=None)
            if pre_start_stop == "ownership_lost":
                return ExecutionResult(accepted=False, status=None)
            if pre_start_stop == "cancelled":
                await self._best_effort_stop(
                    handle,
                    execution=execution,
                    secret_values=secret_values,
                )
                return await self._finish(
                    execution,
                    target=TaskStatus.CANCELLED,
                    exit_code=None,
                    error_message="task was stopped before runtime start",
                    error_category=ErrorCategory.CANCELLED,
                )
            await self._mark_starting(execution)
            log_ready = asyncio.Event()
            log_task = asyncio.create_task(
                self._collect_logs(
                    handle,
                    execution,
                    ready=log_ready,
                    secret_values=secret_values,
                )
            )
            await self._before_deadline(log_ready.wait(), deadline)
            if log_task.done():
                await log_task
            await self._record_runtime_handle(execution, handle)
            await self._before_deadline(self.runtime.start(handle), deadline)
            await self._mark_running(execution)
            await self._system_log(execution, f"{handle.resource_kind} {handle.display_id} started")
            outcome = await self._wait_for_outcome(handle, execution, deadline)
            if outcome.reason in {
                "cancelled",
                "timed_out",
                "ownership_lost",
                "log_limit_exceeded",
            }:
                await self._stop_owned(handle, execution)
            if outcome.reason == "relinquish":
                await self._preserve_kubernetes_handoff_lease(execution, task, handle)
            if outcome.reason in {"ownership_lost", "relinquish"}:
                self.logger.warning(
                    (
                        "runtime relinquished for worker restart"
                        if outcome.reason == "relinquish"
                        else "container stopped after fencing token was revoked"
                    ),
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
                error_category = ErrorCategory.CANCELLED
                error_code = None
            elif outcome.reason == "timed_out":
                target = TaskStatus.TIMED_OUT
                error = f"task exceeded timeout of {task.timeout_seconds} seconds"
                error_category = ErrorCategory.TIMEOUT
                error_code = None
            elif outcome.reason == "log_limit_exceeded":
                target = TaskStatus.FAILED
                error = (
                    "task log output exceeded the configured limit of "
                    f"{self.settings.max_task_log_bytes} bytes"
                )
                error_category = ErrorCategory.USER_ERROR
                error_code = None
            elif outcome.exit_code == 0:
                if self.artifact_workspace is not None and prepared_artifacts is not None:
                    published = await self._before_deadline(
                        self.artifact_workspace.publish_outputs(prepared_artifacts),
                        deadline,
                    )
                    if published:
                        await self._system_log(
                            execution,
                            f"published {len(published)} task artifact output(s)",
                        )
                target = TaskStatus.SUCCEEDED
                error = None
                error_category = None
                error_code = None
            elif outcome.exit_code == 137:
                target = TaskStatus.FAILED
                error = "container was terminated by the out-of-memory killer"
                error_category = ErrorCategory.RESOURCE_ERROR
                error_code = ErrorCode.OOM_KILLED
            else:
                target = TaskStatus.FAILED
                error = f"container exited with code {outcome.exit_code}"
                error_category = ErrorCategory.USER_ERROR
                error_code = None
            await self._system_log(
                execution,
                f"container exited: status={target.value} exit_code={outcome.exit_code}",
            )
            return await self._finish(
                execution,
                target=target,
                exit_code=outcome.exit_code,
                error_message=error,
                error_category=error_category,
                error_code=error_code,
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
        except RuntimeFailure as exc:
            if handle is not None:
                await self._best_effort_stop(
                    handle,
                    execution=execution,
                    secret_values=secret_values,
                )
            safe_error = redact_text(str(exc), secret_values)
            await self._best_effort_log(execution, f"runtime failed: {safe_error}")
            self.logger.error(
                "classified runtime failure",
                task_id=str(execution.task_id),
                worker_id=self.worker_id,
                execution_id=str(execution.execution_id),
                error_category=exc.error_category.value,
                error_code=exc.error_code.value if exc.error_code is not None else None,
                error=safe_error,
            )
            return await self._finish(
                execution,
                target=TaskStatus.FAILED,
                exit_code=exc.exit_code,
                error_message=safe_error,
                error_category=exc.error_category,
                error_code=exc.error_code,
            )
        except (ExecutionTimedOut, TimeoutError):
            if handle is not None:
                await self._best_effort_stop(
                    handle,
                    execution=execution,
                    secret_values=secret_values,
                )
            await self._best_effort_log(execution, "task timed out")
            return await self._finish(
                execution,
                target=TaskStatus.TIMED_OUT,
                exit_code=None,
                error_message="task timed out during runtime setup or execution",
                error_category=ErrorCategory.TIMEOUT,
            )
        except Exception as exc:
            if handle is not None:
                await self._best_effort_stop(
                    handle,
                    execution=execution,
                    secret_values=secret_values,
                )
            safe_error = redact_text(str(exc), secret_values)
            await self._best_effort_log(execution, f"execution failed: {safe_error}")
            # Tracebacks can render arbitrary exception arguments. Avoid attaching
            # one after secrets have been resolved; the sanitized error remains
            # correlated by task and fencing identifiers.
            self.logger.error(
                "task execution failed",
                task_id=str(execution.task_id),
                worker_id=self.worker_id,
                execution_id=str(execution.execution_id),
                error=safe_error,
            )
            return await self._finish(
                execution,
                target=TaskStatus.FAILED,
                exit_code=None,
                error_message=safe_error,
                error_category=ErrorCategory.INTERNAL_ERROR,
            )
        finally:
            if log_task is not None:
                if not log_task.done():
                    log_task.cancel()
                await asyncio.gather(log_task, return_exceptions=True)
            if handle is not None:
                try:
                    relinquishing = (
                        handle.runtime_type == "kubernetes"
                        and runtime_handle_durable
                        and execution.relinquish_requested.is_set()
                    )
                    if relinquishing:
                        self.logger.info(
                            "durable Kubernetes Job preserved for replacement worker adoption",
                            task_id=str(execution.task_id),
                            execution_id=str(execution.execution_id),
                            runtime_object_id=handle.object_id,
                        )
                    elif handle.runtime_type == "kubernetes" and not runtime_handle_durable:
                        # A newly prepared Job is suspended until its handle is
                        # durable. Exact Kubernetes UID/session/spec fences make
                        # this a safe compensation path even when the DB write
                        # failed before cleanup ownership could be established.
                        await self.runtime.cleanup(handle)
                    elif await self._runtime_cleanup_owned(handle, execution):
                        await self.runtime.cleanup(handle)
                    else:
                        self.logger.warning(
                            "runtime cleanup skipped after DB ownership fence changed",
                            task_id=str(execution.task_id),
                            execution_id=str(execution.execution_id),
                            runtime_object_id=handle.object_id,
                        )
                except Exception as exc:
                    self.logger.error(
                        "runtime cleanup failed",
                        task_id=str(execution.task_id),
                        runtime_type=handle.runtime_type,
                        runtime_object_id=handle.object_id,
                        error=redact_text(str(exc), secret_values),
                    )
            if (
                self.artifact_workspace is not None
                and prepared_artifacts is not None
                and not (
                    handle is not None
                    and handle.runtime_type == "kubernetes"
                    and runtime_handle_durable
                    and execution.relinquish_requested.is_set()
                )
            ):
                try:
                    await self.artifact_workspace.cleanup(prepared_artifacts)
                except Exception as exc:
                    self.logger.error(
                        "artifact workspace cleanup failed",
                        task_id=str(execution.task_id),
                        execution_id=str(execution.execution_id),
                        error=redact_text(str(exc), secret_values),
                    )
            resolved_secrets.clear()
            runtime_environment.clear()
            secret_values = ()

    async def _mark_pulling(self, execution: ActiveExecution) -> Task:
        async with self.database.session() as session, session.begin():
            return await TaskRepository.mark_pulling(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                lease_seconds=self.settings.task_lease_seconds,
                worker_session_id=self.worker_session_id,
            )

    async def _record_runtime_handle(
        self,
        execution: ActiveExecution,
        handle: RuntimeHandle,
    ) -> None:
        if handle.runtime_type != "kubernetes":
            return
        if self.worker_session_id is None or handle.controller_session_id != self.worker_session_id:
            raise StaleExecutionError("Kubernetes Job handle controller session is stale")
        try:
            runtime_worker_session_id = uuid.UUID(handle.labels["mini-ai-cloud/worker-session-id"])
        except (KeyError, ValueError) as exc:
            raise RuntimeError(
                "Kubernetes Job handle has an invalid creation worker session"
            ) from exc
        async with self.database.session() as session, session.begin():
            await TaskRepository.record_runtime_handle(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                runtime_type=handle.runtime_type,
                resource_kind=handle.resource_kind,
                resource_name=handle.object_id,
                namespace=handle.namespace,
                resource_uid=handle.resource_uid,
                runtime_worker_session_id=runtime_worker_session_id,
                spec_hash=handle.spec_hash,
                observed_pod_name=handle.observation.pod_name,
                observed_pod_uid=handle.observation.pod_uid,
                worker_session_id=self.worker_session_id,
            )

    async def _resolve_secrets(self, task: Task, execution: ActiveExecution) -> ResolvedTaskSecrets:
        cipher = (
            SecretCipher.from_settings(self.settings)
            if self.settings.secret_master_key.strip()
            else None
        )
        async with self.database.session() as session, session.begin():
            return await TaskSecretBindingRepository.resolve_for_execution(
                session,
                task_id=task.id,
                project_id=task.project_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                cipher=cipher,
                worker_session_id=self.worker_session_id,
            )

    async def _mark_running(self, execution: ActiveExecution) -> None:
        async with self.database.session() as session, session.begin():
            await TaskRepository.mark_running(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                lease_seconds=self.settings.task_lease_seconds,
                worker_session_id=self.worker_session_id,
            )

    async def _preserve_kubernetes_handoff_lease(
        self,
        execution: ActiveExecution,
        task: Task,
        handle: RuntimeHandle,
    ) -> None:
        if handle.runtime_type != "kubernetes" or self.worker_session_id is None:
            raise StaleExecutionError("only a fenced Kubernetes Job can be relinquished")
        # Kubernetes enforces the task timeout as Job.activeDeadlineSeconds. Keep
        # the database lease through that deadline and Pod termination grace so
        # lease recovery cannot start an overlapping retry.
        async with self.database.session() as session, session.begin():
            await TaskRepository.preserve_kubernetes_handoff_lease(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                worker_session_id=self.worker_session_id,
                cleanup_grace_seconds=self.settings.kubernetes_cleanup_grace_seconds,
            )

    async def _mark_starting(self, execution: ActiveExecution) -> None:
        async with self.database.session() as session, session.begin():
            await TaskRepository.mark_starting(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                lease_seconds=self.settings.task_lease_seconds,
                worker_session_id=self.worker_session_id,
            )

    async def _wait_for_outcome(
        self, handle: RuntimeHandle, execution: ActiveExecution, deadline: float
    ) -> WaitOutcome:
        wait_task = asyncio.create_task(self.runtime.wait(handle))
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
                # A blocking runtime wait exits after stop() runs in the caller.
                wait_task.cancel()
            await asyncio.gather(stop_task, wait_task, return_exceptions=True)

    async def _watch_for_stop(self, execution: ActiveExecution) -> str:
        while True:
            if execution.relinquish_requested.is_set():
                return "relinquish"
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
                    worker_session_id=self.worker_session_id,
                ):
                    return "cancelled"
            await asyncio.sleep(0.25)

    async def _pre_start_stop_reason(self, execution: ActiveExecution) -> str | None:
        if execution.relinquish_requested.is_set():
            return "relinquish"
        if execution.ownership_lost.is_set():
            return "ownership_lost"
        async with self.database.session() as session:
            try:
                if await TaskRepository.cancellation_requested(
                    session,
                    task_id=execution.task_id,
                    worker_id=self.worker_id,
                    execution_id=execution.execution_id,
                    worker_session_id=self.worker_session_id,
                ):
                    return "cancelled"
            except StaleExecutionError:
                return "ownership_lost"
        return None

    async def _collect_logs(
        self,
        handle: RuntimeHandle,
        execution: ActiveExecution,
        *,
        ready: asyncio.Event,
        secret_values: tuple[str, ...] = (),
    ) -> None:
        async def persist(stream: LogStream, content: str) -> None:
            await self._persist_log(execution, stream, content)

        coalescer = _LogCoalescer(
            max_record_bytes=self.settings.max_log_chunk_bytes,
            persist=persist,
        )
        redactors = {
            LogStream.STDOUT: StreamingSecretRedactor(secret_values),
            LogStream.STDERR: StreamingSecretRedactor(secret_values),
        }
        cursor_mode = handle.runtime_type == "kubernetes"
        latest_cursor = {
            LogStream.STDOUT: handle.observation.log_cursor_bytes,
            LogStream.STDERR: handle.observation.log_cursor_bytes,
        }
        saw_durable_cursor = False

        async def finish_redactors() -> None:
            for stream in (LogStream.STDOUT, LogStream.STDERR):
                tail = redactors[stream].finish()
                if (
                    cursor_mode
                    and saw_durable_cursor
                    and (tail or latest_cursor[stream] > handle.observation.log_cursor_bytes)
                ):
                    await self._persist_cursor_log(
                        handle,
                        execution,
                        stream,
                        tail,
                        cursor_bytes=latest_cursor[stream],
                    )
                elif tail:
                    await coalescer.add(stream, tail)

        total = handle.observation.log_cursor_bytes if cursor_mode else 0
        logs = self.runtime.logs(handle, ready=ready).__aiter__()

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
                remaining = max(0, self.settings.max_task_log_bytes - total)
                accepted = item.content[:remaining]
                if accepted:
                    redacted = redactors[stream].feed(accepted)
                    if item.cursor_bytes is not None:
                        if not cursor_mode:
                            raise RuntimeError("only Kubernetes logs may carry a durable cursor")
                        saw_durable_cursor = True
                        accepted_cursor = item.cursor_bytes - (len(item.content) - len(accepted))
                        if accepted_cursor < latest_cursor[stream]:
                            raise RuntimeError("Kubernetes log cursor moved backwards")
                        latest_cursor[stream] = accepted_cursor
                        safe_cursor = accepted_cursor - redactors[stream].pending_bytes
                        if redacted or safe_cursor > handle.observation.log_cursor_bytes:
                            await self._persist_cursor_log(
                                handle,
                                execution,
                                stream,
                                redacted,
                                cursor_bytes=safe_cursor,
                            )
                    elif cursor_mode and handle.observation.log_cursor_bytes:
                        raise RuntimeError("adopted Kubernetes logs require a durable cursor")
                    elif redacted:
                        await coalescer.add(stream, redacted)
                    total += len(accepted)
                    if not cursor_mode and coalescer.has_pending and flush_deadline is None:
                        flush_deadline = time.monotonic() + _LOG_COALESCE_FLUSH_INTERVAL_SECONDS
                    elif not cursor_mode and not coalescer.has_pending:
                        flush_deadline = None
                if len(accepted) < len(item.content) or total >= self.settings.max_task_log_bytes:
                    await finish_redactors()
                    if not cursor_mode:
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
            await finish_redactors()
            if not cursor_mode:
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
                worker_id=self.worker_id,
                worker_session_id=self.worker_session_id,
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

    async def _persist_cursor_log(
        self,
        handle: RuntimeHandle,
        execution: ActiveExecution,
        stream: LogStream,
        content: bytes,
        *,
        cursor_bytes: int,
    ) -> None:
        if (
            self.worker_session_id is None
            or handle.resource_uid is None
            or handle.spec_hash is None
        ):
            raise StaleExecutionError("Kubernetes log cursor is missing an execution fence")
        if cursor_bytes < handle.observation.log_cursor_bytes:
            raise StaleExecutionError("Kubernetes log cursor must be monotonic")
        chunks = [
            content[offset : offset + self.settings.max_log_chunk_bytes]
            for offset in range(0, len(content), self.settings.max_log_chunk_bytes)
        ]
        persisted_logs = []
        async with self.database.session() as session, session.begin():
            for chunk in chunks:
                persisted_logs.append(
                    await TaskRepository.append_log(
                        session,
                        task_id=execution.task_id,
                        execution_id=execution.execution_id,
                        stream=stream,
                        content=chunk.decode("utf-8", "replace"),
                        worker_id=self.worker_id,
                        worker_session_id=self.worker_session_id,
                    )
                )
            await TaskRepository.advance_runtime_log_cursor(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                cursor_bytes=cursor_bytes,
                resource_name=handle.object_id,
                resource_uid=handle.resource_uid,
                spec_hash=handle.spec_hash,
                worker_session_id=self.worker_session_id,
            )
        handle.observation.log_cursor_bytes = cursor_bytes
        for log in persisted_logs:
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

    async def _best_effort_stop(
        self,
        handle: RuntimeHandle,
        *,
        execution: ActiveExecution,
        secret_values: tuple[str, ...] = (),
    ) -> None:
        try:
            await self._stop_owned(handle, execution)
        except StaleExecutionError:
            return
        except Exception as exc:
            self.logger.error(
                "failed to stop runtime object",
                runtime_type=handle.runtime_type,
                runtime_object_id=handle.object_id,
                error=redact_text(str(exc), secret_values),
            )

    async def _stop_owned(
        self,
        handle: RuntimeHandle,
        execution: ActiveExecution,
    ) -> None:
        if not await self._runtime_cleanup_owned(handle, execution):
            raise StaleExecutionError(
                "runtime deletion skipped because controller ownership changed"
            )
        await self.runtime.stop(handle)

    async def _runtime_cleanup_owned(
        self,
        handle: RuntimeHandle,
        execution: ActiveExecution,
    ) -> bool:
        if handle.runtime_type != "kubernetes":
            return True
        if self.worker_session_id is None:
            return False
        async with self.database.session() as session:
            return await TaskRepository.runtime_cleanup_owned(
                session,
                task_id=execution.task_id,
                worker_id=self.worker_id,
                execution_id=execution.execution_id,
                worker_session_id=self.worker_session_id,
                runtime_type=handle.runtime_type,
                resource_name=handle.object_id,
                resource_uid=handle.resource_uid,
                spec_hash=handle.spec_hash,
            )

    def _execution_spec(
        self,
        task: Task,
        execution: ActiveExecution,
        *,
        environment: dict[str, str] | None = None,
        mounts: tuple[RuntimeMount, ...] = (),
    ) -> ExecutionSpec:
        return ExecutionSpec(
            task_id=task.id,
            execution_id=execution.execution_id,
            worker_id=self.worker_id,
            image=task.image,
            command=tuple(task.command),
            environment=dict(task.environment) if environment is None else environment,
            timeout_seconds=task.timeout_seconds,
            cpu_limit=task.cpu_limit,
            memory_limit_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            network_enabled=task.network_enabled,
            labels=dict(task.labels),
            project_id=getattr(task, "project_id", None),
            gpu_device_ids=tuple(getattr(task, "gpu_device_ids", None) or ()),
            runtime_type=task.runtime_type.value,
            selected_vendor=task.selected_vendor,
            selected_kind=task.selected_kind,
            selected_model=task.selected_model,
            runtime_profile_id=task.runtime_profile_id,
            runtime_profile_version=task.runtime_profile_version,
            runtime_profile_digest=task.runtime_profile_digest,
            model_variant_id=task.model_variant_id,
            allocation_authority=task.allocation_authority,
            mounts=mounts,
            worker_session_id=self.worker_session_id,
        )

    async def _finish(
        self,
        execution: ActiveExecution,
        *,
        target: TaskStatus,
        exit_code: int | None,
        error_message: str | None,
        error_category: ErrorCategory | None = None,
        error_code: ErrorCode | None = None,
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
                error_category=error_category,
                error_code=error_code,
                retry_max_backoff_seconds=self.settings.retry_max_backoff_seconds,
                cpu_price_per_hour=self.settings.cpu_price_per_hour,
                gpu_price_per_hour=self.settings.gpu_price_per_hour,
                memory_price_per_gb_hour=self.settings.memory_price_per_gb_hour,
                worker_session_id=self.worker_session_id,
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
