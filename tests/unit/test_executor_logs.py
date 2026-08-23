import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest
from docker.models.containers import Container

from core.config import Settings
from core.database import Database
from core.enums import LogStream
from core.redis import RedisQueue
from worker.docker_runtime import DockerRuntime, RuntimeLog
from worker.executor import TaskExecutor
from worker.heartbeat import ActiveExecution


class _RuntimeStub:
    def __init__(self, items: list[RuntimeLog]) -> None:
        self.items = items

    async def stream_logs(
        self,
        container: Container,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        del container
        if ready is not None:
            ready.set()
        for item in self.items:
            yield item


class _BlockingRuntime:
    def __init__(self) -> None:
        self.fragment_consumed = asyncio.Event()

    async def stream_logs(
        self,
        container: Container,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        del container
        if ready is not None:
            ready.set()
        yield RuntimeLog("stdout", b"tail-before-cancel")
        self.fragment_consumed.set()
        await asyncio.Event().wait()


def _executor(
    runtime: _RuntimeStub | _BlockingRuntime,
    *,
    max_task_log_bytes: int = 64 * 1024,
    max_log_chunk_bytes: int = 64 * 1024,
) -> TaskExecutor:
    return TaskExecutor(
        cast(Database, object()),
        cast(RedisQueue, object()),
        cast(DockerRuntime, runtime),
        worker_id="worker-test",
        settings=Settings(
            control_plane_enabled=False,
            max_task_log_bytes=max_task_log_bytes,
            max_log_chunk_bytes=max_log_chunk_bytes,
        ),
    )


def _execution() -> ActiveExecution:
    return ActiveExecution(task_id=uuid.uuid4(), execution_id=uuid.uuid4())


def _capture_persisted(
    executor: TaskExecutor,
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[LogStream, str]]:
    records: list[tuple[LogStream, str]] = []

    async def persist(
        execution: ActiveExecution,
        stream: LogStream,
        content: str,
    ) -> None:
        del execution
        records.append((stream, content))

    monkeypatch.setattr(executor, "_persist_log", persist)
    return records


async def test_collect_logs_coalesces_one_byte_fragments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _RuntimeStub([RuntimeLog("stdout", b"x") for _ in range(20_000)])
    executor = _executor(runtime)
    execution = _execution()
    records = _capture_persisted(executor, monkeypatch)

    await executor._collect_logs(
        cast(Container, object()),
        execution,
        ready=asyncio.Event(),
    )

    output_records = [content for stream, content in records if stream == LogStream.STDOUT]
    assert len(output_records) == 2
    assert [len(content.encode()) for content in output_records] == [16 * 1024, 3616]
    assert "".join(output_records) == "x" * 20_000
    max_record_bytes = executor.settings.max_log_chunk_bytes
    assert all(len(content.encode()) <= max_record_bytes for content in output_records)


async def test_collect_logs_keeps_interleaved_streams_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items = [RuntimeLog("stderr", b"first-error")]
    items += [
        item
        for _ in range(500)
        for item in (RuntimeLog("stdout", b"o"), RuntimeLog("stderr", b"e"))
    ]
    executor = _executor(_RuntimeStub(items))
    records = _capture_persisted(executor, monkeypatch)

    await executor._collect_logs(
        cast(Container, object()),
        _execution(),
        ready=asyncio.Event(),
    )

    assert records == [
        (LogStream.STDERR, "first-error" + "e" * 500),
        (LogStream.STDOUT, "o" * 500),
    ]


async def test_collect_logs_flushes_tail_when_stream_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _executor(_RuntimeStub([RuntimeLog("stderr", b"short tail")]))
    records = _capture_persisted(executor, monkeypatch)

    await executor._collect_logs(
        cast(Container, object()),
        _execution(),
        ready=asyncio.Event(),
    )

    assert records == [(LogStream.STDERR, "short tail")]


async def test_collect_logs_flushes_small_fragment_while_stream_is_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _BlockingRuntime()
    executor = _executor(runtime)
    records: list[tuple[LogStream, str]] = []
    persisted = asyncio.Event()

    async def persist(
        execution: ActiveExecution,
        stream: LogStream,
        content: str,
    ) -> None:
        del execution
        records.append((stream, content))
        persisted.set()

    monkeypatch.setattr(executor, "_persist_log", persist)
    task = asyncio.create_task(
        executor._collect_logs(
            cast(Container, object()),
            _execution(),
            ready=asyncio.Event(),
        )
    )
    await runtime.fragment_consumed.wait()

    await asyncio.wait_for(persisted.wait(), timeout=1.0)

    assert task.done() is False
    assert records == [(LogStream.STDOUT, "tail-before-cancel")]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_collect_logs_flushes_tail_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _BlockingRuntime()
    executor = _executor(runtime)
    records = _capture_persisted(executor, monkeypatch)
    task = asyncio.create_task(
        executor._collect_logs(
            cast(Container, object()),
            _execution(),
            ready=asyncio.Event(),
        )
    )
    await runtime.fragment_consumed.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert records == [(LogStream.STDOUT, "tail-before-cancel")]


async def test_collect_logs_enforces_exact_total_byte_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _RuntimeStub(
        [
            RuntimeLog("stdout", b"a" * 700),
            RuntimeLog("stderr", b"b" * 700),
            RuntimeLog("stdout", b"ignored"),
        ]
    )
    executor = _executor(
        runtime,
        max_task_log_bytes=1024,
        max_log_chunk_bytes=1024,
    )
    execution = _execution()
    records = _capture_persisted(executor, monkeypatch)

    await executor._collect_logs(
        cast(Container, object()),
        execution,
        ready=asyncio.Event(),
    )

    output_records = [record for record in records if record[0] != LogStream.SYSTEM]
    assert output_records == [
        (LogStream.STDOUT, "a" * 700),
        (LogStream.STDERR, "b" * 324),
    ]
    assert sum(len(content.encode()) for _stream, content in output_records) == 1024
    assert records[-1] == (
        LogStream.SYSTEM,
        "task log limit reached; stopping container to protect the control plane",
    )
    assert execution.log_limit_exceeded.is_set()
