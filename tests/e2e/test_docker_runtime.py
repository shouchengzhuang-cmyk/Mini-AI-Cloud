import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from docker.models.containers import Container

from models.task import Task
from worker.docker_runtime import DockerRuntime, DockerRuntimeError, RuntimeLog
from worker.runtime import ExecutionSpec, RuntimeMount

pytestmark = [pytest.mark.e2e, pytest.mark.docker]

IMAGE = "alpine:3.21"
PIDS_LIMIT = 64
MEMORY_LIMIT_MB = 64
_image_ready = False


@dataclass(slots=True)
class DockerHarness:
    runtime: DockerRuntime
    containers: list[Container] = field(default_factory=list)
    task_ids: set[uuid.UUID] = field(default_factory=set)

    async def create(self, task: Task) -> Container:
        self.task_ids.add(task.id)
        container = await self.runtime.create_container(
            task,
            execution_id=uuid.uuid4(),
            worker_id="e2e-docker-runtime",
        )
        self.containers.append(container)
        return container

    async def cleanup(self) -> None:
        for container in reversed(self.containers):
            await self.runtime.remove_container(container)

        # Also remove a container if creation succeeded but the test was interrupted
        # before its handle was appended to ``containers``.
        for task_id in self.task_ids:
            leftovers = await asyncio.to_thread(
                self.runtime.client.containers.list,
                all=True,
                filters={"label": f"mini-ai-cloud.task_id={task_id}"},
            )
            for container in leftovers:
                await self.runtime.remove_container(container)


@pytest_asyncio.fixture
async def docker_harness() -> AsyncIterator[DockerHarness]:
    global _image_ready

    runtime = DockerRuntime(
        # Keep synthetic test containers outside any concurrently running local
        # stack's orphan-recovery domain.
        cluster_id=f"mini-ai-cloud-e2e-{uuid.uuid4().hex}",
        pids_limit=PIDS_LIMIT,
        tmpfs_size_mb=16,
        stop_timeout=1,
    )
    try:
        try:
            await runtime.version()
        except DockerRuntimeError as exc:
            pytest.skip(f"Docker Engine is unavailable: {exc}")
        if not _image_ready:
            await runtime.pull_image(IMAGE)
            _image_ready = True

        harness = DockerHarness(runtime)
        try:
            yield harness
        finally:
            await harness.cleanup()
    finally:
        await runtime.close()


def _task(
    command: list[str],
    *,
    environment: dict[str, str] | None = None,
    network_enabled: bool = False,
) -> Task:
    return Task(
        id=uuid.uuid4(),
        image=IMAGE,
        command=command,
        environment=environment or {},
        cpu_limit=0.5,
        memory_limit_mb=MEMORY_LIMIT_MB,
        gpu_count=0,
        network_enabled=network_enabled,
        labels={},
    )


async def _collect_logs(
    runtime: DockerRuntime,
    container: Container,
    *,
    ready: asyncio.Event,
) -> list[RuntimeLog]:
    return [item async for item in runtime.stream_logs(container, ready=ready)]


async def _start_with_log_stream(
    harness: DockerHarness, task: Task
) -> tuple[Container, asyncio.Task[list[RuntimeLog]]]:
    container = await harness.create(task)
    ready = asyncio.Event()
    log_task = asyncio.create_task(_collect_logs(harness.runtime, container, ready=ready))
    await asyncio.wait_for(ready.wait(), timeout=10)
    if log_task.done():
        await log_task
    await harness.runtime.start_container(container)
    return container, log_task


async def _run_and_collect(
    harness: DockerHarness, task: Task
) -> tuple[Container, int, list[RuntimeLog]]:
    container, log_task = await _start_with_log_stream(harness, task)
    exit_code = await asyncio.wait_for(harness.runtime.wait_container(container), timeout=20)
    logs = await asyncio.wait_for(log_task, timeout=10)
    return container, exit_code, logs


async def test_stdout_stderr_and_exit_zero(docker_harness: DockerHarness) -> None:
    _container, exit_code, logs = await _run_and_collect(
        docker_harness,
        _task(["sh", "-c", "echo stdout-line; echo stderr-line >&2"]),
    )

    stdout = b"".join(item.content for item in logs if item.stream == "stdout")
    stderr = b"".join(item.content for item in logs if item.stream == "stderr")
    assert exit_code == 0
    assert b"stdout-line" in stdout
    assert b"stderr-line" in stderr


async def test_nonzero_exit_code_is_preserved(docker_harness: DockerHarness) -> None:
    _container, exit_code, logs = await _run_and_collect(
        docker_harness, _task(["sh", "-c", "echo expected-failure >&2; exit 17"])
    )

    stderr = b"".join(item.content for item in logs if item.stream == "stderr")
    assert exit_code == 17
    assert b"expected-failure" in stderr


async def test_environment_reaches_container(docker_harness: DockerHarness) -> None:
    _container, exit_code, logs = await _run_and_collect(
        docker_harness,
        _task(
            ["sh", "-c", "printf '%s' \"$PLATFORM_TEST_VALUE\""],
            environment={"PLATFORM_TEST_VALUE": "environment-ok"},
        ),
    )

    stdout = b"".join(item.content for item in logs if item.stream == "stdout")
    assert exit_code == 0
    assert stdout == b"environment-ok"


async def test_zero_exit_code_is_preserved(docker_harness: DockerHarness) -> None:
    _container, exit_code, logs = await _run_and_collect(
        docker_harness,
        _task(["sh", "-c", "exit 0"]),
    )

    assert exit_code == 0
    assert logs == []


async def test_container_security_and_resource_configuration(
    docker_harness: DockerHarness,
) -> None:
    task = _task(["sh", "-c", "sleep 1"])
    container = await docker_harness.create(task)
    await asyncio.to_thread(container.reload)

    host_config: dict[str, Any] = container.attrs["HostConfig"]
    config: dict[str, Any] = container.attrs["Config"]
    assert host_config["NetworkMode"] == "none"
    assert host_config["ReadonlyRootfs"] is True
    assert set(host_config["CapDrop"]) == {"ALL"}
    assert host_config["Privileged"] is False
    assert host_config["PidsLimit"] == PIDS_LIMIT
    assert host_config["Memory"] == MEMORY_LIMIT_MB * 1024 * 1024
    assert host_config["NanoCpus"] == 500_000_000
    assert host_config["Binds"] is None
    security_options = set(host_config["SecurityOpt"])
    assert security_options & {"no-new-privileges:true", "no-new-privileges=true"}
    assert config["Labels"]["mini-ai-cloud.managed"] == "true"
    assert config["Labels"]["mini-ai-cloud.task_id"] == str(task.id)


async def test_named_volume_subpaths_isolate_task_artifact_files(
    docker_harness: DockerHarness,
    tmp_path: Path,
) -> None:
    volume_name = f"mini-ai-artifact-e2e-{uuid.uuid4().hex}"
    volume = await asyncio.to_thread(
        docker_harness.runtime.client.volumes.create,
        name=volume_name,
        labels={"mini-ai-cloud.e2e": "true"},
    )
    handle = None
    input_probe = tmp_path / "input.bin"
    output_probe = tmp_path / "output.bin"
    input_probe.write_bytes(b"volume-input")
    output_probe.touch()
    task = _task(
        [
            "sh",
            "-c",
            "cat /workspace/inputs/input.bin; printf volume-output > /output/result.bin",
        ]
    )
    spec = ExecutionSpec(
        task_id=task.id,
        execution_id=uuid.uuid4(),
        worker_id="e2e-volume-subpath",
        image=task.image,
        command=tuple(task.command),
        environment={},
        timeout_seconds=30,
        cpu_limit=task.cpu_limit,
        memory_limit_mb=task.memory_limit_mb,
        gpu_count=0,
        network_enabled=False,
        labels={},
        mounts=(
            RuntimeMount(
                str(input_probe),
                "/workspace/inputs/input.bin",
                True,
                volume_name=volume_name,
                volume_subpath="task/inputs/input.bin",
            ),
            RuntimeMount(
                str(output_probe),
                "/output/result.bin",
                False,
                volume_name=volume_name,
                volume_subpath="task/outputs/result.bin",
            ),
        ),
    )
    try:
        await asyncio.to_thread(
            docker_harness.runtime.client.containers.run,
            IMAGE,
            [
                "sh",
                "-c",
                (
                    "mkdir -p /data/task/inputs /data/task/outputs; "
                    "printf volume-input > /data/task/inputs/input.bin; "
                    ": > /data/task/outputs/result.bin; "
                    "chmod 0444 /data/task/inputs/input.bin; "
                    "chmod 0666 /data/task/outputs/result.bin"
                ),
            ],
            volumes={volume_name: {"bind": "/data", "mode": "rw"}},
            network_disabled=True,
            remove=True,
        )
        handle = await docker_harness.runtime.prepare(spec)
        ready = asyncio.Event()
        log_task = asyncio.create_task(
            _collect_logs(
                docker_harness.runtime,
                cast(Container, handle.native),
                ready=ready,
            )
        )
        await asyncio.wait_for(ready.wait(), timeout=10)
        await docker_harness.runtime.start(handle)
        exit_code = await asyncio.wait_for(docker_harness.runtime.wait(handle), timeout=20)
        logs = await asyncio.wait_for(log_task, timeout=10)
        stored_output = await asyncio.to_thread(
            docker_harness.runtime.client.containers.run,
            IMAGE,
            ["cat", "/data/task/outputs/result.bin"],
            volumes={volume_name: {"bind": "/data", "mode": "ro"}},
            network_disabled=True,
            remove=True,
        )

        assert exit_code == 0
        assert b"volume-input" in b"".join(item.content for item in logs)
        assert stored_output == b"volume-output"
    finally:
        if handle is not None:
            await docker_harness.runtime.cleanup(handle)
        await asyncio.to_thread(volume.remove, force=True)


async def test_timeout_stops_running_container(docker_harness: DockerHarness) -> None:
    container, log_task = await _start_with_log_stream(
        docker_harness,
        _task(["sh", "-c", "echo timeout-started; sleep 60"]),
    )
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(docker_harness.runtime.wait_container(container), timeout=0.25)

    await asyncio.wait_for(docker_harness.runtime.stop_container(container), timeout=10)
    exit_code = await asyncio.wait_for(docker_harness.runtime.wait_container(container), timeout=10)
    logs = await asyncio.wait_for(log_task, timeout=10)
    await asyncio.to_thread(container.reload)

    assert exit_code != 0
    assert container.status == "exited"
    assert b"timeout-started" in b"".join(item.content for item in logs)


async def test_cancel_via_stop_terminates_running_container(
    docker_harness: DockerHarness,
) -> None:
    container, log_task = await _start_with_log_stream(
        docker_harness,
        _task(["sh", "-c", "echo cancel-started; sleep 60"]),
    )
    await asyncio.to_thread(container.reload)
    assert container.status == "running"

    await asyncio.wait_for(docker_harness.runtime.stop_container(container), timeout=10)
    exit_code = await asyncio.wait_for(docker_harness.runtime.wait_container(container), timeout=10)
    logs = await asyncio.wait_for(log_task, timeout=10)
    await asyncio.to_thread(container.reload)

    assert exit_code != 0
    assert container.status == "exited"
    assert b"cancel-started" in b"".join(item.content for item in logs)
