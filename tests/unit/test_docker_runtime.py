import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from docker.errors import APIError, ImageNotFound

from core.enums import ErrorCategory, ErrorCode
from models.task import Task
from worker.docker_runtime import (
    DockerContainerStartFailed,
    DockerImagePullFailed,
    DockerOomKilled,
    DockerRuntime,
)
from worker.runtime import ComputeRuntime, ExecutionSpec, RuntimeMount


def _task(**overrides: Any) -> Task:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "image": "alpine:3.21",
        "command": ["sh", "-c", "echo safe"],
        "environment": {"EXAMPLE": "value"},
        "cpu_limit": 1.5,
        "memory_limit_mb": 256,
        "gpu_count": 0,
        "network_enabled": False,
    }
    values.update(overrides)
    return Task(**values)


def _spec(task: Task, *, execution_id: uuid.UUID | None = None) -> ExecutionSpec:
    return ExecutionSpec(
        task_id=task.id,
        execution_id=execution_id or uuid.uuid4(),
        worker_id="worker-test",
        image=task.image,
        command=tuple(task.command),
        environment=dict(task.environment),
        timeout_seconds=60,
        cpu_limit=task.cpu_limit,
        memory_limit_mb=task.memory_limit_mb,
        gpu_count=task.gpu_count,
        network_enabled=task.network_enabled,
        labels={},
        gpu_device_ids=tuple(task.gpu_device_ids or ()),
    )


def test_docker_runtime_implements_compute_runtime_protocol() -> None:
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=MagicMock(),
    )

    assert isinstance(runtime, ComputeRuntime)


@pytest.mark.asyncio
async def test_compute_runtime_lifecycle_delegates_to_docker_container() -> None:
    client = MagicMock()
    container = MagicMock()
    container.id = "container-full-id"
    container.short_id = "container-short"
    container.wait.return_value = {"StatusCode": 17}
    client.containers.create.return_value = container
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    task = _task()

    handle = await runtime.prepare(_spec(task))
    await runtime.start(handle)
    exit_code = await runtime.wait(handle)
    await runtime.stop(handle)
    await runtime.cleanup(handle)

    assert handle.runtime_type == "docker"
    assert handle.resource_kind == "container"
    assert handle.object_id == container.id
    assert handle.display_id == container.short_id
    assert exit_code == 17
    client.images.get.assert_called_once_with(task.image)
    container.start.assert_called_once_with()
    container.wait.assert_called_once_with()
    container.stop.assert_called_once_with(timeout=5)
    container.remove.assert_called_once_with(force=True, v=True)


@pytest.mark.asyncio
async def test_remove_container_treats_concurrent_removal_as_idempotent() -> None:
    client = MagicMock()
    container = MagicMock()
    response = MagicMock(status_code=409)
    container.remove.side_effect = APIError(
        "Conflict",
        response=response,
        explanation="removal of container abc is already in progress",
    )
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    await runtime.remove_container(container)

    container.remove.assert_called_once_with(force=True, v=True)


@pytest.mark.asyncio
async def test_pull_image_reuses_cache_by_default() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    await runtime.pull_image("alpine:3.21")

    client.images.get.assert_called_once_with("alpine:3.21")
    client.images.pull.assert_not_called()


@pytest.mark.asyncio
async def test_pull_image_fetches_missing_cache_entry() -> None:
    client = MagicMock()
    client.images.get.side_effect = ImageNotFound("missing")
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    await runtime.pull_image("alpine:3.21")

    client.images.pull.assert_called_once_with("alpine:3.21")


@pytest.mark.asyncio
async def test_pull_image_failure_has_stable_taxonomy() -> None:
    client = MagicMock()
    client.images.get.side_effect = ImageNotFound("missing")
    client.images.pull.side_effect = ImageNotFound("missing")
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    with pytest.raises(DockerImagePullFailed) as caught:
        await runtime.pull_image("missing.invalid/image:latest")

    assert caught.value.error_category == ErrorCategory.INFRA_ERROR
    assert caught.value.error_code == ErrorCode.IMAGE_PULL_FAILED


@pytest.mark.asyncio
async def test_wait_classifies_exit_137_as_oom_killed() -> None:
    client = MagicMock()
    container = MagicMock()
    container.wait.return_value = {"StatusCode": 137}
    container.attrs = {"State": {"OOMKilled": False}}
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    with pytest.raises(DockerOomKilled) as caught:
        await runtime.wait_container(container)

    assert caught.value.exit_code == 137
    assert caught.value.error_category == ErrorCategory.RESOURCE_ERROR
    assert caught.value.error_code == ErrorCode.OOM_KILLED


@pytest.mark.asyncio
async def test_create_container_enforces_security_and_resource_limits() -> None:
    client = MagicMock()
    expected_container = MagicMock()
    client.containers.create.return_value = expected_container
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    task = _task()
    execution_id = uuid.uuid4()

    container = await runtime.create_container(task, execution_id=execution_id)

    assert container is expected_container
    options = client.containers.create.call_args.kwargs
    assert options["image"] == task.image
    assert options["command"] == task.command
    assert options["environment"] == task.environment
    assert options["network_mode"] == "none"
    assert options["privileged"] is False
    assert options["read_only"] is True
    assert options["security_opt"] == ["no-new-privileges=true"]
    assert options["cap_drop"] == ["ALL"]
    assert options["pids_limit"] == 128
    assert options["mem_limit"] == "256m"
    assert options["nano_cpus"] == 1_500_000_000
    assert options["tmpfs"] == {"/tmp": "rw,noexec,nosuid,nodev,size=32m,mode=1777"}
    assert options["labels"] == {
        "mini-docker-cloud.task_id": str(task.id),
        "mini-docker-cloud.execution_id": str(execution_id),
        "mini-docker-cloud.managed": "true",
        "mini-docker-cloud.cluster_id": "mini-docker-cloud-local",
    }
    assert {"volumes", "devices", "pid_mode", "ipc_mode", "userns_mode"}.isdisjoint(options)
    assert "device_requests" not in options


@pytest.mark.asyncio
async def test_prepare_mounts_only_declared_artifact_files(tmp_path: Path) -> None:
    client = MagicMock()
    container = MagicMock(id="container-id", short_id="container")
    client.containers.create.return_value = container
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    source = tmp_path / "input.bin"
    output = tmp_path / "output.bin"
    source.write_bytes(b"input")
    output.touch()
    spec = replace(
        _spec(_task()),
        mounts=(
            RuntimeMount(str(source), "/workspace/inputs/input.bin", True),
            RuntimeMount(str(output), "/output/model.bin", False),
        ),
    )

    await runtime.prepare(spec)

    options = client.containers.create.call_args.kwargs
    assert options["read_only"] is True
    assert options["volumes"] == {
        str(source.resolve()): {
            "bind": "/workspace/inputs/input.bin",
            "mode": "ro",
        },
        str(output.resolve()): {"bind": "/output/model.bin", "mode": "rw"},
    }


@pytest.mark.asyncio
async def test_prepare_uses_single_file_volume_subpaths_for_sibling_containers(
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.containers.create.return_value = MagicMock(id="container-id", short_id="container")
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    source = tmp_path / "input.bin"
    source.write_bytes(b"input")
    spec = replace(
        _spec(_task()),
        mounts=(
            RuntimeMount(
                str(source),
                "/workspace/inputs/input.bin",
                True,
                volume_name="mini-cloud-workspaces",
                volume_subpath="task-123/inputs/input.bin",
            ),
        ),
    )

    await runtime.prepare(spec)

    options = client.containers.create.call_args.kwargs
    assert "volumes" not in options
    assert options["mounts"] == [
        {
            "Target": "/workspace/inputs/input.bin",
            "Source": "mini-cloud-workspaces",
            "Type": "volume",
            "ReadOnly": True,
            "VolumeOptions": {"Subpath": "task-123/inputs/input.bin"},
        }
    ]


@pytest.mark.asyncio
async def test_managed_container_scan_is_scoped_to_cluster() -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        cluster_id="cluster-a",
        client=client,
    )

    assert await runtime.list_worker_managed_containers() == []

    client.containers.list.assert_called_once_with(
        all=True,
        filters={
            "label": [
                "mini-docker-cloud.managed=true",
                "mini-docker-cloud.cluster_id=cluster-a",
                "mini-docker-cloud.worker_id",
            ]
        },
    )


@pytest.mark.asyncio
async def test_network_enabled_uses_bridge_never_host_network() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    await runtime.create_container(_task(network_enabled=True), execution_id=uuid.uuid4())

    network_mode = client.containers.create.call_args.kwargs["network_mode"]
    assert network_mode == "bridge"
    assert network_mode != "host"


@pytest.mark.asyncio
async def test_gpu_request_rejects_count_only_allocation() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    with pytest.raises(DockerContainerStartFailed, match="concrete scheduler-assigned"):
        await runtime.create_container(_task(gpu_count=2), execution_id=uuid.uuid4())

    client.containers.create.assert_not_called()


@pytest.mark.asyncio
async def test_gpu_request_prefers_concrete_scheduler_device_ids() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    spec = replace(
        _spec(_task(gpu_count=2)),
        gpu_device_ids=("GPU-aaaa", "GPU-bbbb"),
    )

    await runtime.prepare(spec)

    request = client.containers.create.call_args.kwargs["device_requests"][0]
    assert request["DeviceIDs"] == ["GPU-aaaa", "GPU-bbbb"]
    assert request["Count"] == 0
    assert request["Capabilities"] == [["gpu"]]


@pytest.mark.asyncio
async def test_gpu_request_rejects_count_and_concrete_device_mismatch() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )
    spec = replace(
        _spec(_task(gpu_count=2)),
        gpu_device_ids=("GPU-aaaa",),
    )

    with pytest.raises(DockerContainerStartFailed, match="allocation mismatch"):
        await runtime.prepare(spec)

    client.containers.create.assert_not_called()
