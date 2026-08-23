import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from docker.errors import ImageNotFound

from models.task import Task
from worker.docker_runtime import DockerRuntime


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
async def test_gpu_request_is_added_only_for_gpu_task() -> None:
    client = MagicMock()
    runtime = DockerRuntime(
        pids_limit=128,
        tmpfs_size_mb=32,
        stop_timeout=5,
        client=client,
    )

    await runtime.create_container(_task(gpu_count=2), execution_id=uuid.uuid4())

    request = client.containers.create.call_args.kwargs["device_requests"][0]
    assert request["Count"] == 2
    assert request["Capabilities"] == [["gpu"]]
