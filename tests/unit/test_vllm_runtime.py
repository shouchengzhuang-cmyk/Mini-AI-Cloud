import uuid
from dataclasses import replace
from typing import Any

import pytest

from worker.vllm_runtime import (
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    MANAGED_LABEL,
    PROJECT_ID_LABEL,
    REPLICA_ID_LABEL,
    SERVICE_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    DockerVLLMRuntimeAdapter,
    VLLMLaunchRequest,
    build_vllm_launch_spec,
)


def _request() -> VLLMLaunchRequest:
    return VLLMLaunchRequest(
        service_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        replica_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        project_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        execution_id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        generation=3,
        image="vllm/vllm-openai:pinned-test",
        model="org/model-v1",
        gpu_device_ids=("GPU-aaaa", "GPU-bbbb"),
        revision="revision-1",
        port=8000,
        gpu_memory_utilization=0.85,
        max_model_len=8192,
        extra_arguments=("--enable-prefix-caching",),
    )


def test_build_vllm_openai_server_spec_with_exact_gpu_visibility() -> None:
    request = _request()

    spec = build_vllm_launch_spec(request)

    assert spec.image == request.image
    assert spec.model == request.model
    assert spec.gpu_device_ids == request.gpu_device_ids
    assert spec.container_port == 8000
    assert spec.environment == {
        "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "NVIDIA_VISIBLE_DEVICES": "GPU-aaaa,GPU-bbbb",
    }
    assert spec.argv == (
        "python3",
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--model",
        "org/model-v1",
        "--served-model-name",
        "org/model-v1",
        "--dtype",
        "auto",
        "--tensor-parallel-size",
        "2",
        "--gpu-memory-utilization",
        "0.85",
        "--revision",
        "revision-1",
        "--max-model-len",
        "8192",
        "--enable-prefix-caching",
    )
    assert spec.labels == {
        SERVICE_ID_LABEL: str(request.service_id),
        REPLICA_ID_LABEL: str(request.replica_id),
        PROJECT_ID_LABEL: str(request.project_id),
        EXECUTION_ID_LABEL: str(request.execution_id),
        GENERATION_LABEL: "3",
        MANAGED_LABEL: "true",
    }


def test_build_vllm_cpu_visible_spec_uses_no_nvidia_devices() -> None:
    spec = build_vllm_launch_spec(replace(_request(), gpu_device_ids=()))

    assert spec.environment["NVIDIA_VISIBLE_DEVICES"] == "void"
    tensor_parallel_index = spec.argv.index("--tensor-parallel-size")
    assert spec.argv[tensor_parallel_index + 1] == "1"


@pytest.mark.parametrize(
    ("launch_request", "message"),
    [
        (replace(_request(), image=" "), "image"),
        (replace(_request(), model=" "), "model"),
        (replace(_request(), generation=0), "generation"),
        (replace(_request(), gpu_device_ids=("GPU-a", "GPU-a")), "unique"),
        (replace(_request(), gpu_device_ids=("GPU-a,b",)), "commas"),
        (replace(_request(), tensor_parallel_size=1), "visible GPU"),
        (replace(_request(), tensor_parallel_size=3), "visible GPU"),
        (replace(_request(), dtype="int8"), "dtype"),
        (replace(_request(), revision="bad revision"), "revision"),
        (replace(_request(), gpu_memory_utilization=0), "gpu_memory_utilization"),
        (replace(_request(), max_model_len=0), "max_model_len"),
        (replace(_request(), extra_arguments=("--host=example",)), "--host"),
        (replace(_request(), extra_arguments=("--dtype", "half")), "--dtype"),
        (replace(_request(), extra_arguments=("--api-key", "secret")), "--api-key"),
        (replace(_request(), extra_arguments=("--trust-remote-code",)), "--trust-remote-code"),
    ],
)
def test_build_vllm_spec_rejects_invalid_or_reserved_configuration(
    launch_request: VLLMLaunchRequest,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_vllm_launch_spec(launch_request)


class _Container:
    def __init__(self, labels: dict[str, str]) -> None:
        self.id = "container-id"
        self.short_id = "container"
        self.labels = labels
        self.status = "created"
        self.image = _Image()
        self.attrs: dict[str, Any] = {
            "State": {
                "Status": "created",
                "Running": False,
                "ExitCode": 0,
                "OOMKilled": False,
            },
            "NetworkSettings": {"Ports": {"8000/tcp": [{"HostPort": "32123"}]}},
        }
        self.stopped = False
        self.removed = False

    def start(self) -> None:
        self.status = "running"
        self.attrs["State"] = {
            "Status": "running",
            "Running": True,
            "ExitCode": 0,
            "OOMKilled": False,
        }

    def reload(self) -> None:
        return None

    def stop(self, *, timeout: int) -> None:
        assert timeout == 7
        self.stopped = True

    def remove(self, *, force: bool, v: bool) -> None:
        assert force and v
        self.removed = True


class _Image:
    def __init__(self) -> None:
        self.attrs = {"RepoDigests": [f"vllm/vllm-openai@sha256:{'a' * 64}"]}


class _Containers:
    def __init__(self) -> None:
        self.created_options: dict[str, Any] | None = None
        self.container: _Container | None = None
        self.list_filters: dict[str, Any] | None = None

    def create(self, **options: Any) -> _Container:
        self.created_options = options
        self.container = _Container(options["labels"])
        return self.container

    def get(self, _object_id: str) -> _Container:
        assert self.container is not None
        return self.container

    def list(self, *, all: bool, filters: dict[str, Any]) -> list[_Container]:
        assert all
        self.list_filters = filters
        return [self.container] if self.container is not None else []


class _Images:
    def __init__(self) -> None:
        self.inspected: list[str] = []

    def get(self, image: str) -> object:
        self.inspected.append(image)
        return object()


class _DockerClient:
    def __init__(self) -> None:
        self.containers = _Containers()
        self.images = _Images()
        self.closed = False

    def version(self) -> dict[str, str]:
        return {"Version": "test-engine"}

    def close(self) -> None:
        self.closed = True


async def test_docker_vllm_adapter_publishes_port_with_exact_gpu_ids() -> None:
    client = _DockerClient()
    adapter = DockerVLLMRuntimeAdapter(
        cluster_id="test-cluster",
        endpoint_host="10.0.0.20",
        cache_volume="test-vllm-cache",
        stop_timeout=7,
        client=client,
    )
    launch_spec = build_vllm_launch_spec(_request())
    worker_session_id = uuid.UUID("55555555-5555-5555-5555-555555555555")

    prepared = await adapter.prepare(
        launch_spec,
        worker_id="vllm-worker",
        worker_session_id=worker_session_id,
        cpu_millicores=2500,
        memory_mb=8192,
    )
    options = client.containers.created_options
    assert options is not None
    assert options["read_only"] is True
    assert options["privileged"] is False
    assert options["network_mode"] == "bridge"
    assert options["ports"] == {"8000/tcp": ("127.0.0.1", None)}
    assert options["nano_cpus"] == 2_500_000_000
    assert options["mem_limit"] == "8192m"
    assert options["volumes"] == {
        ("test-vllm-cache-p33333333333333333333333333333333-s11111111111111111111111111111111"): {
            "bind": "/var/cache/huggingface",
            "mode": "rw",
        }
    }
    request = options["device_requests"][0]
    assert request["DeviceIDs"] == ["GPU-aaaa", "GPU-bbbb"]
    assert "all" not in str(request).lower()
    assert options["environment"]["NVIDIA_VISIBLE_DEVICES"] == "GPU-aaaa,GPU-bbbb"
    assert options["labels"][WORKER_ID_LABEL] == "vllm-worker"
    assert options["labels"][WORKER_SESSION_ID_LABEL] == str(worker_session_id)

    running = await adapter.start(prepared)
    assert running.endpoint_url == "http://10.0.0.20:32123"
    assert running.image_digest == f"sha256:{'a' * 64}"
    state = await adapter.inspect(running)
    assert state.running is True
    assert state.status == "running"
    listed = await adapter.list_managed(worker_id="vllm-worker")
    assert len(listed) == 1
    assert listed[0].labels[WORKER_SESSION_ID_LABEL] == str(worker_session_id)
    await adapter.stop(running)
    await adapter.cleanup(running)
    assert client.containers.container is not None
    assert client.containers.container.stopped
    assert client.containers.container.removed
    await adapter.close()
    assert client.closed


async def test_docker_vllm_adapter_cpu_launch_never_requests_all_gpus() -> None:
    client = _DockerClient()
    adapter = DockerVLLMRuntimeAdapter(
        cluster_id="test-cluster",
        endpoint_host="127.0.0.1",
        client=client,
    )
    spec = build_vllm_launch_spec(replace(_request(), gpu_device_ids=()))

    await adapter.prepare(
        spec,
        worker_id="vllm-worker",
        worker_session_id=uuid.uuid4(),
        cpu_millicores=1000,
        memory_mb=1024,
    )

    assert client.containers.created_options is not None
    assert "device_requests" not in client.containers.created_options
    assert client.containers.created_options["environment"]["NVIDIA_VISIBLE_DEVICES"] == "void"


async def test_docker_vllm_adapter_isolates_writable_cache_by_project_and_service() -> None:
    first_client = _DockerClient()
    second_client = _DockerClient()
    first = DockerVLLMRuntimeAdapter(
        cluster_id="test-cluster",
        endpoint_host="127.0.0.1",
        cache_volume="shared-prefix",
        client=first_client,
    )
    second = DockerVLLMRuntimeAdapter(
        cluster_id="test-cluster",
        endpoint_host="127.0.0.1",
        cache_volume="shared-prefix",
        client=second_client,
    )
    other_request = replace(
        _request(),
        project_id=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        service_id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    )

    await first.prepare(
        build_vllm_launch_spec(_request()),
        worker_id="worker-a",
        worker_session_id=uuid.uuid4(),
        cpu_millicores=1000,
        memory_mb=1024,
    )
    await second.prepare(
        build_vllm_launch_spec(other_request),
        worker_id="worker-b",
        worker_session_id=uuid.uuid4(),
        cpu_millicores=1000,
        memory_mb=1024,
    )

    assert first_client.containers.created_options is not None
    assert second_client.containers.created_options is not None
    first_volume = next(iter(first_client.containers.created_options["volumes"]))
    second_volume = next(iter(second_client.containers.created_options["volumes"]))
    assert first_volume != second_volume
    assert "33333333333333333333333333333333" in first_volume
    assert "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in second_volume
