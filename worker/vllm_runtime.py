from __future__ import annotations

import asyncio
import hashlib
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, cast

from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from docker.types import DeviceRequest

import docker

SERVICE_ID_LABEL = "mini-ai-cloud.service-id"
REPLICA_ID_LABEL = "mini-ai-cloud.replica-id"
PROJECT_ID_LABEL = "mini-ai-cloud.project-id"
EXECUTION_ID_LABEL = "mini-ai-cloud.execution-id"
GENERATION_LABEL = "mini-ai-cloud.generation"
MANAGED_LABEL = "mini-ai-cloud.managed"
CLUSTER_ID_LABEL = "mini-ai-cloud.cluster-id"
WORKER_ID_LABEL = "mini-ai-cloud.worker-id"
WORKER_SESSION_ID_LABEL = "mini-ai-cloud.worker-session-id"
RUNTIME_LABEL = "mini-ai-cloud.runtime"
GPU_DEVICE_IDS_LABEL = "mini-ai-cloud.gpu-device-ids"

_DOCKER_VOLUME_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$")

_RESERVED_EXTRA_ARGUMENTS = frozenset(
    {
        "--api-key",
        "--dtype",
        "--host",
        "--max-model-len",
        "--model",
        "--port",
        "--served-model-name",
        "--tensor-parallel-size",
    }
)


@dataclass(frozen=True, slots=True)
class VLLMLaunchRequest:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    project_id: uuid.UUID
    execution_id: uuid.UUID
    generation: int
    image: str
    model: str
    gpu_device_ids: tuple[str, ...]
    port: int = 8000
    tensor_parallel_size: int | None = None
    dtype: str = "auto"
    max_model_len: int | None = None
    extra_arguments: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class VLLMLaunchSpec:
    image: str
    model: str
    argv: tuple[str, ...]
    environment: Mapping[str, str]
    gpu_device_ids: tuple[str, ...]
    container_port: int
    labels: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class VLLMContainerHandle:
    object_id: str
    display_id: str
    endpoint_url: str | None = None
    labels: Mapping[str, str] = MappingProxyType({})
    native: object | None = None


@dataclass(frozen=True, slots=True)
class VLLMContainerState:
    status: str
    running: bool
    exit_code: int | None
    oom_killed: bool


class VLLMRuntimeError(RuntimeError):
    """A service replica container lifecycle operation failed."""


class VLLMReplicaRuntime(Protocol):
    """Small runtime boundary used by the service replica controller."""

    async def version(self) -> str: ...

    async def prepare(
        self,
        spec: VLLMLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
    ) -> VLLMContainerHandle: ...

    async def start(self, handle: VLLMContainerHandle) -> VLLMContainerHandle: ...

    async def inspect(self, handle: VLLMContainerHandle) -> VLLMContainerState: ...

    async def stop(self, handle: VLLMContainerHandle) -> None: ...

    async def cleanup(self, handle: VLLMContainerHandle) -> None: ...

    async def list_managed(self, *, worker_id: str) -> Sequence[VLLMContainerHandle]: ...

    async def close(self) -> None: ...


class DockerVLLMRuntimeAdapter:
    """Launch vLLM replicas on Docker with an exact, fenced device set.

    This is intentionally separate from the batch ``ComputeRuntime`` lifecycle:
    service containers expose a long-lived port and are observed rather than
    awaited to completion.  It still follows the same prepare/start/stop/cleanup
    shape and uses the same Docker security baseline.
    """

    def __init__(
        self,
        *,
        cluster_id: str,
        endpoint_host: str,
        publish_address: str = "127.0.0.1",
        cache_volume: str = "mini-ai-cloud-vllm-cache",
        pids_limit: int = 2048,
        tmpfs_size_mb: int = 512,
        stop_timeout: int = 10,
        always_pull: bool = False,
        client: Any | None = None,
    ) -> None:
        if not cluster_id.strip():
            raise ValueError("cluster_id must not be blank")
        self.endpoint_host = _validate_endpoint_host(endpoint_host)
        if not publish_address.strip() or "/" in publish_address or "://" in publish_address:
            raise ValueError("publish_address must be an IP address or hostname")
        if not _DOCKER_VOLUME_NAME.fullmatch(cache_volume.strip()):
            raise ValueError("cache_volume must be a safe Docker volume name")
        if pids_limit < 16 or tmpfs_size_mb < 1 or stop_timeout < 0:
            raise ValueError("Docker runtime limits are invalid")
        self.cluster_id = cluster_id.strip()
        self.publish_address = publish_address.strip()
        self.cache_volume = cache_volume.strip()
        self.pids_limit = pids_limit
        self.tmpfs_size_mb = tmpfs_size_mb
        self.stop_timeout = stop_timeout
        self.always_pull = always_pull
        self.client: Any = client or docker.from_env()  # type: ignore[attr-defined]

    async def version(self) -> str:
        try:
            details = await asyncio.to_thread(self.client.version)
        except DockerException as exc:
            raise VLLMRuntimeError(f"Docker Engine is unavailable: {exc}") from exc
        return str(details.get("Version", "unknown"))

    async def prepare(
        self,
        spec: VLLMLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        cpu_millicores: int,
        memory_mb: int,
    ) -> VLLMContainerHandle:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if cpu_millicores < 1 or memory_mb < 16:
            raise ValueError("service CPU and memory limits are invalid")
        # A GPU-backed launch must always carry exact scheduler-selected IDs.
        device_requests = (
            [DeviceRequest(device_ids=list(spec.gpu_device_ids), capabilities=[["gpu"]])]
            if spec.gpu_device_ids
            else None
        )
        await self._pull_image(spec.image)
        labels = {
            **dict(spec.labels),
            CLUSTER_ID_LABEL: self.cluster_id,
            WORKER_ID_LABEL: worker_id,
            WORKER_SESSION_ID_LABEL: str(worker_session_id),
            RUNTIME_LABEL: "vllm-docker",
            GPU_DEVICE_IDS_LABEL: ",".join(spec.gpu_device_ids),
        }
        replica_id = labels[REPLICA_ID_LABEL]
        execution_id = labels[EXECUTION_ID_LABEL]
        cache_volume = _service_cache_volume_name(self.cache_volume, labels)
        container_port = f"{spec.container_port}/tcp"
        environment = {
            **dict(spec.environment),
            "HF_HOME": "/var/cache/huggingface",
        }
        options: dict[str, Any] = {
            "image": spec.image,
            "command": list(spec.argv),
            "environment": environment,
            "detach": True,
            "stdin_open": False,
            "tty": False,
            "network_mode": "bridge",
            "ports": {container_port: (self.publish_address, None)},
            "read_only": True,
            "tmpfs": {"/tmp": (f"rw,noexec,nosuid,nodev,size={self.tmpfs_size_mb}m,mode=1777")},
            "volumes": {
                cache_volume: {
                    "bind": "/var/cache/huggingface",
                    "mode": "rw",
                }
            },
            "mem_limit": f"{memory_mb}m",
            "nano_cpus": max(1, cpu_millicores * 1_000_000),
            "pids_limit": self.pids_limit,
            "shm_size": f"{max(64, min(1024, memory_mb // 4))}m",
            "security_opt": ["no-new-privileges=true"],
            "cap_drop": ["ALL"],
            "privileged": False,
            "init": True,
            "labels": labels,
            "name": f"mini-ai-vllm-{replica_id[:12]}-{execution_id[:12]}",
        }
        if device_requests is not None:
            options["device_requests"] = device_requests
        try:
            container = await asyncio.to_thread(self.client.containers.create, **options)
        except (APIError, DockerException) as exc:
            raise VLLMRuntimeError(f"failed to create vLLM container: {exc}") from exc
        return self._handle(container)

    async def start(self, handle: VLLMContainerHandle) -> VLLMContainerHandle:
        container = self._container(handle)
        try:
            await asyncio.to_thread(container.start)
            await asyncio.to_thread(container.reload)
        except (APIError, DockerException) as exc:
            raise VLLMRuntimeError(f"failed to start vLLM container: {exc}") from exc
        attrs = getattr(container, "attrs", None)
        endpoint_url = _published_endpoint(
            attrs,
            endpoint_host=self.endpoint_host,
        )
        return VLLMContainerHandle(
            object_id=str(container.id),
            display_id=str(container.short_id),
            endpoint_url=endpoint_url,
            labels=handle.labels,
            native=container,
        )

    async def inspect(self, handle: VLLMContainerHandle) -> VLLMContainerState:
        container = self._container(handle)
        try:
            await asyncio.to_thread(container.reload)
        except NotFound:
            return VLLMContainerState(
                status="missing", running=False, exit_code=None, oom_killed=False
            )
        except DockerException as exc:
            raise VLLMRuntimeError(f"failed to inspect vLLM container: {exc}") from exc
        attrs = getattr(container, "attrs", None)
        state = attrs.get("State", {}) if isinstance(attrs, dict) else {}
        status = str(state.get("Status", getattr(container, "status", "unknown")))
        raw_exit_code = state.get("ExitCode")
        exit_code = raw_exit_code if isinstance(raw_exit_code, int) else None
        return VLLMContainerState(
            status=status,
            running=state.get("Running") is True or status in {"running", "restarting"},
            exit_code=exit_code,
            oom_killed=state.get("OOMKilled") is True,
        )

    async def stop(self, handle: VLLMContainerHandle) -> None:
        container = self._container(handle)
        try:
            await asyncio.to_thread(container.stop, timeout=self.stop_timeout)
        except NotFound:
            return
        except DockerException as exc:
            raise VLLMRuntimeError(f"failed to stop vLLM container: {exc}") from exc

    async def cleanup(self, handle: VLLMContainerHandle) -> None:
        container = self._container(handle)
        try:
            await asyncio.to_thread(container.remove, force=True, v=True)
        except NotFound:
            return
        except DockerException as exc:
            raise VLLMRuntimeError(f"failed to remove vLLM container: {exc}") from exc

    async def list_managed(self, *, worker_id: str) -> Sequence[VLLMContainerHandle]:
        try:
            containers = await asyncio.to_thread(
                self.client.containers.list,
                all=True,
                filters={
                    "label": [
                        f"{MANAGED_LABEL}=true",
                        f"{CLUSTER_ID_LABEL}={self.cluster_id}",
                        f"{WORKER_ID_LABEL}={worker_id}",
                        f"{RUNTIME_LABEL}=vllm-docker",
                    ]
                },
            )
        except DockerException as exc:
            raise VLLMRuntimeError(f"failed to list managed vLLM containers: {exc}") from exc
        return tuple(self._handle(container) for container in containers)

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)

    async def _pull_image(self, image: str) -> None:
        if not self.always_pull:
            try:
                await asyncio.to_thread(self.client.images.get, image)
                return
            except ImageNotFound:
                pass
            except (APIError, DockerException) as exc:
                raise VLLMRuntimeError(f"failed to inspect vLLM image {image}: {exc}") from exc
        try:
            await asyncio.to_thread(self.client.images.pull, image)
        except (ImageNotFound, APIError, DockerException) as exc:
            raise VLLMRuntimeError(f"failed to pull vLLM image {image}: {exc}") from exc

    def _container(self, handle: VLLMContainerHandle) -> Container:
        if handle.native is not None:
            return cast(Container, handle.native)
        try:
            return cast(Container, self.client.containers.get(handle.object_id))
        except (NotFound, DockerException) as exc:
            raise VLLMRuntimeError(f"vLLM container is unavailable: {handle.object_id}") from exc

    @staticmethod
    def _handle(container: Container) -> VLLMContainerHandle:
        raw_labels = getattr(container, "labels", None)
        labels = (
            MappingProxyType({str(key): str(value) for key, value in raw_labels.items()})
            if isinstance(raw_labels, dict)
            else MappingProxyType({})
        )
        return VLLMContainerHandle(
            object_id=str(container.id),
            display_id=str(container.short_id),
            labels=labels,
            native=container,
        )


def build_vllm_launch_spec(request: VLLMLaunchRequest) -> VLLMLaunchSpec:
    """Build a container-neutral vLLM OpenAI server launch specification."""

    image = request.image.strip()
    model = request.model.strip()
    dtype = request.dtype.strip()
    if not image or any(character.isspace() for character in image):
        raise ValueError("image must be a non-blank container image reference without whitespace")
    if not model:
        raise ValueError("model must not be blank")
    if request.generation < 1:
        raise ValueError("generation must be at least one")
    if request.port < 1 or request.port > 65535:
        raise ValueError("port must be between 1 and 65535")
    if not dtype:
        raise ValueError("dtype must not be blank")
    if request.max_model_len is not None and request.max_model_len < 1:
        raise ValueError("max_model_len must be at least one")

    devices = tuple(device.strip() for device in request.gpu_device_ids)
    if any(not device or "," in device for device in devices):
        raise ValueError("GPU device IDs must be non-blank and must not contain commas")
    if len(set(devices)) != len(devices):
        raise ValueError("GPU device IDs must be unique")

    tensor_parallel_size = request.tensor_parallel_size
    if tensor_parallel_size is None:
        tensor_parallel_size = max(1, len(devices))
    if tensor_parallel_size < 1:
        raise ValueError("tensor_parallel_size must be at least one")
    if devices and tensor_parallel_size > len(devices):
        raise ValueError("tensor_parallel_size must not exceed visible GPU device count")
    if not devices and tensor_parallel_size != 1:
        raise ValueError("CPU-visible launch specs require tensor_parallel_size=1")

    _validate_extra_arguments(request.extra_arguments)
    argv: tuple[str, ...] = (
        "python3",
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--host",
        "0.0.0.0",
        "--port",
        str(request.port),
        "--model",
        model,
        "--served-model-name",
        model,
        "--dtype",
        dtype,
        "--tensor-parallel-size",
        str(tensor_parallel_size),
    )
    if request.max_model_len is not None:
        argv += ("--max-model-len", str(request.max_model_len))
    argv += request.extra_arguments

    environment = MappingProxyType(
        {
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "HF_HUB_DISABLE_TELEMETRY": "1",
            "NVIDIA_VISIBLE_DEVICES": ",".join(devices) if devices else "void",
        }
    )
    labels = MappingProxyType(
        {
            SERVICE_ID_LABEL: str(request.service_id),
            REPLICA_ID_LABEL: str(request.replica_id),
            PROJECT_ID_LABEL: str(request.project_id),
            EXECUTION_ID_LABEL: str(request.execution_id),
            GENERATION_LABEL: str(request.generation),
            MANAGED_LABEL: "true",
        }
    )
    return VLLMLaunchSpec(
        image=image,
        model=model,
        argv=argv,
        environment=environment,
        gpu_device_ids=devices,
        container_port=request.port,
        labels=labels,
    )


def _service_cache_volume_name(base: str, labels: Mapping[str, str]) -> str:
    """Keep writable model caches inside one project/service trust boundary."""

    try:
        project_id = uuid.UUID(labels[PROJECT_ID_LABEL]).hex
        service_id = uuid.UUID(labels[SERVICE_ID_LABEL]).hex
    except (KeyError, ValueError) as exc:
        raise VLLMRuntimeError(
            "vLLM launch labels are missing a valid project or service ID"
        ) from exc
    suffix = f"-p{project_id}-s{service_id}"
    maximum_base_length = 255 - len(suffix)
    if len(base) > maximum_base_length:
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]
        maximum_prefix_length = maximum_base_length - len(digest) - 1
        base = f"{base[:maximum_prefix_length].rstrip('._-')}-{digest}"
    return f"{base}{suffix}"


def _validate_extra_arguments(arguments: tuple[str, ...]) -> None:
    for argument in arguments:
        if not argument or "\x00" in argument:
            raise ValueError("extra vLLM arguments must be non-empty and contain no NUL bytes")
        flag = argument.partition("=")[0]
        if flag in _RESERVED_EXTRA_ARGUMENTS:
            raise ValueError(f"extra vLLM arguments must not override {flag}")


def _validate_endpoint_host(value: str) -> str:
    host = value.strip().strip("[]")
    if not host or any(character.isspace() for character in host) or "/" in host or "://" in host:
        raise ValueError("endpoint_host must be an IP address or hostname without a port")
    return host


def _published_endpoint(attrs: object, *, endpoint_host: str) -> str:
    if not isinstance(attrs, dict):
        raise VLLMRuntimeError("Docker did not return container network settings")
    network_settings = attrs.get("NetworkSettings")
    ports = network_settings.get("Ports") if isinstance(network_settings, dict) else None
    if not isinstance(ports, dict):
        raise VLLMRuntimeError("Docker did not publish the vLLM container port")
    bindings = next(
        (
            value
            for key, value in ports.items()
            if isinstance(key, str) and key.endswith("/tcp") and isinstance(value, list)
        ),
        None,
    )
    if not bindings or not isinstance(bindings[0], dict):
        raise VLLMRuntimeError("Docker did not publish the vLLM container port")
    raw_port = bindings[0].get("HostPort")
    try:
        port = int(str(raw_port))
    except (TypeError, ValueError) as exc:
        raise VLLMRuntimeError("Docker returned an invalid vLLM host port") from exc
    if not 1 <= port <= 65535:
        raise VLLMRuntimeError("Docker returned an invalid vLLM host port")
    url_host = f"[{endpoint_host}]" if ":" in endpoint_host else endpoint_host
    return f"http://{url_host}:{port}"
