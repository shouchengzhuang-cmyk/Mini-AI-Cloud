import asyncio
import concurrent.futures
import threading
import uuid
from collections.abc import AsyncIterator
from pathlib import Path, PurePosixPath
from typing import Any, cast

from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from docker.types import DeviceRequest, Mount

import docker
from core.enums import ErrorCategory, ErrorCode
from models.task import Task
from worker.runtime import (
    ExecutionSpec,
    RuntimeFailure,
    RuntimeHandle,
    RuntimeLog,
)


class DockerRuntimeError(RuntimeFailure):
    """A Docker Engine operation failed in a user-facing way."""


class GpuRuntimeUnavailable(DockerRuntimeError):
    """The task requested GPUs but the NVIDIA container runtime is unavailable."""

    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.GPU_UNAVAILABLE,
        )


class DockerImagePullFailed(DockerRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.IMAGE_PULL_FAILED,
        )


class DockerContainerStartFailed(DockerRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.CONTAINER_START_FAILED,
        )


class DockerOomKilled(DockerRuntimeError):
    def __init__(self, message: str, *, exit_code: int = 137) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.OOM_KILLED,
            exit_code=exit_code,
        )


class DockerRuntime:
    runtime_type = "docker"

    def __init__(
        self,
        *,
        pids_limit: int,
        tmpfs_size_mb: int,
        stop_timeout: int,
        always_pull: bool = False,
        cluster_id: str = "mini-docker-cloud-local",
        client: Any | None = None,
    ) -> None:
        self.client: Any = client or docker.from_env()  # type: ignore[attr-defined]
        self.pids_limit = pids_limit
        self.tmpfs_size_mb = tmpfs_size_mb
        self.stop_timeout = stop_timeout
        self.always_pull = always_pull
        self.cluster_id = cluster_id

    async def version(self) -> str:
        try:
            details = await asyncio.to_thread(self.client.version)
        except DockerException as exc:
            raise DockerRuntimeError(f"Docker Engine is unavailable: {exc}") from exc
        return str(details.get("Version", "unknown"))

    async def pull_image(self, image: str) -> None:
        if not self.always_pull:
            try:
                await asyncio.to_thread(self.client.images.get, image)
                return
            except ImageNotFound:
                pass
            except (APIError, DockerException) as exc:
                raise DockerImagePullFailed(
                    f"failed to inspect Docker image {image}: {exc}"
                ) from exc
        try:
            await asyncio.to_thread(self.client.images.pull, image)
        except ImageNotFound as exc:
            raise DockerImagePullFailed(f"Docker image was not found: {image}") from exc
        except (APIError, DockerException) as exc:
            raise DockerImagePullFailed(f"failed to pull Docker image {image}: {exc}") from exc

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        """Pull and create a container for the generic ComputeRuntime lifecycle."""

        await self.pull_image(spec.image)
        container = await self._create_container(spec)
        return RuntimeHandle(
            runtime_type=self.runtime_type,
            resource_kind="container",
            object_id=str(container.id),
            display_id=str(container.short_id),
            native=container,
        )

    async def start(self, handle: RuntimeHandle) -> None:
        await self.start_container(self._container_from_handle(handle))

    async def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        async for item in self.stream_logs(self._container_from_handle(handle), ready=ready):
            yield item

    async def wait(self, handle: RuntimeHandle) -> int:
        return await self.wait_container(self._container_from_handle(handle))

    async def stop(self, handle: RuntimeHandle) -> None:
        await self.stop_container(self._container_from_handle(handle))

    async def cleanup(self, handle: RuntimeHandle) -> None:
        await self.remove_container(self._container_from_handle(handle))

    async def create_container(
        self,
        task: Task,
        *,
        execution_id: uuid.UUID,
        worker_id: str | None = None,
    ) -> Container:
        spec = ExecutionSpec(
            task_id=task.id,
            execution_id=execution_id,
            worker_id=worker_id or "",
            image=task.image,
            command=tuple(task.command),
            environment=dict(task.environment),
            timeout_seconds=int(task.timeout_seconds or 0),
            cpu_limit=task.cpu_limit,
            memory_limit_mb=task.memory_limit_mb,
            gpu_count=task.gpu_count,
            network_enabled=task.network_enabled,
            labels=dict(task.labels or {}),
            gpu_device_ids=tuple(task.gpu_device_ids or ()),
        )
        return await self._create_container(spec, include_worker_label=worker_id is not None)

    async def _create_container(
        self,
        spec: ExecutionSpec,
        *,
        include_worker_label: bool = True,
    ) -> Container:
        device_requests = _gpu_device_requests(spec)

        labels = {
            "mini-docker-cloud.task_id": str(spec.task_id),
            "mini-docker-cloud.execution_id": str(spec.execution_id),
            "mini-docker-cloud.managed": "true",
            "mini-docker-cloud.cluster_id": self.cluster_id,
        }
        if include_worker_label:
            labels["mini-docker-cloud.worker_id"] = spec.worker_id

        options: dict[str, Any] = {
            "image": spec.image,
            "command": list(spec.command),
            "environment": dict(spec.environment),
            "detach": True,
            "stdin_open": False,
            "tty": False,
            "network_mode": "bridge" if spec.network_enabled else "none",
            "read_only": True,
            "tmpfs": {"/tmp": (f"rw,noexec,nosuid,nodev,size={self.tmpfs_size_mb}m,mode=1777")},
            "mem_limit": f"{spec.memory_limit_mb}m",
            "nano_cpus": max(1, int(spec.cpu_limit * 1_000_000_000)),
            "pids_limit": self.pids_limit,
            "security_opt": ["no-new-privileges=true"],
            "cap_drop": ["ALL"],
            "privileged": False,
            "init": True,
            "labels": labels,
        }
        if device_requests is not None:
            options["device_requests"] = device_requests
        if spec.mounts:
            volumes: dict[str, dict[str, str]] = {}
            engine_mounts: list[Mount] = []
            container_paths: set[str] = set()
            for mount in spec.mounts:
                source = Path(mount.host_path)
                try:
                    resolved_source = await asyncio.to_thread(_resolved_mount_file, source)
                except (OSError, ValueError) as exc:
                    raise DockerContainerStartFailed(
                        f"artifact mount source is unavailable: {source}"
                    ) from exc
                target = PurePosixPath(mount.container_path)
                if (
                    not target.is_absolute()
                    or ".." in target.parts
                    or str(target) != mount.container_path
                    or mount.container_path in container_paths
                ):
                    raise DockerContainerStartFailed("artifact mount specification is invalid")
                container_paths.add(mount.container_path)
                if mount.volume_name is not None:
                    subpath = PurePosixPath(mount.volume_subpath or "")
                    if (
                        not mount.volume_name
                        or subpath.is_absolute()
                        or not subpath.parts
                        or any(part in {"", ".", ".."} for part in subpath.parts)
                        or subpath.as_posix() != mount.volume_subpath
                    ):
                        raise DockerContainerStartFailed(
                            "artifact volume mount specification is invalid"
                        )
                    engine_mounts.append(
                        Mount(
                            target=mount.container_path,
                            source=mount.volume_name,
                            type="volume",
                            read_only=mount.read_only,
                            subpath=mount.volume_subpath,
                        )
                    )
                    continue
                volumes[str(resolved_source)] = {
                    "bind": mount.container_path,
                    "mode": "ro" if mount.read_only else "rw",
                }
            if engine_mounts:
                engine_mounts.extend(
                    Mount(
                        target=value["bind"],
                        source=source,
                        type="bind",
                        read_only=value["mode"] == "ro",
                    )
                    for source, value in volumes.items()
                )
                options["mounts"] = engine_mounts
            else:
                options["volumes"] = volumes
        try:
            return await asyncio.to_thread(self.client.containers.create, **options)
        except APIError as exc:
            message = str(exc)
            if spec.gpu_count > 0 and ("gpu" in message.lower() or "nvidia" in message.lower()):
                raise GpuRuntimeUnavailable(
                    "GPU task requested, but the NVIDIA Docker runtime is unavailable"
                ) from exc
            raise DockerContainerStartFailed(f"failed to create task container: {message}") from exc
        except DockerException as exc:
            raise DockerContainerStartFailed(f"failed to create task container: {exc}") from exc

    def _container_from_handle(self, handle: RuntimeHandle) -> Container:
        if handle.runtime_type != self.runtime_type:
            raise DockerRuntimeError(
                f"cannot use {handle.runtime_type!r} handle with Docker runtime"
            )
        if handle.native is not None:
            return cast(Container, handle.native)
        try:
            return cast(Container, self.client.containers.get(handle.object_id))
        except NotFound as exc:
            raise DockerRuntimeError(
                f"Docker container no longer exists: {handle.object_id}"
            ) from exc
        except DockerException as exc:
            raise DockerRuntimeError(
                f"failed to inspect task container {handle.object_id}: {exc}"
            ) from exc

    async def start_container(self, container: Container) -> None:
        try:
            await asyncio.to_thread(container.start)
        except DockerException as exc:
            raise DockerContainerStartFailed(f"failed to start task container: {exc}") from exc

    async def stream_logs(
        self, container: Container, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[RuntimeLog]:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[RuntimeLog | BaseException | None] = asyncio.Queue(maxsize=256)
        stream_holder: dict[str, Any] = {}
        stop_requested = threading.Event()

        def put(item: RuntimeLog | BaseException | None) -> bool:
            future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
            while not stop_requested.is_set():
                try:
                    future.result(timeout=0.5)
                    return True
                except concurrent.futures.TimeoutError:
                    continue
                except (RuntimeError, concurrent.futures.CancelledError):
                    return False
            future.cancel()
            return False

        def consume() -> None:
            try:
                output = container.attach(stream=True, logs=True, demux=True)
                stream_holder["output"] = output
                if ready is not None:
                    loop.call_soon_threadsafe(ready.set)
                for item in output:
                    if isinstance(item, tuple):
                        stdout, stderr = item
                        if stdout and not put(RuntimeLog("stdout", stdout)):
                            break
                        if stderr and not put(RuntimeLog("stderr", stderr)):
                            break
                    elif item and not put(RuntimeLog("stdout", item)):
                        break
            except BaseException as exc:
                if ready is not None:
                    loop.call_soon_threadsafe(ready.set)
                put(exc)
            finally:
                put(None)

        consumer = asyncio.create_task(asyncio.to_thread(consume))
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                if isinstance(item, BaseException):
                    raise DockerRuntimeError(f"failed to stream container logs: {item}")
                yield item
        finally:
            stop_requested.set()
            output = stream_holder.get("output")
            close = getattr(output, "close", None)
            if callable(close):
                await asyncio.to_thread(close)
            if not consumer.done():
                consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

    async def wait_container(self, container: Container) -> int:
        try:
            result = await asyncio.to_thread(container.wait)
        except DockerException as exc:
            raise DockerRuntimeError(f"failed while waiting for task container: {exc}") from exc
        exit_code = int(result.get("StatusCode", -1))
        oom_killed = False
        try:
            await asyncio.to_thread(container.reload)
            attrs = getattr(container, "attrs", None)
            if isinstance(attrs, dict):
                state = attrs.get("State")
                if isinstance(state, dict):
                    oom_killed = state.get("OOMKilled") is True
        except (NotFound, DockerException):
            # The terminal status is still authoritative if inspection races cleanup.
            pass
        if oom_killed or exit_code == 137:
            raise DockerOomKilled(
                "Docker container was terminated by the out-of-memory killer",
                exit_code=exit_code,
            )
        return exit_code

    async def stop_container(self, container: Container) -> None:
        try:
            await asyncio.to_thread(container.stop, timeout=self.stop_timeout)
        except NotFound:
            return
        except DockerException as exc:
            raise DockerRuntimeError(f"failed to stop task container: {exc}") from exc

    async def remove_container(self, container: Container) -> None:
        try:
            await asyncio.to_thread(container.remove, force=True, v=True)
        except NotFound:
            return
        except DockerException as exc:
            if _container_removal_already_in_progress(exc):
                return
            raise DockerRuntimeError(f"failed to remove task container: {exc}") from exc

    async def list_worker_managed_containers(self) -> list[Container]:
        try:
            return await asyncio.to_thread(
                self.client.containers.list,
                all=True,
                filters={
                    "label": [
                        "mini-docker-cloud.managed=true",
                        f"mini-docker-cloud.cluster_id={self.cluster_id}",
                        "mini-docker-cloud.worker_id",
                    ]
                },
            )
        except DockerException as exc:
            raise DockerRuntimeError(f"failed to list managed containers: {exc}") from exc

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)


def _resolved_mount_file(path: Path) -> Path:
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("artifact mount source is not a regular file")
    return resolved


def _container_removal_already_in_progress(exc: DockerException) -> bool:
    if not isinstance(exc, APIError) or exc.status_code != 409:
        return False
    explanation = str(exc.explanation or exc).lower()
    return "removal of container" in explanation and "already in progress" in explanation


def _gpu_device_requests(spec: ExecutionSpec) -> list[DeviceRequest] | None:
    device_ids = tuple(device_id.strip() for device_id in spec.gpu_device_ids)
    if any(not device_id for device_id in device_ids):
        raise DockerContainerStartFailed("GPU device IDs must not be blank")
    if len(set(device_ids)) != len(device_ids):
        raise DockerContainerStartFailed("GPU device IDs must be unique")
    if device_ids:
        if spec.gpu_count != len(device_ids):
            raise DockerContainerStartFailed(
                "GPU allocation mismatch: gpu_count must equal the number of concrete device IDs"
            )
        return [DeviceRequest(device_ids=list(device_ids), capabilities=[["gpu"]])]
    if spec.gpu_count > 0:
        raise DockerContainerStartFailed("GPU task requires concrete scheduler-assigned device IDs")
    return None
