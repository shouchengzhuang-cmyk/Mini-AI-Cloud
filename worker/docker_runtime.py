import asyncio
import concurrent.futures
import threading
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from docker.types import DeviceRequest

import docker
from models.task import Task


class DockerRuntimeError(RuntimeError):
    """A Docker Engine operation failed in a user-facing way."""


class GpuRuntimeUnavailable(DockerRuntimeError):
    """The task requested GPUs but the NVIDIA container runtime is unavailable."""


@dataclass(frozen=True, slots=True)
class RuntimeLog:
    stream: str
    content: bytes


class DockerRuntime:
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
                raise DockerRuntimeError(f"failed to inspect Docker image {image}: {exc}") from exc
        try:
            await asyncio.to_thread(self.client.images.pull, image)
        except ImageNotFound as exc:
            raise DockerRuntimeError(f"Docker image was not found: {image}") from exc
        except (APIError, DockerException) as exc:
            raise DockerRuntimeError(f"failed to pull Docker image {image}: {exc}") from exc

    async def create_container(
        self,
        task: Task,
        *,
        execution_id: uuid.UUID,
        worker_id: str | None = None,
    ) -> Container:
        device_requests: list[DeviceRequest] | None = None
        if task.gpu_count > 0:
            device_requests = [DeviceRequest(count=task.gpu_count, capabilities=[["gpu"]])]

        labels = {
            "mini-docker-cloud.task_id": str(task.id),
            "mini-docker-cloud.execution_id": str(execution_id),
            "mini-docker-cloud.managed": "true",
            "mini-docker-cloud.cluster_id": self.cluster_id,
        }
        if worker_id is not None:
            labels["mini-docker-cloud.worker_id"] = worker_id

        options: dict[str, Any] = {
            "image": task.image,
            "command": task.command,
            "environment": task.environment,
            "detach": True,
            "stdin_open": False,
            "tty": False,
            "network_mode": "bridge" if task.network_enabled else "none",
            "read_only": True,
            "tmpfs": {"/tmp": (f"rw,noexec,nosuid,nodev,size={self.tmpfs_size_mb}m,mode=1777")},
            "mem_limit": f"{task.memory_limit_mb}m",
            "nano_cpus": max(1, int(task.cpu_limit * 1_000_000_000)),
            "pids_limit": self.pids_limit,
            "security_opt": ["no-new-privileges=true"],
            "cap_drop": ["ALL"],
            "privileged": False,
            "init": True,
            "labels": labels,
        }
        if device_requests is not None:
            options["device_requests"] = device_requests
        try:
            return await asyncio.to_thread(self.client.containers.create, **options)
        except APIError as exc:
            message = str(exc)
            if task.gpu_count > 0 and ("gpu" in message.lower() or "nvidia" in message.lower()):
                raise GpuRuntimeUnavailable(
                    "GPU task requested, but the NVIDIA Docker runtime is unavailable"
                ) from exc
            raise DockerRuntimeError(f"failed to create task container: {message}") from exc
        except DockerException as exc:
            raise DockerRuntimeError(f"failed to create task container: {exc}") from exc

    async def start_container(self, container: Container) -> None:
        try:
            await asyncio.to_thread(container.start)
        except DockerException as exc:
            raise DockerRuntimeError(f"failed to start task container: {exc}") from exc

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
        return int(result.get("StatusCode", -1))

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
