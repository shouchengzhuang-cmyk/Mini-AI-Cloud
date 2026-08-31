import asyncio
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from core.enums import ErrorCategory, ErrorCode


class RuntimeFailure(RuntimeError):
    """A classified runtime failure safe to persist on the fenced execution."""

    def __init__(
        self,
        message: str,
        *,
        error_category: ErrorCategory = ErrorCategory.INFRA_ERROR,
        error_code: ErrorCode | None = None,
        exit_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_category = error_category
        self.error_code = error_code
        self.exit_code = exit_code


class RuntimeImagePullFailed(RuntimeFailure):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.IMAGE_PULL_FAILED,
        )


class RuntimeContainerStartFailed(RuntimeFailure):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.CONTAINER_START_FAILED,
        )


class RuntimeGpuUnavailable(RuntimeFailure):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.GPU_UNAVAILABLE,
        )


class RuntimeOomKilled(RuntimeFailure):
    def __init__(self, message: str, *, exit_code: int = 137) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.OOM_KILLED,
            exit_code=exit_code,
        )


@dataclass(frozen=True, slots=True)
class RuntimeMount:
    """One runtime-neutral, file-scoped bind mount.

    Artifact workspaces generate the host path; user input only controls a
    separately validated container path. Keeping mounts file-scoped avoids
    exposing an entire Worker directory to an untrusted workload.
    """

    host_path: str
    container_path: str
    read_only: bool
    volume_name: str | None = None
    volume_subpath: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionSpec:
    """Runtime-neutral snapshot of one fenced task execution."""

    task_id: uuid.UUID
    execution_id: uuid.UUID
    worker_id: str
    image: str
    command: tuple[str, ...]
    environment: Mapping[str, str] = field(repr=False)
    timeout_seconds: int
    cpu_limit: float
    memory_limit_mb: int
    gpu_count: int
    network_enabled: bool
    labels: Mapping[str, str]
    project_id: uuid.UUID | None = None
    gpu_device_ids: tuple[str, ...] = ()
    runtime_type: str = "docker"
    selected_vendor: str | None = None
    selected_kind: str | None = None
    selected_model: str | None = None
    runtime_profile_id: str | None = None
    runtime_profile_version: str | None = None
    runtime_profile_digest: str | None = None
    model_variant_id: uuid.UUID | None = None
    allocation_authority: str | None = None
    kubernetes_node_name: str | None = None
    mounts: tuple[RuntimeMount, ...] = ()
    worker_session_id: uuid.UUID | None = None


@dataclass(slots=True)
class RuntimeObservation:
    """Mutable, non-identity observations attached to a frozen runtime handle."""

    pod_name: str | None = None
    pod_uid: str | None = None
    log_cursor_bytes: int = 0


@dataclass(frozen=True, slots=True)
class RuntimeHandle:
    """Opaque identity returned by a ComputeRuntime after preparation."""

    runtime_type: str
    resource_kind: str
    object_id: str
    display_id: str
    native: object | None = field(default=None, repr=False, compare=False)
    namespace: str | None = None
    resource_uid: str | None = None
    resource_version: str | None = None
    controller_session_id: uuid.UUID | None = None
    spec_hash: str | None = None
    labels: Mapping[str, str] = field(
        default_factory=dict,
        repr=False,
        compare=False,
        hash=False,
    )
    observation: RuntimeObservation = field(
        default_factory=RuntimeObservation,
        repr=False,
        compare=False,
        hash=False,
    )


@dataclass(frozen=True, slots=True)
class RuntimeLog:
    stream: str
    content: bytes
    cursor_bytes: int | None = None


@runtime_checkable
class ComputeRuntime(Protocol):
    """Backend-neutral lifecycle used by the task executor."""

    runtime_type: str

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle: ...

    async def start(self, handle: RuntimeHandle) -> None: ...

    def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]: ...

    async def wait(self, handle: RuntimeHandle) -> int: ...

    async def stop(self, handle: RuntimeHandle) -> None: ...

    async def cleanup(self, handle: RuntimeHandle) -> None: ...
