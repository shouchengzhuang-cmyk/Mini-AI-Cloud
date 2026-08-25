from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlsplit

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

SERVICE_ID_LABEL = "mini-ai-cloud/service-id"
REPLICA_ID_LABEL = "mini-ai-cloud/replica-id"
PROJECT_ID_LABEL = "mini-ai-cloud/project-id"
EXECUTION_ID_LABEL = "mini-ai-cloud/execution-id"
GENERATION_LABEL = "mini-ai-cloud/generation"
MANAGED_LABEL = "mini-ai-cloud/managed"
CLUSTER_ID_LABEL = "mini-ai-cloud/cluster-id"
WORKER_ID_LABEL = "mini-ai-cloud/worker-id"
WORKER_SESSION_ID_LABEL = "mini-ai-cloud/worker-session-id"
RUNTIME_LABEL = "mini-ai-cloud/runtime"
RESOURCE_KIND_LABEL = "mini-ai-cloud/resource-kind"
SPEC_HASH_LABEL = "mini-ai-cloud/spec-hash"

RUNTIME_LABEL_VALUE = "kubernetes-serving"
POD_RESOURCE_KIND = "serving-pod"
SERVICE_RESOURCE_KIND = "serving-service"
CONTAINER_NAME = "inference"
TMP_VOLUME_NAME = "tmp"

_DNS_1123_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_LABEL_VALUE = re.compile(r"^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_MAX_ERROR_LENGTH = 512
_MAX_RESOURCE_NAME_LENGTH = 128
_CONTRACT_LABEL_KEYS = (
    SERVICE_ID_LABEL,
    REPLICA_ID_LABEL,
    PROJECT_ID_LABEL,
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    MANAGED_LABEL,
    CLUSTER_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    RUNTIME_LABEL,
)


@dataclass(frozen=True, slots=True)
class KubernetesServingLaunchSpec:
    service_id: uuid.UUID
    replica_id: uuid.UUID
    project_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    image: str
    model: str
    cpu_millicores: int
    memory_mb: int
    startup_delay_seconds: float = 0.0
    chunk_delay_seconds: float = 0.0
    container_port: int = 8000


@dataclass(frozen=True, slots=True)
class KubernetesServingHandle:
    object_id: str
    display_id: str
    endpoint_url: str
    image_digest: str | None = None
    labels: Mapping[str, str] = MappingProxyType({})
    uid: str | None = None
    service_name: str | None = None
    service_uid: str | None = None
    native: object | None = None


@dataclass(frozen=True, slots=True)
class KubernetesServingState:
    phase: str
    running: bool
    ready: bool
    missing: bool
    deleting: bool
    exit_code: int | None
    oom_killed: bool
    reason: str | None
    message: str | None
    endpoint_url: str | None
    image_digest: str | None = None


@dataclass(frozen=True, slots=True)
class KubernetesServingOwnershipIdentity:
    """Canonical identity labels that are safe to correlate with a DB execution."""

    service_id: uuid.UUID
    replica_id: uuid.UUID
    project_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    cluster_id: str
    worker_id: str
    worker_session_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class KubernetesServingRecoveryConflict:
    """A single managed resource quarantined during restart discovery."""

    resource_kind: str
    resource_name: str
    reason: str
    message: str
    ownership: KubernetesServingOwnershipIdentity | None = None


class KubernetesServingRuntimeError(RuntimeError):
    """A Kubernetes serving resource lifecycle operation failed."""


class KubernetesServingOwnershipError(KubernetesServingRuntimeError):
    """A resource cannot be adopted or deleted because its fence does not match."""


@runtime_checkable
class KubernetesServingRuntime(Protocol):
    """Runtime boundary consumed by the Kubernetes service replica controller."""

    async def version(self) -> str: ...

    async def prepare(
        self,
        spec: KubernetesServingLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
    ) -> KubernetesServingHandle: ...

    async def start(self, handle: KubernetesServingHandle) -> KubernetesServingHandle: ...

    async def inspect(self, handle: KubernetesServingHandle) -> KubernetesServingState: ...

    async def request_stop(self, handle: KubernetesServingHandle) -> None: ...

    async def force_cleanup(self, handle: KubernetesServingHandle) -> None: ...

    async def list_managed(self, *, worker_id: str) -> Sequence[KubernetesServingHandle]: ...

    @property
    def recovery_conflicts(self) -> Sequence[KubernetesServingRecoveryConflict]: ...

    async def close(self) -> None: ...


class KubernetesServingRuntimeAdapter:
    """Manage one long-lived Pod and one exact-selector Service per replica.

    Kubernetes starts a Pod as part of creation, so ``prepare`` creates both
    resources and ``start`` is an ownership-checked observation barrier.  The
    adapter never treats controller shutdown as workload shutdown; ``close``
    only closes a client owned by this instance.
    """

    def __init__(
        self,
        *,
        namespace: str,
        cluster_id: str,
        kubeconfig: str | None = None,
        in_cluster: bool = False,
        termination_grace_seconds: int = 30,
        readiness_probe_timeout_seconds: float = 1.0,
        readiness_probe_period_seconds: float = 1.0,
        api: Any | None = None,
        version_api: Any | None = None,
    ) -> None:
        normalized_namespace = namespace.strip()
        normalized_cluster_id = cluster_id.strip()
        if not _DNS_1123_LABEL.fullmatch(normalized_namespace):
            raise ValueError("namespace must be a DNS-1123 label")
        if not _LABEL_VALUE.fullmatch(normalized_cluster_id):
            raise ValueError("cluster_id must be a Kubernetes label value")
        if termination_grace_seconds < 0 or termination_grace_seconds > 3600:
            raise ValueError("termination_grace_seconds must be between zero and 3600")
        if (
            isinstance(readiness_probe_timeout_seconds, bool)
            or isinstance(readiness_probe_period_seconds, bool)
            or not math.isfinite(readiness_probe_timeout_seconds)
            or not math.isfinite(readiness_probe_period_seconds)
            or readiness_probe_timeout_seconds <= 0
            or readiness_probe_period_seconds <= 0
            or readiness_probe_timeout_seconds > 3600
            or readiness_probe_period_seconds > 3600
        ):
            raise ValueError(
                "readiness probe intervals must be finite, greater than zero and at most 3600"
            )
        self.namespace = normalized_namespace
        self.cluster_id = normalized_cluster_id
        self.kubeconfig = kubeconfig
        self.in_cluster = in_cluster
        self.termination_grace_seconds = termination_grace_seconds
        self.readiness_probe_timeout_seconds = math.ceil(readiness_probe_timeout_seconds)
        self.readiness_probe_period_seconds = math.ceil(readiness_probe_period_seconds)
        self._api = api
        self._version_api = version_api
        self._owns_api = api is None
        self._client_lock = asyncio.Lock()
        self._recovery_conflicts: list[KubernetesServingRecoveryConflict] = []

    @property
    def recovery_conflicts(self) -> Sequence[KubernetesServingRecoveryConflict]:
        """Per-resource conflicts from the most recent managed-resource listing attempt."""

        return tuple(self._recovery_conflicts)

    async def version(self) -> str:
        version_api = await self._ensure_version_api()
        try:
            details = await version_api.get_code()
        except ApiException as exc:
            raise self._operation_error("read Kubernetes version", exc) from exc
        return str(getattr(details, "git_version", None) or "unknown")

    async def prepare(
        self,
        spec: KubernetesServingLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
    ) -> KubernetesServingHandle:
        self._validate_launch_spec(spec)
        normalized_worker_id = worker_id.strip()
        _validate_worker_id(normalized_worker_id)
        api = await self._ensure_api()
        selector = self._selector_labels(
            spec,
            worker_id=normalized_worker_id,
            worker_session_id=worker_session_id,
        )
        pod = self._build_pod(spec, selector)
        service = self._build_service(spec, selector)

        observed_pod = await self._create_or_adopt_pod(api, pod, spec, selector)
        observed_service = await self._create_or_adopt_service(api, service, spec, selector)
        return self._handle(observed_pod, service=observed_service)

    async def start(self, handle: KubernetesServingHandle) -> KubernetesServingHandle:
        api = await self._ensure_api()
        expected_labels = self._expected_handle_labels(handle)
        try:
            pod = await api.read_namespaced_pod(
                name=handle.object_id,
                namespace=self.namespace,
            )
            service = await api.read_namespaced_service(
                name=self._service_name(handle),
                namespace=self.namespace,
            )
        except ApiException as exc:
            raise self._operation_error("start Kubernetes serving replica", exc) from exc
        self._validate_observed_pod(pod, expected_labels=expected_labels)
        self._validate_observed_service(
            service,
            expected_selector=expected_labels,
            expected_port=_container_port(pod),
        )
        return self._handle(pod, service=service)

    async def inspect(self, handle: KubernetesServingHandle) -> KubernetesServingState:
        api = await self._ensure_api()
        expected_labels = self._expected_handle_labels(handle)
        try:
            pod = await api.read_namespaced_pod_status(
                name=handle.object_id,
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return KubernetesServingState(
                    phase="Missing",
                    running=False,
                    ready=False,
                    missing=True,
                    deleting=False,
                    exit_code=None,
                    oom_killed=False,
                    reason="NotFound",
                    message="Kubernetes serving Pod is missing",
                    endpoint_url=None,
                )
            raise self._operation_error("inspect Kubernetes serving Pod", exc) from exc
        self._validate_observed_pod(pod, expected_labels=expected_labels)

        service_missing = False
        service_deleting = False
        try:
            service = await api.read_namespaced_service(
                name=self._service_name(handle),
                namespace=self.namespace,
            )
            self._validate_observed_service(
                service,
                expected_selector=expected_labels,
                expected_port=_container_port(pod),
            )
            service_deleting = _deleting(service)
        except ApiException as exc:
            if exc.status != 404:
                raise self._operation_error("inspect Kubernetes serving Service", exc) from exc
            service_missing = True

        status = getattr(pod, "status", None)
        phase = str(getattr(status, "phase", None) or "Unknown")
        deleting = _deleting(pod) or service_deleting
        ready = _pod_ready(status) and not deleting and not service_missing
        exit_code, oom_killed, reason, message = _pod_failure(status)
        if service_missing and reason is None:
            reason = "ServiceMissing"
            message = "Kubernetes serving Service is missing"
        return KubernetesServingState(
            phase=phase,
            running=phase.lower() == "running" and not deleting,
            ready=ready,
            missing=False,
            deleting=deleting,
            exit_code=exit_code,
            oom_killed=oom_killed,
            reason=reason,
            message=_bounded(message),
            endpoint_url=handle.endpoint_url if not service_missing else None,
            image_digest=_pod_image_digest(status),
        )

    async def request_stop(self, handle: KubernetesServingHandle) -> None:
        await self._delete_resources(handle, grace_seconds=self.termination_grace_seconds)

    async def force_cleanup(self, handle: KubernetesServingHandle) -> None:
        await self._delete_resources(handle, grace_seconds=0)

    async def list_managed(self, *, worker_id: str) -> Sequence[KubernetesServingHandle]:
        normalized_worker_id = worker_id.strip()
        _validate_worker_id(normalized_worker_id)
        api = await self._ensure_api()
        self._recovery_conflicts = []
        selector = ",".join(
            (
                f"{MANAGED_LABEL}=true",
                f"{CLUSTER_ID_LABEL}={self.cluster_id}",
                f"{WORKER_ID_LABEL}={normalized_worker_id}",
                f"{RUNTIME_LABEL}={RUNTIME_LABEL_VALUE}",
                f"{RESOURCE_KIND_LABEL}={POD_RESOURCE_KIND}",
            )
        )
        try:
            pod_list = await api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=selector,
            )
            service_list = await api.list_namespaced_service(
                namespace=self.namespace,
                label_selector=",".join(
                    (
                        f"{MANAGED_LABEL}=true",
                        f"{CLUSTER_ID_LABEL}={self.cluster_id}",
                        f"{WORKER_ID_LABEL}={normalized_worker_id}",
                        f"{RUNTIME_LABEL}={RUNTIME_LABEL_VALUE}",
                        f"{RESOURCE_KIND_LABEL}={SERVICE_RESOURCE_KIND}",
                    )
                ),
            )
        except ApiException as exc:
            raise self._operation_error("list managed Kubernetes serving resources", exc) from exc

        services: dict[str, object] = {}
        for service in getattr(service_list, "items", None) or []:
            observed_service_name = _observed_resource_name(service)
            if not observed_service_name:
                self._record_recovery_conflict(
                    resource_kind="service",
                    resource=service,
                    worker_id=normalized_worker_id,
                    error=KubernetesServingOwnershipError(
                        "managed Kubernetes serving Service has no name"
                    ),
                )
                continue
            services[observed_service_name] = service

        handles: list[KubernetesServingHandle] = []
        matched_services: set[str] = set()
        observed_pod_names = {
            name
            for pod in (getattr(pod_list, "items", None) or [])
            if (name := _observed_resource_name(pod))
        }
        for pod in getattr(pod_list, "items", None) or []:
            service_name: str | None = None
            try:
                labels = self._managed_pod_labels(pod)
                service_name = _service_name_from_labels(labels)
                service = services.get(service_name)
                if service is not None:
                    # Mark the pair before validation so a drifted endpoint is
                    # quarantined with its Pod instead of being returned later
                    # as a deletable Service-only orphan.
                    matched_services.add(service_name)
                    try:
                        self._validate_observed_service(
                            service,
                            expected_selector=_selector_from_resource_labels(labels),
                            expected_port=_container_port(pod),
                        )
                    except KubernetesServingOwnershipError as exc:
                        self._record_recovery_conflict(
                            resource_kind="service",
                            resource=service,
                            worker_id=normalized_worker_id,
                            error=exc,
                        )
                        continue
                handles.append(self._handle(pod, service=service))
            except KubernetesServingOwnershipError as exc:
                if service_name is not None:
                    matched_services.add(service_name)
                self._record_recovery_conflict(
                    resource_kind="pod",
                    resource=pod,
                    worker_id=normalized_worker_id,
                    error=exc,
                )
                continue
        for service_name, service in services.items():
            if service_name in matched_services:
                continue
            try:
                handle = self._service_only_handle(service)
                if handle.object_id in observed_pod_names:
                    # The Pod pass already recorded the resource-level conflict.
                    # Do not turn its otherwise valid Service into a deletable
                    # orphan handle or emit a duplicate warning for the pair.
                    continue
                handles.append(handle)
            except KubernetesServingOwnershipError as exc:
                self._record_recovery_conflict(
                    resource_kind="service",
                    resource=service,
                    worker_id=normalized_worker_id,
                    error=exc,
                )
        return tuple(handles)

    def _ownership_identity(
        self,
        resource: object,
        *,
        resource_kind: str,
        worker_id: str,
    ) -> KubernetesServingOwnershipIdentity | None:
        """Parse identity only when every DB correlation fence is canonical."""

        expected_resource_kind = {
            "pod": POD_RESOURCE_KIND,
            "service": SERVICE_RESOURCE_KIND,
        }.get(resource_kind)
        if expected_resource_kind is None:
            return None
        try:
            labels = _resource_labels(resource)
        except KubernetesServingOwnershipError:
            return None
        if any(
            (
                labels.get(MANAGED_LABEL) != "true",
                labels.get(CLUSTER_ID_LABEL) != self.cluster_id,
                labels.get(WORKER_ID_LABEL) != worker_id,
                labels.get(RUNTIME_LABEL) != RUNTIME_LABEL_VALUE,
                labels.get(RESOURCE_KIND_LABEL) != expected_resource_kind,
            )
        ):
            return None
        try:
            service_id = uuid.UUID(labels[SERVICE_ID_LABEL])
            replica_id = uuid.UUID(labels[REPLICA_ID_LABEL])
            project_id = uuid.UUID(labels[PROJECT_ID_LABEL])
            execution_id = uuid.UUID(labels[EXECUTION_ID_LABEL])
            worker_session_id = uuid.UUID(labels[WORKER_SESSION_ID_LABEL])
            generation = int(labels[GENERATION_LABEL])
        except (KeyError, TypeError, ValueError):
            return None
        canonical_values = {
            SERVICE_ID_LABEL: str(service_id),
            REPLICA_ID_LABEL: str(replica_id),
            PROJECT_ID_LABEL: str(project_id),
            EXECUTION_ID_LABEL: str(execution_id),
            WORKER_SESSION_ID_LABEL: str(worker_session_id),
            GENERATION_LABEL: str(generation),
        }
        if generation < 1 or any(
            labels.get(key) != value for key, value in canonical_values.items()
        ):
            return None
        expected_name = _resource_name(
            "sr" if resource_kind == "pod" else "ss",
            service_id=service_id,
            replica_id=replica_id,
            generation=generation,
            execution_id=execution_id,
        )
        if _observed_resource_name(resource) != expected_name:
            return None
        return KubernetesServingOwnershipIdentity(
            service_id=service_id,
            replica_id=replica_id,
            project_id=project_id,
            generation=generation,
            execution_id=execution_id,
            cluster_id=self.cluster_id,
            worker_id=worker_id,
            worker_session_id=worker_session_id,
        )

    def _record_recovery_conflict(
        self,
        *,
        resource_kind: str,
        resource: object,
        worker_id: str,
        error: KubernetesServingOwnershipError,
    ) -> None:
        self._recovery_conflicts.append(
            KubernetesServingRecoveryConflict(
                resource_kind=resource_kind,
                resource_name=(
                    _observed_resource_name(resource)[:_MAX_RESOURCE_NAME_LENGTH] or "<unknown>"
                ),
                reason="ownership_conflict",
                message=_bounded(str(error)) or "Kubernetes serving ownership conflict",
                ownership=self._ownership_identity(
                    resource,
                    resource_kind=resource_kind,
                    worker_id=worker_id,
                ),
            )
        )

    async def close(self) -> None:
        if not self._owns_api or self._api is None:
            return
        api_client = getattr(self._api, "api_client", None)
        close = getattr(api_client, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result
        self._api = None
        self._version_api = None

    @staticmethod
    def pod_name(spec: KubernetesServingLaunchSpec) -> str:
        return _resource_name(
            "sr",
            service_id=spec.service_id,
            replica_id=spec.replica_id,
            generation=spec.generation,
            execution_id=spec.execution_id,
        )

    @staticmethod
    def service_name(spec: KubernetesServingLaunchSpec) -> str:
        return _resource_name(
            "ss",
            service_id=spec.service_id,
            replica_id=spec.replica_id,
            generation=spec.generation,
            execution_id=spec.execution_id,
        )

    async def _create_or_adopt_pod(
        self,
        api: Any,
        pod: client.V1Pod,
        spec: KubernetesServingLaunchSpec,
        selector: Mapping[str, str],
    ) -> object:
        try:
            observed = await api.create_namespaced_pod(namespace=self.namespace, body=pod)
        except ApiException as exc:
            if exc.status != 409:
                raise self._operation_error("create Kubernetes serving Pod", exc) from exc
            try:
                observed = await api.read_namespaced_pod(
                    name=self.pod_name(spec),
                    namespace=self.namespace,
                )
            except ApiException as read_exc:
                raise self._operation_error("adopt Kubernetes serving Pod", read_exc) from read_exc
        self._validate_observed_pod(observed, expected_labels=selector, expected_spec=spec)
        return observed

    async def _create_or_adopt_service(
        self,
        api: Any,
        service: client.V1Service,
        spec: KubernetesServingLaunchSpec,
        selector: Mapping[str, str],
    ) -> object:
        try:
            observed = await api.create_namespaced_service(namespace=self.namespace, body=service)
        except ApiException as exc:
            if exc.status != 409:
                raise self._operation_error("create Kubernetes serving Service", exc) from exc
            try:
                observed = await api.read_namespaced_service(
                    name=self.service_name(spec),
                    namespace=self.namespace,
                )
            except ApiException as read_exc:
                raise self._operation_error(
                    "adopt Kubernetes serving Service", read_exc
                ) from read_exc
        self._validate_observed_service(
            observed,
            expected_selector=selector,
            expected_port=spec.container_port,
        )
        return observed

    async def _delete_resources(
        self,
        handle: KubernetesServingHandle,
        *,
        grace_seconds: int,
    ) -> None:
        api = await self._ensure_api()
        expected_labels = self._expected_handle_labels(handle)
        await self._delete_service(
            api,
            handle,
            expected_labels=expected_labels,
        )
        await self._delete_pod(
            api,
            handle,
            expected_labels=expected_labels,
            grace_seconds=grace_seconds,
        )

    async def _delete_service(
        self,
        api: Any,
        handle: KubernetesServingHandle,
        *,
        expected_labels: Mapping[str, str],
    ) -> None:
        name = self._service_name(handle)
        try:
            service = await api.read_namespaced_service(name=name, namespace=self.namespace)
        except ApiException as exc:
            if exc.status == 404:
                return
            raise self._operation_error(
                "inspect Kubernetes serving Service for deletion", exc
            ) from exc
        self._validate_observed_service(
            service,
            expected_selector=expected_labels,
            expected_port=_endpoint_port(handle.endpoint_url),
        )
        uid = _required_uid(service, resource="Service")
        if handle.service_uid is not None and handle.service_uid != uid:
            raise KubernetesServingOwnershipError(
                f"refusing to delete Kubernetes serving Service {name}: UID fence mismatch"
            )
        body = client.V1DeleteOptions(
            grace_period_seconds=0,
            preconditions=client.V1Preconditions(uid=uid),
            propagation_policy="Background",
        )
        try:
            await api.delete_namespaced_service(
                name=name,
                namespace=self.namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise self._operation_error("delete Kubernetes serving Service", exc) from exc

    async def _delete_pod(
        self,
        api: Any,
        handle: KubernetesServingHandle,
        *,
        expected_labels: Mapping[str, str],
        grace_seconds: int,
    ) -> None:
        try:
            pod = await api.read_namespaced_pod(
                name=handle.object_id,
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise self._operation_error("inspect Kubernetes serving Pod for deletion", exc) from exc
        self._validate_observed_pod(pod, expected_labels=expected_labels)
        uid = _required_uid(pod, resource="Pod")
        if handle.uid is not None and handle.uid != uid:
            raise KubernetesServingOwnershipError(
                f"refusing to delete Kubernetes serving Pod {handle.object_id}: UID fence mismatch"
            )
        body = client.V1DeleteOptions(
            grace_period_seconds=grace_seconds,
            preconditions=client.V1Preconditions(uid=uid),
            propagation_policy="Background",
        )
        try:
            await api.delete_namespaced_pod(
                name=handle.object_id,
                namespace=self.namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise self._operation_error("delete Kubernetes serving Pod", exc) from exc

    async def _ensure_api(self) -> Any:
        if self._api is not None:
            return self._api
        async with self._client_lock:
            if self._api is not None:
                return self._api
            if self.in_cluster:
                loaded = config.load_incluster_config()
                if inspect.isawaitable(loaded):
                    await loaded
            else:
                await config.load_kube_config(config_file=self.kubeconfig)
            self._api = client.CoreV1Api()
        return self._api

    async def _ensure_version_api(self) -> Any:
        if self._version_api is not None:
            return self._version_api
        api = await self._ensure_api()
        async with self._client_lock:
            if self._version_api is None:
                api_client = getattr(api, "api_client", None)
                self._version_api = (
                    client.VersionApi(api_client=api_client)
                    if api_client is not None
                    else client.VersionApi()
                )
        return self._version_api

    def _selector_labels(
        self,
        spec: KubernetesServingLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
    ) -> dict[str, str]:
        selector = {
            SERVICE_ID_LABEL: str(spec.service_id),
            REPLICA_ID_LABEL: str(spec.replica_id),
            PROJECT_ID_LABEL: str(spec.project_id),
            EXECUTION_ID_LABEL: str(spec.execution_id),
            GENERATION_LABEL: str(spec.generation),
            MANAGED_LABEL: "true",
            CLUSTER_ID_LABEL: self.cluster_id,
            WORKER_ID_LABEL: worker_id,
            WORKER_SESSION_ID_LABEL: str(worker_session_id),
            RUNTIME_LABEL: RUNTIME_LABEL_VALUE,
        }
        expected_pod = self._build_pod(spec, selector)
        selector[SPEC_HASH_LABEL] = _pod_contract_hash(expected_pod)
        return selector

    def _build_pod(
        self,
        spec: KubernetesServingLaunchSpec,
        selector: Mapping[str, str],
    ) -> client.V1Pod:
        resources = {
            "cpu": f"{spec.cpu_millicores}m",
            "memory": f"{spec.memory_mb}Mi",
        }
        container = client.V1Container(
            name=CONTAINER_NAME,
            image=spec.image,
            image_pull_policy="IfNotPresent",
            command=list(_fake_inference_command(spec)),
            env=[
                client.V1EnvVar(name="PYTHONDONTWRITEBYTECODE", value="1"),
                client.V1EnvVar(name="TMPDIR", value="/tmp"),
            ],
            ports=[client.V1ContainerPort(name="http", container_port=spec.container_port)],
            readiness_probe=client.V1Probe(
                http_get=client.V1HTTPGetAction(path="/health", port=spec.container_port),
                initial_delay_seconds=0,
                period_seconds=self.readiness_probe_period_seconds,
                timeout_seconds=self.readiness_probe_timeout_seconds,
                failure_threshold=1,
                success_threshold=1,
            ),
            resources=client.V1ResourceRequirements(
                requests=dict(resources),
                limits=dict(resources),
            ),
            security_context=client.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=client.V1Capabilities(drop=["ALL"]),
                privileged=False,
                read_only_root_filesystem=True,
                run_as_non_root=True,
            ),
            volume_mounts=[client.V1VolumeMount(name=TMP_VOLUME_NAME, mount_path="/tmp")],
        )
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=self.pod_name(spec),
                labels={**dict(selector), RESOURCE_KIND_LABEL: POD_RESOURCE_KIND},
            ),
            spec=client.V1PodSpec(
                automount_service_account_token=False,
                containers=[container],
                host_ipc=False,
                host_network=False,
                host_pid=False,
                restart_policy="Never",
                security_context=client.V1PodSecurityContext(
                    run_as_non_root=True,
                    run_as_user=10001,
                    run_as_group=10001,
                    seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
                ),
                termination_grace_period_seconds=self.termination_grace_seconds,
                volumes=[
                    client.V1Volume(
                        name=TMP_VOLUME_NAME,
                        empty_dir=client.V1EmptyDirVolumeSource(
                            medium="Memory",
                            size_limit="64Mi",
                        ),
                    )
                ],
            ),
        )

    def _build_service(
        self,
        spec: KubernetesServingLaunchSpec,
        selector: Mapping[str, str],
    ) -> client.V1Service:
        return client.V1Service(
            metadata=client.V1ObjectMeta(
                name=self.service_name(spec),
                labels={**dict(selector), RESOURCE_KIND_LABEL: SERVICE_RESOURCE_KIND},
            ),
            spec=client.V1ServiceSpec(
                ports=[
                    client.V1ServicePort(
                        name="http",
                        port=spec.container_port,
                        protocol="TCP",
                        target_port=spec.container_port,
                    )
                ],
                publish_not_ready_addresses=False,
                selector=dict(selector),
                type="ClusterIP",
            ),
        )

    def _validate_observed_pod(
        self,
        pod: object,
        *,
        expected_labels: Mapping[str, str],
        expected_spec: KubernetesServingLaunchSpec | None = None,
    ) -> None:
        labels = _resource_labels(pod)
        _validate_labels(
            labels,
            {**dict(expected_labels), RESOURCE_KIND_LABEL: POD_RESOURCE_KIND},
            resource="Pod",
        )
        pod_spec = getattr(pod, "spec", None)
        containers = getattr(pod_spec, "containers", None) or []
        if len(containers) != 1 or getattr(containers[0], "name", None) != CONTAINER_NAME:
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Pod with unexpected containers"
            )
        container = containers[0]
        pod_security = getattr(pod_spec, "security_context", None)
        container_security = getattr(container, "security_context", None)
        volumes = getattr(pod_spec, "volumes", None) or []
        volume_mounts = getattr(container, "volume_mounts", None) or []
        secure = (
            getattr(pod_spec, "automount_service_account_token", None) is False
            and getattr(pod_spec, "host_network", None) is not True
            and getattr(pod_spec, "host_pid", None) is not True
            and getattr(pod_spec, "host_ipc", None) is not True
            and getattr(pod_spec, "restart_policy", None) == "Never"
            and getattr(pod_security, "run_as_non_root", None) is True
            and getattr(getattr(pod_security, "seccomp_profile", None), "type", None)
            == "RuntimeDefault"
            and getattr(container_security, "allow_privilege_escalation", None) is False
            and getattr(container_security, "privileged", None) is False
            and getattr(container_security, "read_only_root_filesystem", None) is True
            and getattr(container_security, "run_as_non_root", None) is True
            and list(getattr(getattr(container_security, "capabilities", None), "drop", None) or [])
            == ["ALL"]
            and len(volumes) == 1
            and getattr(volumes[0], "name", None) == TMP_VOLUME_NAME
            and getattr(volumes[0], "empty_dir", None) is not None
            and getattr(volumes[0], "host_path", None) is None
            and len(volume_mounts) == 1
            and getattr(volume_mounts[0], "name", None) == TMP_VOLUME_NAME
            and getattr(volume_mounts[0], "mount_path", None) == "/tmp"
        )
        if not secure:
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Pod outside the security baseline"
            )
        resources = getattr(container, "resources", None)
        if getattr(resources, "requests", None) != getattr(resources, "limits", None):
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Pod without equal requests and limits"
            )
        observed_contract = _pod_contract(pod)
        observed_hash = _pod_contract_hash(pod)
        if expected_spec is None:
            if labels.get(SPEC_HASH_LABEL) != observed_hash:
                raise KubernetesServingOwnershipError(
                    "refusing to adopt Kubernetes serving Pod with mismatched spec hash"
                )
            return

        expected_pod = self._build_pod(expected_spec, expected_labels)
        expected_contract = _pod_contract(expected_pod)
        expected_hash = _canonical_contract_hash(expected_contract)
        if labels.get(SPEC_HASH_LABEL) != expected_hash or observed_contract != expected_contract:
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Pod with mismatched launch spec"
            )

    def _validate_observed_service(
        self,
        service: object,
        *,
        expected_selector: Mapping[str, str],
        expected_port: int | None = None,
    ) -> None:
        labels = _resource_labels(service)
        _validate_labels(
            labels,
            {**dict(expected_selector), RESOURCE_KIND_LABEL: SERVICE_RESOURCE_KIND},
            resource="Service",
        )
        service_spec = getattr(service, "spec", None)
        selector = getattr(service_spec, "selector", None) or {}
        ports = getattr(service_spec, "ports", None) or []
        if selector != dict(expected_selector) or len(ports) != 1:
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Service with mismatched selector or ports"
            )
        port = ports[0]
        if (
            getattr(service_spec, "type", None) != "ClusterIP"
            or getattr(port, "protocol", None) != "TCP"
            or getattr(port, "name", None) != "http"
            or getattr(service_spec, "publish_not_ready_addresses", None) is True
            or not isinstance(getattr(port, "port", None), int)
            or isinstance(getattr(port, "port", None), bool)
            or getattr(port, "port", None) != getattr(port, "target_port", None)
            or (expected_port is not None and getattr(port, "port", None) != expected_port)
        ):
            raise KubernetesServingOwnershipError(
                "refusing to adopt Kubernetes serving Service outside the endpoint contract"
            )

    def _managed_pod_labels(self, pod: object) -> Mapping[str, str]:
        labels = _resource_labels(pod)
        required = {
            SERVICE_ID_LABEL,
            REPLICA_ID_LABEL,
            PROJECT_ID_LABEL,
            EXECUTION_ID_LABEL,
            GENERATION_LABEL,
            MANAGED_LABEL,
            CLUSTER_ID_LABEL,
            WORKER_ID_LABEL,
            WORKER_SESSION_ID_LABEL,
            RUNTIME_LABEL,
            RESOURCE_KIND_LABEL,
            SPEC_HASH_LABEL,
        }
        if (
            any(not labels.get(key) for key in required)
            or labels.get(MANAGED_LABEL) != "true"
            or labels.get(CLUSTER_ID_LABEL) != self.cluster_id
            or labels.get(RUNTIME_LABEL) != RUNTIME_LABEL_VALUE
            or labels.get(RESOURCE_KIND_LABEL) != POD_RESOURCE_KIND
        ):
            raise KubernetesServingOwnershipError(
                "managed Kubernetes serving Pod has incomplete or mismatched fencing labels"
            )
        self._validate_observed_pod(pod, expected_labels=_selector_from_resource_labels(labels))
        return MappingProxyType(dict(labels))

    def _handle(
        self,
        pod: object,
        *,
        service: object | None,
    ) -> KubernetesServingHandle:
        metadata = getattr(pod, "metadata", None)
        name = str(getattr(metadata, "name", ""))
        if not name:
            raise KubernetesServingOwnershipError("Kubernetes serving Pod has no name")
        labels = self._managed_pod_labels(pod)
        service_name = _service_name_from_labels(labels)
        observed_service_name = str(
            getattr(getattr(service, "metadata", None), "name", "") if service is not None else ""
        )
        if observed_service_name and observed_service_name != service_name:
            raise KubernetesServingOwnershipError(
                "Kubernetes serving Service name does not match its Pod fence"
            )
        port = _container_port(pod)
        return KubernetesServingHandle(
            object_id=name,
            display_id=name,
            endpoint_url=(f"http://{service_name}.{self.namespace}.svc.cluster.local:{port}"),
            image_digest=_pod_image_digest(getattr(pod, "status", None)),
            labels=labels,
            uid=_optional_uid(pod),
            service_name=service_name,
            service_uid=_optional_uid(service) if service is not None else None,
            native=pod,
        )

    def _service_only_handle(self, service: object) -> KubernetesServingHandle:
        # A Service cannot prove the image/process/resource portion of the
        # workload hash without its Pod.  Keep it discoverable only as an
        # orphan-cleanup handle; ``inspect`` will report the derived Pod missing
        # and the controller must never publish this handle as ready.
        labels = self._managed_service_labels(service)
        service_name = _service_name_from_labels(labels)
        observed_name = str(getattr(getattr(service, "metadata", None), "name", ""))
        if observed_name != service_name:
            raise KubernetesServingOwnershipError(
                "Kubernetes serving Service name does not match its identity fence"
            )
        service_spec = getattr(service, "spec", None)
        ports = getattr(service_spec, "ports", None) or []
        port = getattr(ports[0], "port", None) if len(ports) == 1 else None
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise KubernetesServingOwnershipError(
                "Kubernetes serving Service endpoint port is invalid"
            )
        selector = _selector_from_resource_labels(labels)
        pod_labels = MappingProxyType({**selector, RESOURCE_KIND_LABEL: POD_RESOURCE_KIND})
        pod_name = _pod_name_from_labels(labels)
        return KubernetesServingHandle(
            object_id=pod_name,
            display_id=pod_name,
            endpoint_url=(f"http://{service_name}.{self.namespace}.svc.cluster.local:{port}"),
            labels=pod_labels,
            uid=None,
            service_name=service_name,
            service_uid=_optional_uid(service),
            native=None,
        )

    def _expected_handle_labels(self, handle: KubernetesServingHandle) -> Mapping[str, str]:
        labels = dict(handle.labels)
        required_values = {
            MANAGED_LABEL: "true",
            CLUSTER_ID_LABEL: self.cluster_id,
            RUNTIME_LABEL: RUNTIME_LABEL_VALUE,
            RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
        }
        _validate_labels(labels, required_values, resource="handle")
        return MappingProxyType(_selector_from_resource_labels(labels))

    def _managed_service_labels(self, service: object) -> Mapping[str, str]:
        labels = _resource_labels(service)
        selector = _selector_from_resource_labels(labels)
        required = {
            SERVICE_ID_LABEL,
            REPLICA_ID_LABEL,
            PROJECT_ID_LABEL,
            EXECUTION_ID_LABEL,
            GENERATION_LABEL,
            MANAGED_LABEL,
            CLUSTER_ID_LABEL,
            WORKER_ID_LABEL,
            WORKER_SESSION_ID_LABEL,
            RUNTIME_LABEL,
            SPEC_HASH_LABEL,
        }
        if (
            any(not selector.get(key) for key in required)
            or selector.get(MANAGED_LABEL) != "true"
            or selector.get(CLUSTER_ID_LABEL) != self.cluster_id
            or selector.get(RUNTIME_LABEL) != RUNTIME_LABEL_VALUE
            or labels.get(RESOURCE_KIND_LABEL) != SERVICE_RESOURCE_KIND
        ):
            raise KubernetesServingOwnershipError(
                "managed Kubernetes serving Service has incomplete or mismatched fencing labels"
            )
        self._validate_observed_service(service, expected_selector=selector)
        return MappingProxyType(dict(labels))

    @staticmethod
    def _service_name(handle: KubernetesServingHandle) -> str:
        return handle.service_name or _service_name_from_labels(handle.labels)

    @staticmethod
    def _validate_launch_spec(spec: KubernetesServingLaunchSpec) -> None:
        image = spec.image.strip()
        model = spec.model.strip()
        if not image or image != spec.image or any(character.isspace() for character in image):
            raise ValueError("image must be a non-blank reference without whitespace")
        if not model or any(ord(character) < 32 for character in model):
            raise ValueError("model must not be blank or contain control characters")
        if spec.generation < 1:
            raise ValueError("generation must be at least one")
        if spec.cpu_millicores < 1 or spec.memory_mb < 16:
            raise ValueError("service CPU and memory limits are invalid")
        if spec.container_port < 1 or spec.container_port > 65535:
            raise ValueError("container_port must be between 1 and 65535")
        if (
            isinstance(spec.startup_delay_seconds, bool)
            or isinstance(spec.chunk_delay_seconds, bool)
            or not math.isfinite(spec.startup_delay_seconds)
            or not math.isfinite(spec.chunk_delay_seconds)
            or spec.startup_delay_seconds < 0
            or spec.chunk_delay_seconds < 0
        ):
            raise ValueError("startup and chunk delays must be finite and non-negative")

    @staticmethod
    def _operation_error(operation: str, exc: ApiException) -> KubernetesServingRuntimeError:
        detail = exc.reason or exc.body or str(exc)
        return KubernetesServingRuntimeError(f"failed to {operation}: {_bounded(str(detail))}")


def _resource_name(
    prefix: str,
    *,
    service_id: uuid.UUID,
    replica_id: uuid.UUID,
    generation: int,
    execution_id: uuid.UUID,
) -> str:
    canonical = f"{service_id}:{replica_id}:{generation}:{execution_id}"
    digest = hashlib.sha256(canonical.encode()).hexdigest()[:16]
    stem = f"mac-{prefix}-{replica_id.hex[:16]}-g{generation}"
    max_stem = 63 - len(digest) - 1
    stem = stem[:max_stem].rstrip("-")
    return f"{stem}-{digest}"


def _service_name_from_labels(labels: Mapping[str, str]) -> str:
    return _resource_name_from_labels("ss", labels)


def _pod_name_from_labels(labels: Mapping[str, str]) -> str:
    return _resource_name_from_labels("sr", labels)


def _resource_name_from_labels(prefix: str, labels: Mapping[str, str]) -> str:
    try:
        return _resource_name(
            prefix,
            service_id=uuid.UUID(labels[SERVICE_ID_LABEL]),
            replica_id=uuid.UUID(labels[REPLICA_ID_LABEL]),
            generation=int(labels[GENERATION_LABEL]),
            execution_id=uuid.UUID(labels[EXECUTION_ID_LABEL]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KubernetesServingOwnershipError(
            "Kubernetes serving resource has invalid identity labels"
        ) from exc


def _pod_contract_hash(pod: object) -> str:
    return _canonical_contract_hash(_pod_contract(pod))


def _canonical_contract_hash(contract: Mapping[str, object]) -> str:
    encoded = json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:32]


def _pod_contract(pod: object) -> dict[str, object]:
    """Project a Pod onto the complete serving-owned workload contract.

    Kubernetes defaults unrelated scheduling fields on admission, so hashing the
    entire API object would be unstable.  This projection instead includes every
    field owned by this adapter: identity fences, process invocation, endpoint,
    resources, probes, security, termination and writable storage.  It is also
    reproducible from a listed Pod, which lets restart recovery verify the
    workload against its ``spec-hash`` without requiring a database launch spec.
    """

    labels = _resource_labels(pod)
    pod_spec = getattr(pod, "spec", None)
    containers = getattr(pod_spec, "containers", None) or []
    container = containers[0] if len(containers) == 1 else None
    pod_security = getattr(pod_spec, "security_context", None)
    pod_seccomp = getattr(pod_security, "seccomp_profile", None)
    container_security = getattr(container, "security_context", None)
    capabilities = getattr(container_security, "capabilities", None)
    resources = getattr(container, "resources", None)
    probe = getattr(container, "readiness_probe", None)
    http_get = getattr(probe, "http_get", None)
    volumes = getattr(pod_spec, "volumes", None) or []
    volume = volumes[0] if len(volumes) == 1 else None
    empty_dir = getattr(volume, "empty_dir", None)
    volume_mounts = getattr(container, "volume_mounts", None) or []

    return {
        "fencing_labels": {key: labels.get(key) for key in _CONTRACT_LABEL_KEYS},
        "pod": {
            "automount_service_account_token": getattr(
                pod_spec, "automount_service_account_token", None
            ),
            "host_ipc": getattr(pod_spec, "host_ipc", None) is True,
            "host_network": getattr(pod_spec, "host_network", None) is True,
            "host_pid": getattr(pod_spec, "host_pid", None) is True,
            "share_process_namespace": getattr(pod_spec, "share_process_namespace", None) is True,
            "restart_policy": getattr(pod_spec, "restart_policy", None),
            "termination_grace_period_seconds": getattr(
                pod_spec, "termination_grace_period_seconds", None
            ),
            "init_container_count": len(getattr(pod_spec, "init_containers", None) or []),
            "ephemeral_container_count": len(getattr(pod_spec, "ephemeral_containers", None) or []),
            "security_context": {
                "run_as_non_root": getattr(pod_security, "run_as_non_root", None),
                "run_as_user": getattr(pod_security, "run_as_user", None),
                "run_as_group": getattr(pod_security, "run_as_group", None),
                "fs_group": getattr(pod_security, "fs_group", None),
                "supplemental_groups": tuple(
                    getattr(pod_security, "supplemental_groups", None) or ()
                ),
                "seccomp_type": getattr(pod_seccomp, "type", None),
                "seccomp_localhost_profile": getattr(pod_seccomp, "localhost_profile", None),
            },
        },
        "container": {
            "count": len(containers),
            "name": getattr(container, "name", None),
            "image": getattr(container, "image", None),
            "image_pull_policy": getattr(container, "image_pull_policy", None),
            "command": tuple(getattr(container, "command", None) or ()),
            "args": tuple(getattr(container, "args", None) or ()),
            "working_dir": getattr(container, "working_dir", None),
            "env_from_count": len(getattr(container, "env_from", None) or []),
            "env": tuple(
                {
                    "name": getattr(item, "name", None),
                    "value": getattr(item, "value", None),
                    "value_from": getattr(item, "value_from", None) is not None,
                }
                for item in (getattr(container, "env", None) or [])
            ),
            "ports": tuple(
                {
                    "name": getattr(item, "name", None),
                    "container_port": getattr(item, "container_port", None),
                    "host_ip": getattr(item, "host_ip", None),
                    "host_port": getattr(item, "host_port", None),
                    "protocol": getattr(item, "protocol", None) or "TCP",
                }
                for item in (getattr(container, "ports", None) or [])
            ),
            "resources": {
                "requests": _string_mapping(getattr(resources, "requests", None)),
                "limits": _string_mapping(getattr(resources, "limits", None)),
                "claims": tuple(str(item) for item in (getattr(resources, "claims", None) or [])),
            },
            "readiness_probe": {
                "present": probe is not None,
                "http_path": getattr(http_get, "path", None),
                "http_port": getattr(http_get, "port", None),
                "http_host": getattr(http_get, "host", None) or "",
                "http_scheme": getattr(http_get, "scheme", None) or "HTTP",
                "http_header_count": len(getattr(http_get, "http_headers", None) or []),
                "exec_present": getattr(probe, "_exec", None) is not None,
                "grpc_present": getattr(probe, "grpc", None) is not None,
                "tcp_socket_present": getattr(probe, "tcp_socket", None) is not None,
                "initial_delay_seconds": getattr(probe, "initial_delay_seconds", None) or 0,
                "period_seconds": getattr(probe, "period_seconds", None),
                "timeout_seconds": getattr(probe, "timeout_seconds", None),
                "failure_threshold": getattr(probe, "failure_threshold", None),
                "success_threshold": getattr(probe, "success_threshold", None),
                "termination_grace_period_seconds": getattr(
                    probe, "termination_grace_period_seconds", None
                ),
            },
            "other_probes": {
                "liveness": getattr(container, "liveness_probe", None) is not None,
                "startup": getattr(container, "startup_probe", None) is not None,
            },
            "lifecycle_present": getattr(container, "lifecycle", None) is not None,
            "security_context": {
                "allow_privilege_escalation": getattr(
                    container_security, "allow_privilege_escalation", None
                ),
                "privileged": getattr(container_security, "privileged", None),
                "read_only_root_filesystem": getattr(
                    container_security, "read_only_root_filesystem", None
                ),
                "run_as_non_root": getattr(container_security, "run_as_non_root", None),
                "run_as_user": getattr(container_security, "run_as_user", None),
                "run_as_group": getattr(container_security, "run_as_group", None),
                "capabilities_add": tuple(getattr(capabilities, "add", None) or ()),
                "capabilities_drop": tuple(getattr(capabilities, "drop", None) or ()),
            },
            "volume_mounts": tuple(
                {
                    "name": getattr(item, "name", None),
                    "mount_path": getattr(item, "mount_path", None),
                    "read_only": getattr(item, "read_only", None) is True,
                    "sub_path": getattr(item, "sub_path", None) or "",
                    "sub_path_expr": getattr(item, "sub_path_expr", None) or "",
                    "mount_propagation": getattr(item, "mount_propagation", None),
                }
                for item in volume_mounts
            ),
        },
        "volumes": {
            "count": len(volumes),
            "name": getattr(volume, "name", None),
            "sources": _volume_sources(volume),
            "empty_dir": {
                "present": empty_dir is not None,
                "medium": getattr(empty_dir, "medium", None) or "",
                "size_limit": str(getattr(empty_dir, "size_limit", None) or ""),
            },
            "host_path_present": getattr(volume, "host_path", None) is not None,
            "projected_present": getattr(volume, "projected", None) is not None,
            "secret_present": getattr(volume, "secret", None) is not None,
            "config_map_present": getattr(volume, "config_map", None) is not None,
            "persistent_volume_claim_present": getattr(volume, "persistent_volume_claim", None)
            is not None,
            "csi_present": getattr(volume, "csi", None) is not None,
            "downward_api_present": getattr(volume, "downward_api", None) is not None,
        },
    }


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _volume_sources(volume: object) -> tuple[str, ...]:
    attribute_map = getattr(volume, "attribute_map", None)
    if not isinstance(attribute_map, Mapping):
        return ()
    return tuple(
        sorted(
            str(name)
            for name in attribute_map
            if name != "name" and getattr(volume, str(name), None) is not None
        )
    )


def _endpoint_port(endpoint_url: str) -> int:
    try:
        port = urlsplit(endpoint_url).port
    except ValueError as exc:
        raise KubernetesServingOwnershipError(
            "Kubernetes serving handle endpoint port is invalid"
        ) from exc
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise KubernetesServingOwnershipError("Kubernetes serving handle endpoint port is invalid")
    return port


def _fake_inference_command(spec: KubernetesServingLaunchSpec) -> tuple[str, ...]:
    return (
        "python",
        "-m",
        "scripts.fake_inference",
        "--host",
        "0.0.0.0",
        "--port",
        str(spec.container_port),
        "--model",
        spec.model,
        "--replica-id",
        str(spec.replica_id),
        "--execution-id",
        str(spec.execution_id),
        "--startup-delay-seconds",
        str(spec.startup_delay_seconds),
        "--chunk-delay-seconds",
        str(spec.chunk_delay_seconds),
    )


def _validate_worker_id(worker_id: str) -> None:
    if not _LABEL_VALUE.fullmatch(worker_id):
        raise ValueError("worker_id must be a Kubernetes label value")


def _resource_labels(resource: object) -> dict[str, str]:
    metadata = getattr(resource, "metadata", None)
    raw_labels = getattr(metadata, "labels", None) or {}
    if not isinstance(raw_labels, Mapping):
        raise KubernetesServingOwnershipError(
            "Kubernetes serving resource labels are not a mapping"
        )
    return {str(key): str(value) for key, value in raw_labels.items()}


def _observed_resource_name(resource: object) -> str:
    return str(getattr(getattr(resource, "metadata", None), "name", "") or "")


def _selector_from_resource_labels(labels: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in labels.items() if key != RESOURCE_KIND_LABEL}


def _validate_labels(
    observed: Mapping[str, str],
    expected: Mapping[str, str],
    *,
    resource: str,
) -> None:
    if any(observed.get(key) != value for key, value in expected.items()):
        raise KubernetesServingOwnershipError(
            f"refusing to manage Kubernetes serving {resource} with mismatched fencing labels"
        )


def _pod_ready(status: object) -> bool:
    for condition in getattr(status, "conditions", None) or []:
        if getattr(condition, "type", None) == "Ready":
            return str(getattr(condition, "status", "")).lower() == "true"
    return False


def _pod_failure(status: object) -> tuple[int | None, bool, str | None, str | None]:
    exit_code: int | None = None
    oom_killed = False
    reason: str | None = None
    message: str | None = None
    for container_status in getattr(status, "container_statuses", None) or []:
        state = getattr(container_status, "state", None)
        waiting = getattr(state, "waiting", None)
        waiting_reason = str(getattr(waiting, "reason", None) or "")
        if waiting_reason:
            reason = waiting_reason
            message = str(getattr(waiting, "message", None) or waiting_reason)
            if waiting_reason in {"ErrImagePull", "ImagePullBackOff", "InvalidImageName"}:
                return None, False, waiting_reason, _bounded(message)
        terminated = getattr(state, "terminated", None)
        if terminated is None:
            continue
        raw_exit_code = getattr(terminated, "exit_code", None)
        if isinstance(raw_exit_code, int) and not isinstance(raw_exit_code, bool):
            exit_code = raw_exit_code
        terminated_reason = str(getattr(terminated, "reason", None) or "")
        reason = terminated_reason or reason or "ContainerTerminated"
        message = str(getattr(terminated, "message", None) or reason)
        oom_killed = terminated_reason == "OOMKilled" or exit_code == 137
        if oom_killed:
            reason = "OOMKilled"
        return exit_code, oom_killed, reason, _bounded(message)

    for condition in getattr(status, "conditions", None) or []:
        if (
            getattr(condition, "type", None) == "PodScheduled"
            and str(getattr(condition, "status", "")).lower() == "false"
        ):
            reason = str(getattr(condition, "reason", None) or "Unschedulable")
            message = str(getattr(condition, "message", None) or reason)
            break
    if reason is None and str(getattr(status, "phase", "")) == "Failed":
        reason = str(getattr(status, "reason", None) or "PodFailed")
        message = str(getattr(status, "message", None) or reason)
    return exit_code, oom_killed, reason, _bounded(message)


def _pod_image_digest(status: object) -> str | None:
    for container_status in getattr(status, "container_statuses", None) or []:
        image_id = str(getattr(container_status, "image_id", None) or "")
        match = _IMAGE_DIGEST.search(image_id.lower())
        if match is not None:
            return match.group(0)
    return None


def _container_port(pod: object) -> int:
    pod_spec = getattr(pod, "spec", None)
    containers = getattr(pod_spec, "containers", None) or []
    ports = (getattr(containers[0], "ports", None) or []) if containers else []
    if len(ports) != 1:
        raise KubernetesServingOwnershipError(
            "Kubernetes serving Pod does not expose exactly one endpoint port"
        )
    port = getattr(ports[0], "container_port", None)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise KubernetesServingOwnershipError("Kubernetes serving Pod endpoint port is invalid")
    return port


def _deleting(resource: object) -> bool:
    return getattr(getattr(resource, "metadata", None), "deletion_timestamp", None) is not None


def _optional_uid(resource: object) -> str | None:
    value = getattr(getattr(resource, "metadata", None), "uid", None)
    return str(value) if value else None


def _required_uid(item: object, *, resource: str) -> str:
    uid = _optional_uid(item)
    if uid is None:
        raise KubernetesServingOwnershipError(
            f"refusing to delete Kubernetes serving {resource} without a UID fence"
        )
    return uid


def _bounded(value: str | None) -> str | None:
    if value is None:
        return None
    return value[:_MAX_ERROR_LENGTH]
