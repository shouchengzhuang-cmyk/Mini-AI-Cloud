import asyncio
import base64
import hashlib
import inspect
import json
import re
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from core.enums import AllocationAuthority, ErrorCategory, ErrorCode
from core.kubernetes_names import validate_kubernetes_dns_subdomain
from core.runtime_profiles import (
    RuntimeProfile,
    RuntimeProfileCatalog,
    RuntimeProfileCompatibilityError,
)
from worker.runtime import ExecutionSpec, RuntimeFailure, RuntimeHandle, RuntimeLog

TASK_ID_LABEL = "mini-ai-cloud/task-id"
PROJECT_ID_LABEL = "mini-ai-cloud/project-id"
EXECUTION_ID_LABEL = "mini-ai-cloud/execution-id"
WORKER_ID_LABEL = "mini-ai-cloud/worker-id"
WORKER_SESSION_ID_LABEL = "mini-ai-cloud/worker-session-id"
CLUSTER_ID_LABEL = "mini-ai-cloud/cluster-id"
SPEC_HASH_LABEL = "mini-ai-cloud/spec-hash"
MANAGED_LABEL = "mini-ai-cloud/managed"
RESOURCE_KIND_LABEL = "mini-ai-cloud/resource-kind"
ACCELERATOR_VENDOR_LABEL = "mini-ai-cloud/accelerator-vendor"
ACCELERATOR_KIND_LABEL = "mini-ai-cloud/accelerator-kind"
RUNTIME_PROFILE_ID_LABEL = "mini-ai-cloud/runtime-profile-id"
RUNTIME_PROFILE_VERSION_LABEL = "mini-ai-cloud/runtime-profile-version"
RUNTIME_PROFILE_DIGEST_LABEL = "mini-ai-cloud/runtime-profile-digest"
RUNTIME_PROFILE_DIGEST_ANNOTATION = "mini-ai-cloud/runtime-profile-digest"
ACCELERATOR_RESOURCE_ANNOTATION = "mini-ai-cloud/accelerator-resource"
ACCELERATOR_COUNT_ANNOTATION = "mini-ai-cloud/accelerator-count"
ALLOCATION_AUTHORITY_ANNOTATION = "mini-ai-cloud/allocation-authority"
CONTROLLER_SESSION_ANNOTATION = "mini-ai-cloud/controller-session-id"
NETWORK_POLICY_RESOURCE_KIND = "task-deny-all"
BATCH_JOB_RESOURCE_KIND = "batch-job"
TASK_CONTAINER_NAME = "task"
_LEGACY_PROJECT_ID = uuid.UUID(int=0)
_LEGACY_WORKER_SESSION_ID = uuid.UUID(int=0)
_LABEL_VALUE = re.compile(r"^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
_MAX_REASON_LENGTH = 512


@dataclass(frozen=True, slots=True)
class KubernetesRecoveryConflict:
    resource_name: str
    reason: str


class KubernetesRuntimeError(RuntimeFailure):
    """A Kubernetes operation failed or an existing Pod failed fencing checks."""


class KubernetesImagePullFailed(KubernetesRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.IMAGE_PULL_FAILED,
        )


class KubernetesContainerStartFailed(KubernetesRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.INFRA_ERROR,
            error_code=ErrorCode.CONTAINER_START_FAILED,
        )


class KubernetesGpuUnavailable(KubernetesRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.GPU_UNAVAILABLE,
        )


class KubernetesOomKilled(KubernetesRuntimeError):
    def __init__(self, message: str, *, exit_code: int = 137) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.RESOURCE_ERROR,
            error_code=ErrorCode.OOM_KILLED,
            exit_code=exit_code,
        )


class KubernetesArtifactsUnsupported(KubernetesRuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            error_category=ErrorCategory.USER_ERROR,
            error_code=ErrorCode.KUBERNETES_ARTIFACTS_UNSUPPORTED,
        )


class KubernetesDeadlineExceeded(TimeoutError):
    """The Kubernetes Job reached its active deadline."""


class KubernetesRuntime:
    """ComputeRuntime backed by one fenced ``batch/v1`` Job per execution."""

    runtime_type = "kubernetes"

    def __init__(
        self,
        *,
        namespace: str,
        cluster_id: str = "mini-ai-cloud-local",
        app_env: str = "development",
        node_name: str | None = None,
        cleanup_grace_seconds: int = 30,
        kubeconfig: str | None = None,
        in_cluster: bool = False,
        service_account_name: str | None = None,
        image_pull_secrets: Sequence[str] = (),
        poll_interval: float = 0.25,
        api: Any | None = None,
        core_api: Any | None = None,
        networking_api: Any | None = None,
        runtime_profile_catalog: RuntimeProfileCatalog | None = None,
    ) -> None:
        if not namespace.strip():
            raise ValueError("namespace must not be blank")
        if not _LABEL_VALUE.fullmatch(cluster_id.strip()):
            raise ValueError("cluster_id must be a Kubernetes label value")
        if app_env not in {"development", "test", "production"}:
            raise ValueError("app_env must be development, test, or production")
        if cleanup_grace_seconds < 0:
            raise ValueError("cleanup_grace_seconds must not be negative")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")
        normalized_service_account = (
            service_account_name.strip()
            if service_account_name is not None and service_account_name.strip()
            else None
        )
        if normalized_service_account is not None:
            validate_kubernetes_dns_subdomain(
                normalized_service_account,
                field_name="service_account_name",
            )
        normalized_image_pull_secrets: list[str] = []
        for item in image_pull_secrets:
            normalized = item.strip()
            if not normalized or normalized in normalized_image_pull_secrets:
                continue
            validate_kubernetes_dns_subdomain(
                normalized,
                field_name="image_pull_secrets",
            )
            normalized_image_pull_secrets.append(normalized)
        self.namespace = namespace.strip()
        self.cluster_id = cluster_id.strip()
        self.app_env = app_env
        self.node_name = node_name.strip() if node_name is not None and node_name.strip() else None
        self.cleanup_grace_seconds = cleanup_grace_seconds
        self.kubeconfig = kubeconfig
        self.in_cluster = in_cluster
        self.service_account_name = normalized_service_account
        self.image_pull_secrets = tuple(normalized_image_pull_secrets)
        self.poll_interval = poll_interval
        self._api = api
        self._core_api = core_api
        self._networking_api = networking_api
        self._owns_api = api is None
        self.runtime_profile_catalog = runtime_profile_catalog
        self._client_lock = asyncio.Lock()
        self._network_policy_labels: dict[str, dict[str, str]] = {}
        self._recovery_conflicts: list[KubernetesRecoveryConflict] = []

    @property
    def recovery_conflicts(self) -> Sequence[KubernetesRecoveryConflict]:
        return tuple(self._recovery_conflicts)

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        api = await self._ensure_api()
        job = self._build_job(spec)
        job_name = str(job.metadata.name)
        labels = dict(job.metadata.labels or {})
        if not spec.network_enabled:
            await self._ensure_network_policy(spec, labels)
        # The policy intentionally survives a failed Pod create. A concurrent,
        # idempotent prepare may already have adopted it, so rollback here could
        # remove isolation from that execution. An empty fenced selector is safe.
        try:
            created = await api.create_namespaced_job(
                namespace=self.namespace,
                body=job,
            )
        except ApiException as exc:
            if exc.status != 409:
                detail = self._operation_error("create", job_name, exc)
                raise KubernetesContainerStartFailed(str(detail)) from exc
            try:
                created = await api.read_namespaced_job(
                    name=job_name,
                    namespace=self.namespace,
                )
            except ApiException as read_exc:
                raise self._operation_error("adopt", job_name, read_exc) from read_exc

        controller_session_id = spec.worker_session_id or _LEGACY_WORKER_SESSION_ID
        self._validate_adopted_job(
            created,
            expected_labels=labels,
            expected_spec=spec,
            expected_controller_session_id=controller_session_id,
        )
        handle = RuntimeHandle(
            runtime_type=self.runtime_type,
            resource_kind="job",
            object_id=job_name,
            display_id=job_name,
            native=created,
            namespace=self.namespace,
            resource_uid=_required_uid(created, resource="Job"),
            resource_version=_required_resource_version(created, resource="Job"),
            controller_session_id=controller_session_id,
            spec_hash=labels[SPEC_HASH_LABEL],
            labels=MappingProxyType(labels),
        )
        pod = await self._controlled_pod(handle, required=False)
        if pod is not None:
            self._bind_observed_pod(handle, pod)
        return handle

    async def start(self, handle: RuntimeHandle) -> None:
        """Job creation starts execution; this verifies the fenced Job still exists."""

        self._validate_handle(handle)
        await self._read_validated_job(handle, operation="start")
        pod = await self._controlled_pod(handle, required=False)
        if pod is not None:
            self._bind_observed_pod(handle, pod)

    async def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        self._validate_handle(handle)
        api = await self._ensure_core_api()
        source: object | None = None
        final_read = False
        try:
            while True:
                pod = await self._controlled_pod(handle, required=False)
                if pod is None:
                    job = await self._read_validated_job(handle, operation="stream logs for")
                    terminal = _job_terminal_state(getattr(job, "status", None))
                    if terminal is not None:
                        if ready is not None:
                            ready.set()
                        if terminal == "deadline":
                            raise KubernetesDeadlineExceeded(
                                f"Kubernetes Job {handle.object_id} exceeded its active deadline"
                            )
                        if terminal == "failed":
                            raise KubernetesContainerStartFailed(
                                f"Kubernetes Job {handle.object_id} failed before creating its Pod"
                            )
                        return
                    await asyncio.sleep(self.poll_interval)
                    continue
                self._bind_observed_pod(handle, pod)
                try:
                    source = await api.read_namespaced_pod_log(
                        name=handle.observation.pod_name,
                        namespace=self.namespace,
                        container=TASK_CONTAINER_NAME,
                        follow=not final_read,
                        _preload_content=False,
                    )
                    response_error = _log_response_error(source)
                    if response_error is not None:
                        await _close_log_source(source)
                        source = None
                        raise response_error
                    break
                except ApiException as exc:
                    if exc.status != 400:
                        raise
                    pod = await api.read_namespaced_pod_status(
                        name=handle.observation.pod_name,
                        namespace=self.namespace,
                    )
                    self._validate_controlled_pod(pod, handle)
                    status = getattr(pod, "status", None)
                    failure = _pod_runtime_failure(
                        status, handle.observation.pod_name or "<unknown>"
                    )
                    if failure is not None:
                        raise failure from exc
                    phase = getattr(status, "phase", None)
                    if phase in {"Succeeded", "Failed"}:
                        if final_read:
                            if ready is not None:
                                ready.set()
                            return
                        final_read = True
                    await asyncio.sleep(self.poll_interval)

            if ready is not None:
                ready.set()
            async for chunk in _iter_log_chunks(source):
                if chunk:
                    yield RuntimeLog(stream="stdout", content=chunk)
        except asyncio.CancelledError:
            raise
        except ApiException as exc:
            if ready is not None:
                ready.set()
            raise self._operation_error("stream logs for", handle.object_id, exc) from exc
        except Exception:
            if ready is not None:
                ready.set()
            raise
        finally:
            if source is not None:
                await _close_log_source(source)

    async def wait(self, handle: RuntimeHandle) -> int:
        self._validate_handle(handle)
        while True:
            job = await self._read_validated_job(handle, operation="wait for")
            terminal = _job_terminal_state(getattr(job, "status", None))
            if terminal == "deadline":
                raise KubernetesDeadlineExceeded(
                    f"Kubernetes Job {handle.object_id} exceeded its active deadline"
                )
            pod = await self._controlled_pod(handle, required=False)
            status: object | None = None
            if pod is not None:
                self._bind_observed_pod(handle, pod)
                status = getattr(pod, "status", None)
                failure = _pod_runtime_failure(status, handle.observation.pod_name or "<unknown>")
                if failure is not None:
                    raise failure
            if terminal == "complete":
                return _pod_exit_code(status, default=0)
            if terminal == "failed":
                return _pod_exit_code(status, default=1)
            await asyncio.sleep(self.poll_interval)

    async def stop(self, handle: RuntimeHandle) -> None:
        await self._delete(handle, grace_seconds=self.cleanup_grace_seconds)

    async def cleanup(self, handle: RuntimeHandle) -> None:
        await self._delete(handle, grace_seconds=0)
        await self._delete_network_policy(handle)

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
        self._core_api = None
        self._networking_api = None
        self._network_policy_labels.clear()

    async def _delete(self, handle: RuntimeHandle, *, grace_seconds: int) -> None:
        self._validate_handle(handle)
        api = await self._ensure_api()
        try:
            job = await api.read_namespaced_job(name=handle.object_id, namespace=self.namespace)
        except ApiException as exc:
            if exc.status == 404:
                return
            raise self._operation_error("inspect before deleting", handle.object_id, exc) from exc
        self._validate_adopted_job(
            job,
            expected_labels=handle.labels,
            expected_controller_session_id=handle.controller_session_id,
        )
        uid = _required_uid(job, resource="Job")
        if handle.resource_uid is None or uid != handle.resource_uid:
            raise KubernetesRuntimeError(
                f"refusing to delete Kubernetes Job {handle.object_id}: UID fence mismatch"
            )
        resource_version = _required_resource_version(job, resource="Job")
        body = client.V1DeleteOptions(
            grace_period_seconds=grace_seconds,
            preconditions=client.V1Preconditions(
                uid=uid,
                resource_version=resource_version,
            ),
            propagation_policy="Foreground",
        )
        try:
            await api.delete_namespaced_job(
                name=handle.object_id,
                namespace=self.namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise self._operation_error("delete", handle.object_id, exc) from exc

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
            self._api = client.BatchV1Api()
        return self._api

    async def _ensure_core_api(self) -> Any:
        if self._core_api is not None:
            return self._core_api
        batch_api = await self._ensure_api()
        async with self._client_lock:
            if self._core_api is None:
                api_client = getattr(batch_api, "api_client", None)
                self._core_api = (
                    client.CoreV1Api(api_client=api_client)
                    if api_client is not None
                    else client.CoreV1Api()
                )
        return self._core_api

    async def _ensure_networking_api(self) -> Any:
        if self._networking_api is not None:
            return self._networking_api
        core_api = await self._ensure_core_api()
        async with self._client_lock:
            if self._networking_api is None:
                api_client = getattr(core_api, "api_client", None)
                if api_client is None:
                    self._networking_api = client.NetworkingV1Api()
                else:
                    self._networking_api = client.NetworkingV1Api(api_client=api_client)
        return self._networking_api

    async def list_managed(self, *, worker_id: str) -> Sequence[RuntimeHandle]:
        """List restart-adoptable Jobs, quarantining malformed resources."""

        api = await self._ensure_api()
        self._recovery_conflicts = []
        selector = ",".join(
            (
                f"{MANAGED_LABEL}=true",
                f"{RESOURCE_KIND_LABEL}={BATCH_JOB_RESOURCE_KIND}",
                f"{CLUSTER_ID_LABEL}={self.cluster_id}",
                f"{WORKER_ID_LABEL}={worker_id}",
            )
        )
        try:
            observed = await api.list_namespaced_job(
                namespace=self.namespace,
                label_selector=selector,
            )
        except ApiException as exc:
            raise self._operation_error("list managed", "*", exc) from exc
        handles: list[RuntimeHandle] = []
        for job in getattr(observed, "items", None) or []:
            name = str(getattr(getattr(job, "metadata", None), "name", "") or "<unknown>")
            try:
                labels = dict(getattr(getattr(job, "metadata", None), "labels", None) or {})
                self._validate_adopted_job(job, expected_labels=labels)
                controller_session_id = _controller_session_id(job)
                handle = RuntimeHandle(
                    runtime_type=self.runtime_type,
                    resource_kind="job",
                    object_id=name,
                    display_id=name,
                    native=job,
                    namespace=self.namespace,
                    resource_uid=_required_uid(job, resource="Job"),
                    resource_version=_required_resource_version(job, resource="Job"),
                    controller_session_id=controller_session_id,
                    spec_hash=labels[SPEC_HASH_LABEL],
                    labels=MappingProxyType(labels),
                )
                pod = await self._controlled_pod(handle, required=False)
                if pod is not None:
                    self._bind_observed_pod(handle, pod)
                handles.append(handle)
            except KubernetesRuntimeError as exc:
                self._recovery_conflicts.append(
                    KubernetesRecoveryConflict(resource_name=name, reason=_bounded(str(exc)))
                )
        return tuple(handles)

    async def transfer_controller(
        self,
        handle: RuntimeHandle,
        *,
        controller_session_id: uuid.UUID,
    ) -> RuntimeHandle:
        """CAS-transfer the mutable Job controller fence to a new Worker session."""

        self._validate_handle(handle)
        api = await self._ensure_api()
        patched: object | None = None
        for attempt in range(3):
            job = await self._read_validated_job(
                handle,
                operation="inspect before controller transfer",
            )
            resource_version = _required_resource_version(job, resource="Job")
            body = {
                "metadata": {
                    "resourceVersion": resource_version,
                    "annotations": {
                        CONTROLLER_SESSION_ANNOTATION: str(controller_session_id),
                    },
                }
            }
            try:
                patched = await api.patch_namespaced_job(
                    name=handle.object_id,
                    namespace=self.namespace,
                    body=body,
                )
                break
            except ApiException as exc:
                if exc.status == 409 and attempt < 2:
                    continue
                raise self._operation_error(
                    "CAS-transfer controller for", handle.object_id, exc
                ) from exc
        assert patched is not None
        self._validate_adopted_job(
            patched,
            expected_labels=handle.labels,
            expected_controller_session_id=controller_session_id,
        )
        uid = _required_uid(patched, resource="Job")
        if uid != handle.resource_uid:
            raise KubernetesRuntimeError(
                f"refusing controller transfer for Kubernetes Job {handle.object_id}: "
                "UID fence mismatch"
            )
        return replace(
            handle,
            native=patched,
            resource_version=_required_resource_version(patched, resource="Job"),
            controller_session_id=controller_session_id,
        )

    async def _read_validated_job(self, handle: RuntimeHandle, *, operation: str) -> object:
        api = await self._ensure_api()
        try:
            job = await api.read_namespaced_job(
                name=handle.object_id,
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                raise KubernetesRuntimeError(
                    f"Kubernetes Job {handle.object_id} is missing",
                    error_category=ErrorCategory.INFRA_ERROR,
                    error_code=ErrorCode.WORKER_LOST,
                ) from exc
            raise self._operation_error(operation, handle.object_id, exc) from exc
        self._validate_adopted_job(
            job,
            expected_labels=handle.labels,
            expected_controller_session_id=handle.controller_session_id,
        )
        uid = _required_uid(job, resource="Job")
        if handle.resource_uid is None or uid != handle.resource_uid:
            raise KubernetesRuntimeError(
                f"Kubernetes Job {handle.object_id} UID changed for an active execution"
            )
        return job

    async def _controlled_pod(
        self,
        handle: RuntimeHandle,
        *,
        required: bool,
    ) -> object | None:
        self._validate_handle(handle)
        await self._read_validated_job(handle, operation="inspect controlled Pods for")
        core_api = await self._ensure_core_api()
        selector = ",".join(
            f"{key}={value}"
            for key, value in handle.labels.items()
            if key
            in {
                MANAGED_LABEL,
                RESOURCE_KIND_LABEL,
                TASK_ID_LABEL,
                EXECUTION_ID_LABEL,
                SPEC_HASH_LABEL,
            }
        )
        try:
            listed = await core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=selector,
            )
        except ApiException as exc:
            raise self._operation_error("list controlled Pods for", handle.object_id, exc) from exc
        pods = [
            pod
            for pod in (getattr(listed, "items", None) or [])
            if _is_controlled_by_job(pod, handle.resource_uid)
        ]
        if len(pods) > 1:
            raise KubernetesRuntimeError(
                f"Kubernetes Job {handle.object_id} has multiple controlled Pods"
            )
        if not pods:
            if required:
                raise KubernetesRuntimeError(
                    f"Kubernetes Job {handle.object_id} has no controlled Pod"
                )
            return None
        pod = pods[0]
        self._validate_controlled_pod(pod, handle)
        return pod

    def _validate_controlled_pod(self, pod: object, handle: RuntimeHandle) -> None:
        labels = dict(getattr(getattr(pod, "metadata", None), "labels", None) or {})
        mismatched = {
            key: (labels.get(key), value)
            for key, value in handle.labels.items()
            if labels.get(key) != value
        }
        if mismatched or not _is_controlled_by_job(pod, handle.resource_uid):
            raise KubernetesRuntimeError(
                f"refusing to use Pod for Kubernetes Job {handle.object_id}: "
                "execution fence mismatch"
            )
        uid = _required_uid(pod, resource="Pod")
        if handle.observation.pod_uid is not None and handle.observation.pod_uid != uid:
            raise KubernetesRuntimeError(
                f"Kubernetes Job {handle.object_id} Pod UID changed for an active execution"
            )

    @staticmethod
    def _bind_observed_pod(handle: RuntimeHandle, pod: object) -> None:
        metadata = getattr(pod, "metadata", None)
        handle.observation.pod_name = str(getattr(metadata, "name", "") or "")
        handle.observation.pod_uid = _required_uid(pod, resource="Pod")

    async def _ensure_network_policy(
        self,
        spec: ExecutionSpec,
        execution_labels: Mapping[str, str],
    ) -> None:
        api = await self._ensure_networking_api()
        policy = self._build_network_policy(spec, execution_labels)
        policy_name = str(policy.metadata.name)
        try:
            observed = await api.create_namespaced_network_policy(
                namespace=self.namespace,
                body=policy,
            )
        except ApiException as exc:
            if exc.status != 409:
                raise self._network_policy_operation_error("create", policy_name, exc) from exc
            try:
                observed = await api.read_namespaced_network_policy(
                    name=policy_name,
                    namespace=self.namespace,
                )
            except ApiException as read_exc:
                raise self._network_policy_operation_error(
                    "adopt", policy_name, read_exc
                ) from read_exc
        self._validate_network_policy(observed, expected_labels=execution_labels)
        self._network_policy_labels[policy_name] = dict(execution_labels)

    async def _delete_network_policy(self, handle: RuntimeHandle) -> None:
        self._validate_handle(handle)
        api = await self._ensure_networking_api()
        policy_name = self.network_policy_name(handle.object_id)
        expected_labels = self._network_policy_labels.get(policy_name) or dict(handle.labels)
        try:
            policy = await api.read_namespaced_network_policy(
                name=policy_name,
                namespace=self.namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                self._network_policy_labels.pop(policy_name, None)
                return
            raise self._network_policy_operation_error("inspect", policy_name, exc) from exc

        self._validate_network_policy(policy, expected_labels=expected_labels)
        metadata = getattr(policy, "metadata", None)
        uid = getattr(metadata, "uid", None)
        kwargs: dict[str, object] = {
            "name": policy_name,
            "namespace": self.namespace,
            "propagation_policy": "Background",
        }
        if uid:
            kwargs["body"] = client.V1DeleteOptions(
                preconditions=client.V1Preconditions(uid=str(uid))
            )
        try:
            await api.delete_namespaced_network_policy(**kwargs)
        except ApiException as exc:
            if exc.status != 404:
                raise self._network_policy_operation_error("delete", policy_name, exc) from exc
        self._network_policy_labels.pop(policy_name, None)

    def _build_network_policy(
        self,
        spec: ExecutionSpec,
        execution_labels: Mapping[str, str],
    ) -> client.V1NetworkPolicy:
        return client.V1NetworkPolicy(
            metadata=client.V1ObjectMeta(
                name=self.network_policy_name(self.job_name(spec.task_id, spec.execution_id)),
                labels={
                    **execution_labels,
                    RESOURCE_KIND_LABEL: NETWORK_POLICY_RESOURCE_KIND,
                },
            ),
            spec=client.V1NetworkPolicySpec(
                pod_selector=client.V1LabelSelector(match_labels=dict(execution_labels)),
                policy_types=["Ingress", "Egress"],
                ingress=[],
                egress=[],
            ),
        )

    def _validate_network_policy(
        self,
        policy: object,
        *,
        expected_labels: Mapping[str, str] | None = None,
    ) -> None:
        metadata = getattr(policy, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        if expected_labels is None:
            expected_labels = {
                key: str(labels.get(key, ""))
                for key in (
                    TASK_ID_LABEL,
                    PROJECT_ID_LABEL,
                    EXECUTION_ID_LABEL,
                    WORKER_ID_LABEL,
                    WORKER_SESSION_ID_LABEL,
                    CLUSTER_ID_LABEL,
                    SPEC_HASH_LABEL,
                    MANAGED_LABEL,
                )
            }
        if (
            any(not value for value in expected_labels.values())
            or expected_labels.get(MANAGED_LABEL) != "true"
        ):
            raise KubernetesRuntimeError(
                "refusing to manage NetworkPolicy without complete execution fencing labels"
            )
        policy_labels = {
            **dict(expected_labels),
            RESOURCE_KIND_LABEL: NETWORK_POLICY_RESOURCE_KIND,
        }
        mismatched = {
            key: (labels.get(key), value)
            for key, value in policy_labels.items()
            if labels.get(key) != value
        }
        if mismatched:
            raise KubernetesRuntimeError(
                "refusing to manage NetworkPolicy with mismatched execution labels"
            )

        policy_spec = getattr(policy, "spec", None)
        selector = getattr(getattr(policy_spec, "pod_selector", None), "match_labels", None) or {}
        policy_types = set(getattr(policy_spec, "policy_types", None) or [])
        ingress = getattr(policy_spec, "ingress", None)
        egress = getattr(policy_spec, "egress", None)
        if (
            selector != dict(expected_labels)
            or policy_types != {"Ingress", "Egress"}
            or ingress not in (None, [])
            or egress not in (None, [])
        ):
            raise KubernetesRuntimeError(
                "refusing to manage NetworkPolicy without exact deny-all isolation"
            )

    def _build_job(self, spec: ExecutionSpec) -> client.V1Job:
        profile = self._resolve_runtime_profile(spec)
        if spec.mounts and self.app_env == "production":
            raise KubernetesArtifactsUnsupported(
                "production Kubernetes tasks with artifacts are unsupported"
            )
        if spec.mounts and profile is not None:
            raise KubernetesArtifactsUnsupported(
                "legacy Kubernetes artifact mounts cannot be combined with Runtime Profiles"
            )
        labels = self._execution_labels(spec)
        template = self._build_pod_template(spec, profile=profile, labels=labels)
        annotations = self._profile_annotations(spec, profile)
        annotations[CONTROLLER_SESSION_ANNOTATION] = str(
            spec.worker_session_id or _LEGACY_WORKER_SESSION_ID
        )
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=self.job_name(spec.task_id, spec.execution_id),
                labels=dict(labels),
                annotations=annotations,
            ),
            spec=client.V1JobSpec(
                active_deadline_seconds=max(1, spec.timeout_seconds),
                backoff_limit=0,
                completions=1,
                parallelism=1,
                template=template,
            ),
        )
        spec_hash = _job_contract_hash(job)
        job.metadata.labels[SPEC_HASH_LABEL] = spec_hash
        assert job.spec.template.metadata is not None
        job.spec.template.metadata.labels[SPEC_HASH_LABEL] = spec_hash
        return job

    def _build_pod_template(
        self,
        spec: ExecutionSpec,
        *,
        profile: RuntimeProfile | None,
        labels: Mapping[str, str],
    ) -> client.V1PodTemplateSpec:
        resources: dict[str, str] = {
            "cpu": f"{max(1, round(spec.cpu_limit * 1000))}m",
            "memory": f"{spec.memory_limit_mb}Mi",
        }
        if profile is not None:
            resources[profile.kubernetes.resource_name] = str(spec.gpu_count)
        elif spec.gpu_count > 0:
            raise KubernetesGpuUnavailable(
                "accelerator Kubernetes tasks require an exact Runtime Profile"
            )
        annotations = self._profile_annotations(spec, profile)
        volumes: list[client.V1Volume] = []
        volume_mounts: list[client.V1VolumeMount] = []
        container_paths: set[str] = set()
        for index, mount in enumerate(spec.mounts):
            source = Path(mount.host_path)
            try:
                resolved_source = source.resolve(strict=True)
            except OSError as exc:
                raise KubernetesContainerStartFailed(
                    f"artifact mount source is unavailable: {source}"
                ) from exc
            target = PurePosixPath(mount.container_path)
            if (
                not target.is_absolute()
                or ".." in target.parts
                or str(target) != mount.container_path
                or mount.container_path in container_paths
                or not resolved_source.is_file()
            ):
                raise KubernetesContainerStartFailed("artifact mount specification is invalid")
            container_paths.add(mount.container_path)
            volume_name = f"artifact-{index}"
            volumes.append(
                client.V1Volume(
                    name=volume_name,
                    host_path=client.V1HostPathVolumeSource(
                        path=str(resolved_source),
                        type="File",
                    ),
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(
                    name=volume_name,
                    mount_path=mount.container_path,
                    read_only=mount.read_only,
                )
            )
        environment = [
            client.V1EnvVar(name=name, value=value)
            for name, value in sorted(spec.environment.items())
        ]
        if profile is not None and profile.kubernetes.device_visibility is not None:
            visibility = profile.kubernetes.device_visibility
            environment.append(
                client.V1EnvVar(
                    name=visibility.environment_name,
                    value_from=client.V1EnvVarSource(
                        field_ref=client.V1ObjectFieldSelector(
                            api_version="v1",
                            field_path=(f"metadata.annotations['{visibility.annotation_key}']"),
                        )
                    ),
                )
            )
        affinity: client.V1Affinity | None = None
        tolerations: list[client.V1Toleration] | None = None
        if profile is not None:
            if profile.kubernetes.node_affinity:
                affinity = client.V1Affinity(
                    node_affinity=client.V1NodeAffinity(
                        required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                            node_selector_terms=[
                                client.V1NodeSelectorTerm(
                                    match_expressions=[
                                        client.V1NodeSelectorRequirement(
                                            key=requirement.key,
                                            operator=requirement.operator,
                                            values=(
                                                list(requirement.values)
                                                if requirement.values
                                                else None
                                            ),
                                        )
                                        for requirement in profile.kubernetes.node_affinity
                                    ]
                                )
                            ]
                        )
                    )
                )
            tolerations = [
                client.V1Toleration(
                    key=item.key,
                    operator=item.operator,
                    value=item.value,
                    effect=item.effect,
                )
                for item in profile.kubernetes.tolerations
            ]
        container = client.V1Container(
            name="task",
            image=spec.image,
            command=list(spec.command),
            env=environment,
            resources=client.V1ResourceRequirements(
                requests=dict(resources),
                limits=dict(resources),
            ),
            security_context=client.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=client.V1Capabilities(drop=["ALL"]),
                privileged=False,
                read_only_root_filesystem=True,
            ),
            volume_mounts=volume_mounts or None,
        )
        return client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels=dict(labels),
                annotations=annotations or None,
            ),
            spec=client.V1PodSpec(
                affinity=affinity,
                automount_service_account_token=False,
                containers=[container],
                node_name=(self.node_name if spec.mounts and profile is None else None),
                node_selector=(
                    dict(profile.kubernetes.node_selector) if profile is not None else None
                ),
                restart_policy="Never",
                service_account_name=self.service_account_name,
                image_pull_secrets=(
                    [client.V1LocalObjectReference(name=name) for name in self.image_pull_secrets]
                    if self.image_pull_secrets
                    else None
                ),
                runtime_class_name=(
                    profile.kubernetes.runtime_class_name if profile is not None else None
                ),
                scheduler_name=(profile.kubernetes.scheduler_name if profile is not None else None),
                security_context=client.V1PodSecurityContext(
                    run_as_non_root=True,
                    # Official Python and Alpine images do not declare USER. An
                    # explicit unprivileged identity keeps those images usable
                    # while still making kubelet enforce the non-root contract.
                    run_as_user=65532,
                    run_as_group=65532,
                    seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
                ),
                tolerations=tolerations,
                volumes=volumes or None,
            ),
        )

    def _validate_adopted_job(
        self,
        job: object,
        *,
        expected_labels: Mapping[str, str],
        expected_spec: ExecutionSpec | None = None,
        expected_controller_session_id: uuid.UUID | None = None,
    ) -> None:
        metadata = getattr(job, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        controller_session_id = _controller_session_id(job)
        mismatched = {
            key: (labels.get(key), value)
            for key, value in expected_labels.items()
            if labels.get(key) != value
        }
        required = {
            TASK_ID_LABEL,
            PROJECT_ID_LABEL,
            EXECUTION_ID_LABEL,
            WORKER_ID_LABEL,
            WORKER_SESSION_ID_LABEL,
            CLUSTER_ID_LABEL,
            MANAGED_LABEL,
            RESOURCE_KIND_LABEL,
            SPEC_HASH_LABEL,
            RUNTIME_PROFILE_DIGEST_LABEL,
        }
        if (
            mismatched
            or any(not labels.get(key) for key in required)
            or labels.get(MANAGED_LABEL) != "true"
            or labels.get(RESOURCE_KIND_LABEL) != BATCH_JOB_RESOURCE_KIND
            or labels.get(CLUSTER_ID_LABEL) != self.cluster_id
        ):
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job with mismatched execution labels"
            )
        if (
            expected_controller_session_id is not None
            and controller_session_id != expected_controller_session_id
        ):
            raise KubernetesRuntimeError(
                "refusing to manage Kubernetes Job with mismatched controller session"
            )
        job_spec = getattr(job, "spec", None)
        template_labels = (
            getattr(getattr(getattr(job_spec, "template", None), "metadata", None), "labels", None)
            or {}
        )
        if any(template_labels.get(key) != value for key, value in labels.items()):
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job with mismatched Pod template labels"
            )
        self._validate_job_baseline(job)
        observed_hash = _job_contract_hash(job)
        if labels.get(SPEC_HASH_LABEL) != observed_hash:
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job with mismatched spec hash"
            )
        if expected_spec is not None:
            expected_job = self._build_job(expected_spec)
            if _job_contract(job) != _job_contract(expected_job):
                raise KubernetesRuntimeError(
                    "refusing to adopt Kubernetes Job with mismatched launch spec"
                )

    def _validate_job_baseline(self, job: object) -> None:
        job_spec = getattr(job, "spec", None)
        template = getattr(job_spec, "template", None)
        pod_spec = getattr(template, "spec", None)
        containers = getattr(pod_spec, "containers", None) or []
        if (
            getattr(job_spec, "backoff_limit", None) != 0
            or getattr(job_spec, "completions", None) != 1
            or getattr(job_spec, "parallelism", None) != 1
            or int(getattr(job_spec, "active_deadline_seconds", 0) or 0) < 1
            or getattr(pod_spec, "restart_policy", None) != "Never"
            or len(containers) != 1
            or getattr(containers[0], "name", None) != TASK_CONTAINER_NAME
        ):
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job outside the batch execution baseline"
            )
        container = containers[0]
        resources = getattr(container, "resources", None)
        requests = _string_mapping(getattr(resources, "requests", None))
        limits = _string_mapping(getattr(resources, "limits", None))
        if requests != limits or not {"cpu", "memory"}.issubset(requests):
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job without explicit equal requests and limits"
            )
        pod_security = getattr(pod_spec, "security_context", None)
        container_security = getattr(container, "security_context", None)
        if (
            getattr(pod_spec, "automount_service_account_token", None) is not False
            or getattr(pod_security, "run_as_non_root", None) is not True
            or getattr(getattr(pod_security, "seccomp_profile", None), "type", None)
            != "RuntimeDefault"
            or getattr(container_security, "allow_privilege_escalation", None) is not False
            or getattr(container_security, "privileged", None) is not False
            or getattr(container_security, "read_only_root_filesystem", None) is not True
            or list(getattr(getattr(container_security, "capabilities", None), "drop", None) or [])
            != ["ALL"]
        ):
            raise KubernetesRuntimeError(
                "refusing to adopt Kubernetes Job outside the security baseline"
            )
        volumes = getattr(pod_spec, "volumes", None) or []
        has_host_path = any(getattr(volume, "host_path", None) is not None for volume in volumes)
        if self.app_env == "production" and (
            getattr(pod_spec, "node_name", None) is not None or has_host_path
        ):
            raise KubernetesRuntimeError(
                "refusing to adopt production Kubernetes Job with nodeName or hostPath"
            )

    def _execution_labels(self, spec: ExecutionSpec) -> dict[str, str]:
        labels = {
            TASK_ID_LABEL: str(spec.task_id),
            PROJECT_ID_LABEL: str(spec.project_id or _LEGACY_PROJECT_ID),
            EXECUTION_ID_LABEL: str(spec.execution_id),
            WORKER_ID_LABEL: spec.worker_id,
            WORKER_SESSION_ID_LABEL: str(spec.worker_session_id or _LEGACY_WORKER_SESSION_ID),
            CLUSTER_ID_LABEL: self.cluster_id,
            MANAGED_LABEL: "true",
            RESOURCE_KIND_LABEL: BATCH_JOB_RESOURCE_KIND,
            RUNTIME_PROFILE_DIGEST_LABEL: (
                _profile_digest_label(spec.runtime_profile_digest)
                if spec.runtime_profile_digest is not None
                else "none"
            ),
        }
        if spec.selected_vendor is not None:
            labels[ACCELERATOR_VENDOR_LABEL] = spec.selected_vendor
        if spec.selected_kind is not None:
            labels[ACCELERATOR_KIND_LABEL] = spec.selected_kind
        if spec.runtime_profile_id is not None:
            labels[RUNTIME_PROFILE_ID_LABEL] = spec.runtime_profile_id
        if spec.runtime_profile_version is not None:
            labels[RUNTIME_PROFILE_VERSION_LABEL] = spec.runtime_profile_version
        for key, value in labels.items():
            if not _LABEL_VALUE.fullmatch(value):
                raise KubernetesRuntimeError(
                    f"Kubernetes execution label {key} is not a valid label value"
                )
        return labels

    def _resolve_runtime_profile(self, spec: ExecutionSpec) -> RuntimeProfile | None:
        snapshot = (
            spec.selected_vendor,
            spec.selected_kind,
            spec.runtime_profile_id,
            spec.runtime_profile_version,
            spec.runtime_profile_digest,
            spec.allocation_authority,
        )
        if not any(value is not None for value in snapshot):
            return None
        if spec.gpu_count <= 0 or not all(value is not None for value in snapshot):
            raise KubernetesGpuUnavailable("incomplete Kubernetes accelerator snapshot")
        if spec.allocation_authority != AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value:
            raise KubernetesGpuUnavailable(
                "vendor-aware Kubernetes tasks require kubernetes_device_plugin authority"
            )
        if self.runtime_profile_catalog is None:
            raise KubernetesGpuUnavailable("runtime profile catalog is unavailable")
        assert spec.runtime_profile_id is not None
        assert spec.runtime_profile_version is not None
        assert spec.runtime_profile_digest is not None
        try:
            profile = self.runtime_profile_catalog.load_exact(
                profile_id=spec.runtime_profile_id,
                profile_version=spec.runtime_profile_version,
                semantic_digest=spec.runtime_profile_digest,
            )
        except RuntimeProfileCompatibilityError as exc:
            raise KubernetesGpuUnavailable(f"runtime profile is unavailable: {exc}") from exc
        if (
            profile.vendor.value != spec.selected_vendor
            or profile.kind.value != spec.selected_kind
            or profile.allocation_authority.value != spec.allocation_authority
        ):
            raise KubernetesGpuUnavailable(
                "runtime profile does not match the immutable accelerator snapshot"
            )
        return profile

    @staticmethod
    def _profile_annotations(
        spec: ExecutionSpec,
        profile: RuntimeProfile | None,
    ) -> dict[str, str]:
        if profile is None:
            return {}
        return {
            RUNTIME_PROFILE_DIGEST_ANNOTATION: profile.semantic_digest(),
            ACCELERATOR_RESOURCE_ANNOTATION: profile.kubernetes.resource_name,
            ACCELERATOR_COUNT_ANNOTATION: str(spec.gpu_count),
            ALLOCATION_AUTHORITY_ANNOTATION: profile.allocation_authority.value,
        }

    def _validate_handle(self, handle: RuntimeHandle) -> None:
        if handle.runtime_type != self.runtime_type or handle.resource_kind != "job":
            raise KubernetesRuntimeError(
                f"cannot use {handle.runtime_type!r} handle with Kubernetes runtime"
            )
        if (
            handle.namespace != self.namespace
            or handle.resource_uid is None
            or handle.resource_version is None
            or handle.controller_session_id is None
            or handle.spec_hash is None
            or handle.labels.get(SPEC_HASH_LABEL) != handle.spec_hash
            or handle.labels.get(EXECUTION_ID_LABEL) is None
            or handle.labels.get(WORKER_SESSION_ID_LABEL) is None
            or handle.labels.get(CLUSTER_ID_LABEL) != self.cluster_id
        ):
            raise KubernetesRuntimeError("Kubernetes Job handle has incomplete fencing identity")

    @staticmethod
    def job_name(task_id: uuid.UUID, execution_id: uuid.UUID) -> str:
        return f"mini-ai-job-{task_id.hex[:12]}-{execution_id.hex[:12]}"

    pod_name = job_name

    @staticmethod
    def network_policy_name(pod_name: str) -> str:
        return f"{pod_name}-deny-all"

    @staticmethod
    def _operation_error(
        operation: str, pod_name: str, exc: ApiException
    ) -> KubernetesRuntimeError:
        if exc.reason:
            detail = exc.reason
        elif isinstance(exc.body, bytes):
            detail = exc.body.decode("utf-8", "replace")
        elif exc.body:
            detail = str(exc.body)
        else:
            detail = str(exc)
        return KubernetesRuntimeError(f"failed to {operation} Kubernetes Job {pod_name}: {detail}")

    @staticmethod
    def _network_policy_operation_error(
        operation: str, policy_name: str, exc: ApiException
    ) -> KubernetesRuntimeError:
        if exc.reason:
            detail = exc.reason
        elif isinstance(exc.body, bytes):
            detail = exc.body.decode("utf-8", "replace")
        elif exc.body:
            detail = str(exc.body)
        else:
            detail = str(exc)
        return KubernetesRuntimeError(
            f"failed to {operation} Kubernetes NetworkPolicy {policy_name}: {detail}"
        )


_CONTRACT_LABEL_KEYS = (
    TASK_ID_LABEL,
    PROJECT_ID_LABEL,
    EXECUTION_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    CLUSTER_ID_LABEL,
    MANAGED_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_PROFILE_DIGEST_LABEL,
    ACCELERATOR_VENDOR_LABEL,
    ACCELERATOR_KIND_LABEL,
    RUNTIME_PROFILE_ID_LABEL,
    RUNTIME_PROFILE_VERSION_LABEL,
)
_CONTRACT_ANNOTATION_KEYS = (
    RUNTIME_PROFILE_DIGEST_ANNOTATION,
    ACCELERATOR_RESOURCE_ANNOTATION,
    ACCELERATOR_COUNT_ANNOTATION,
    ALLOCATION_AUTHORITY_ANNOTATION,
)


def _job_contract_hash(job: object) -> str:
    encoded = json.dumps(_job_contract(job), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:32]


def _job_contract(job: object) -> dict[str, object]:
    metadata = getattr(job, "metadata", None)
    labels = getattr(metadata, "labels", None) or {}
    annotations = getattr(metadata, "annotations", None) or {}
    job_spec = getattr(job, "spec", None)
    template = getattr(job_spec, "template", None)
    template_metadata = getattr(template, "metadata", None)
    template_labels = getattr(template_metadata, "labels", None) or {}
    template_annotations = getattr(template_metadata, "annotations", None) or {}
    pod_spec = getattr(template, "spec", None)
    service_account_name = getattr(pod_spec, "service_account_name", None)
    image_pull_secrets = getattr(pod_spec, "image_pull_secrets", None) or []
    containers = getattr(pod_spec, "containers", None) or []
    container = containers[0] if len(containers) == 1 else None
    resources = getattr(container, "resources", None)
    return {
        "labels": {key: labels.get(key) for key in _CONTRACT_LABEL_KEYS if labels.get(key)},
        "annotations": {
            key: annotations.get(key) for key in _CONTRACT_ANNOTATION_KEYS if annotations.get(key)
        },
        "job": {
            "active_deadline_seconds": getattr(job_spec, "active_deadline_seconds", None),
            "backoff_limit": getattr(job_spec, "backoff_limit", None),
            "completions": getattr(job_spec, "completions", None),
            "parallelism": getattr(job_spec, "parallelism", None),
        },
        "template": {
            "labels": {
                key: template_labels.get(key)
                for key in _CONTRACT_LABEL_KEYS
                if template_labels.get(key)
            },
            "annotations": {
                key: template_annotations.get(key)
                for key in _CONTRACT_ANNOTATION_KEYS
                if template_annotations.get(key)
            },
        },
        "pod": {
            "affinity": _model_contract(getattr(pod_spec, "affinity", None)),
            "automount_service_account_token": getattr(
                pod_spec, "automount_service_account_token", None
            ),
            "node_name": getattr(pod_spec, "node_name", None),
            "node_selector": _string_mapping(getattr(pod_spec, "node_selector", None)),
            "restart_policy": getattr(pod_spec, "restart_policy", None),
            **(
                {
                    "service_account_name": service_account_name,
                    "image_pull_secrets": _model_contract(image_pull_secrets),
                }
                if service_account_name not in (None, "default") or image_pull_secrets
                else {}
            ),
            "runtime_class_name": getattr(pod_spec, "runtime_class_name", None),
            # The Kubernetes API defaults an omitted schedulerName to
            # ``default-scheduler`` before returning the created Job.  Treat the
            # implicit and explicit forms as the same launch contract so an
            # otherwise unchanged Job keeps its pre-create spec hash.
            "scheduler_name": (getattr(pod_spec, "scheduler_name", None) or "default-scheduler"),
            "security_context": _model_contract(getattr(pod_spec, "security_context", None)),
            "tolerations": _model_contract(getattr(pod_spec, "tolerations", None) or []),
            "volumes": _model_contract(getattr(pod_spec, "volumes", None) or []),
        },
        "container": {
            "count": len(containers),
            "name": getattr(container, "name", None),
            "image": getattr(container, "image", None),
            "command": list(getattr(container, "command", None) or []),
            "args": list(getattr(container, "args", None) or []),
            "env": _model_contract(getattr(container, "env", None) or []),
            "requests": _string_mapping(getattr(resources, "requests", None)),
            "limits": _string_mapping(getattr(resources, "limits", None)),
            "security_context": _model_contract(getattr(container, "security_context", None)),
            "volume_mounts": _model_contract(getattr(container, "volume_mounts", None) or []),
        },
    }


def _model_contract(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return {str(key): _model_contract(item) for key, item in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_model_contract(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _model_contract(to_dict())
    return str(value)


def _string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in sorted(value.items())}


def _profile_digest_label(digest: str) -> str:
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise KubernetesRuntimeError("runtime profile digest must be canonical sha256")
    encoded = base64.b32encode(bytes.fromhex(digest.removeprefix("sha256:")))
    return encoded.decode("ascii").rstrip("=").lower()


def _required_uid(observed: object, *, resource: str) -> str:
    uid = str(getattr(getattr(observed, "metadata", None), "uid", "") or "")
    if not uid:
        raise KubernetesRuntimeError(f"Kubernetes {resource} has no UID")
    return uid


def _required_resource_version(observed: object, *, resource: str) -> str:
    resource_version = str(
        getattr(getattr(observed, "metadata", None), "resource_version", "") or ""
    )
    if not resource_version:
        raise KubernetesRuntimeError(f"Kubernetes {resource} has no resourceVersion")
    return resource_version


def _controller_session_id(job: object) -> uuid.UUID:
    annotations = getattr(getattr(job, "metadata", None), "annotations", None) or {}
    try:
        return uuid.UUID(str(annotations.get(CONTROLLER_SESSION_ANNOTATION, "")))
    except ValueError as exc:
        raise KubernetesRuntimeError(
            "Kubernetes Job has no valid controller-session annotation"
        ) from exc


def _is_controlled_by_job(pod: object, job_uid: str | None) -> bool:
    if not job_uid:
        return False
    references = getattr(getattr(pod, "metadata", None), "owner_references", None) or []
    return any(
        getattr(reference, "controller", None) is True
        and getattr(reference, "kind", None) == "Job"
        and str(getattr(reference, "uid", "") or "") == job_uid
        for reference in references
    )


def _job_terminal_state(status: object) -> str | None:
    for condition in getattr(status, "conditions", None) or []:
        if str(getattr(condition, "status", "") or "").lower() != "true":
            continue
        condition_type = str(getattr(condition, "type", "") or "")
        reason = str(getattr(condition, "reason", "") or "")
        if condition_type == "Complete":
            return "complete"
        if condition_type in {"Failed", "FailureTarget"}:
            return "deadline" if reason == "DeadlineExceeded" else "failed"
    if int(getattr(status, "succeeded", 0) or 0) > 0:
        return "complete"
    # ``failed`` counts failed Pods; it is updated before the Job controller
    # publishes the authoritative Failed condition (and its terminal reason).
    # Returning here can therefore misclassify an active-deadline failure as a
    # generic non-zero exit while the DeadlineExceeded condition is in flight.
    return None


def _bounded(value: str) -> str:
    return value[:_MAX_REASON_LENGTH]


def _pod_exit_code(status: object | None, *, default: int) -> int:
    container_statuses = getattr(status, "container_statuses", None) or []
    for container_status in container_statuses:
        state = getattr(container_status, "state", None)
        terminated = getattr(state, "terminated", None)
        exit_code = getattr(terminated, "exit_code", None)
        if exit_code is not None:
            return int(exit_code)
    return default


def _pod_runtime_failure(status: object, pod_name: str) -> KubernetesRuntimeError | None:
    container_statuses = getattr(status, "container_statuses", None) or []
    for container_status in container_statuses:
        state = getattr(container_status, "state", None)
        waiting = getattr(state, "waiting", None)
        waiting_reason = str(getattr(waiting, "reason", "") or "")
        if waiting_reason in {"ErrImagePull", "ImagePullBackOff", "InvalidImageName"}:
            return KubernetesImagePullFailed(
                f"Kubernetes Pod {pod_name} could not pull its image: {waiting_reason}"
            )
        terminated = getattr(state, "terminated", None)
        if terminated is None:
            continue
        exit_code = int(getattr(terminated, "exit_code", 1) or 0)
        reason = str(getattr(terminated, "reason", "") or "")
        if reason == "OOMKilled" or exit_code == 137:
            return KubernetesOomKilled(
                f"Kubernetes Pod {pod_name} was terminated by the out-of-memory killer",
                exit_code=exit_code,
            )

    conditions = getattr(status, "conditions", None) or []
    for condition in conditions:
        reason = str(getattr(condition, "reason", "") or "")
        message = str(getattr(condition, "message", "") or "")
        if reason == "Unschedulable":
            return KubernetesContainerStartFailed(
                _bounded(
                    f"Kubernetes Pod {pod_name} is unschedulable: "
                    f"{message or reason or 'unknown scheduling reason'}"
                )
            )
    return None


async def _iter_log_chunks(source: object) -> AsyncIterator[bytes]:
    payload = getattr(source, "content", source)
    if isinstance(payload, str):
        yield payload.encode()
        return
    if isinstance(payload, bytes):
        yield payload
        return

    iter_any = getattr(payload, "iter_any", None)
    if callable(iter_any):
        async for chunk in iter_any():
            yield _as_bytes(chunk)
        return
    if hasattr(payload, "__aiter__"):
        async for chunk in payload:
            yield _as_bytes(chunk)
        return

    read = getattr(payload, "read", None)
    if callable(read):
        while True:
            chunk = read()
            if inspect.isawaitable(chunk):
                chunk = await chunk
            if not chunk:
                return
            yield _as_bytes(chunk)
        return
    raise TypeError("Kubernetes log response is not streamable")


def _log_response_error(source: object) -> ApiException | None:
    """Surface non-2xx streaming responses that kubernetes-asyncio leaves unchecked."""

    status = getattr(source, "status", None)
    if not isinstance(status, int) or 200 <= status < 300:
        return None
    reason = str(getattr(source, "reason", "") or "")
    return ApiException(status=status, reason=reason or None)


async def _close_log_source(source: object) -> None:
    for method_name in ("close", "release"):
        method = getattr(source, method_name, None)
        if not callable(method):
            continue
        result = method()
        if inspect.isawaitable(result):
            await result
        return


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise TypeError(f"unsupported Kubernetes log chunk type: {type(value).__name__}")
