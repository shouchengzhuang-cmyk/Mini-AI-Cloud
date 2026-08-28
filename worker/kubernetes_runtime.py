import asyncio
import inspect
import uuid
from collections.abc import AsyncIterator, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from core.enums import AllocationAuthority, ErrorCategory, ErrorCode
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
MANAGED_LABEL = "mini-ai-cloud/managed"
RESOURCE_KIND_LABEL = "mini-ai-cloud/resource-kind"
ACCELERATOR_VENDOR_LABEL = "mini-ai-cloud/accelerator-vendor"
ACCELERATOR_KIND_LABEL = "mini-ai-cloud/accelerator-kind"
RUNTIME_PROFILE_ID_LABEL = "mini-ai-cloud/runtime-profile-id"
RUNTIME_PROFILE_VERSION_LABEL = "mini-ai-cloud/runtime-profile-version"
RUNTIME_PROFILE_DIGEST_ANNOTATION = "mini-ai-cloud/runtime-profile-digest"
ACCELERATOR_RESOURCE_ANNOTATION = "mini-ai-cloud/accelerator-resource"
ACCELERATOR_COUNT_ANNOTATION = "mini-ai-cloud/accelerator-count"
ALLOCATION_AUTHORITY_ANNOTATION = "mini-ai-cloud/allocation-authority"
NETWORK_POLICY_RESOURCE_KIND = "task-deny-all"
_LEGACY_PROJECT_ID = uuid.UUID(int=0)


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


class KubernetesRuntime:
    """ComputeRuntime implementation backed by one restart-free Pod per execution."""

    runtime_type = "kubernetes"

    def __init__(
        self,
        *,
        namespace: str,
        node_name: str,
        cleanup_grace_seconds: int = 30,
        kubeconfig: str | None = None,
        in_cluster: bool = False,
        poll_interval: float = 0.25,
        api: Any | None = None,
        networking_api: Any | None = None,
        runtime_profile_catalog: RuntimeProfileCatalog | None = None,
    ) -> None:
        if not namespace.strip():
            raise ValueError("namespace must not be blank")
        if not node_name.strip():
            raise ValueError("node_name must not be blank")
        if cleanup_grace_seconds < 0:
            raise ValueError("cleanup_grace_seconds must not be negative")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")
        self.namespace = namespace.strip()
        self.node_name = node_name.strip()
        self.cleanup_grace_seconds = cleanup_grace_seconds
        self.kubeconfig = kubeconfig
        self.in_cluster = in_cluster
        self.poll_interval = poll_interval
        self._api = api
        self._networking_api = networking_api
        self._owns_api = api is None
        self.runtime_profile_catalog = runtime_profile_catalog
        self._client_lock = asyncio.Lock()
        self._network_policy_labels: dict[str, dict[str, str]] = {}

    async def prepare(self, spec: ExecutionSpec) -> RuntimeHandle:
        api = await self._ensure_api()
        pod = self._build_pod(spec)
        pod_name = str(pod.metadata.name)
        if not spec.network_enabled:
            await self._ensure_network_policy(spec)
        # The policy intentionally survives a failed Pod create. A concurrent,
        # idempotent prepare may already have adopted it, so rollback here could
        # remove isolation from that execution. An empty fenced selector is safe.
        try:
            created = await api.create_namespaced_pod(
                namespace=self.namespace,
                body=pod,
            )
        except ApiException as exc:
            if exc.status != 409:
                detail = self._operation_error("create", pod_name, exc)
                raise KubernetesContainerStartFailed(str(detail)) from exc
            try:
                created = await api.read_namespaced_pod(
                    name=pod_name,
                    namespace=self.namespace,
                )
            except ApiException as read_exc:
                raise self._operation_error("adopt", pod_name, read_exc) from read_exc

        self._validate_adopted_pod(created, spec)
        return RuntimeHandle(
            runtime_type=self.runtime_type,
            resource_kind="pod",
            object_id=pod_name,
            display_id=pod_name,
            native=created,
        )

    async def start(self, handle: RuntimeHandle) -> None:
        """Pod creation starts execution; this verifies the prepared Pod still exists."""

        self._validate_handle(handle)
        api = await self._ensure_api()
        try:
            await api.read_namespaced_pod(
                name=handle.object_id,
                namespace=self.namespace,
            )
        except ApiException as exc:
            detail = self._operation_error("start", handle.object_id, exc)
            raise KubernetesContainerStartFailed(str(detail)) from exc

    async def logs(
        self,
        handle: RuntimeHandle,
        *,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[RuntimeLog]:
        self._validate_handle(handle)
        api = await self._ensure_api()
        source: object | None = None
        try:
            while True:
                try:
                    source = await api.read_namespaced_pod_log(
                        name=handle.object_id,
                        namespace=self.namespace,
                        container="task",
                        follow=True,
                        _preload_content=False,
                    )
                    break
                except ApiException as exc:
                    if exc.status != 400:
                        raise
                    pod = await api.read_namespaced_pod_status(
                        name=handle.object_id,
                        namespace=self.namespace,
                    )
                    status = getattr(pod, "status", None)
                    failure = _pod_runtime_failure(status, handle.object_id)
                    if failure is not None:
                        raise failure from exc
                    phase = getattr(status, "phase", None)
                    if phase in {"Succeeded", "Failed"}:
                        if ready is not None:
                            ready.set()
                        return
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
        api = await self._ensure_api()
        while True:
            try:
                pod = await api.read_namespaced_pod_status(
                    name=handle.object_id,
                    namespace=self.namespace,
                )
            except ApiException as exc:
                raise self._operation_error("wait for", handle.object_id, exc) from exc
            status = getattr(pod, "status", None)
            failure = _pod_runtime_failure(status, handle.object_id)
            if failure is not None:
                raise failure
            phase = getattr(status, "phase", None)
            if phase == "Succeeded":
                return _pod_exit_code(status, default=0)
            if phase == "Failed":
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
        self._networking_api = None
        self._network_policy_labels.clear()

    async def _delete(self, handle: RuntimeHandle, *, grace_seconds: int) -> None:
        self._validate_handle(handle)
        api = await self._ensure_api()
        try:
            await api.delete_namespaced_pod(
                name=handle.object_id,
                namespace=self.namespace,
                grace_period_seconds=grace_seconds,
                propagation_policy="Background",
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
            self._api = client.CoreV1Api()
        return self._api

    async def _ensure_networking_api(self) -> Any:
        if self._networking_api is not None:
            return self._networking_api
        core_api = await self._ensure_api()
        async with self._client_lock:
            if self._networking_api is None:
                api_client = getattr(core_api, "api_client", None)
                if api_client is None:
                    self._networking_api = client.NetworkingV1Api()
                else:
                    self._networking_api = client.NetworkingV1Api(api_client=api_client)
        return self._networking_api

    async def _ensure_network_policy(self, spec: ExecutionSpec) -> None:
        api = await self._ensure_networking_api()
        policy = self._build_network_policy(spec)
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
        self._validate_network_policy(observed, spec=spec)
        self._network_policy_labels[policy_name] = self._execution_labels(spec)

    async def _delete_network_policy(self, handle: RuntimeHandle) -> None:
        self._validate_handle(handle)
        api = await self._ensure_networking_api()
        policy_name = self.network_policy_name(handle.object_id)
        expected_labels = self._network_policy_labels.get(policy_name)
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

    def _build_network_policy(self, spec: ExecutionSpec) -> client.V1NetworkPolicy:
        execution_labels = self._execution_labels(spec)
        return client.V1NetworkPolicy(
            metadata=client.V1ObjectMeta(
                name=self.network_policy_name(self.pod_name(spec.task_id, spec.execution_id)),
                labels={
                    **execution_labels,
                    RESOURCE_KIND_LABEL: NETWORK_POLICY_RESOURCE_KIND,
                },
            ),
            spec=client.V1NetworkPolicySpec(
                pod_selector=client.V1LabelSelector(match_labels=execution_labels),
                policy_types=["Ingress", "Egress"],
                ingress=[],
                egress=[],
            ),
        )

    def _validate_network_policy(
        self,
        policy: object,
        *,
        spec: ExecutionSpec | None = None,
        expected_labels: Mapping[str, str] | None = None,
    ) -> None:
        metadata = getattr(policy, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        if spec is not None:
            expected_labels = self._execution_labels(spec)
        if expected_labels is None:
            expected_labels = {
                key: str(labels.get(key, ""))
                for key in (
                    TASK_ID_LABEL,
                    PROJECT_ID_LABEL,
                    EXECUTION_ID_LABEL,
                    WORKER_ID_LABEL,
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
        mismatched = {
            key: (labels.get(key), value)
            for key, value in expected_labels.items()
            if labels.get(key) != value
        }
        if labels.get(RESOURCE_KIND_LABEL) != NETWORK_POLICY_RESOURCE_KIND or mismatched:
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

    def _build_pod(self, spec: ExecutionSpec) -> client.V1Pod:
        profile = self._resolve_runtime_profile(spec)
        resources: dict[str, str] = {
            "cpu": f"{max(1, round(spec.cpu_limit * 1000))}m",
            "memory": f"{spec.memory_limit_mb}Mi",
        }
        if profile is not None:
            resources[profile.kubernetes.resource_name] = str(spec.gpu_count)
        elif spec.gpu_count > 0:
            resources["nvidia.com/gpu"] = str(spec.gpu_count)
        labels = self._execution_labels(spec)
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
        return client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=self.pod_name(spec.task_id, spec.execution_id),
                labels=labels,
                annotations=annotations or None,
            ),
            spec=client.V1PodSpec(
                affinity=affinity,
                automount_service_account_token=False,
                active_deadline_seconds=max(1, spec.timeout_seconds),
                containers=[container],
                node_name=self.node_name,
                node_selector=(
                    dict(profile.kubernetes.node_selector) if profile is not None else None
                ),
                restart_policy="Never",
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

    def _validate_adopted_pod(self, pod: object, spec: ExecutionSpec) -> None:
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        expected = self._execution_labels(spec)
        mismatched = {
            key: (labels.get(key), value)
            for key, value in expected.items()
            if labels.get(key) != value
        }
        if mismatched:
            raise KubernetesRuntimeError(
                "refusing to adopt Pod with mismatched execution labels: "
                f"{self.pod_name(spec.task_id, spec.execution_id)}"
            )
        profile = self._resolve_runtime_profile(spec)
        expected_annotations = self._profile_annotations(spec, profile)
        annotations = getattr(metadata, "annotations", None) or {}
        if any(annotations.get(key) != value for key, value in expected_annotations.items()):
            raise KubernetesRuntimeError(
                "refusing to adopt Pod with mismatched accelerator profile annotations: "
                f"{self.pod_name(spec.task_id, spec.execution_id)}"
            )

    @staticmethod
    def _execution_labels(spec: ExecutionSpec) -> dict[str, str]:
        labels = {
            TASK_ID_LABEL: str(spec.task_id),
            PROJECT_ID_LABEL: str(spec.project_id or _LEGACY_PROJECT_ID),
            EXECUTION_ID_LABEL: str(spec.execution_id),
            WORKER_ID_LABEL: spec.worker_id,
            MANAGED_LABEL: "true",
        }
        if spec.selected_vendor is not None:
            labels[ACCELERATOR_VENDOR_LABEL] = spec.selected_vendor
        if spec.selected_kind is not None:
            labels[ACCELERATOR_KIND_LABEL] = spec.selected_kind
        if spec.runtime_profile_id is not None:
            labels[RUNTIME_PROFILE_ID_LABEL] = spec.runtime_profile_id
        if spec.runtime_profile_version is not None:
            labels[RUNTIME_PROFILE_VERSION_LABEL] = spec.runtime_profile_version
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
        if handle.runtime_type != self.runtime_type:
            raise KubernetesRuntimeError(
                f"cannot use {handle.runtime_type!r} handle with Kubernetes runtime"
            )

    @staticmethod
    def pod_name(task_id: uuid.UUID, execution_id: uuid.UUID) -> str:
        return f"mini-ai-{task_id.hex[:12]}-{execution_id.hex[:12]}"

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
        return KubernetesRuntimeError(f"failed to {operation} Kubernetes Pod {pod_name}: {detail}")

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


def _pod_exit_code(status: object, *, default: int) -> int:
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
        normalized = message.lower()
        if reason == "Unschedulable" and (
            "nvidia.com/gpu" in normalized or ("gpu" in normalized and "insufficient" in normalized)
        ):
            return KubernetesGpuUnavailable(
                f"Kubernetes Pod {pod_name} cannot be scheduled because GPU capacity is unavailable"
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
