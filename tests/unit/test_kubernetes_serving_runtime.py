import re
import uuid
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from kubernetes_asyncio.client.exceptions import ApiException

from worker.kubernetes_serving_runtime import (
    CLUSTER_ID_LABEL,
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    MANAGED_LABEL,
    POD_RESOURCE_KIND,
    PROJECT_ID_LABEL,
    REPLICA_ID_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_LABEL,
    RUNTIME_LABEL_VALUE,
    SERVICE_ID_LABEL,
    SERVICE_RESOURCE_KIND,
    SPEC_HASH_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    KubernetesServingHandle,
    KubernetesServingLaunchSpec,
    KubernetesServingOwnershipError,
    KubernetesServingRuntime,
    KubernetesServingRuntimeAdapter,
)

SERVICE_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
REPLICA_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
PROJECT_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
EXECUTION_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")
WORKER_SESSION_ID = uuid.UUID("55555555-5555-5555-5555-555555555555")


def _spec() -> KubernetesServingLaunchSpec:
    return KubernetesServingLaunchSpec(
        service_id=SERVICE_ID,
        replica_id=REPLICA_ID,
        project_id=PROJECT_ID,
        generation=7,
        execution_id=EXECUTION_ID,
        image="mini-docker-cloud:kind-test",
        model="fake-model",
        cpu_millicores=250,
        memory_mb=256,
        startup_delay_seconds=1.5,
        chunk_delay_seconds=0.05,
    )


def _runtime(api: object, *, version_api: object | None = None) -> KubernetesServingRuntimeAdapter:
    return KubernetesServingRuntimeAdapter(
        namespace="serving-tests",
        cluster_id="kind-serving-test",
        termination_grace_seconds=17,
        api=api,
        version_api=version_api,
    )


def _set_uid(resource: Any, uid: str) -> Any:
    resource.metadata.uid = uid
    return resource


def _prepare_api() -> SimpleNamespace:
    async def create_pod(*, namespace: str, body: object) -> object:
        assert namespace == "serving-tests"
        return _set_uid(body, "pod-uid")

    async def create_service(*, namespace: str, body: object) -> object:
        assert namespace == "serving-tests"
        return _set_uid(body, "service-uid")

    return SimpleNamespace(
        create_namespaced_pod=AsyncMock(side_effect=create_pod),
        create_namespaced_service=AsyncMock(side_effect=create_service),
    )


async def _prepare(
    runtime: KubernetesServingRuntimeAdapter,
) -> tuple[KubernetesServingHandle, Any, Any]:
    handle = await runtime.prepare(
        _spec(),
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    api = runtime._api
    assert api is not None
    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    service = api.create_namespaced_service.call_args.kwargs["body"]
    return handle, pod, service


def test_runtime_implements_serving_protocol_and_validates_configuration() -> None:
    assert isinstance(_runtime(SimpleNamespace()), KubernetesServingRuntime)

    configured = KubernetesServingRuntimeAdapter(
        namespace="valid",
        cluster_id="kind",
        readiness_probe_timeout_seconds=1.1,
        readiness_probe_period_seconds=2.1,
        api=SimpleNamespace(),
    )
    assert configured.readiness_probe_timeout_seconds == 2
    assert configured.readiness_probe_period_seconds == 3

    with pytest.raises(ValueError, match="DNS-1123"):
        KubernetesServingRuntimeAdapter(namespace="Bad_Namespace", cluster_id="kind")
    with pytest.raises(ValueError, match="label value"):
        KubernetesServingRuntimeAdapter(namespace="valid", cluster_id="bad/cluster")
    with pytest.raises(ValueError, match="between zero"):
        KubernetesServingRuntimeAdapter(
            namespace="valid", cluster_id="kind", termination_grace_seconds=-1
        )
    with pytest.raises(ValueError, match="greater than zero"):
        KubernetesServingRuntimeAdapter(
            namespace="valid",
            cluster_id="kind",
            readiness_probe_timeout_seconds=0,
        )


def test_resource_names_are_deterministic_bounded_dns_1123_and_fenced() -> None:
    spec = _spec()
    very_large_generation = replace(spec, generation=10**100)

    for item in (spec, very_large_generation):
        pod_name = KubernetesServingRuntimeAdapter.pod_name(item)
        service_name = KubernetesServingRuntimeAdapter.service_name(item)
        assert len(pod_name) <= 63
        assert len(service_name) <= 63
        assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", pod_name)
        assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", service_name)
        assert pod_name == KubernetesServingRuntimeAdapter.pod_name(item)
        assert service_name == KubernetesServingRuntimeAdapter.service_name(item)

    assert KubernetesServingRuntimeAdapter.pod_name(
        spec
    ) != KubernetesServingRuntimeAdapter.pod_name(replace(spec, execution_id=uuid.uuid4()))
    assert KubernetesServingRuntimeAdapter.pod_name(
        spec
    ) != KubernetesServingRuntimeAdapter.service_name(spec)


async def test_prepare_builds_secure_fenced_pod_and_exact_cluster_ip_service() -> None:
    api = _prepare_api()
    runtime = _runtime(api)

    handle, pod, service = await _prepare(runtime)

    expected_selector = {
        SERVICE_ID_LABEL: str(SERVICE_ID),
        REPLICA_ID_LABEL: str(REPLICA_ID),
        PROJECT_ID_LABEL: str(PROJECT_ID),
        EXECUTION_ID_LABEL: str(EXECUTION_ID),
        GENERATION_LABEL: "7",
        MANAGED_LABEL: "true",
        CLUSTER_ID_LABEL: "kind-serving-test",
        WORKER_ID_LABEL: "k8s-serving-worker",
        WORKER_SESSION_ID_LABEL: str(WORKER_SESSION_ID),
        RUNTIME_LABEL: RUNTIME_LABEL_VALUE,
        SPEC_HASH_LABEL: pod.metadata.labels[SPEC_HASH_LABEL],
    }
    assert pod.metadata.labels == {**expected_selector, RESOURCE_KIND_LABEL: POD_RESOURCE_KIND}
    assert service.metadata.labels == {
        **expected_selector,
        RESOURCE_KIND_LABEL: SERVICE_RESOURCE_KIND,
    }
    assert service.spec.type == "ClusterIP"
    assert service.spec.selector == expected_selector
    assert service.spec.publish_not_ready_addresses is False
    assert len(service.spec.ports) == 1
    assert service.spec.ports[0].port == 8000
    assert service.spec.ports[0].target_port == 8000

    assert pod.spec.automount_service_account_token is False
    assert pod.spec.host_network is False
    assert pod.spec.host_pid is False
    assert pod.spec.host_ipc is False
    assert pod.spec.restart_policy == "Never"
    assert pod.spec.termination_grace_period_seconds == 17
    assert pod.spec.security_context.run_as_non_root is True
    assert pod.spec.security_context.run_as_user == 10001
    assert pod.spec.security_context.run_as_group == 10001
    assert pod.spec.security_context.seccomp_profile.type == "RuntimeDefault"
    assert len(pod.spec.volumes) == 1
    assert pod.spec.volumes[0].host_path is None
    assert pod.spec.volumes[0].empty_dir.medium == "Memory"
    assert pod.spec.volumes[0].empty_dir.size_limit == "64Mi"

    container = pod.spec.containers[0]
    assert container.image == "mini-docker-cloud:kind-test"
    assert container.image_pull_policy == "IfNotPresent"
    assert container.command[-4:] == [
        "--startup-delay-seconds",
        "1.5",
        "--chunk-delay-seconds",
        "0.05",
    ]
    assert container.resources.requests == {"cpu": "250m", "memory": "256Mi"}
    assert container.resources.limits == container.resources.requests
    assert container.security_context.allow_privilege_escalation is False
    assert container.security_context.privileged is False
    assert container.security_context.read_only_root_filesystem is True
    assert container.security_context.capabilities.drop == ["ALL"]
    assert container.volume_mounts[0].mount_path == "/tmp"
    assert container.readiness_probe.http_get.path == "/health"
    assert container.readiness_probe.http_get.port == 8000

    assert handle.object_id == pod.metadata.name
    assert handle.uid == "pod-uid"
    assert handle.service_name == service.metadata.name
    assert handle.service_uid == "service-uid"
    assert handle.endpoint_url == (
        f"http://{service.metadata.name}.serving-tests.svc.cluster.local:8000"
    )


async def test_prepare_adopts_only_exact_pod_and_service_after_conflict() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    spec = _spec()
    selector = runtime._selector_labels(
        spec,
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    pod = _set_uid(runtime._build_pod(spec, selector), "adopted-pod")
    service = _set_uid(runtime._build_service(spec, selector), "adopted-service")
    api.create_namespaced_pod.side_effect = ApiException(status=409, reason="AlreadyExists")
    api.create_namespaced_service.side_effect = ApiException(status=409, reason="AlreadyExists")
    api.read_namespaced_pod = AsyncMock(return_value=pod)
    api.read_namespaced_service = AsyncMock(return_value=service)

    handle = await runtime.prepare(
        spec,
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )

    assert handle.uid == "adopted-pod"
    assert handle.service_uid == "adopted-service"


@pytest.mark.parametrize(
    "drift",
    [
        "image",
        "command",
        "args",
        "env",
        "resources",
        "probe",
        "container_port",
        "security",
        "termination_grace",
        "volume",
    ],
)
async def test_prepare_rejects_pod_with_exact_labels_but_drifted_workload(
    drift: str,
) -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    spec = _spec()
    selector = runtime._selector_labels(
        spec,
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    pod = runtime._build_pod(spec, selector)
    container = pod.spec.containers[0]
    if drift == "image":
        container.image = "mini-docker-cloud:wrong-image"
    elif drift == "command":
        container.command[0] = "sh"
    elif drift == "args":
        container.args = ["unexpected"]
    elif drift == "env":
        container.env[0].value = "0"
    elif drift == "resources":
        container.resources.requests = {"cpu": "500m", "memory": "512Mi"}
        container.resources.limits = dict(container.resources.requests)
    elif drift == "probe":
        container.readiness_probe.http_get.path = "/wrong"
    elif drift == "container_port":
        container.ports[0].container_port = 9000
    elif drift == "security":
        pod.spec.security_context.run_as_user = 20002
    elif drift == "termination_grace":
        pod.spec.termination_grace_period_seconds = 99
    elif drift == "volume":
        pod.spec.volumes[0].empty_dir.medium = ""
    else:  # pragma: no cover - parametrization is exhaustive.
        raise AssertionError(drift)
    api.create_namespaced_pod.side_effect = ApiException(status=409)
    api.read_namespaced_pod = AsyncMock(return_value=pod)

    with pytest.raises(KubernetesServingOwnershipError, match="mismatched launch spec"):
        await runtime.prepare(
            spec,
            worker_id="k8s-serving-worker",
            worker_session_id=WORKER_SESSION_ID,
        )

    api.create_namespaced_service.assert_not_awaited()


async def test_prepare_rejects_service_with_exact_labels_but_wrong_endpoint_port() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    spec = _spec()
    selector = runtime._selector_labels(
        spec,
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    pod = runtime._build_pod(spec, selector)
    service = runtime._build_service(spec, selector)
    service.spec.ports[0].port = 9000
    service.spec.ports[0].target_port = 9000
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: pod
    api.create_namespaced_service.side_effect = ApiException(status=409)
    api.read_namespaced_service = AsyncMock(return_value=service)

    with pytest.raises(KubernetesServingOwnershipError, match="endpoint contract"):
        await runtime.prepare(
            spec,
            worker_id="k8s-serving-worker",
            worker_session_id=WORKER_SESSION_ID,
        )


async def test_prepare_rejects_stale_execution_and_conflicting_service_selector() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    spec = _spec()
    selector = runtime._selector_labels(
        spec,
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    pod = runtime._build_pod(spec, selector)
    pod.metadata.labels[EXECUTION_ID_LABEL] = str(uuid.uuid4())
    api.create_namespaced_pod.side_effect = ApiException(status=409)
    api.read_namespaced_pod = AsyncMock(return_value=pod)

    with pytest.raises(KubernetesServingOwnershipError, match="fencing labels"):
        await runtime.prepare(
            spec,
            worker_id="k8s-serving-worker",
            worker_session_id=WORKER_SESSION_ID,
        )
    api.create_namespaced_service.assert_not_awaited()

    exact_pod = runtime._build_pod(spec, selector)
    service = runtime._build_service(spec, selector)
    service.spec.selector[EXECUTION_ID_LABEL] = str(uuid.uuid4())
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: exact_pod
    api.create_namespaced_service.side_effect = ApiException(status=409)
    api.read_namespaced_service = AsyncMock(return_value=service)

    with pytest.raises(KubernetesServingOwnershipError, match="selector"):
        await runtime.prepare(
            spec,
            worker_id="k8s-serving-worker",
            worker_session_id=WORKER_SESSION_ID,
        )


async def test_start_and_inspect_publish_only_ready_non_deleting_service() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, service = await _prepare(runtime)
    pod.status = SimpleNamespace(
        phase="Running",
        conditions=[SimpleNamespace(type="Ready", status="True")],
        container_statuses=[
            SimpleNamespace(
                image_id=f"docker-pullable://example@sha256:{'a' * 64}",
                state=SimpleNamespace(running=SimpleNamespace()),
            )
        ],
    )
    api.read_namespaced_pod = AsyncMock(return_value=pod)
    api.read_namespaced_pod_status = AsyncMock(return_value=pod)
    api.read_namespaced_service = AsyncMock(return_value=service)

    refreshed = await runtime.start(handle)
    state = await runtime.inspect(refreshed)

    assert refreshed.image_digest == f"sha256:{'a' * 64}"
    assert state.phase == "Running"
    assert state.running is True
    assert state.ready is True
    assert state.missing is False
    assert state.deleting is False
    assert state.endpoint_url == handle.endpoint_url
    assert state.reason is None

    service.metadata.deletion_timestamp = "terminating"
    deleting = await runtime.inspect(refreshed)
    assert deleting.running is False
    assert deleting.ready is False
    assert deleting.deleting is True


@pytest.mark.parametrize(
    ("status", "expected_reason", "expected_exit", "expected_oom"),
    [
        (
            SimpleNamespace(
                phase="Pending",
                conditions=[],
                container_statuses=[
                    SimpleNamespace(
                        state=SimpleNamespace(
                            waiting=SimpleNamespace(
                                reason="ImagePullBackOff",
                                message="registry unavailable",
                            )
                        )
                    )
                ],
            ),
            "ImagePullBackOff",
            None,
            False,
        ),
        (
            SimpleNamespace(
                phase="Failed",
                conditions=[],
                container_statuses=[
                    SimpleNamespace(
                        state=SimpleNamespace(
                            terminated=SimpleNamespace(
                                exit_code=137,
                                reason="OOMKilled",
                                message="memory limit exceeded",
                            )
                        )
                    )
                ],
            ),
            "OOMKilled",
            137,
            True,
        ),
    ],
)
async def test_inspect_maps_image_pull_and_oom_failures(
    status: object,
    expected_reason: str,
    expected_exit: int | None,
    expected_oom: bool,
) -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, service = await _prepare(runtime)
    pod.status = status
    api.read_namespaced_pod_status = AsyncMock(return_value=pod)
    api.read_namespaced_service = AsyncMock(return_value=service)

    state = await runtime.inspect(handle)

    assert state.reason == expected_reason
    assert state.exit_code == expected_exit
    assert state.oom_killed is expected_oom
    assert state.ready is False


async def test_inspect_reports_missing_pod_and_missing_service() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, _service = await _prepare(runtime)
    api.read_namespaced_pod_status = AsyncMock(side_effect=ApiException(status=404))

    missing = await runtime.inspect(handle)

    assert missing.phase == "Missing"
    assert missing.missing is True
    assert missing.endpoint_url is None

    pod.status = SimpleNamespace(
        phase="Running",
        conditions=[SimpleNamespace(type="Ready", status="True")],
        container_statuses=[],
    )
    api.read_namespaced_pod_status = AsyncMock(return_value=pod)
    api.read_namespaced_service = AsyncMock(side_effect=ApiException(status=404))

    service_missing = await runtime.inspect(handle)
    assert service_missing.running is True
    assert service_missing.ready is False
    assert service_missing.reason == "ServiceMissing"
    assert service_missing.endpoint_url is None


async def test_graceful_and_force_delete_use_uid_preconditions_and_are_idempotent() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, service = await _prepare(runtime)
    api.read_namespaced_pod = AsyncMock(return_value=pod)
    api.read_namespaced_service = AsyncMock(return_value=service)
    api.delete_namespaced_pod = AsyncMock()
    api.delete_namespaced_service = AsyncMock()

    await runtime.request_stop(handle)

    service_body = api.delete_namespaced_service.call_args.kwargs["body"]
    pod_body = api.delete_namespaced_pod.call_args.kwargs["body"]
    assert service_body.preconditions.uid == "service-uid"
    assert service_body.grace_period_seconds == 0
    assert pod_body.preconditions.uid == "pod-uid"
    assert pod_body.grace_period_seconds == 17

    await runtime.force_cleanup(handle)
    forced_pod_body = api.delete_namespaced_pod.call_args.kwargs["body"]
    assert forced_pod_body.grace_period_seconds == 0

    api.read_namespaced_service = AsyncMock(side_effect=ApiException(status=404))
    api.read_namespaced_pod = AsyncMock(side_effect=ApiException(status=404))
    api.delete_namespaced_pod.reset_mock()
    api.delete_namespaced_service.reset_mock()

    await runtime.force_cleanup(handle)

    api.delete_namespaced_pod.assert_not_awaited()
    api.delete_namespaced_service.assert_not_awaited()


async def test_delete_refuses_recreated_pod_with_different_uid() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, _service = await _prepare(runtime)
    pod.metadata.uid = "replacement-pod-uid"
    api.read_namespaced_service = AsyncMock(side_effect=ApiException(status=404))
    api.read_namespaced_pod = AsyncMock(return_value=pod)
    api.delete_namespaced_pod = AsyncMock()

    with pytest.raises(KubernetesServingOwnershipError, match="UID fence mismatch"):
        await runtime.force_cleanup(handle)

    api.delete_namespaced_pod.assert_not_awaited()


async def test_list_managed_uses_bounded_selectors_and_recovers_service_identity() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    handle, pod, service = await _prepare(runtime)
    api.list_namespaced_pod = AsyncMock(return_value=SimpleNamespace(items=[pod]))
    api.list_namespaced_service = AsyncMock(return_value=SimpleNamespace(items=[service]))

    recovered = await runtime.list_managed(worker_id="k8s-serving-worker")

    assert len(recovered) == 1
    assert recovered[0].object_id == handle.object_id
    assert recovered[0].service_uid == "service-uid"
    pod_selector = api.list_namespaced_pod.call_args.kwargs["label_selector"]
    service_selector = api.list_namespaced_service.call_args.kwargs["label_selector"]
    assert f"{CLUSTER_ID_LABEL}=kind-serving-test" in pod_selector
    assert f"{RESOURCE_KIND_LABEL}={POD_RESOURCE_KIND}" in pod_selector
    assert f"{RESOURCE_KIND_LABEL}={SERVICE_RESOURCE_KIND}" in service_selector

    api.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    service_orphan = await runtime.list_managed(worker_id="k8s-serving-worker")
    assert len(service_orphan) == 1
    assert service_orphan[0].object_id == handle.object_id
    assert service_orphan[0].uid is None
    assert service_orphan[0].service_uid == "service-uid"

    api.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])
    pod.metadata.labels.pop(EXECUTION_ID_LABEL)
    with pytest.raises(KubernetesServingOwnershipError, match="incomplete"):
        await runtime.list_managed(worker_id="k8s-serving-worker")


async def test_list_managed_rejects_workload_or_hash_drift_without_database_spec() -> None:
    api = _prepare_api()
    runtime = _runtime(api)
    _handle, pod, service = await _prepare(runtime)
    api.list_namespaced_pod = AsyncMock(return_value=SimpleNamespace(items=[pod]))
    api.list_namespaced_service = AsyncMock(return_value=SimpleNamespace(items=[service]))

    pod.spec.containers[0].image = "mini-docker-cloud:wrong-image"
    with pytest.raises(KubernetesServingOwnershipError, match="mismatched spec hash"):
        await runtime.list_managed(worker_id="k8s-serving-worker")

    selector = runtime._selector_labels(
        _spec(),
        worker_id="k8s-serving-worker",
        worker_session_id=WORKER_SESSION_ID,
    )
    exact_pod = runtime._build_pod(_spec(), selector)
    exact_pod.metadata.labels[SPEC_HASH_LABEL] = "0" * 32
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[exact_pod])
    with pytest.raises(KubernetesServingOwnershipError, match="mismatched spec hash"):
        await runtime.list_managed(worker_id="k8s-serving-worker")


async def test_version_and_close_only_close_owned_client() -> None:
    version_api = SimpleNamespace(
        get_code=AsyncMock(return_value=SimpleNamespace(git_version="v1.32.0"))
    )
    injected_client_close = AsyncMock()
    injected_api = SimpleNamespace(
        api_client=SimpleNamespace(close=injected_client_close),
    )
    runtime = _runtime(injected_api, version_api=version_api)

    assert await runtime.version() == "v1.32.0"
    await runtime.close()
    injected_client_close.assert_not_awaited()

    owned_close = AsyncMock()
    owned_api = SimpleNamespace(api_client=SimpleNamespace(close=owned_close))
    owned = _runtime(owned_api)
    owned._owns_api = True
    await owned.close()
    owned_close.assert_awaited_once()
    assert owned._api is None
