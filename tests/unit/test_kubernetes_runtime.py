import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes_asyncio.client.exceptions import ApiException

from core.enums import ErrorCategory, ErrorCode
from core.runtime_profiles import RuntimeProfileCatalog
from worker.kubernetes_runtime import (
    ACCELERATOR_COUNT_ANNOTATION,
    ACCELERATOR_KIND_LABEL,
    ACCELERATOR_RESOURCE_ANNOTATION,
    ACCELERATOR_VENDOR_LABEL,
    ALLOCATION_AUTHORITY_ANNOTATION,
    EXECUTION_ID_LABEL,
    MANAGED_LABEL,
    NETWORK_POLICY_RESOURCE_KIND,
    PROJECT_ID_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_PROFILE_DIGEST_ANNOTATION,
    RUNTIME_PROFILE_ID_LABEL,
    RUNTIME_PROFILE_VERSION_LABEL,
    TASK_ID_LABEL,
    WORKER_ID_LABEL,
    KubernetesGpuUnavailable,
    KubernetesImagePullFailed,
    KubernetesOomKilled,
    KubernetesRuntime,
    KubernetesRuntimeError,
)
from worker.runtime import ComputeRuntime, ExecutionSpec, RuntimeHandle, RuntimeMount

REPOSITORY_ROOT = Path(__file__).parents[2]


def _spec() -> ExecutionSpec:
    return ExecutionSpec(
        task_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        project_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        execution_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        worker_id="worker-k8s-a",
        image="python:3.12-slim",
        command=("python", "-c", "print('ok')"),
        environment={"Z_VAR": "last", "A_VAR": "first"},
        timeout_seconds=45,
        cpu_limit=1.5,
        memory_limit_mb=512,
        gpu_count=2,
        network_enabled=False,
        labels={"region": "local"},
    )


def _fake_networking_api() -> SimpleNamespace:
    policies: dict[str, object] = {}

    async def create_policy(*, namespace: str, body: object) -> object:
        assert namespace == "runtime-tests"
        name = str(getattr(getattr(body, "metadata", None), "name", ""))
        policies[name] = body
        return body

    async def read_policy(*, name: str, namespace: str) -> object:
        assert namespace == "runtime-tests"
        try:
            return policies[name]
        except KeyError as exc:
            raise ApiException(status=404, reason="NotFound") from exc

    async def delete_policy(*, name: str, namespace: str, **_: object) -> None:
        assert namespace == "runtime-tests"
        policies.pop(name, None)

    return SimpleNamespace(
        policies=policies,
        create_namespaced_network_policy=AsyncMock(side_effect=create_policy),
        read_namespaced_network_policy=AsyncMock(side_effect=read_policy),
        delete_namespaced_network_policy=AsyncMock(side_effect=delete_policy),
    )


def _runtime(
    api: object,
    *,
    networking_api: object | None = None,
    runtime_profile_catalog: RuntimeProfileCatalog | None = None,
) -> KubernetesRuntime:
    return KubernetesRuntime(
        namespace="runtime-tests",
        node_name="gpu-node-a",
        cleanup_grace_seconds=7,
        poll_interval=0.001,
        api=api,
        networking_api=networking_api or _fake_networking_api(),
        runtime_profile_catalog=runtime_profile_catalog,
    )


def _handle() -> RuntimeHandle:
    spec = _spec()
    name = KubernetesRuntime.pod_name(spec.task_id, spec.execution_id)
    return RuntimeHandle(
        runtime_type="kubernetes",
        resource_kind="pod",
        object_id=name,
        display_id=name,
    )


def test_kubernetes_runtime_implements_compute_runtime_protocol() -> None:
    assert isinstance(_runtime(SimpleNamespace()), ComputeRuntime)


async def test_prepare_builds_fenced_pod_with_resources_node_and_deadline() -> None:
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())

    async def create_pod(*, namespace: str, body: object) -> object:
        assert namespace == "runtime-tests"
        return body

    api.create_namespaced_pod.side_effect = create_pod
    runtime = _runtime(api)
    spec = _spec()

    handle = await runtime.prepare(spec)

    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    expected_name = KubernetesRuntime.pod_name(spec.task_id, spec.execution_id)
    assert pod.metadata.name == expected_name
    assert pod.metadata.labels == {
        TASK_ID_LABEL: str(spec.task_id),
        PROJECT_ID_LABEL: str(spec.project_id),
        EXECUTION_ID_LABEL: str(spec.execution_id),
        WORKER_ID_LABEL: spec.worker_id,
        MANAGED_LABEL: "true",
    }
    assert pod.spec.node_name == "gpu-node-a"
    assert pod.spec.restart_policy == "Never"
    assert pod.spec.active_deadline_seconds == 45
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.security_context.run_as_non_root is True
    assert pod.spec.security_context.run_as_user == 65532
    assert pod.spec.security_context.run_as_group == 65532
    assert pod.spec.security_context.seccomp_profile.type == "RuntimeDefault"
    container = pod.spec.containers[0]
    assert container.name == "task"
    assert container.image == spec.image
    assert container.command == list(spec.command)
    assert [(item.name, item.value) for item in container.env] == [
        ("A_VAR", "first"),
        ("Z_VAR", "last"),
    ]
    expected_resources = {
        "cpu": "1500m",
        "memory": "512Mi",
        "nvidia.com/gpu": "2",
    }
    assert container.resources.requests == expected_resources
    assert container.resources.limits == expected_resources
    assert container.security_context.allow_privilege_escalation is False
    assert container.security_context.capabilities.drop == ["ALL"]
    assert container.security_context.privileged is False
    assert container.security_context.read_only_root_filesystem is True
    assert handle.runtime_type == "kubernetes"
    assert handle.resource_kind == "pod"
    assert handle.object_id == expected_name


async def test_prepare_renders_ascend_resource_from_exact_runtime_profile() -> None:
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: body
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    entry = next(
        profile
        for profile in catalog.manifest.profiles
        if profile.identity == "ascend-vllm-k8s-a2@2.0.0"
    )
    spec = replace(
        _spec(),
        selected_vendor=entry.vendor.value,
        selected_kind=entry.kind.value,
        runtime_profile_id=entry.profile_id,
        runtime_profile_version=entry.profile_version,
        runtime_profile_digest=entry.semantic_digest,
        allocation_authority="kubernetes_device_plugin",
    )

    await _runtime(api, runtime_profile_catalog=catalog).prepare(spec)

    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    container = pod.spec.containers[0]
    assert container.resources.requests["huawei.com/Ascend910"] == "2"
    assert "nvidia.com/gpu" not in container.resources.requests
    assert pod.spec.runtime_class_name == "ascend"
    assert pod.spec.scheduler_name == "volcano"
    assert pod.spec.node_selector == {"accelerator.mini-ai-cloud/vendor": "huawei-ascend"}
    assert pod.spec.tolerations[0].key == "huawei.com/Ascend910"
    assert pod.metadata.labels[ACCELERATOR_VENDOR_LABEL] == "huawei-ascend"
    assert pod.metadata.labels[ACCELERATOR_KIND_LABEL] == "npu"
    assert pod.metadata.labels[RUNTIME_PROFILE_ID_LABEL] == entry.profile_id
    assert pod.metadata.labels[RUNTIME_PROFILE_VERSION_LABEL] == entry.profile_version
    assert pod.metadata.annotations == {
        RUNTIME_PROFILE_DIGEST_ANNOTATION: entry.semantic_digest,
        ACCELERATOR_RESOURCE_ANNOTATION: "huawei.com/Ascend910",
        ACCELERATOR_COUNT_ANNOTATION: "2",
        ALLOCATION_AUTHORITY_ANNOTATION: "kubernetes_device_plugin",
    }
    visibility = next(item for item in container.env if item.name == "ASCEND_VISIBLE_DEVICES")
    assert visibility.value_from.field_ref.field_path == (
        "metadata.annotations['huawei.com/Ascend910']"
    )


def test_vendor_aware_kubernetes_task_fails_closed_on_profile_drift() -> None:
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    entry = next(
        profile for profile in catalog.manifest.profiles if profile.vendor.value == "nvidia"
    )
    spec = replace(
        _spec(),
        selected_vendor=entry.vendor.value,
        selected_kind=entry.kind.value,
        runtime_profile_id=entry.profile_id,
        runtime_profile_version=entry.profile_version,
        runtime_profile_digest="sha256:" + "0" * 64,
        allocation_authority="kubernetes_device_plugin",
    )

    with pytest.raises(KubernetesGpuUnavailable, match="digest"):
        _runtime(SimpleNamespace(), runtime_profile_catalog=catalog)._build_pod(spec)


async def test_prepare_creates_task_scoped_deny_all_policy_before_pod() -> None:
    calls: list[str] = []
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())
    networking_api = _fake_networking_api()

    async def create_policy(*, namespace: str, body: object) -> object:
        calls.append("policy")
        assert namespace == "runtime-tests"
        return body

    async def create_pod(*, namespace: str, body: object) -> object:
        calls.append("pod")
        assert namespace == "runtime-tests"
        return body

    networking_api.create_namespaced_network_policy.side_effect = create_policy
    api.create_namespaced_pod.side_effect = create_pod
    spec = _spec()

    await _runtime(api, networking_api=networking_api).prepare(spec)

    policy = networking_api.create_namespaced_network_policy.call_args.kwargs["body"]
    expected_labels = {
        TASK_ID_LABEL: str(spec.task_id),
        PROJECT_ID_LABEL: str(spec.project_id),
        EXECUTION_ID_LABEL: str(spec.execution_id),
        WORKER_ID_LABEL: spec.worker_id,
        MANAGED_LABEL: "true",
    }
    assert calls == ["policy", "pod"]
    assert policy.metadata.labels == {
        **expected_labels,
        RESOURCE_KIND_LABEL: NETWORK_POLICY_RESOURCE_KIND,
    }
    assert policy.spec.pod_selector.match_labels == expected_labels
    assert set(policy.spec.policy_types) == {"Ingress", "Egress"}
    assert policy.spec.ingress == []
    assert policy.spec.egress == []


async def test_prepare_skips_policy_when_task_network_is_enabled() -> None:
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: body
    networking_api = _fake_networking_api()

    await _runtime(api, networking_api=networking_api).prepare(
        replace(_spec(), network_enabled=True)
    )

    networking_api.create_namespaced_network_policy.assert_not_awaited()


async def test_failed_pod_create_retains_deny_all_policy_for_safe_retry() -> None:
    api = SimpleNamespace(
        create_namespaced_pod=AsyncMock(
            side_effect=ApiException(status=500, reason="control-plane unavailable")
        )
    )
    networking_api = _fake_networking_api()

    with pytest.raises(KubernetesRuntimeError, match="control-plane unavailable"):
        await _runtime(api, networking_api=networking_api).prepare(_spec())

    assert len(networking_api.policies) == 1
    networking_api.delete_namespaced_network_policy.assert_not_awaited()


async def test_prepare_pins_file_scoped_artifact_host_paths(tmp_path: Path) -> None:
    input_path = tmp_path / "input.bin"
    output_path = tmp_path / "output.bin"
    input_path.write_bytes(b"input")
    output_path.touch()
    spec = replace(
        _spec(),
        mounts=(
            RuntimeMount(str(input_path), "/workspace/inputs/input.bin", True),
            RuntimeMount(str(output_path), "/output/model.bin", False),
        ),
    )
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())

    async def create_pod(*, namespace: str, body: object) -> object:
        assert namespace == "runtime-tests"
        return body

    api.create_namespaced_pod.side_effect = create_pod

    await _runtime(api).prepare(spec)

    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    assert pod.spec.node_name == "gpu-node-a"
    assert [volume.host_path.path for volume in pod.spec.volumes] == [
        str(input_path.resolve()),
        str(output_path.resolve()),
    ]
    assert [volume.host_path.type for volume in pod.spec.volumes] == ["File", "File"]
    assert [
        (mount.name, mount.mount_path, mount.read_only)
        for mount in pod.spec.containers[0].volume_mounts
    ] == [
        ("artifact-0", "/workspace/inputs/input.bin", True),
        ("artifact-1", "/output/model.bin", False),
    ]


async def test_prepare_adopts_same_execution_after_already_exists() -> None:
    spec = _spec()
    existing = SimpleNamespace(
        metadata=SimpleNamespace(
            labels={
                TASK_ID_LABEL: str(spec.task_id),
                PROJECT_ID_LABEL: str(spec.project_id),
                EXECUTION_ID_LABEL: str(spec.execution_id),
                WORKER_ID_LABEL: spec.worker_id,
                MANAGED_LABEL: "true",
            }
        )
    )
    api = SimpleNamespace(
        create_namespaced_pod=AsyncMock(
            side_effect=ApiException(status=409, reason="AlreadyExists")
        ),
        read_namespaced_pod=AsyncMock(return_value=existing),
    )

    handle = await _runtime(api).prepare(spec)

    api.read_namespaced_pod.assert_awaited_once_with(
        name=handle.object_id,
        namespace="runtime-tests",
    )
    assert handle.native is existing


async def test_prepare_adopts_only_matching_deny_all_policy_after_conflict() -> None:
    spec = _spec()
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: body
    networking_api = _fake_networking_api()
    runtime = _runtime(api, networking_api=networking_api)
    existing_policy = runtime._build_network_policy(spec)
    networking_api.create_namespaced_network_policy.side_effect = ApiException(
        status=409,
        reason="AlreadyExists",
    )
    networking_api.read_namespaced_network_policy.return_value = existing_policy
    networking_api.read_namespaced_network_policy.side_effect = None

    await runtime.prepare(spec)

    networking_api.read_namespaced_network_policy.assert_awaited_once_with(
        name=runtime.network_policy_name(runtime.pod_name(spec.task_id, spec.execution_id)),
        namespace="runtime-tests",
    )


async def test_prepare_refuses_conflicting_policy_with_narrower_selector() -> None:
    spec = _spec()
    api = SimpleNamespace(create_namespaced_pod=AsyncMock())
    networking_api = _fake_networking_api()
    runtime = _runtime(api, networking_api=networking_api)
    conflicting_policy = runtime._build_network_policy(spec)
    conflicting_policy.spec.pod_selector.match_labels = {
        **conflicting_policy.spec.pod_selector.match_labels,
        EXECUTION_ID_LABEL: str(uuid.uuid4()),
    }
    networking_api.create_namespaced_network_policy.side_effect = ApiException(status=409)
    networking_api.read_namespaced_network_policy.return_value = conflicting_policy
    networking_api.read_namespaced_network_policy.side_effect = None

    with pytest.raises(KubernetesRuntimeError, match="exact deny-all isolation"):
        await runtime.prepare(spec)

    api.create_namespaced_pod.assert_not_awaited()


async def test_prepare_refuses_to_adopt_pod_from_stale_execution() -> None:
    spec = _spec()
    existing = SimpleNamespace(
        metadata=SimpleNamespace(
            labels={
                TASK_ID_LABEL: str(spec.task_id),
                PROJECT_ID_LABEL: str(spec.project_id),
                EXECUTION_ID_LABEL: str(uuid.uuid4()),
                WORKER_ID_LABEL: spec.worker_id,
                MANAGED_LABEL: "true",
            }
        )
    )
    api = SimpleNamespace(
        create_namespaced_pod=AsyncMock(side_effect=ApiException(status=409)),
        read_namespaced_pod=AsyncMock(return_value=existing),
    )

    with pytest.raises(KubernetesRuntimeError, match="mismatched execution labels"):
        await _runtime(api).prepare(spec)


async def test_logs_wait_start_stop_and_cleanup_use_pod_lifecycle() -> None:
    async def chunks() -> AsyncIterator[bytes | str]:
        yield b"stdout-one\n"
        yield "stdout-two\n"

    terminated = SimpleNamespace(exit_code=17)
    pod = SimpleNamespace(
        status=SimpleNamespace(
            phase="Succeeded",
            container_statuses=[SimpleNamespace(state=SimpleNamespace(terminated=terminated))],
        )
    )
    api = SimpleNamespace(
        read_namespaced_pod=AsyncMock(return_value=pod),
        read_namespaced_pod_log=AsyncMock(return_value=chunks()),
        read_namespaced_pod_status=AsyncMock(return_value=pod),
        delete_namespaced_pod=AsyncMock(),
    )
    runtime = _runtime(api)
    handle = _handle()
    ready = asyncio.Event()

    await runtime.start(handle)
    logs = [item async for item in runtime.logs(handle, ready=ready)]
    exit_code = await runtime.wait(handle)
    await runtime.stop(handle)
    await runtime.cleanup(handle)

    assert ready.is_set()
    assert [(item.stream, item.content) for item in logs] == [
        ("stdout", b"stdout-one\n"),
        ("stdout", b"stdout-two\n"),
    ]
    assert exit_code == 17
    assert api.delete_namespaced_pod.await_count == 2
    stop_call, cleanup_call = api.delete_namespaced_pod.await_args_list
    assert stop_call.kwargs["grace_period_seconds"] == 7
    assert cleanup_call.kwargs["grace_period_seconds"] == 0
    assert stop_call.kwargs["propagation_policy"] == "Background"


async def test_cleanup_deletes_only_its_task_scoped_network_policy() -> None:
    api = SimpleNamespace(
        create_namespaced_pod=AsyncMock(),
        delete_namespaced_pod=AsyncMock(),
    )
    api.create_namespaced_pod.side_effect = lambda *, namespace, body: body
    networking_api = _fake_networking_api()
    runtime = _runtime(api, networking_api=networking_api)

    handle = await runtime.prepare(_spec())
    assert networking_api.policies

    await runtime.cleanup(handle)

    assert networking_api.policies == {}
    networking_api.delete_namespaced_network_policy.assert_awaited_once()


async def test_cleanup_refuses_to_delete_non_managed_policy() -> None:
    api = SimpleNamespace(delete_namespaced_pod=AsyncMock())
    networking_api = _fake_networking_api()
    runtime = _runtime(api, networking_api=networking_api)
    policy = runtime._build_network_policy(_spec())
    policy.metadata.labels.pop(RESOURCE_KIND_LABEL)
    networking_api.read_namespaced_network_policy.return_value = policy
    networking_api.read_namespaced_network_policy.side_effect = None

    with pytest.raises(KubernetesRuntimeError, match="mismatched execution labels"):
        await runtime.cleanup(_handle())

    networking_api.delete_namespaced_network_policy.assert_not_awaited()


async def test_wait_polls_until_failed_and_classifies_exit_137_as_oom() -> None:
    running = SimpleNamespace(status=SimpleNamespace(phase="Running"))
    failed = SimpleNamespace(
        status=SimpleNamespace(
            phase="Failed",
            container_statuses=[
                SimpleNamespace(state=SimpleNamespace(terminated=SimpleNamespace(exit_code=137)))
            ],
        )
    )
    api = SimpleNamespace(read_namespaced_pod_status=AsyncMock(side_effect=[running, failed]))

    with pytest.raises(KubernetesOomKilled) as caught:
        await _runtime(api).wait(_handle())

    assert caught.value.exit_code == 137
    assert caught.value.error_category == ErrorCategory.RESOURCE_ERROR
    assert caught.value.error_code == ErrorCode.OOM_KILLED
    assert api.read_namespaced_pod_status.await_count == 2


async def test_wait_classifies_image_pull_backoff() -> None:
    waiting = SimpleNamespace(
        status=SimpleNamespace(
            phase="Pending",
            container_statuses=[
                SimpleNamespace(
                    state=SimpleNamespace(waiting=SimpleNamespace(reason="ImagePullBackOff"))
                )
            ],
        )
    )
    api = SimpleNamespace(read_namespaced_pod_status=AsyncMock(return_value=waiting))

    with pytest.raises(KubernetesImagePullFailed) as caught:
        await _runtime(api).wait(_handle())

    assert caught.value.error_category == ErrorCategory.INFRA_ERROR
    assert caught.value.error_code == ErrorCode.IMAGE_PULL_FAILED


@pytest.mark.parametrize("operation", ["stop", "cleanup"])
async def test_delete_is_idempotent_when_pod_is_already_gone(operation: str) -> None:
    api = SimpleNamespace(delete_namespaced_pod=AsyncMock(side_effect=ApiException(status=404)))
    runtime = _runtime(api)

    await getattr(runtime, operation)(_handle())


async def test_runtime_rejects_foreign_handle() -> None:
    runtime = _runtime(SimpleNamespace())
    foreign = RuntimeHandle(
        runtime_type="docker",
        resource_kind="container",
        object_id="container-id",
        display_id="container",
    )

    with pytest.raises(KubernetesRuntimeError, match="docker"):
        await runtime.cleanup(foreign)
