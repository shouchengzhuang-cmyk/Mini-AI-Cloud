import asyncio
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from kubernetes_asyncio import client
from kubernetes_asyncio.client.exceptions import ApiException

from core.enums import ErrorCode
from core.runtime_profiles import RuntimeProfileCatalog
from worker.kubernetes_runtime import (
    BATCH_JOB_RESOURCE_KIND,
    CLUSTER_ID_LABEL,
    CONTROLLER_SESSION_ANNOTATION,
    EXECUTION_ID_LABEL,
    MANAGED_LABEL,
    PROJECT_ID_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_PROFILE_DIGEST_LABEL,
    SPEC_HASH_LABEL,
    TASK_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    KubernetesArtifactsUnsupported,
    KubernetesDeadlineExceeded,
    KubernetesGpuUnavailable,
    KubernetesImagePullFailed,
    KubernetesOomKilled,
    KubernetesRuntime,
    KubernetesRuntimeError,
)
from worker.runtime import ExecutionSpec, RuntimeHandle, RuntimeMount

pytestmark = pytest.mark.asyncio

REPOSITORY_ROOT = Path(__file__).parents[2]
WORKER_SESSION_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")


def _spec(**changes: object) -> ExecutionSpec:
    spec = ExecutionSpec(
        task_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        project_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        execution_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        worker_id="worker-k8s-a",
        worker_session_id=WORKER_SESSION_ID,
        image="python:3.12-alpine",
        command=("python", "-c", "print('ok')"),
        environment={"Z_VAR": "last", "A_VAR": "first"},
        timeout_seconds=45,
        cpu_limit=1.5,
        memory_limit_mb=768,
        gpu_count=0,
        network_enabled=True,
        labels={},
        runtime_type="kubernetes",
    )
    return replace(spec, **changes)  # type: ignore[arg-type]


def _catalog() -> RuntimeProfileCatalog:
    return RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")


def _profile_spec(identity: str) -> tuple[RuntimeProfileCatalog, ExecutionSpec]:
    catalog = _catalog()
    entry = next(item for item in catalog.manifest.profiles if item.identity == identity)
    return catalog, replace(
        _spec(),
        gpu_count=2,
        selected_vendor=entry.vendor.value,
        selected_kind=entry.kind.value,
        runtime_profile_id=entry.profile_id,
        runtime_profile_version=entry.profile_version,
        runtime_profile_digest=entry.semantic_digest,
        allocation_authority="kubernetes_device_plugin",
    )


def _job(runtime: KubernetesRuntime, spec: ExecutionSpec, *, uid: str = "job-uid") -> Any:
    job = runtime._build_job(spec)
    job.metadata.uid = uid
    job.metadata.resource_version = "1"
    return job


def _pod(
    job: Any,
    *,
    uid: str = "pod-uid",
    status: Any | None = None,
    controller_uid: str | None = None,
) -> Any:
    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name="controlled-pod",
            uid=uid,
            labels=dict(job.spec.template.metadata.labels),
            owner_references=[
                client.V1OwnerReference(
                    api_version="batch/v1",
                    kind="Job",
                    name=job.metadata.name,
                    uid=controller_uid or job.metadata.uid,
                    controller=True,
                    block_owner_deletion=True,
                )
            ],
        ),
        spec=job.spec.template.spec,
        status=status,
    )


def _apis(*, pods: list[object] | None = None) -> tuple[SimpleNamespace, SimpleNamespace]:
    batch = SimpleNamespace(
        create_namespaced_job=AsyncMock(),
        read_namespaced_job=AsyncMock(),
        delete_namespaced_job=AsyncMock(),
        patch_namespaced_job=AsyncMock(),
        list_namespaced_job=AsyncMock(),
    )
    core = SimpleNamespace(
        list_namespaced_pod=AsyncMock(return_value=SimpleNamespace(items=pods or [])),
        read_namespaced_pod_log=AsyncMock(),
        read_namespaced_pod_status=AsyncMock(),
    )

    async def create_job(*, namespace: str, body: Any) -> Any:
        assert namespace == "runtime-tests"
        body.metadata.uid = "job-uid"
        body.metadata.resource_version = "1"
        batch.read_namespaced_job.return_value = body
        return body

    batch.create_namespaced_job.side_effect = create_job
    return batch, core


class _StreamingLogContent:
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def iter_any(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk


class _StreamingLogResponse:
    def __init__(self, status: int, reason: str, *chunks: bytes) -> None:
        self.status = status
        self.reason = reason
        self.content = _StreamingLogContent(*chunks)
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _runtime(
    batch: object,
    core: object,
    *,
    app_env: str = "test",
    service_account_name: str | None = None,
    image_pull_secrets: Sequence[str] = (),
    networking_api: object | None = None,
    runtime_profile_catalog: RuntimeProfileCatalog | None = None,
) -> KubernetesRuntime:
    return KubernetesRuntime(
        namespace="runtime-tests",
        cluster_id="cluster-a",
        app_env=app_env,
        node_name="legacy-node-a",
        cleanup_grace_seconds=7,
        poll_interval=0.001,
        api=batch,
        core_api=core,
        networking_api=networking_api,
        service_account_name=service_account_name,
        image_pull_secrets=image_pull_secrets,
        runtime_profile_catalog=runtime_profile_catalog,
    )


async def test_prepare_builds_fenced_cpu_job_without_node_name_or_extended_resource() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core, app_env="production")
    spec = _spec()

    handle = await runtime.prepare(spec)

    job = batch.create_namespaced_job.call_args.kwargs["body"]
    pod_spec = job.spec.template.spec
    resources = pod_spec.containers[0].resources
    expected_labels = {
        TASK_ID_LABEL: str(spec.task_id),
        PROJECT_ID_LABEL: str(spec.project_id),
        EXECUTION_ID_LABEL: str(spec.execution_id),
        WORKER_ID_LABEL: spec.worker_id,
        WORKER_SESSION_ID_LABEL: str(WORKER_SESSION_ID),
        CLUSTER_ID_LABEL: "cluster-a",
        MANAGED_LABEL: "true",
        RESOURCE_KIND_LABEL: BATCH_JOB_RESOURCE_KIND,
        RUNTIME_PROFILE_DIGEST_LABEL: "none",
    }
    assert job.api_version == "batch/v1"
    assert job.kind == "Job"
    assert job.spec.backoff_limit == 0
    assert job.spec.active_deadline_seconds == 45
    assert job.spec.completions == job.spec.parallelism == 1
    assert job.spec.suspend is True
    assert pod_spec.restart_policy == "Never"
    assert pod_spec.node_name is None
    assert pod_spec.scheduler_name is None
    assert resources.requests == resources.limits == {"cpu": "1500m", "memory": "768Mi"}
    assert all("/" not in key for key in resources.requests)
    assert job.metadata.labels == job.spec.template.metadata.labels
    assert {**expected_labels, SPEC_HASH_LABEL: handle.spec_hash} == job.metadata.labels
    assert handle.resource_kind == "job"
    assert handle.resource_uid == "job-uid"
    assert handle.resource_version == "1"
    assert handle.controller_session_id == WORKER_SESSION_ID
    assert handle.namespace == "runtime-tests"
    assert handle.observation.pod_name is None


async def test_start_unsuspends_exact_durable_job_with_resource_version_cas() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    handle = await runtime.prepare(spec)
    started = _job(runtime, spec)
    started.spec.suspend = False
    started.metadata.resource_version = "2"
    batch.patch_namespaced_job.return_value = started

    await runtime.start(handle)

    batch.patch_namespaced_job.assert_awaited_once_with(
        name=handle.object_id,
        namespace="runtime-tests",
        body={"metadata": {"resourceVersion": "1"}, "spec": {"suspend": False}},
    )


async def test_suspended_job_releases_log_attach_barrier_before_start() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    handle = await runtime.prepare(_spec())
    ready = asyncio.Event()
    stream = runtime.logs(handle, ready=ready)

    async def consume() -> None:
        async for _item in stream:
            return

    consumer = asyncio.create_task(consume())

    await asyncio.wait_for(ready.wait(), timeout=0.2)

    consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.parametrize(
    ("worker_id", "replicas", "deleting", "expected"),
    [
        ("mini-ai-worker-0", 1, False, True),
        ("mini-ai-worker-1", 1, False, False),
        ("mini-ai-worker-0", 1, True, False),
        ("custom-worker", 1, False, False),
    ],
)
async def test_replacement_worker_expected_uses_statefulset_desired_ordinals(
    worker_id: str,
    replicas: int,
    deleting: bool,
    expected: bool,
) -> None:
    batch, core = _apis()
    statefulset = client.V1StatefulSet(
        metadata=client.V1ObjectMeta(
            name="mini-ai-worker",
            deletion_timestamp=(datetime(2026, 8, 30, tzinfo=UTC) if deleting else None),
        ),
        spec=client.V1StatefulSetSpec(
            replicas=replicas,
            selector=client.V1LabelSelector(match_labels={"app": "worker"}),
            service_name="mini-ai-worker",
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "worker"}),
                spec=client.V1PodSpec(
                    containers=[client.V1Container(name="worker", image="example/worker")]
                ),
            ),
        ),
    )
    apps = SimpleNamespace(
        read_namespaced_stateful_set=AsyncMock(return_value=statefulset),
    )
    runtime = KubernetesRuntime(
        namespace="runtime-tests",
        cluster_id="cluster-a",
        app_env="test",
        api=batch,
        core_api=core,
        apps_api=apps,
        worker_pod_namespace="system-tests",
        worker_statefulset_name="mini-ai-worker",
    )

    assert await runtime.replacement_worker_expected(worker_id=worker_id) is expected
    if worker_id.startswith("mini-ai-worker-"):
        apps.read_namespaced_stateful_set.assert_awaited_once_with(
            name="mini-ai-worker",
            namespace="system-tests",
        )
    else:
        apps.read_namespaced_stateful_set.assert_not_awaited()


async def test_prepare_applies_workload_credentials_to_batch_job() -> None:
    batch, core = _apis()
    runtime = _runtime(
        batch,
        core,
        service_account_name="task-runtime",
        image_pull_secrets=("registry-pull", "secondary-registry", "registry-pull"),
    )

    await runtime.prepare(_spec())

    pod_spec = batch.create_namespaced_job.call_args.kwargs["body"].spec.template.spec
    assert pod_spec.service_account_name == "task-runtime"
    assert [item.name for item in pod_spec.image_pull_secrets] == [
        "registry-pull",
        "secondary-registry",
    ]


async def test_prepare_accepts_api_server_defaulted_scheduler_name() -> None:
    batch, core = _apis()

    async def create_defaulted_job(*, namespace: str, body: Any) -> Any:
        assert namespace == "runtime-tests"
        body.metadata.uid = "job-uid"
        body.metadata.resource_version = "1"
        body.spec.template.spec.scheduler_name = "default-scheduler"
        batch.read_namespaced_job.return_value = body
        return body

    batch.create_namespaced_job.side_effect = create_defaulted_job

    handle = await _runtime(batch, core).prepare(_spec())
    created = cast(Any, handle.native)

    assert handle.resource_uid == "job-uid"
    assert created.spec.template.spec.scheduler_name == "default-scheduler"


@pytest.mark.parametrize(
    ("identity", "resource_name", "runtime_class", "scheduler"),
    [
        ("nvidia-vllm-k8s@2.0.0", "nvidia.com/gpu", "nvidia", None),
        ("ascend-vllm-k8s-a2@2.0.0", "huawei.com/Ascend910", "ascend", "volcano"),
    ],
)
async def test_profile_drives_accelerator_job_placement_without_node_name(
    identity: str,
    resource_name: str,
    runtime_class: str,
    scheduler: str | None,
) -> None:
    catalog, spec = _profile_spec(identity)
    batch, core = _apis()

    await _runtime(batch, core, runtime_profile_catalog=catalog).prepare(spec)

    job = batch.create_namespaced_job.call_args.kwargs["body"]
    pod_spec = job.spec.template.spec
    resources = pod_spec.containers[0].resources.requests
    assert resources[resource_name] == "2"
    assert pod_spec.runtime_class_name == runtime_class
    assert pod_spec.scheduler_name == scheduler
    assert pod_spec.node_name is None
    assert pod_spec.node_selector
    assert pod_spec.tolerations
    if identity.startswith("nvidia"):
        assert pod_spec.affinity is not None


async def test_accelerator_job_without_profile_fails_closed_without_nvidia_fallback() -> None:
    batch, core = _apis()
    with pytest.raises(KubernetesGpuUnavailable, match="exact Runtime Profile"):
        await _runtime(batch, core).prepare(replace(_spec(), gpu_count=1))
    batch.create_namespaced_job.assert_not_awaited()


async def test_create_conflict_adopts_only_exact_job() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    existing = _job(runtime, spec)
    batch.create_namespaced_job.side_effect = ApiException(status=409, reason="AlreadyExists")
    batch.read_namespaced_job.return_value = existing

    handle = await runtime.prepare(spec)

    assert handle.resource_uid == "job-uid"
    batch.read_namespaced_job.assert_any_await(
        name=runtime.job_name(spec.task_id, spec.execution_id),
        namespace="runtime-tests",
    )


async def test_create_conflict_adopts_api_server_defaulted_pod_fields() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    existing = _job(runtime, spec)
    existing.spec.template.spec.scheduler_name = "default-scheduler"
    existing.spec.template.spec.service_account_name = "default"
    batch.create_namespaced_job.side_effect = ApiException(status=409, reason="AlreadyExists")
    batch.read_namespaced_job.return_value = existing

    handle = await runtime.prepare(spec)
    adopted = cast(Any, handle.native)

    assert handle.resource_uid == "job-uid"
    assert adopted.spec.template.spec.scheduler_name == "default-scheduler"
    assert adopted.spec.template.spec.service_account_name == "default"


@pytest.mark.parametrize("drift", ["spec", "scheduler", "session"])
async def test_create_conflict_quarantines_spec_hash_or_session_drift(drift: str) -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    existing = _job(runtime, spec)
    if drift == "spec":
        existing.spec.template.spec.containers[0].image = "attacker/image:latest"
    elif drift == "scheduler":
        existing.spec.template.spec.scheduler_name = "attacker-scheduler"
    else:
        existing.metadata.labels[WORKER_SESSION_ID_LABEL] = str(uuid.uuid4())
    batch.create_namespaced_job.side_effect = ApiException(status=409)
    batch.read_namespaced_job.return_value = existing

    with pytest.raises(KubernetesRuntimeError, match="mismatched"):
        await runtime.prepare(spec)


async def test_logs_read_only_controlled_task_container_and_capture_pod_identity() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    pod = _pod(job)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])

    async def chunks() -> AsyncIterator[bytes]:
        yield b"one\n"
        yield b"two\n"

    core.read_namespaced_pod_log.return_value = chunks()
    handle = await runtime.prepare(spec)
    ready = asyncio.Event()

    logs = [item async for item in runtime.logs(handle, ready=ready)]

    assert ready.is_set()
    assert [item.content for item in logs] == [b"one\n", b"two\n"]
    assert [item.cursor_bytes for item in logs] == [4, 8]
    assert handle.observation.pod_name == "controlled-pod"
    assert handle.observation.pod_uid == "pod-uid"
    assert core.read_namespaced_pod_log.call_args.kwargs["container"] == "task"


async def test_adopted_logs_skip_the_durable_raw_byte_prefix() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job)])

    async def chunks() -> AsyncIterator[bytes]:
        yield b"old-"
        yield b"prefix-new\n"

    core.read_namespaced_pod_log.return_value = chunks()
    handle = await runtime.prepare(spec)
    handle.observation.log_cursor_bytes = len(b"old-prefix-")

    logs = [item async for item in runtime.logs(handle, ready=asyncio.Event())]

    assert [(item.content, item.cursor_bytes) for item in logs] == [(b"new\n", 15)]


async def test_logs_retry_terminal_streaming_400_with_non_follow_read() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job)])
    core.read_namespaced_pod_status.return_value = _pod(
        job,
        status=SimpleNamespace(phase="Succeeded", container_statuses=[]),
    )
    waiting = _StreamingLogResponse(400, "Bad Request", b"container is waiting\n")
    completed = _StreamingLogResponse(200, "OK", b"KIND_BATCH_SUCCESS\n")
    core.read_namespaced_pod_log.side_effect = [waiting, completed]
    handle = await runtime.prepare(spec)
    ready = asyncio.Event()

    logs = [item async for item in runtime.logs(handle, ready=ready)]

    assert ready.is_set()
    assert [item.content for item in logs] == [b"KIND_BATCH_SUCCESS\n"]
    assert waiting.closed
    assert completed.closed
    calls = core.read_namespaced_pod_log.call_args_list
    assert [call.kwargs["follow"] for call in calls] == [True, False]


async def test_logs_retry_pending_streaming_400_without_persisting_error_body() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job)])
    core.read_namespaced_pod_status.return_value = _pod(
        job,
        status=SimpleNamespace(phase="Pending", container_statuses=[]),
    )
    waiting = _StreamingLogResponse(400, "Bad Request", b"container is waiting\n")
    running = _StreamingLogResponse(200, "OK", b"KIND_BATCH_SUCCESS\n")
    core.read_namespaced_pod_log.side_effect = [waiting, running]
    handle = await runtime.prepare(spec)
    ready = asyncio.Event()

    logs = [item async for item in runtime.logs(handle, ready=ready)]

    assert ready.is_set()
    assert [item.content for item in logs] == [b"KIND_BATCH_SUCCESS\n"]
    assert waiting.closed
    assert running.closed
    calls = core.read_namespaced_pod_log.call_args_list
    assert [call.kwargs["follow"] for call in calls] == [True, True]


async def test_logs_reject_unchecked_streaming_http_error() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job)])
    forbidden = _StreamingLogResponse(403, "Forbidden", b"not task output\n")
    core.read_namespaced_pod_log.return_value = forbidden
    handle = await runtime.prepare(spec)
    ready = asyncio.Event()

    with pytest.raises(KubernetesRuntimeError, match="Forbidden"):
        _ = [item async for item in runtime.logs(handle, ready=ready)]

    assert ready.is_set()
    assert forbidden.closed


async def test_pod_from_foreign_job_controller_is_never_adopted() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    job = _job(runtime, spec)
    core.list_namespaced_pod.return_value = SimpleNamespace(
        items=[_pod(job, controller_uid="foreign-job-uid")]
    )
    batch.read_namespaced_job.return_value = job
    handle = RuntimeHandle(
        runtime_type="kubernetes",
        resource_kind="job",
        object_id=job.metadata.name,
        display_id=job.metadata.name,
        namespace="runtime-tests",
        resource_uid="job-uid",
        resource_version="1",
        controller_session_id=WORKER_SESSION_ID,
        spec_hash=job.metadata.labels[SPEC_HASH_LABEL],
        labels=job.metadata.labels,
    )

    with pytest.raises(KubernetesRuntimeError, match="no controlled Pod"):
        await runtime._controlled_pod(handle, required=True)


@pytest.mark.parametrize(("exit_code", "terminal"), [(0, "complete"), (9, "failed")])
async def test_wait_maps_job_terminal_state(exit_code: int, terminal: str) -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    handle = await runtime.prepare(spec)
    job = cast(Any, handle.native)
    condition_type = "Complete" if terminal == "complete" else "Failed"
    job.status = SimpleNamespace(
        conditions=[SimpleNamespace(type=condition_type, status="True", reason="")]
    )
    pod_status = SimpleNamespace(
        phase="Succeeded" if exit_code == 0 else "Failed",
        container_statuses=[
            SimpleNamespace(
                state=SimpleNamespace(
                    waiting=None,
                    terminated=SimpleNamespace(exit_code=exit_code, reason="Completed"),
                )
            )
        ],
    )
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job, status=pod_status)])
    batch.read_namespaced_job.return_value = job

    assert await runtime.wait(handle) == exit_code


async def test_wait_classifies_oom_image_pull_and_deadline() -> None:
    for status, error in (
        (
            SimpleNamespace(
                phase="Failed",
                container_statuses=[
                    SimpleNamespace(
                        state=SimpleNamespace(
                            waiting=None,
                            terminated=SimpleNamespace(exit_code=137, reason="OOMKilled"),
                        )
                    )
                ],
            ),
            KubernetesOomKilled,
        ),
        (
            SimpleNamespace(
                phase="Pending",
                container_statuses=[
                    SimpleNamespace(
                        state=SimpleNamespace(
                            waiting=SimpleNamespace(reason="ImagePullBackOff"),
                            terminated=None,
                        )
                    )
                ],
            ),
            KubernetesImagePullFailed,
        ),
    ):
        batch, core = _apis()
        runtime = _runtime(batch, core)
        spec = _spec()
        handle = await runtime.prepare(spec)
        job = cast(Any, handle.native)
        job.status = SimpleNamespace(active=1, conditions=[])
        core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job, status=status)])
        batch.read_namespaced_job.return_value = job
        with pytest.raises(error):
            await runtime.wait(handle)

    batch, core = _apis()
    runtime = _runtime(batch, core)
    handle = await runtime.prepare(_spec())
    job = cast(Any, handle.native)
    job.status = SimpleNamespace(
        conditions=[SimpleNamespace(type="Failed", status="True", reason="DeadlineExceeded")]
    )
    batch.read_namespaced_job.return_value = job
    with pytest.raises(KubernetesDeadlineExceeded):
        await runtime.wait(handle)


async def test_wait_ignores_failed_pod_count_until_deadline_condition_arrives() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    handle = await runtime.prepare(spec)

    failed_pod_observation = _job(runtime, spec)
    failed_pod_observation.status = SimpleNamespace(failed=1, conditions=[])
    deadline_observation = _job(runtime, spec)
    deadline_observation.status = SimpleNamespace(
        failed=1,
        conditions=[
            SimpleNamespace(type="FailureTarget", status="True", reason="DeadlineExceeded")
        ],
    )
    batch.read_namespaced_job.reset_mock()
    batch.read_namespaced_job.side_effect = [
        failed_pod_observation,
        failed_pod_observation,
        deadline_observation,
    ]

    with pytest.raises(KubernetesDeadlineExceeded):
        await runtime.wait(handle)

    assert batch.read_namespaced_job.await_count == 3


async def test_wait_prioritizes_deadline_failure_target_over_oom_shaped_pod() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    handle = await runtime.prepare(spec)
    job = cast(Any, handle.native)
    job.status = SimpleNamespace(
        failed=1,
        conditions=[
            SimpleNamespace(type="FailureTarget", status="True", reason="DeadlineExceeded")
        ],
    )
    pod_status = SimpleNamespace(
        phase="Failed",
        container_statuses=[
            SimpleNamespace(
                state=SimpleNamespace(
                    waiting=None,
                    terminated=SimpleNamespace(exit_code=137, reason="OOMKilled"),
                )
            )
        ],
    )
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job, status=pod_status)])
    core.list_namespaced_pod.reset_mock()
    batch.read_namespaced_job.return_value = job

    with pytest.raises(KubernetesDeadlineExceeded):
        await runtime.wait(handle)

    core.list_namespaced_pod.assert_not_awaited()


async def test_wait_reports_bounded_unschedulable_reason_without_long_tail() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    handle = await runtime.prepare(_spec())
    job = cast(Any, handle.native)
    job.status = SimpleNamespace(active=1, conditions=[])
    long_tail = "DO-NOT-PERSIST-THIS-TAIL"
    scheduling_message = "no matching accelerator node; " + "x" * 2048 + long_tail
    pod_status = SimpleNamespace(
        phase="Pending",
        container_statuses=[],
        conditions=[
            SimpleNamespace(
                reason="Unschedulable",
                message=scheduling_message,
            )
        ],
    )
    batch.read_namespaced_job.return_value = job
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[_pod(job, status=pod_status)])

    with pytest.raises(KubernetesRuntimeError) as caught:
        await runtime.wait(handle)

    assert caught.value.error_code == ErrorCode.CONTAINER_START_FAILED
    assert len(str(caught.value)) == 512
    assert long_tail not in str(caught.value)


async def test_missing_job_is_classified_as_worker_lost() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    handle = await runtime.prepare(_spec())
    batch.read_namespaced_job.side_effect = ApiException(status=404, reason="NotFound")

    with pytest.raises(KubernetesRuntimeError) as caught:
        await runtime.wait(handle)

    assert caught.value.error_code == ErrorCode.WORKER_LOST
    assert "is missing" in str(caught.value)


async def test_cancel_uses_job_uid_precondition_and_refuses_recreated_job() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    handle = await runtime.prepare(spec)
    live = batch.create_namespaced_job.call_args.kwargs["body"]
    batch.read_namespaced_job.return_value = live

    await runtime.stop(handle)

    body = batch.delete_namespaced_job.call_args.kwargs["body"]
    assert body.preconditions.uid == "job-uid"
    assert body.preconditions.resource_version == "1"
    assert body.grace_period_seconds == 7
    assert body.propagation_policy == "Foreground"

    live.metadata.uid = "replacement-uid"
    with pytest.raises(KubernetesRuntimeError, match="UID fence mismatch"):
        await runtime.stop(handle)


async def test_controller_transfer_cas_blocks_stale_delete_after_db_gate() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    spec = _spec()
    stale_handle = await runtime.prepare(spec)
    original = stale_handle.native
    batch.read_namespaced_job.return_value = original
    next_session = uuid.uuid4()
    patched = _job(runtime, spec)
    patched.metadata.annotations[CONTROLLER_SESSION_ANNOTATION] = str(next_session)
    patched.metadata.resource_version = "2"
    batch.patch_namespaced_job.return_value = patched

    current_handle = await runtime.transfer_controller(
        stale_handle,
        controller_session_id=next_session,
    )

    patch_body = batch.patch_namespaced_job.call_args.kwargs["body"]
    assert patch_body == {
        "metadata": {
            "resourceVersion": "1",
            "annotations": {CONTROLLER_SESSION_ANNOTATION: str(next_session)},
        }
    }
    assert current_handle.controller_session_id == next_session
    assert current_handle.resource_version == "2"
    assert current_handle.spec_hash == stale_handle.spec_hash

    # The old process may already have passed its DB gate.  The mutable Job
    # annotation is a second fence checked immediately before deletion.
    batch.read_namespaced_job.return_value = patched
    with pytest.raises(KubernetesRuntimeError, match="mismatched controller session"):
        await runtime.stop(stale_handle)
    batch.delete_namespaced_job.assert_not_awaited()

    await runtime.stop(current_handle)
    preconditions = batch.delete_namespaced_job.call_args.kwargs["body"].preconditions
    assert preconditions.uid == "job-uid"
    assert preconditions.resource_version == "2"


async def test_list_managed_recovers_exact_job_and_pod_after_restart() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    job = _job(runtime, _spec())
    pod = _pod(job)
    batch.list_namespaced_job.return_value = SimpleNamespace(items=[job])
    batch.read_namespaced_job.return_value = job
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])

    recovered = await runtime.list_managed(worker_id="worker-k8s-a")

    assert len(recovered) == 1
    assert recovered[0].resource_uid == "job-uid"
    assert recovered[0].observation.pod_uid == "pod-uid"
    assert runtime.recovery_conflicts == ()
    selector = batch.list_namespaced_job.call_args.kwargs["label_selector"]
    assert f"{RESOURCE_KIND_LABEL}={BATCH_JOB_RESOURCE_KIND}" in selector


async def test_list_managed_quarantines_spec_drift_without_adoption() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    job = _job(runtime, _spec())
    job.spec.template.spec.containers[0].command = ["malicious"]
    batch.list_namespaced_job.return_value = SimpleNamespace(items=[job])
    batch.read_namespaced_job.return_value = job

    assert await runtime.list_managed(worker_id="worker-k8s-a") == ()
    assert len(runtime.recovery_conflicts) == 1
    assert "spec hash" in runtime.recovery_conflicts[0].reason


async def test_production_artifact_request_fails_closed_before_api_call(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"input")
    spec = replace(
        _spec(),
        mounts=(RuntimeMount(str(source), "/workspace/input.bin", True),),
    )
    batch, core = _apis()

    with pytest.raises(KubernetesArtifactsUnsupported) as caught:
        await _runtime(batch, core, app_env="production").prepare(spec)

    assert caught.value.error_code == ErrorCode.KUBERNETES_ARTIFACTS_UNSUPPORTED
    batch.create_namespaced_job.assert_not_awaited()


async def test_retry_execution_gets_distinct_job_name_and_handle_identity_is_frozen() -> None:
    batch, core = _apis()
    runtime = _runtime(batch, core)
    first = await runtime.prepare(_spec())
    second_spec = replace(_spec(), execution_id=uuid.uuid4())
    second = await runtime.prepare(second_spec)

    assert first.object_id != second.object_id
    assert first.labels[EXECUTION_ID_LABEL] != second.labels[EXECUTION_ID_LABEL]
    with pytest.raises(FrozenInstanceError):
        first.object_id = "mutated"  # type: ignore[misc]
