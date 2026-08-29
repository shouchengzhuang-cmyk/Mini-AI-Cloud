from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import Table, event
from sqlalchemy.dialects import postgresql

from api.services.kubernetes_replica_runtime import (
    KubernetesReplicaRuntimeController,
    StaleKubernetesServingController,
    _as_utc,
    _claim_from_models,
    _pod_metric_state,
)
from core.database import Database
from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    AllocationAuthority,
    ModelAvailabilityStatus,
    RuntimeType,
)
from core.metrics import K8S_SERVING_LAUNCH_FAILURES, K8S_SERVING_REPLACEMENTS
from core.runtime_profiles import RuntimeProfileCatalog, RuntimeProfileCompatibilityError
from models.base import Base
from models.identity import Project, User
from models.model_variant import LogicalModel, ModelVariant
from models.outbox import OutboxEvent
from models.registry import RegisteredModel
from models.scheduling import GPUDevice as PersistedGPUDevice
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
    ServingRuntime,
)
from models.usage import ProjectQuota, ProjectQuotaState
from models.worker import Worker
from repositories.admission import _active_service_accelerators
from repositories.services import ServiceRepository
from repositories.workers import WorkerRepository
from worker.kubernetes_serving_runtime import (
    CLUSTER_ID_LABEL,
    EXECUTION_ID_LABEL,
    GENERATION_LABEL,
    MANAGED_LABEL,
    PROJECT_ID_LABEL,
    REPLICA_ID_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_LABEL,
    SERVICE_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    KubernetesServingHandle,
    KubernetesServingLaunchSpec,
    KubernetesServingOwnershipError,
    KubernetesServingOwnershipIdentity,
    KubernetesServingRecoveryConflict,
    KubernetesServingState,
)

PROJECT_ID = uuid.UUID("d1000000-0000-0000-0000-000000000001")
REPOSITORY_ROOT = Path(__file__).parents[2]
pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def kubernetes_controller_database(tmp_path: Path) -> AsyncIterator[Database]:
    database = Database(
        f"sqlite+aiosqlite:///{(tmp_path / 'kubernetes-controller.sqlite3').as_posix()}"
    )

    @event.listens_for(database.engine.sync_engine, "connect")
    def configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()

    async with database.engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                cast(Table, User.__table__),
                cast(Table, Project.__table__),
                cast(Table, ProjectQuota.__table__),
                cast(Table, ProjectQuotaState.__table__),
                cast(Table, Worker.__table__),
                cast(Table, PersistedGPUDevice.__table__),
                cast(Table, RegisteredModel.__table__),
                cast(Table, LogicalModel.__table__),
                cast(Table, ModelVariant.__table__),
                cast(Table, ModelService.__table__),
                cast(Table, ServiceReplica.__table__),
                cast(Table, OutboxEvent.__table__),
            ],
        )
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=PROJECT_ID,
                name="Kubernetes Controller Tests",
                slug="kubernetes-controller-tests",
            )
        )
    try:
        yield database
    finally:
        await database.dispose()


class _Runtime:
    def __init__(self) -> None:
        self.prepared: list[KubernetesServingLaunchSpec] = []
        self.handles: dict[str, KubernetesServingHandle] = {}
        self.states: dict[str, KubernetesServingState] = {}
        self.stop_requested: list[str] = []
        self.force_cleaned: list[str] = []
        self.inspect_hook: Callable[[], Awaitable[None]] | None = None
        self.recovery_conflicts: Sequence[KubernetesServingRecoveryConflict] = ()
        self.list_error: Exception | None = None
        self.closed = False

    async def version(self) -> str:
        return "mock-kubernetes"

    async def prepare(
        self,
        spec: KubernetesServingLaunchSpec,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
    ) -> KubernetesServingHandle:
        self.prepared.append(spec)
        object_id = f"pod-{spec.replica_id.hex[:8]}-{spec.execution_id.hex[:8]}"
        labels = MappingProxyType(
            {
                SERVICE_ID_LABEL: str(spec.service_id),
                REPLICA_ID_LABEL: str(spec.replica_id),
                PROJECT_ID_LABEL: str(spec.project_id),
                GENERATION_LABEL: str(spec.generation),
                EXECUTION_ID_LABEL: str(spec.execution_id),
                MANAGED_LABEL: "true",
                CLUSTER_ID_LABEL: "kind-serving",
                WORKER_ID_LABEL: worker_id,
                WORKER_SESSION_ID_LABEL: str(worker_session_id),
                RUNTIME_LABEL: "kubernetes-serving",
                RESOURCE_KIND_LABEL: "serving-pod",
            }
        )
        handle = KubernetesServingHandle(
            object_id=object_id,
            display_id=object_id,
            endpoint_url=f"http://{object_id}.test.svc.cluster.local:8000",
            labels=labels,
            uid=f"uid-{object_id}",
            service_name=f"service-{object_id}",
            service_uid=f"service-uid-{object_id}",
            node_name=(spec.eligible_node_names[0] if spec.eligible_node_names else None),
        )
        self.handles[object_id] = handle
        self.states[object_id] = _state(phase="Pending")
        return handle

    async def start(self, handle: KubernetesServingHandle) -> KubernetesServingHandle:
        self.states[handle.object_id] = _state(
            phase="Running",
            running=True,
            node_name=handle.node_name,
        )
        return handle

    async def inspect(self, handle: KubernetesServingHandle) -> KubernetesServingState:
        if self.inspect_hook is not None:
            hook, self.inspect_hook = self.inspect_hook, None
            await hook()
        return self.states.get(handle.object_id, _state(missing=True))

    async def request_stop(self, handle: KubernetesServingHandle) -> None:
        self.stop_requested.append(handle.object_id)
        self.states[handle.object_id] = _state(
            phase="Running",
            running=True,
            deleting=True,
            endpoint_url=handle.endpoint_url,
        )

    async def force_cleanup(self, handle: KubernetesServingHandle) -> None:
        self.force_cleaned.append(handle.object_id)
        self.handles.pop(handle.object_id, None)
        self.states[handle.object_id] = _state(missing=True)

    async def list_managed(self, *, worker_id: str) -> Sequence[KubernetesServingHandle]:
        if self.list_error is not None:
            raise self.list_error
        return tuple(
            handle
            for handle in self.handles.values()
            if handle.labels.get(WORKER_ID_LABEL) == worker_id
        )

    async def close(self) -> None:
        self.closed = True


def _state(
    *,
    phase: str = "Unknown",
    running: bool = False,
    ready: bool = False,
    missing: bool = False,
    deleting: bool = False,
    exit_code: int | None = None,
    oom_killed: bool = False,
    reason: str | None = None,
    endpoint_url: str | None = None,
    image_digest: str | None = None,
    node_name: str | None = None,
) -> KubernetesServingState:
    return KubernetesServingState(
        phase=phase,
        running=running,
        ready=ready,
        missing=missing,
        deleting=deleting,
        exit_code=exit_code,
        oom_killed=oom_killed,
        reason=reason,
        message=None,
        endpoint_url=endpoint_url,
        image_digest=image_digest,
        node_name=node_name,
    )


def _ownership(handle: KubernetesServingHandle) -> KubernetesServingOwnershipIdentity:
    return KubernetesServingOwnershipIdentity(
        service_id=uuid.UUID(handle.labels[SERVICE_ID_LABEL]),
        replica_id=uuid.UUID(handle.labels[REPLICA_ID_LABEL]),
        project_id=uuid.UUID(handle.labels[PROJECT_ID_LABEL]),
        generation=int(handle.labels[GENERATION_LABEL]),
        execution_id=uuid.UUID(handle.labels[EXECUTION_ID_LABEL]),
        cluster_id=handle.labels[CLUSTER_ID_LABEL],
        worker_id=handle.labels[WORKER_ID_LABEL],
        worker_session_id=uuid.UUID(handle.labels[WORKER_SESSION_ID_LABEL]),
    )


def _controller(
    database: Database,
    runtime: _Runtime,
    **overrides: object,
) -> KubernetesReplicaRuntimeController:
    options: dict[str, object] = {
        "app_env": "test",
        "cluster_id": "kind-serving",
        "image": "mini-ai-cloud:kind-serving",
        "fake_enabled": True,
        "batch_size": 10,
        "startup_timeout_seconds": 10,
        "drain_timeout_seconds": 5,
        "poll_interval_seconds": 0.01,
        "lease_seconds": 30,
        "failure_backoff_seconds": 5,
        "termination_grace_seconds": 1,
        "fake_startup_delay_seconds": 0.5,
        "fake_chunk_delay_seconds": 0.02,
    }
    options.update(overrides)
    return KubernetesReplicaRuntimeController(database, runtime, **options)  # type: ignore[arg-type]


async def _create_service(
    database: Database,
    *,
    desired_replicas: int = 1,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name=f"k8s-fake-{uuid.uuid4().hex[:8]}",
            model="fake-kind-model",
            runtime=ServingRuntime.FAKE,
            runtime_type=RuntimeType.KUBERNETES,
            image="mini-ai-cloud:kind-serving",
            cpu_millicores=250,
            memory_mb=128,
            gpu_count=0,
            gpu_memory_mb=0,
            tensor_parallel_size=1,
            desired_replicas=desired_replicas,
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id


async def _create_profile_service(
    database: Database,
    *,
    desired_replicas: int = 1,
) -> tuple[uuid.UUID, RuntimeProfileCatalog]:
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    entry = next(
        item for item in catalog.manifest.profiles if item.identity == "nvidia-vllm-k8s@2.0.0"
    )
    profile = catalog.load_exact(
        profile_id=entry.profile_id,
        profile_version=entry.profile_version,
        semantic_digest=entry.semantic_digest,
    )
    logical_model_id = uuid.uuid4()
    variant_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        session.add(
            LogicalModel(
                id=logical_model_id,
                project_id=PROJECT_ID,
                name=f"controller-logical-{uuid.uuid4().hex[:8]}",
                public_name=f"controller-public-{uuid.uuid4().hex[:8]}",
                status=ModelAvailabilityStatus.READY,
            )
        )
        session.add(
            ModelVariant(
                id=variant_id,
                logical_model_id=logical_model_id,
                name="controller-nvidia-a100",
                vendor=AcceleratorVendor.NVIDIA,
                kind=AcceleratorKind.GPU,
                runtime_profile_id=profile.id,
                runtime_profile_version=profile.version,
                runtime_profile_digest=profile.semantic_digest(),
                artifact_source="org/controller-model",
                artifact_revision="revision-1",
                artifact_digest="sha256:" + "b" * 64,
                architecture="test-architecture",
                dtype="float16",
                status=ModelAvailabilityStatus.READY,
            )
        )
        await session.flush()
        service = await ServiceRepository.create(
            session,
            project_id=PROJECT_ID,
            name=f"k8s-profile-{uuid.uuid4().hex[:8]}",
            model="org/controller-model",
            runtime=ServingRuntime.VLLM,
            runtime_type=RuntimeType.KUBERNETES,
            image=profile.image.reference,
            cpu_millicores=1_000,
            memory_mb=4_096,
            gpu_count=2,
            gpu_memory_mb=40_960,
            tensor_parallel_size=2,
            dtype="float16",
            desired_replicas=desired_replicas,
            logical_model_id=logical_model_id,
            model_variant_id=variant_id,
            selected_vendor=AcceleratorVendor.NVIDIA,
            selected_kind=AcceleratorKind.GPU.value,
            selected_model="NVIDIA A100",
            runtime_profile_id=profile.id,
            runtime_profile_version=profile.version,
            runtime_profile_digest=profile.semantic_digest(),
            allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value,
            accelerator_resource_name=profile.kubernetes.resource_name,
            selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY.value,
            eligible_node_names=("gpu-node-a", "gpu-node-b"),
        )
        await ServiceRepository.reconcile_locked(session, service)
        return service.id, catalog


async def _replicas(database: Database, service_id: uuid.UUID) -> list[ServiceReplica]:
    async with database.session() as session:
        return await ServiceRepository.list_replicas(session, service_id)


def _profile_backed_models() -> tuple[ModelService, ServiceReplica, RuntimeProfileCatalog]:
    catalog = RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles/manifest.json")
    entry = next(
        item for item in catalog.manifest.profiles if item.identity == "nvidia-vllm-k8s@2.0.0"
    )
    profile = catalog.load_exact(
        profile_id=entry.profile_id,
        profile_version=entry.profile_version,
        semantic_digest=entry.semantic_digest,
    )
    service_id = uuid.uuid4()
    service = ModelService(
        id=service_id,
        project_id=PROJECT_ID,
        name="profile-backed-service",
        model="org/model",
        runtime=ServingRuntime.VLLM,
        runtime_type=RuntimeType.KUBERNETES,
        image="service-image-is-not-authoritative",
        cpu_millicores=1_000,
        memory_mb=4_096,
        gpu_count=0,
        tensor_parallel_size=2,
        desired_replicas=1,
        generation=1,
        # These mutable service fields deliberately disagree with the replica.
        selected_vendor="huawei-ascend",
        selected_kind="npu",
        runtime_profile_id="service-profile-must-not-be-used",
        runtime_profile_version="1.0.0",
        runtime_profile_digest="sha256:" + "0" * 64,
        allocation_authority=AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value,
    )
    replica = ServiceReplica(
        id=uuid.uuid4(),
        service_id=service_id,
        runtime=ServingRuntime.VLLM,
        generation=1,
        ordinal=0,
        status=ReplicaStatus.STARTING,
        execution_id=uuid.uuid4(),
        model_variant_id=uuid.uuid4(),
        selected_vendor=profile.vendor.value,
        selected_kind=profile.kind.value,
        selected_model="NVIDIA A100",
        runtime_profile_id=profile.id,
        runtime_profile_version=profile.version,
        runtime_profile_digest=profile.semantic_digest(),
        allocation_authority=AllocationAuthority.KUBERNETES_DEVICE_PLUGIN.value,
        accelerator_resource_name=profile.kubernetes.resource_name,
        selection_policy=AcceleratorSelectionPolicy.NVIDIA_ONLY.value,
        eligible_node_names=["gpu-node-a", "gpu-node-b"],
        started_at=datetime.now(UTC),
    )
    return service, replica, catalog


def test_profile_backed_claim_uses_only_the_replica_admission_snapshot() -> None:
    service, replica, catalog = _profile_backed_models()

    claim = _claim_from_models(replica, service, None, catalog)

    assert claim is not None
    assert claim.runtime_profile is not None
    assert claim.runtime_profile.id == replica.runtime_profile_id
    assert claim.runtime_profile.semantic_digest() == replica.runtime_profile_digest
    assert claim.image == claim.runtime_profile.image.reference
    assert claim.accelerator_count == 2
    assert claim.tensor_parallel_size == 2
    assert claim.eligible_node_names == ("gpu-node-a", "gpu-node-b")
    assert not hasattr(claim, "concrete_device_ids")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("runtime_profile_version", None, "incomplete replica accelerator snapshot"),
        (
            "allocation_authority",
            AllocationAuthority.CONTROL_PLANE_EXACT_DEVICE.value,
            "requires kubernetes_device_plugin authority",
        ),
        ("runtime_profile_digest", "sha256:" + "0" * 64, "immutable manifest"),
        ("accelerator_resource_name", "nvidia.com/mig", "resource does not match"),
        ("eligible_node_names", [], "eligible-node snapshot is empty"),
    ],
)
def test_profile_backed_claim_fails_closed_on_snapshot_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    service, replica, catalog = _profile_backed_models()
    setattr(replica, field, value)

    with pytest.raises(RuntimeProfileCompatibilityError, match=message):
        _claim_from_models(replica, service, None, catalog)


def test_fake_cpu_claim_remains_profile_free() -> None:
    service_id = uuid.uuid4()
    service = ModelService(
        id=service_id,
        project_id=PROJECT_ID,
        name="fake-cpu-service",
        model="fake-model",
        runtime=ServingRuntime.FAKE,
        runtime_type=RuntimeType.KUBERNETES,
        image="mini-ai-cloud:kind-serving",
        cpu_millicores=250,
        memory_mb=128,
        gpu_count=0,
        tensor_parallel_size=1,
        desired_replicas=1,
        generation=1,
    )
    replica = ServiceReplica(
        id=uuid.uuid4(),
        service_id=service_id,
        runtime=ServingRuntime.FAKE,
        generation=1,
        ordinal=0,
        status=ReplicaStatus.STARTING,
        execution_id=uuid.uuid4(),
        started_at=datetime.now(UTC),
    )

    claim = _claim_from_models(replica, service, None, None)

    assert claim is not None
    assert claim.runtime_profile is None
    assert claim.accelerator_count == 0
    assert claim.tensor_parallel_size == 1


async def test_legacy_pending_replica_fails_without_blocking_valid_sibling(
    kubernetes_controller_database: Database,
) -> None:
    service_id, catalog = await _create_profile_service(
        kubernetes_controller_database,
        desired_replicas=2,
    )
    async with kubernetes_controller_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(
            session,
            service_id,
            for_update=True,
        )
        replicas[0].eligible_node_names = []

    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await controller.startup()
    result = await controller.run_once()

    replicas = await _replicas(kubernetes_controller_database, service_id)
    assert result.claimed == 1
    assert replicas[0].status == ReplicaStatus.FAILED
    assert replicas[0].error_code == "KUBERNETES_RUNTIME_PROFILE_INVALID"
    assert replicas[1].status == ReplicaStatus.LOADING
    assert replicas[1].assigned_node_name == "gpu-node-a"
    await controller.close()


def test_claim_query_is_exact_and_skip_locked() -> None:
    compiled = str(
        KubernetesReplicaRuntimeController.claim_candidates_query(10).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "model_services.runtime" in compiled
    assert "model_services.runtime_type" in compiled


def test_controller_rejects_unsafe_fake_mode_but_allows_profile_only_mode() -> None:
    database = cast(Database, object())
    runtime = cast(_Runtime, object())
    with pytest.raises(ValueError, match="outside development and test"):
        KubernetesReplicaRuntimeController(
            database,
            runtime,
            app_env="production",
            cluster_id="kind-serving",
            image="mini-ai-cloud:test",
            fake_enabled=True,
        )
    controller = KubernetesReplicaRuntimeController(
        database,
        runtime,
        app_env="production",
        cluster_id="kind-serving",
        image=None,
        fake_enabled=False,
    )

    assert controller.fake_enabled is False


def test_default_worker_id_is_stable_kubernetes_label_value() -> None:
    database = cast(Database, object())
    runtime = cast(_Runtime, object())
    cluster_id = "a" * 63

    first = KubernetesReplicaRuntimeController(
        database,
        runtime,
        app_env="test",
        cluster_id=cluster_id,
        image="mini-ai-cloud:test",
        fake_enabled=True,
    )
    second = KubernetesReplicaRuntimeController(
        database,
        runtime,
        app_env="test",
        cluster_id=cluster_id,
        image="mini-ai-cloud:test",
        fake_enabled=True,
    )
    different = KubernetesReplicaRuntimeController(
        database,
        runtime,
        app_env="test",
        cluster_id="b" * 63,
        image="mini-ai-cloud:test",
        fake_enabled=True,
    )

    assert first.worker_id == second.worker_id
    assert first.worker_id != different.worker_id
    assert len(first.worker_id) <= 63


def test_pod_metric_state_never_treats_unready_running_pod_as_ready() -> None:
    assert _pod_metric_state(_state(phase="Running", running=True)) == "not_ready"
    assert (
        _pod_metric_state(
            _state(
                phase="Running",
                running=True,
                ready=True,
                endpoint_url="http://replica.test.svc.cluster.local:8000",
            )
        )
        == "ready"
    )
    assert (
        _pod_metric_state(
            _state(
                phase="Running",
                running=True,
                ready=True,
                deleting=True,
            )
        )
        == "terminating"
    )


async def test_global_managed_resource_discovery_failure_aborts_recovery_cycle(
    kubernetes_controller_database: Database,
) -> None:
    runtime = _Runtime()
    runtime.list_error = TimeoutError("Kubernetes API unavailable")
    controller = _controller(kubernetes_controller_database, runtime)
    assert controller.admission_ready is False

    with pytest.raises(TimeoutError, match="Kubernetes API unavailable"):
        await controller.startup()

    assert controller.active_pod_count == 0
    assert controller._recovered is False
    assert controller.admission_ready is False

    runtime.list_error = None
    await controller.startup()
    assert controller.admission_ready is True
    await controller.close()
    assert controller.admission_ready is False


async def test_admission_readiness_recovers_after_global_cycle_failure(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    assert controller.admission_ready is True

    original_heartbeat = controller._heartbeat_worker

    async def fail_heartbeat() -> None:
        raise TimeoutError("PostgreSQL heartbeat unavailable")

    monkeypatch.setattr(controller, "_heartbeat_worker", fail_heartbeat)
    with pytest.raises(TimeoutError, match="PostgreSQL heartbeat unavailable"):
        await controller.run_once()
    assert controller.admission_ready is False

    monkeypatch.setattr(controller, "_heartbeat_worker", original_heartbeat)
    await controller.run_once()
    assert controller.admission_ready is True
    await controller.close()
    assert controller.admission_ready is False


async def test_pod_ready_is_required_before_replica_becomes_healthy(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    try:
        await controller.startup()
        launched = await controller.run_once()
        assert launched.claimed == launched.launched == 1
        assert runtime.prepared[0].startup_delay_seconds == 0.5
        assert runtime.prepared[0].chunk_delay_seconds == 0.02

        replica = (await _replicas(kubernetes_controller_database, service_id))[0]
        assert replica.status == ReplicaStatus.LOADING
        assert replica.health == ReplicaHealth.UNKNOWN
        assert replica.endpoint_url is not None
        assert replica.image_digest is None
        assert replica.ready_at is None

        handle = next(iter(runtime.handles.values()))
        runtime.states[handle.object_id] = _state(
            phase="Running",
            running=True,
            ready=True,
            endpoint_url=handle.endpoint_url,
            image_digest=f"sha256:{'a' * 64}",
        )
        ready = await controller.run_once()
        assert ready.ready == 1
        replica = (await _replicas(kubernetes_controller_database, service_id))[0]
        assert replica.status == ReplicaStatus.RUNNING
        assert replica.health == ReplicaHealth.HEALTHY
        assert replica.image_digest == f"sha256:{'a' * 64}"
        assert replica.ready_at is not None
    finally:
        await controller.close()
    assert runtime.closed
    assert runtime.force_cleaned == []


async def test_drain_waits_for_active_request_then_stops_gracefully(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
    )
    await controller.run_once()

    async with kubernetes_controller_database.session() as session, session.begin():
        selection = await ServiceRepository.choose_healthy_endpoint(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
        )
        assert selection is not None
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert service is not None
        await ServiceRepository.reconcile_locked(session, service, drain_timeout_seconds=30)

    await controller.run_once()
    assert runtime.stop_requested == []

    async with kubernetes_controller_database.session() as session, session.begin():
        assert await ServiceRepository.release_endpoint_request(
            session,
            replica_id=selection.replica_id,
            generation=selection.generation,
            execution_id=selection.execution_id,
        )
    await controller.run_once()
    assert runtime.stop_requested == [handle.object_id]
    assert runtime.force_cleaned == []

    runtime.states[handle.object_id] = _state(missing=True)
    stopped = await controller.run_once()
    assert stopped.stopped >= 0
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status == ReplicaStatus.STOPPED
    await controller.close()


async def test_kubernetes_drain_timeout_caps_repository_deadline(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        drain_timeout_seconds=0,
    )
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
    )
    await controller.run_once()

    async with kubernetes_controller_database.session() as session, session.begin():
        selection = await ServiceRepository.choose_healthy_endpoint(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
        )
        assert selection is not None
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert service is not None
        await ServiceRepository.reconcile_locked(session, service, drain_timeout_seconds=300)

    result = await controller.run_once()
    assert result.stopped == 1
    assert runtime.force_cleaned == [handle.object_id]
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status == ReplicaStatus.STOPPED
    await controller.close()


async def test_new_worker_session_fences_old_controller_without_deleting_pod(
    kubernetes_controller_database: Database,
) -> None:
    await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    await controller.run_once()
    assert runtime.handles
    assert controller.admission_ready is True

    async with kubernetes_controller_database.session() as session, session.begin():
        await WorkerRepository.register(
            session,
            worker_id=controller.worker_id,
            worker_session_id=uuid.uuid4(),
            hostname="replacement-controller",
            node_name="kind-serving",
            concurrency=10,
            cpu_count=1,
            memory_total_mb=16,
            docker_version="mock-kubernetes",
            labels={"runtime": "kubernetes"},
            runtime_types=[RuntimeType.KUBERNETES.value],
            gpu_count=0,
            gpu_model=None,
            gpu_memory_mb=0,
        )

    with pytest.raises(StaleKubernetesServingController):
        await controller.run_once()
    assert controller.admission_ready is False
    assert runtime.stop_requested == []
    assert runtime.force_cleaned == []
    await controller.close()


async def test_session_change_during_inspect_prevents_missing_pod_cleanup(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    await controller.run_once()
    assert controller.admission_ready is True
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(missing=True)

    async def replace_worker_session() -> None:
        async with kubernetes_controller_database.session() as session, session.begin():
            await WorkerRepository.register(
                session,
                worker_id=controller.worker_id,
                worker_session_id=uuid.uuid4(),
                hostname="replacement-controller",
                node_name="kind-serving",
                concurrency=10,
                cpu_count=1,
                memory_total_mb=16,
                docker_version="mock-kubernetes",
                labels={"runtime": "kubernetes"},
                runtime_types=[RuntimeType.KUBERNETES.value],
                gpu_count=0,
                gpu_model=None,
                gpu_memory_mb=0,
            )

    runtime.inspect_hook = replace_worker_session
    result = await controller.run_once()

    assert result.stale == 1
    assert controller.admission_ready is False
    assert runtime.force_cleaned == []
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status == ReplicaStatus.LOADING
    await controller.close()


async def test_startup_adopts_existing_execution_and_close_preserves_it(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    first = _controller(kubernetes_controller_database, runtime)
    await first.startup()
    await first.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
    )
    await first.run_once()
    original_execution = (await _replicas(kubernetes_controller_database, service_id))[
        0
    ].execution_id
    await first.close()
    assert runtime.force_cleaned == []

    runtime.closed = False
    runtime.recovery_conflicts = (
        KubernetesServingRecoveryConflict(
            resource_kind="pod",
            resource_name="drifted-pod",
            reason="ownership_conflict",
            message="workload contract mismatch",
        ),
    )
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
    )
    startup = await replacement.startup()
    assert startup.recovered == 1
    assert startup.recovery_conflicts == 1
    assert len(runtime.prepared) == 1
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.execution_id == original_execution
    assert replica.status == ReplicaStatus.RUNNING
    await replacement.close()
    assert runtime.force_cleaned == []


async def test_recovery_isolates_legacy_replica_and_adopts_valid_sibling(
    kubernetes_controller_database: Database,
) -> None:
    service_id, catalog = await _create_profile_service(
        kubernetes_controller_database,
        desired_replicas=2,
    )
    runtime = _Runtime()
    first = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await first.startup()
    launched = await first.run_once()
    assert launched.claimed == 2
    replicas = await _replicas(kubernetes_controller_database, service_id)
    legacy_id = replicas[0].id
    valid_id = replicas[1].id
    legacy_handle_id = next(
        object_id
        for object_id, handle in runtime.handles.items()
        if handle.labels[REPLICA_ID_LABEL] == str(legacy_id)
    )
    await first.close()

    async with kubernetes_controller_database.session() as session, session.begin():
        legacy = await session.get(ServiceReplica, legacy_id, with_for_update=True)
        assert legacy is not None
        legacy.eligible_node_names = []

    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
        runtime_profile_catalog=catalog,
    )
    startup = await replacement.startup()

    replicas = await _replicas(kubernetes_controller_database, service_id)
    by_id = {replica.id: replica for replica in replicas}
    assert startup.recovered == 1
    assert startup.orphans_cleaned == 1
    assert by_id[legacy_id].status == ReplicaStatus.FAILED
    assert by_id[legacy_id].health == ReplicaHealth.UNHEALTHY
    assert by_id[legacy_id].endpoint_url is None
    assert by_id[valid_id].status == ReplicaStatus.LOADING
    assert legacy_handle_id in runtime.force_cleaned
    await replacement.close()


async def test_recovery_reconstructs_pre_0016_placement_from_owned_pods(
    kubernetes_controller_database: Database,
) -> None:
    service_id, catalog = await _create_profile_service(
        kubernetes_controller_database,
        desired_replicas=2,
    )
    runtime = _Runtime()
    first = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await first.startup()
    launched = await first.run_once()
    assert launched.claimed == 2
    await first.close()

    async with kubernetes_controller_database.session() as session, session.begin():
        service = await session.get(ModelService, service_id, with_for_update=True)
        assert service is not None
        service.eligible_node_names = []
        replicas = await ServiceRepository.list_replicas(
            session,
            service_id,
            for_update=True,
        )
        for replica in replicas:
            replica.eligible_node_names = []
            replica.assigned_node_name = None

    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
        runtime_profile_catalog=catalog,
    )
    startup = await replacement.startup()

    async with kubernetes_controller_database.session() as session:
        service = await session.get(ModelService, service_id)
        assert service is not None
        replicas = await ServiceRepository.list_replicas(session, service_id)
    assert startup.recovered == 2
    assert startup.orphans_cleaned == 0
    assert service.eligible_node_names == ["gpu-node-a"]
    assert all(replica.eligible_node_names == ["gpu-node-a"] for replica in replicas)
    assert all(replica.assigned_node_name == "gpu-node-a" for replica in replicas)
    assert runtime.force_cleaned == []
    await replacement.close()


async def test_contract_drift_cleanup_refusal_quarantines_without_releasing_capacity(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
        node_name=handle.node_name,
    )
    await controller.run_once()
    before = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert before.lease_expires_at is not None

    async def reject_drifted_inspect(
        _handle: KubernetesServingHandle,
    ) -> KubernetesServingState:
        raise KubernetesServingOwnershipError("immutable Pod contract drift")

    async def reject_drifted_cleanup(_handle: KubernetesServingHandle) -> None:
        raise KubernetesServingOwnershipError("cleanup contract fence mismatch")

    monkeypatch.setattr(runtime, "inspect", reject_drifted_inspect)
    monkeypatch.setattr(runtime, "force_cleanup", reject_drifted_cleanup)
    await controller.run_once()

    quarantined = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert quarantined.status == ReplicaStatus.RUNNING
    assert quarantined.health == ReplicaHealth.UNHEALTHY
    assert quarantined.endpoint_url is None
    assert quarantined.lease_expires_at is None
    assert controller._handles[quarantined.id].quarantined is True
    async with kubernetes_controller_database.session() as session:
        usage = await _active_service_accelerators(session)
    assert len(usage.commitments) == 1
    assert usage.commitments[0].assigned_node_name == handle.node_name
    async with kubernetes_controller_database.session() as session, session.begin():
        recovery = await ServiceRepository.recover_expired_leases(session, limit=10)
    assert recovery.replicas_lost == 0
    after_reaper = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert after_reaper.status == ReplicaStatus.RUNNING

    async with kubernetes_controller_database.session() as session, session.begin():
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert service is not None
        await ServiceRepository.reconcile_locked(session, service)
    await controller.run_once()
    assert runtime.stop_requested == []
    still_reserved = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert still_reserved.status in {
        ReplicaStatus.RUNNING,
        ReplicaStatus.DRAINING,
        ReplicaStatus.STOPPING,
    }
    assert still_reserved.endpoint_url is None
    await controller.close()


async def test_contract_drift_cleanup_success_marks_replica_terminal(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))

    async def reject_drifted_inspect(
        _handle: KubernetesServingHandle,
    ) -> KubernetesServingState:
        raise KubernetesServingOwnershipError("immutable Pod contract drift")

    monkeypatch.setattr(runtime, "inspect", reject_drifted_inspect)
    result = await controller.run_once()

    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert result.failed == 1
    assert replica.status == ReplicaStatus.FAILED
    assert replica.health == ReplicaHealth.UNHEALTHY
    assert replica.endpoint_url is None
    assert handle.object_id in runtime.force_cleaned
    assert replica.id not in controller._handles
    await controller.close()


async def test_restart_retries_persistent_quarantine_before_adopting_or_renewing(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    first = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await first.startup()
    await first.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
        node_name=handle.node_name,
    )
    await first.run_once()

    original_cleanup = runtime.force_cleanup

    async def reject_drifted_inspect(
        _handle: KubernetesServingHandle,
    ) -> KubernetesServingState:
        raise KubernetesServingOwnershipError("immutable Pod contract drift")

    async def reject_drifted_cleanup(_handle: KubernetesServingHandle) -> None:
        raise KubernetesServingOwnershipError("cleanup contract fence mismatch")

    monkeypatch.setattr(runtime, "inspect", reject_drifted_inspect)
    monkeypatch.setattr(runtime, "force_cleanup", reject_drifted_cleanup)
    await first.run_once()
    quarantined = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert quarantined.status == ReplicaStatus.RUNNING
    assert quarantined.lease_expires_at is None
    await first.close()

    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
        runtime_profile_catalog=catalog,
    )
    prepared_before = len(runtime.prepared)
    startup = await replacement.startup()

    persisted = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert startup.recovered == 0
    assert startup.orphans_cleaned == 0
    assert persisted.status == ReplicaStatus.RUNNING
    assert persisted.health == ReplicaHealth.UNHEALTHY
    assert persisted.endpoint_url is None
    assert persisted.lease_expires_at is None
    assert replacement._handles[persisted.id].quarantined is True
    assert len(runtime.prepared) == prepared_before

    monkeypatch.setattr(runtime, "force_cleanup", original_cleanup)
    result = await replacement.run_once()

    cleaned = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert result.failed == 1
    assert cleaned.status == ReplicaStatus.FAILED
    assert cleaned.endpoint_url is None
    assert cleaned.id not in replacement._handles
    assert handle.object_id in runtime.force_cleaned
    await replacement.close()


async def test_stop_before_inspect_ownership_drift_is_persistently_quarantined(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
        node_name=handle.node_name,
    )
    await controller.run_once()
    before = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert before.lease_expires_at is not None
    async with kubernetes_controller_database.session() as session, session.begin():
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert service is not None
        await ServiceRepository.reconcile_locked(session, service)

    async def reject_stop(_handle: KubernetesServingHandle) -> None:
        raise KubernetesServingOwnershipError("replacement Pod UID differs")

    monkeypatch.setattr(runtime, "request_stop", reject_stop)
    monkeypatch.setattr(runtime, "force_cleanup", reject_stop)
    before_prepared = len(runtime.prepared)

    result = await controller.run_once()

    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert result.failed == 0
    assert replica.status in {ReplicaStatus.DRAINING, ReplicaStatus.STOPPING}
    assert replica.health == ReplicaHealth.UNHEALTHY
    assert replica.endpoint_url is None
    assert replica.lease_expires_at is None
    assert controller._handles[replica.id].quarantined is True
    assert len(runtime.prepared) == before_prepared
    await controller.close()


async def test_advance_cleanup_ownership_drift_is_quarantined_without_aborting_cycle(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await controller.startup()
    await controller.run_once()

    async def reject_advance(
        _item: object,
        _state: KubernetesServingState,
    ) -> str:
        raise KubernetesServingOwnershipError("cleanup raced a replacement Pod")

    async def reject_cleanup(_handle: KubernetesServingHandle) -> None:
        raise KubernetesServingOwnershipError("replacement Pod UID differs")

    monkeypatch.setattr(controller, "_advance_item", reject_advance)
    monkeypatch.setattr(runtime, "force_cleanup", reject_cleanup)

    await controller.run_once()

    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status in {
        ReplicaStatus.STARTING,
        ReplicaStatus.LOADING,
        ReplicaStatus.RUNNING,
    }
    assert replica.health == ReplicaHealth.UNHEALTHY
    assert replica.endpoint_url is None
    assert replica.lease_expires_at is None
    assert controller._handles[replica.id].quarantined is True
    await controller.close()


async def test_restart_rebuilds_startup_deadline_from_execution_claim_time(
    kubernetes_controller_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    stale_created_at = datetime.now(UTC) - timedelta(minutes=5)
    async with kubernetes_controller_database.session() as session, session.begin():
        replica = (await ServiceRepository.list_replicas(session, service_id, for_update=True))[0]
        replica.created_at = stale_created_at
        replica.updated_at = stale_created_at

    runtime = _Runtime()
    first = _controller(
        kubernetes_controller_database,
        runtime,
        startup_timeout_seconds=120,
        lease_seconds=300,
    )
    await first.startup()
    claims, _waiting_backoff = await first._claim_pending_replicas()
    assert len(claims) == 1

    original_start = runtime.start

    async def delayed_start(handle: KubernetesServingHandle) -> KubernetesServingHandle:
        await asyncio.sleep(0.05)
        return await original_start(handle)

    monkeypatch.setattr(runtime, "start", delayed_start)

    async def interrupt_before_publish(
        _claim: object,
        _endpoint_url: str,
    ) -> bool:
        raise asyncio.CancelledError

    monkeypatch.setattr(first, "_publish_loading", interrupt_before_publish)
    with pytest.raises(asyncio.CancelledError):
        await first._launch(claims[0])

    claimed = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert claimed.status == ReplicaStatus.STARTING
    assert claimed.started_at is not None
    assert _as_utc(claimed.started_at) > stale_created_at + timedelta(minutes=4)
    claim_started_at = _as_utc(claimed.started_at)
    assert first._handles[claimed.id].startup_deadline == claim_started_at + timedelta(seconds=120)
    await first.close()

    async with kubernetes_controller_database.session() as session, session.begin():
        legacy = (await ServiceRepository.list_replicas(session, service_id, for_update=True))[0]
        legacy.started_at = None
        legacy.updated_at = claim_started_at

    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
        startup_timeout_seconds=120,
        lease_seconds=300,
    )
    startup = await replacement.startup()

    assert startup.recovered == 1
    item = replacement._handles[claimed.id]
    assert item.startup_deadline == claim_started_at + timedelta(seconds=120)
    assert item.startup_deadline > datetime.now(UTC)
    persisted = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert persisted.started_at is not None
    assert _as_utc(persisted.started_at) == claim_started_at
    cycle = await replacement.run_once()
    assert cycle.failed == 0
    recovered = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert recovered.status == ReplicaStatus.LOADING
    assert recovered.started_at is not None
    assert _as_utc(recovered.started_at) == claim_started_at
    assert runtime.force_cleaned == []
    await replacement.close()


async def test_recovery_quarantines_identity_proven_drift_without_loss_or_replacement(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database, desired_replicas=3)
    runtime = _Runtime()
    first = _controller(kubernetes_controller_database, runtime)
    await first.startup()
    await first.run_once()
    handles = list(runtime.handles.values())
    assert len(handles) == 3
    replicas_before = await _replicas(kubernetes_controller_database, service_id)
    executions_before = {replica.id: replica.execution_id for replica in replicas_before}
    await first.close()

    drifted = handles[2]
    runtime.handles.pop(drifted.object_id)
    runtime.recovery_conflicts = (
        KubernetesServingRecoveryConflict(
            resource_kind="pod",
            resource_name=drifted.object_id,
            reason="ownership_conflict",
            message="workload contract mismatch",
            ownership=_ownership(drifted),
        ),
    )
    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
    )

    startup = await replacement.startup()
    cycle = await replacement.run_once()

    assert startup.recovered == 2
    assert startup.recovery_conflicts == 1
    assert startup.orphans_cleaned == 0
    assert cycle.claimed == 0
    assert cycle.failed == 0
    replicas_after = await _replicas(kubernetes_controller_database, service_id)
    assert len(replicas_after) == 3
    assert {replica.id: replica.execution_id for replica in replicas_after} == executions_before
    assert all(
        replica.status in {ReplicaStatus.STARTING, ReplicaStatus.LOADING, ReplicaStatus.RUNNING}
        for replica in replicas_after
    )
    assert len(runtime.prepared) == 3
    assert runtime.force_cleaned == []
    assert runtime.stop_requested == []
    drifted_replica_id = uuid.UUID(drifted.labels[REPLICA_ID_LABEL])
    drifted_replica = next(item for item in replicas_after if item.id == drifted_replica_id)
    assert drifted_replica.health == ReplicaHealth.UNHEALTHY
    assert drifted_replica.endpoint_url is None
    assert drifted_replica.lease_expires_at is None
    assert drifted_replica_id in replacement._quarantined_claims
    await replacement.close()


async def test_unknown_recovery_conflict_blocks_admission_until_discovery_clears(
    kubernetes_controller_database: Database,
) -> None:
    runtime = _Runtime()
    runtime.recovery_conflicts = (
        KubernetesServingRecoveryConflict(
            resource_kind="pod",
            resource_name="untrusted-pod",
            reason="ownership_conflict",
            message="managed labels are incomplete",
        ),
    )
    controller = _controller(kubernetes_controller_database, runtime)

    startup = await controller.startup()

    assert startup.recovery_conflicts == 1
    assert controller.admission_ready is False
    runtime.recovery_conflicts = ()
    await controller.run_once()
    assert controller.admission_ready is True
    await controller.close()


async def test_recovery_quarantines_running_profile_replica_without_observed_node(
    kubernetes_controller_database: Database,
) -> None:
    service_id, catalog = await _create_profile_service(kubernetes_controller_database)
    runtime = _Runtime()
    first = _controller(
        kubernetes_controller_database,
        runtime,
        runtime_profile_catalog=catalog,
    )
    await first.startup()
    await first.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
        node_name=handle.node_name,
    )
    await first.run_once()
    running = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert running.status == ReplicaStatus.RUNNING
    assert running.assigned_node_name == handle.node_name
    await first.close()

    runtime.closed = False
    runtime.handles[handle.object_id] = replace(handle, node_name=None)
    runtime.states[handle.object_id] = _state(phase="Pending", node_name=None)
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
        runtime_profile_catalog=catalog,
    )

    startup = await replacement.startup()

    assert startup.orphans_cleaned == 1
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status == ReplicaStatus.FAILED
    assert replica.health == ReplicaHealth.UNHEALTHY
    assert replica.endpoint_url is None
    assert handle.object_id in runtime.force_cleaned
    await replacement.close()


async def test_restart_missing_pre_ready_counts_launch_failure_but_running_does_not(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database, desired_replicas=3)
    runtime = _Runtime()
    first = _controller(kubernetes_controller_database, runtime)
    await first.startup()
    await first.run_once()
    handles = list(runtime.handles.values())
    runtime.states[handles[0].object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handles[0].endpoint_url,
    )
    await first.run_once()
    async with kubernetes_controller_database.session() as session, session.begin():
        replicas = await ServiceRepository.list_replicas(
            session,
            service_id,
            for_update=True,
        )
        loading = next(replica for replica in replicas if replica.status == ReplicaStatus.LOADING)
        loading.status = ReplicaStatus.STARTING
    replicas_before = await _replicas(kubernetes_controller_database, service_id)
    assert {replica.status for replica in replicas_before} == {
        ReplicaStatus.STARTING,
        ReplicaStatus.LOADING,
        ReplicaStatus.RUNNING,
    }
    await first.close()

    runtime.handles.clear()
    runtime.closed = False
    launch_failures_before = K8S_SERVING_LAUNCH_FAILURES.labels(reason="pod_missing")._value.get()
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
    )

    startup = await replacement.startup()

    assert startup.recovered == 0
    assert (
        K8S_SERVING_LAUNCH_FAILURES.labels(reason="pod_missing")._value.get()
        == launch_failures_before + 2
    )
    replicas_after = await _replicas(kubernetes_controller_database, service_id)
    assert len(replicas_after) == 3
    assert all(replica.status == ReplicaStatus.LOST for replica in replicas_after)
    assert len(runtime.prepared) == 3
    await replacement.close()


async def test_restart_adopts_draining_replica_without_deleting_active_request(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    first = _controller(kubernetes_controller_database, runtime)
    await first.startup()
    await first.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
    )
    await first.run_once()

    async with kubernetes_controller_database.session() as session, session.begin():
        selection = await ServiceRepository.choose_healthy_endpoint(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
        )
        assert selection is not None
        service = await ServiceRepository.set_desired_replicas(
            session,
            service_id=service_id,
            project_id=PROJECT_ID,
            desired_replicas=0,
        )
        assert service is not None
        await ServiceRepository.reconcile_locked(session, service, drain_timeout_seconds=30)
    await first.close()

    runtime.closed = False
    replacement = _controller(
        kubernetes_controller_database,
        runtime,
        worker_id=first.worker_id,
    )
    startup = await replacement.startup()
    assert startup.recovered == 1
    await replacement.run_once()
    assert runtime.stop_requested == []
    assert runtime.force_cleaned == []
    replica = (await _replicas(kubernetes_controller_database, service_id))[0]
    assert replica.status == ReplicaStatus.DRAINING

    async with kubernetes_controller_database.session() as session, session.begin():
        assert await ServiceRepository.release_endpoint_request(
            session,
            replica_id=selection.replica_id,
            generation=selection.generation,
            execution_id=selection.execution_id,
        )
    await replacement.run_once()
    assert runtime.stop_requested == [handle.object_id]
    await replacement.close()


async def test_missing_pod_is_fenced_and_backoff_blocks_fast_relaunch(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    await controller.run_once()
    handle = next(iter(runtime.handles.values()))
    runtime.states[handle.object_id] = _state(
        phase="Running",
        running=True,
        ready=True,
        endpoint_url=handle.endpoint_url,
    )
    await controller.run_once()

    launch_failures_before = K8S_SERVING_LAUNCH_FAILURES.labels(reason="pod_missing")._value.get()
    runtime.states[handle.object_id] = _state(missing=True)
    failed = await controller.run_once()
    assert failed.failed == 1
    assert runtime.force_cleaned == [handle.object_id]
    assert (
        K8S_SERVING_LAUNCH_FAILURES.labels(reason="pod_missing")._value.get()
        == launch_failures_before
    )
    replicas = await _replicas(kubernetes_controller_database, service_id)
    assert replicas[0].status == ReplicaStatus.LOST

    async with kubernetes_controller_database.session() as session, session.begin():
        service = await ServiceRepository.get(session, service_id, for_update=True)
        assert service is not None
        assert service.scheduling_reason == "KUBERNETES_SERVING_BACKOFF"
        await ServiceRepository.reconcile_locked(session, service)
    before = len(runtime.prepared)
    waiting = await controller.run_once()
    assert waiting.waiting_backoff == 1
    assert len(runtime.prepared) == before


async def test_image_pull_and_oom_are_persisted_as_bounded_failures(
    kubernetes_controller_database: Database,
) -> None:
    service_id = await _create_service(kubernetes_controller_database, desired_replicas=2)
    runtime = _Runtime()
    controller = _controller(kubernetes_controller_database, runtime)
    await controller.startup()
    await controller.run_once()
    handles = list(runtime.handles.values())
    image_failures_before = K8S_SERVING_LAUNCH_FAILURES.labels(reason="image_pull")._value.get()
    oom_failures_before = K8S_SERVING_LAUNCH_FAILURES.labels(reason="oom")._value.get()
    replacements_before = K8S_SERVING_REPLACEMENTS.labels(reason="failure_backoff")._value.get()
    runtime.states[handles[0].object_id] = _state(
        phase="Pending",
        reason="ImagePullBackOff",
    )
    runtime.states[handles[1].object_id] = _state(
        phase="Failed",
        exit_code=137,
        oom_killed=True,
        reason="OOMKilled",
    )
    result = await controller.run_once()
    assert result.failed == 2
    assert (
        K8S_SERVING_LAUNCH_FAILURES.labels(reason="image_pull")._value.get()
        == image_failures_before + 1
    )
    assert K8S_SERVING_LAUNCH_FAILURES.labels(reason="oom")._value.get() == oom_failures_before + 1
    assert (
        K8S_SERVING_REPLACEMENTS.labels(reason="failure_backoff")._value.get()
        == replacements_before
    )
    replicas = await _replicas(kubernetes_controller_database, service_id)
    assert {replica.error_code for replica in replicas} == {
        "IMAGE_PULL_FAILED",
        "OOM_KILLED",
    }
    assert all(
        replica.error_message and len(replica.error_message.encode()) <= 4096
        for replica in replicas
    )

    async with kubernetes_controller_database.session() as session, session.begin():
        service = await ServiceRepository.get(session, service_id, for_update=True)
        assert service is not None
        service.scheduling_details = {
            **service.scheduling_details,
            "retry_not_before": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        }
        await ServiceRepository.reconcile_locked(session, service)
    replacement_result = await controller.run_once()
    assert replacement_result.claimed == 2
    assert (
        K8S_SERVING_REPLACEMENTS.labels(reason="failure_backoff")._value.get()
        == replacements_before + 2
    )
    await controller.close()
