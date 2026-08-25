from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
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
    _pod_metric_state,
)
from core.database import Database
from core.enums import RuntimeType
from core.metrics import K8S_SERVING_LAUNCH_FAILURES, K8S_SERVING_REPLACEMENTS
from models.base import Base
from models.identity import Project, User
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
    KubernetesServingOwnershipIdentity,
    KubernetesServingRecoveryConflict,
    KubernetesServingState,
)

PROJECT_ID = uuid.UUID("d1000000-0000-0000-0000-000000000001")
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
        )
        self.handles[object_id] = handle
        self.states[object_id] = _state(phase="Pending")
        return handle

    async def start(self, handle: KubernetesServingHandle) -> KubernetesServingHandle:
        self.states[handle.object_id] = _state(phase="Running", running=True)
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


async def _replicas(database: Database, service_id: uuid.UUID) -> list[ServiceReplica]:
    async with database.session() as session:
        return await ServiceRepository.list_replicas(session, service_id)


def test_claim_query_is_exact_and_skip_locked() -> None:
    compiled = str(
        KubernetesReplicaRuntimeController.claim_candidates_query(10).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "FOR UPDATE SKIP LOCKED" in compiled
    assert "model_services.runtime" in compiled
    assert "model_services.runtime_type" in compiled


def test_controller_rejects_unsafe_fake_modes() -> None:
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
    with pytest.raises(ValueError, match="explicit opt-in"):
        KubernetesReplicaRuntimeController(
            database,
            runtime,
            app_env="test",
            cluster_id="kind-serving",
            image="mini-ai-cloud:test",
            fake_enabled=False,
        )


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
        assert replica.ready_at is None

        handle = next(iter(runtime.handles.values()))
        runtime.states[handle.object_id] = _state(
            phase="Running",
            running=True,
            ready=True,
            endpoint_url=handle.endpoint_url,
        )
        ready = await controller.run_once()
        assert ready.ready == 1
        replica = (await _replicas(kubernetes_controller_database, service_id))[0]
        assert replica.status == ReplicaStatus.RUNNING
        assert replica.health == ReplicaHealth.HEALTHY
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
