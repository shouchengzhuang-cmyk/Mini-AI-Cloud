import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from api.dependencies import get_principal
from api.errors import register_exception_handlers
from api.main import create_app
from api.routes import services
from core.config import Settings
from core.database import Database
from core.image_policy import ImagePolicyAction, ImageRule
from core.rbac import Principal, PrincipalKind
from core.redis import RedisQueue
from models.identity import Project
from models.service import ReplicaHealth, ReplicaStatus, ServiceReplica
from models.usage import ProjectQuotaState
from repositories.quotas import QuotaRepository
from repositories.registry import ImagePolicyRepository, RegisteredModelRepository
from repositories.services import ServiceRepository

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
OTHER_PROJECT_ID = uuid.UUID("10000000-0000-0000-0000-000000000002")
DIGEST = "sha256:" + "b" * 64


class _KubernetesRuntimeAdmission:
    def __init__(self, *, ready: bool) -> None:
        self.admission_ready = ready


@pytest.fixture
def services_settings() -> Settings:
    return Settings(
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        legacy_project_id=str(PROJECT_ID),
        vllm_image=f"example/vllm@{DIGEST}",
    )


@pytest_asyncio.fixture
async def services_app(
    database: Database,
    services_settings: Settings,
) -> FastAPI:
    async with database.session() as session, session.begin():
        session.add(Project(id=PROJECT_ID, name="API Services", slug="api-services"))
        await session.flush()
        await ImagePolicyRepository.replace(
            session,
            project_id=PROJECT_ID,
            default_action=ImagePolicyAction.DENY,
            require_digest=True,
            rules=[
                ImageRule(
                    action=ImagePolicyAction.ALLOW,
                    registry="docker.io",
                    repository_glob="example/vllm",
                    digest=DIGEST,
                )
            ],
        )

    app = FastAPI()
    app.state.database = database
    app.state.settings = services_settings
    app.state.kubernetes_replica_runtime = None
    register_exception_handlers(app)
    app.include_router(services.router)
    app.dependency_overrides[get_principal] = lambda: Principal(
        kind=PrincipalKind.SYSTEM,
        project_id=PROJECT_ID,
    )
    return app


@pytest_asyncio.fixture
async def services_client(services_app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=services_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


def _payload() -> dict[str, object]:
    return {
        "name": "chat-main",
        "model": "org/model-v1",
        "model_revision": "revision-1",
        "runtime": "vllm",
        "runtime_type": "docker",
        "image": f"example/vllm@{DIGEST}",
        "tensor_parallel_size": 1,
        "dtype": "float16",
        "gpu_memory_utilization": 0.85,
        "max_model_len": 8192,
        "replicas": 2,
    }


async def _create_registered_model(
    database: Database,
    *,
    project_id: uuid.UUID = PROJECT_ID,
) -> uuid.UUID:
    async with database.session() as session, session.begin():
        model = await RegisteredModelRepository.create(
            session,
            project_id=project_id,
            name="registry-chat",
            provider="huggingface",
            source="org/registry-model",
            revision="registry-revision",
            runtime="vllm",
            default_gpu_count=4,
            runtime_defaults={
                "gpu_model": "NVIDIA A100",
                "tensor_parallel_size": 4,
                "dtype": "bfloat16",
                "gpu_memory_utilization": 0.8,
                "max_model_len": 16_384,
                "extra_arguments": ["--trust-remote-code"],
            },
            size_bytes=None,
            gpu_memory_mb=40_960,
            architecture="transformer",
            metadata={},
            created_by_user_id=None,
        )
        return model.id


async def test_services_api_create_list_duplicate_and_stop(
    services_client: AsyncClient,
) -> None:
    created = await services_client.post("/api/v1/services", json=_payload())

    assert created.status_code == 201
    created_body = created.json()
    service_id = uuid.UUID(created_body["id"])
    assert created_body["desired_replicas"] == 2
    assert created_body["actual_replicas"] == 0
    assert created_body["healthy_replicas"] == 0
    assert created_body["generation"] == 1
    assert created_body["status"] == "deploying"
    assert created_body["image"] == f"docker.io/example/vllm@{DIGEST}"
    assert created_body["model_revision"] == "revision-1"
    assert created_body["tensor_parallel_size"] == 1
    assert created_body["dtype"] == "float16"
    assert created_body["gpu_memory_utilization"] == 0.85
    assert created_body["max_model_len"] == 8192
    assert created_body["scheduling_reason"] is None
    assert created_body["scheduling_details"] == {}

    listed = await services_client.get("/api/v1/services")
    assert listed.status_code == 200
    assert listed.json()["pagination"]["total"] == 1
    assert listed.json()["items"][0]["id"] == str(service_id)

    duplicate = await services_client.post("/api/v1/services", json=_payload())
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "SERVICE_NAME_ALREADY_EXISTS"

    stopped = await services_client.post(f"/api/v1/services/{service_id}/stop")
    assert stopped.status_code == 200
    assert stopped.json()["desired_replicas"] == 0
    assert stopped.json()["status"] == "stopped"

    replicas = await services_client.get(f"/api/v1/services/{service_id}/replicas")
    assert replicas.status_code == 200
    assert [item["status"] for item in replicas.json()["items"]] == [
        "stopped",
        "stopped",
    ]


async def test_services_api_deploys_a_registered_model_snapshot_with_explicit_overrides(
    services_client: AsyncClient,
    database: Database,
) -> None:
    registered_model_id = await _create_registered_model(database)

    response = await services_client.post(
        "/api/v1/services",
        json={
            "name": "registry-backed",
            "registered_model_id": str(registered_model_id),
            "dtype": "float16",
            "replicas": 0,
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["registered_model_id"] == str(registered_model_id)
    assert body["model"] == "org/registry-model"
    assert body["model_revision"] == "registry-revision"
    assert body["runtime"] == "vllm"
    assert body["runtime_type"] == "docker"
    assert body["image"] == f"docker.io/example/vllm@{DIGEST}"
    assert body["gpu_count"] == 4
    assert body["gpu_memory_mb"] == 40_960
    assert body["gpu_model"] == "NVIDIA A100"
    assert body["tensor_parallel_size"] == 4
    assert body["dtype"] == "float16"
    assert body["gpu_memory_utilization"] == 0.8
    assert body["max_model_len"] == 16_384


async def test_services_api_derives_fake_runtime_from_registered_model(
    services_client: AsyncClient,
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        registered_model = await RegisteredModelRepository.create(
            session,
            project_id=PROJECT_ID,
            name="registry-fake",
            provider="local",
            source="fake/registry-model",
            revision=None,
            runtime="fake",
            default_gpu_count=0,
            runtime_defaults={"tensor_parallel_size": 1},
            size_bytes=None,
            gpu_memory_mb=None,
            architecture=None,
            metadata={},
            created_by_user_id=None,
        )
        registered_model_id = registered_model.id

    response = await services_client.post(
        "/api/v1/services",
        json={
            "name": "registry-fake-service",
            "registered_model_id": str(registered_model_id),
            "replicas": 0,
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["runtime"] == "fake"
    assert body["runtime_type"] == "fake"
    assert body["image"] is None
    assert body["gpu_count"] == 0
    assert body["tensor_parallel_size"] == 1


async def test_services_api_requires_explicit_kubernetes_fake_opt_in_and_image_policy(
    services_client: AsyncClient,
    services_settings: Settings,
) -> None:
    payload = {
        "name": "kubernetes-fake",
        "model": "fake/kubernetes-model",
        "runtime": "fake",
        "runtime_type": "kubernetes",
        "replicas": 0,
    }

    disabled = await services_client.post("/api/v1/services", json=payload)
    assert disabled.status_code == 409
    assert disabled.json()["error"]["code"] == "KUBERNETES_SERVING_DISABLED"

    services_settings.kubernetes_serving_enabled = True
    still_disabled = await services_client.post("/api/v1/services", json=payload)
    assert still_disabled.status_code == 409
    assert still_disabled.json()["error"]["code"] == "KUBERNETES_FAKE_SERVING_DISABLED"

    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    created = await services_client.post("/api/v1/services", json=payload)
    assert created.status_code == 201
    body = created.json()
    assert body["runtime"] == "fake"
    assert body["runtime_type"] == "kubernetes"
    assert body["image"] == f"docker.io/example/vllm@{DIGEST}"


async def test_services_api_preserves_explicit_kubernetes_backend_for_registered_fake_model(
    services_client: AsyncClient,
    services_settings: Settings,
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        registered_model = await RegisteredModelRepository.create(
            session,
            project_id=PROJECT_ID,
            name="registry-kubernetes-fake",
            provider="local",
            source="fake/registry-kubernetes-model",
            revision=None,
            runtime="fake",
            default_gpu_count=0,
            runtime_defaults={"tensor_parallel_size": 1},
            size_bytes=None,
            gpu_memory_mb=None,
            architecture=None,
            metadata={},
            created_by_user_id=None,
        )
        registered_model_id = registered_model.id

    services_settings.kubernetes_serving_enabled = True
    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    rejected_name = "registry-kubernetes-positive"
    rejected = await services_client.post(
        "/api/v1/services",
        json={
            "name": rejected_name,
            "registered_model_id": str(registered_model_id),
            "runtime_type": "kubernetes",
            "replicas": 1,
        },
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"
    async with database.session() as session:
        assert (
            await ServiceRepository.get_by_name(
                session,
                project_id=PROJECT_ID,
                name=rejected_name,
            )
            is None
        )

    response = await services_client.post(
        "/api/v1/services",
        json={
            "name": "registry-kubernetes-service",
            "registered_model_id": str(registered_model_id),
            "runtime_type": "kubernetes",
            "replicas": 0,
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["model"] == "fake/registry-kubernetes-model"
    assert body["runtime"] == "fake"
    assert body["runtime_type"] == "kubernetes"
    assert body["image"] == f"docker.io/example/vllm@{DIGEST}"


async def test_kubernetes_scale_fails_closed_before_mutating_desired_state(
    services_client: AsyncClient,
    services_app: FastAPI,
    services_settings: Settings,
) -> None:
    services_settings.control_plane_enabled = True
    services_settings.kubernetes_serving_enabled = True
    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    services_app.state.kubernetes_replica_runtime = _KubernetesRuntimeAdmission(ready=True)
    created = await services_client.post(
        "/api/v1/services",
        json={
            "name": "kubernetes-disabled-scale",
            "model": "fake/kubernetes-disabled-scale",
            "runtime": "fake",
            "runtime_type": "kubernetes",
            "replicas": 2,
        },
    )
    assert created.status_code == 201
    service_id = created.json()["id"]

    before = await services_client.get(f"/api/v1/services/{service_id}/replicas")
    assert before.status_code == 200
    assert [item["status"] for item in before.json()["items"]] == [
        "pending",
        "pending",
    ]

    services_settings.kubernetes_serving_enabled = False
    for target in (2, 3):
        rejected = await services_client.post(
            f"/api/v1/services/{service_id}/scale",
            json={"replicas": target},
        )
        assert rejected.status_code == 409
        assert rejected.json()["error"]["code"] == "KUBERNETES_SERVING_DISABLED"

        persisted = await services_client.get(f"/api/v1/services/{service_id}")
        assert persisted.status_code == 200
        assert persisted.json()["desired_replicas"] == 2
        replicas = await services_client.get(f"/api/v1/services/{service_id}/replicas")
        assert replicas.status_code == 200
        assert [item["status"] for item in replicas.json()["items"]] == [
            "pending",
            "pending",
        ]


@pytest.mark.parametrize(
    ("unavailable_reason", "error_code"),
    [
        ("fake-opt-in", "KUBERNETES_FAKE_SERVING_DISABLED"),
        ("production", "KUBERNETES_FAKE_SERVING_FORBIDDEN"),
        ("control-plane", "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"),
        ("controller-absent", "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"),
        ("controller-not-ready", "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"),
    ],
)
async def test_kubernetes_positive_scale_rejects_each_unavailable_runtime_condition(
    services_client: AsyncClient,
    services_app: FastAPI,
    services_settings: Settings,
    unavailable_reason: str,
    error_code: str,
) -> None:
    services_settings.control_plane_enabled = True
    services_settings.kubernetes_serving_enabled = True
    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    services_app.state.kubernetes_replica_runtime = _KubernetesRuntimeAdmission(ready=True)
    created = await services_client.post(
        "/api/v1/services",
        json={
            "name": f"kubernetes-unavailable-{unavailable_reason}",
            "model": "fake/kubernetes-unavailable",
            "runtime": "fake",
            "runtime_type": "kubernetes",
            "replicas": 0,
        },
    )
    assert created.status_code == 201
    service_id = created.json()["id"]

    if unavailable_reason == "fake-opt-in":
        services_settings.kubernetes_serving_fake_enabled = False
    elif unavailable_reason == "production":
        services_settings.app_env = "production"
    elif unavailable_reason == "control-plane":
        services_settings.control_plane_enabled = False
    elif unavailable_reason == "controller-absent":
        services_app.state.kubernetes_replica_runtime = None
    else:
        services_app.state.kubernetes_replica_runtime = _KubernetesRuntimeAdmission(ready=False)

    rejected = await services_client.post(
        f"/api/v1/services/{service_id}/scale",
        json={"replicas": 1},
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == error_code

    persisted = await services_client.get(f"/api/v1/services/{service_id}")
    assert persisted.status_code == 200
    assert persisted.json()["desired_replicas"] == 0
    replicas = await services_client.get(f"/api/v1/services/{service_id}/replicas")
    assert replicas.status_code == 200
    assert replicas.json()["items"] == []


@pytest.mark.usefixtures("services_app")
async def test_kubernetes_positive_scale_rejects_when_control_plane_was_not_started(
    services_settings: Settings,
    database: Database,
    redis_queue: RedisQueue,
) -> None:
    services_settings.control_plane_enabled = True
    services_settings.kubernetes_serving_enabled = True
    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    app = create_app(
        settings=services_settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )
    app.dependency_overrides[get_principal] = lambda: Principal(
        kind=PrincipalKind.SYSTEM,
        project_id=PROJECT_ID,
    )
    assert app.state.kubernetes_replica_runtime is None

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        rejected_name = "kubernetes-positive-control-plane-not-started"
        rejected_create = await client.post(
            "/api/v1/services",
            json={
                "name": rejected_name,
                "model": "fake/kubernetes-positive-control-plane-not-started",
                "runtime": "fake",
                "runtime_type": "kubernetes",
                "replicas": 1,
            },
        )
        assert rejected_create.status_code == 409
        assert rejected_create.json()["error"]["code"] == "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"
        created = await client.post(
            "/api/v1/services",
            json={
                "name": "kubernetes-control-plane-not-started",
                "model": "fake/kubernetes-control-plane-not-started",
                "runtime": "fake",
                "runtime_type": "kubernetes",
                "replicas": 0,
            },
        )
        assert created.status_code == 201
        service_id = created.json()["id"]
        rejected = await client.post(
            f"/api/v1/services/{service_id}/scale",
            json={"replicas": 1},
        )

    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "KUBERNETES_SERVING_RUNTIME_UNAVAILABLE"
    async with database.session() as session:
        rejected_service = await ServiceRepository.get_by_name(
            session,
            project_id=PROJECT_ID,
            name=rejected_name,
        )
        service = await ServiceRepository.get(session, uuid.UUID(service_id))
        replicas = await ServiceRepository.list_replicas(session, uuid.UUID(service_id))
    assert rejected_service is None
    assert service is not None and service.desired_replicas == 0
    assert replicas == []


@pytest.mark.parametrize("action", ["scale", "stop"])
async def test_kubernetes_scale_to_zero_records_deferred_cleanup_when_controller_disabled(
    services_client: AsyncClient,
    services_app: FastAPI,
    services_settings: Settings,
    database: Database,
    action: str,
) -> None:
    services_settings.control_plane_enabled = True
    services_settings.kubernetes_serving_enabled = True
    services_settings.kubernetes_serving_fake_enabled = True
    services_settings.kubernetes_serving_image = f"example/vllm@{DIGEST}"
    services_app.state.kubernetes_replica_runtime = _KubernetesRuntimeAdmission(ready=True)
    created = await services_client.post(
        "/api/v1/services",
        json={
            "name": f"kubernetes-disabled-{action}",
            "model": "fake/kubernetes-disabled-stop",
            "runtime": "fake",
            "runtime_type": "kubernetes",
            "replicas": 1,
        },
    )
    assert created.status_code == 201
    service_id = uuid.UUID(created.json()["id"])

    execution_id = uuid.uuid4()
    async with database.session() as session, session.begin():
        replica = await session.scalar(
            select(ServiceReplica).where(ServiceReplica.service_id == service_id)
        )
        assert replica is not None
        replica.status = ReplicaStatus.RUNNING
        replica.health = ReplicaHealth.HEALTHY
        replica.execution_id = execution_id
        replica.endpoint_url = "http://replica.example.invalid:8000"
        replica.active_requests = 1

    services_settings.kubernetes_serving_enabled = False
    services_settings.kubernetes_serving_fake_enabled = False
    services_app.state.kubernetes_replica_runtime = None
    if action == "scale":
        stopped = await services_client.post(
            f"/api/v1/services/{service_id}/scale",
            json={"replicas": 0},
        )
    else:
        stopped = await services_client.post(f"/api/v1/services/{service_id}/stop")

    assert stopped.status_code == 200
    body = stopped.json()
    assert body["desired_replicas"] == 0
    assert body["status"] == "stopping"
    assert body["stopped_at"] is None

    replicas = await services_client.get(f"/api/v1/services/{service_id}/replicas")
    assert replicas.status_code == 200
    items = replicas.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "draining"
    assert items[0]["execution_id"] == str(execution_id)
    assert items[0]["active_requests"] == 1
    assert items[0]["endpoint_url"] == "http://replica.example.invalid:8000"
    assert items[0]["drain_started_at"] is not None
    assert items[0]["drain_deadline"] is not None
    assert items[0]["stopped_at"] is None


async def test_registered_model_delete_keeps_service_snapshot_and_clears_fk(
    services_client: AsyncClient,
    database: Database,
) -> None:
    registered_model_id = await _create_registered_model(database)
    created = await services_client.post(
        "/api/v1/services",
        json={
            "name": "durable-snapshot",
            "registered_model_id": str(registered_model_id),
            "replicas": 0,
        },
    )
    assert created.status_code == 201
    service_id = created.json()["id"]

    async with database.session() as session, session.begin():
        deleted = await RegisteredModelRepository.delete(
            session,
            project_id=PROJECT_ID,
            model_id=registered_model_id,
        )
    assert deleted is True

    fetched = await services_client.get(f"/api/v1/services/{service_id}")
    assert fetched.status_code == 200
    body = fetched.json()
    assert body["registered_model_id"] is None
    assert body["model"] == "org/registry-model"
    assert body["model_revision"] == "registry-revision"
    assert body["gpu_count"] == 4


async def test_services_api_hides_missing_and_cross_project_registered_models(
    services_client: AsyncClient,
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=OTHER_PROJECT_ID,
                name="Other API Services",
                slug="other-api-services",
            )
        )
    cross_project_model_id = await _create_registered_model(
        database,
        project_id=OTHER_PROJECT_ID,
    )

    for registered_model_id in (cross_project_model_id, uuid.uuid4()):
        response = await services_client.post(
            "/api/v1/services",
            json={
                "name": f"hidden-{registered_model_id.hex[:8]}",
                "registered_model_id": str(registered_model_id),
            },
        )

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "MODEL_NOT_FOUND"


async def test_services_api_applies_image_policy_to_vllm_fallback(
    services_client: AsyncClient,
    services_settings: Settings,
    database: Database,
) -> None:
    registered_model_id = await _create_registered_model(database)
    services_settings.vllm_image = "example/vllm:test"

    response = await services_client.post(
        "/api/v1/services",
        json={
            "name": "unpinned-fallback",
            "registered_model_id": str(registered_model_id),
            "replicas": 0,
        },
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IMAGE_POLICY_DENIED"
    assert response.json()["error"]["details"]["reason"] == "digest_required"


async def test_services_api_rejects_unpinned_or_unallowed_images(
    services_client: AsyncClient,
) -> None:
    unpinned = _payload() | {"name": "unpinned", "image": "example/vllm:test"}
    unpinned_response = await services_client.post("/api/v1/services", json=unpinned)
    assert unpinned_response.status_code == 409
    assert unpinned_response.json()["error"]["code"] == "IMAGE_POLICY_DENIED"
    assert unpinned_response.json()["error"]["details"]["reason"] == "digest_required"

    denied = _payload() | {
        "name": "denied",
        "image": f"example/not-allowed@{DIGEST}",
    }
    denied_response = await services_client.post("/api/v1/services", json=denied)
    assert denied_response.status_code == 409
    assert denied_response.json()["error"]["code"] == "IMAGE_POLICY_DENIED"


async def test_services_api_enforces_and_releases_project_quota(
    services_client: AsyncClient,
    database: Database,
) -> None:
    async with database.session() as session, session.begin():
        await QuotaRepository.initialize(session, project_id=PROJECT_ID)
        await QuotaRepository.replace(
            session,
            project_id=PROJECT_ID,
            max_queued_tasks=None,
            max_running_tasks=1,
            max_cpu_millicores=1_000,
            max_memory_mb=1_024,
            max_gpus=0,
            max_services=1,
            max_service_replicas=1,
            max_artifact_bytes=None,
            daily_cost_limit=None,
        )

    created = await services_client.post(
        "/api/v1/services",
        json=_payload() | {"replicas": 1},
    )
    assert created.status_code == 201
    service_id = created.json()["id"]

    rejected = await services_client.post(
        f"/api/v1/services/{service_id}/scale",
        json={"replicas": 2},
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "PROJECT_QUOTA_EXCEEDED"
    assert rejected.json()["error"]["details"] == {
        "resource": "service_replicas",
        "limit": "1",
        "requested": "2",
    }

    stopped = await services_client.post(f"/api/v1/services/{service_id}/stop")
    assert stopped.status_code == 200
    async with database.session() as session:
        state = await session.get(ProjectQuotaState, PROJECT_ID)
    assert state is not None
    assert (state.service_count, state.service_replicas) == (0, 0)
    assert state.service_reserved_cpu_millicores == 0
    assert state.service_reserved_memory_mb == 0
    assert state.service_reserved_gpus == 0
