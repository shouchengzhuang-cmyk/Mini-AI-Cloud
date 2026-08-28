import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request
from starlette.responses import Response

from api.dependencies import get_principal
from api.errors import register_exception_handlers
from api.routes import model_variants
from core.database import Database
from core.enums import ProjectRole
from core.rbac import Principal, PrincipalKind
from core.runtime_profiles import RuntimeProfileCatalog, RuntimeProfileManifestEntry
from models.identity import Project

pytestmark = pytest.mark.integration

PROJECT_ID = uuid.UUID("51000000-0000-0000-0000-000000000001")
REPOSITORY_ROOT = Path(__file__).parents[2]


@pytest_asyncio.fixture
async def variants_app(database: Database) -> FastAPI:
    async with database.session() as session, session.begin():
        session.add(
            Project(
                id=PROJECT_ID,
                name="Model Variants API",
                slug="model-variants-api",
            )
        )

    app = FastAPI()
    app.state.database = database
    app.state.runtime_profile_catalog = RuntimeProfileCatalog.from_path(
        REPOSITORY_ROOT / "runtime_profiles" / "manifest.json"
    )
    app.state.test_principal = Principal(
        kind=PrincipalKind.SYSTEM,
        project_id=PROJECT_ID,
    )

    @app.middleware("http")
    async def attach_test_principal(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request.state.principal = app.state.test_principal
        return await call_next(request)

    register_exception_handlers(app)
    app.include_router(model_variants.router)
    app.dependency_overrides[get_principal] = lambda: Principal(
        kind=PrincipalKind.SYSTEM,
        project_id=PROJECT_ID,
    )
    return app


@pytest_asyncio.fixture
async def variants_client(variants_app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=variants_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


def _profile(catalog: RuntimeProfileCatalog, vendor: str) -> RuntimeProfileManifestEntry:
    return next(profile for profile in catalog.manifest.profiles if profile.vendor.value == vendor)


def _api_key_principal(role: ProjectRole) -> Principal:
    return Principal(
        kind=PrincipalKind.API_KEY,
        project_id=PROJECT_ID,
        user_id=uuid.uuid4(),
        api_key_id=uuid.uuid4(),
        role=role,
        key_prefix=f"mkc_{role.value}",
    )


def _variant_payload(
    catalog: RuntimeProfileCatalog,
    *,
    vendor: str,
    name: str,
    artifact_digest_character: str,
) -> dict[str, object]:
    profile = _profile(catalog, vendor)
    return {
        "name": name,
        "vendor": vendor,
        "kind": "gpu" if vendor == "nvidia" else "npu",
        "runtime_profile_id": profile.profile_id,
        "runtime_profile_version": profile.profile_version,
        "runtime_profile_digest": profile.semantic_digest,
        "artifact_source": f"modelscope/{name}",
        "artifact_revision": f"{name}-revision",
        "artifact_digest": "sha256:" + artifact_digest_character * 64,
        "architecture": "qwen2",
        "dtype": "bfloat16",
        "quantization": None,
        "status": "ready",
    }


async def test_logical_model_variant_lifecycle_enforces_ready_and_audit_invariants(
    variants_client: AsyncClient,
    variants_app: FastAPI,
) -> None:
    created = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models",
        json={"name": "qwen-small", "public_name": "Qwen Small"},
    )
    assert created.status_code == 201
    logical_model_id = created.json()["id"]
    assert created.json()["status"] == "disabled"
    assert created.json()["routing_policy"] == "balanced"
    assert "routing_cursor" not in created.json()

    premature = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/status",
        json={"status": "ready", "reason": "activate before variants"},
    )
    assert premature.status_code == 409
    assert premature.json()["error"]["code"] == "LOGICAL_MODEL_INVARIANT"

    catalog = variants_app.state.runtime_profile_catalog
    nvidia = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants",
        json=_variant_payload(
            catalog,
            vendor="nvidia",
            name="qwen-small-nvidia-bf16",
            artifact_digest_character="a",
        ),
    )
    assert nvidia.status_code == 201
    nvidia_id = nvidia.json()["id"]

    activated = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/status",
        json={"status": "ready", "reason": "NVIDIA variant reviewed"},
    )
    assert activated.status_code == 200
    assert activated.json()["status"] == "ready"

    blocked = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants/"
        f"{nvidia_id}/status",
        json={"status": "degraded", "reason": "probe failed"},
    )
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "LOGICAL_MODEL_INVARIANT"

    ascend = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants",
        json=_variant_payload(
            catalog,
            vendor="huawei-ascend",
            name="qwen-small-ascend-bf16",
            artifact_digest_character="b",
        ),
    )
    assert ascend.status_code == 201
    ascend_id = ascend.json()["id"]
    assert ascend.json()["artifact_digest"] != nvidia.json()["artifact_digest"]

    degraded = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants/"
        f"{nvidia_id}/status",
        json={"status": "degraded", "reason": "NVIDIA backend degraded"},
    )
    assert degraded.status_code == 200
    assert degraded.json()["status"] == "degraded"

    history = await variants_client.get(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/status-history"
    )
    assert history.status_code == 200
    assert history.json()["pagination"]["total"] == 2
    assert [item["to_status"] for item in history.json()["items"]] == ["ready", "disabled"]

    last_ready_delete = await variants_client.delete(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants/{ascend_id}"
    )
    assert last_ready_delete.status_code == 409


async def test_variant_api_rejects_cross_vendor_and_manifest_digest_drift(
    variants_client: AsyncClient,
    variants_app: FastAPI,
) -> None:
    created = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models",
        json={"name": "compatibility", "public_name": "Compatibility"},
    )
    logical_model_id = created.json()["id"]
    catalog = variants_app.state.runtime_profile_catalog

    cross_vendor_payload = _variant_payload(
        catalog,
        vendor="nvidia",
        name="cross-vendor",
        artifact_digest_character="c",
    )
    cross_vendor_payload["vendor"] = "huawei-ascend"
    cross_vendor_payload["kind"] = "npu"
    cross_vendor = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants",
        json=cross_vendor_payload,
    )
    assert cross_vendor.status_code == 409
    assert cross_vendor.json()["error"]["code"] == "MODEL_VARIANT_INCOMPATIBLE"

    drift_payload = _variant_payload(
        catalog,
        vendor="nvidia",
        name="digest-drift",
        artifact_digest_character="d",
    )
    drift_payload["runtime_profile_digest"] = "sha256:" + "f" * 64
    drift = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/variants",
        json=drift_payload,
    )
    assert drift.status_code == 409
    assert drift.json()["error"]["code"] == "MODEL_VARIANT_INCOMPATIBLE"


async def test_logical_model_public_name_conflict_is_stable(
    variants_client: AsyncClient,
) -> None:
    first = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models",
        json={"name": "public-one", "public_name": "Shared Public Name"},
    )
    assert first.status_code == 201

    duplicate = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models",
        json={"name": "public-two", "public_name": "Shared Public Name"},
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "LOGICAL_MODEL_PUBLIC_NAME_ALREADY_EXISTS"


async def test_admin_and_owner_update_typed_routing_policy_without_status_event(
    variants_client: AsyncClient,
    variants_app: FastAPI,
) -> None:
    created = await variants_client.post(
        f"/api/v1/projects/{PROJECT_ID}/logical-models",
        json={
            "name": "routing-control",
            "public_name": "Routing Control",
            "routing_policy": "strict-nvidia",
        },
    )
    assert created.status_code == 201
    logical_model_id = created.json()["id"]
    assert created.json()["routing_policy"] == "strict-nvidia"
    assert created.json()["version"] == 1

    variants_app.state.test_principal = _api_key_principal(ProjectRole.ADMIN)
    admin_update = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/routing-policy",
        json={"routing_policy": "prefer-ascend"},
    )
    assert admin_update.status_code == 200
    assert admin_update.json()["routing_policy"] == "prefer-ascend"
    assert admin_update.json()["version"] == 2

    variants_app.state.test_principal = _api_key_principal(ProjectRole.MEMBER)
    forbidden = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/routing-policy",
        json={"routing_policy": "balanced"},
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["error"]["code"] == "PERMISSION_DENIED"

    variants_app.state.test_principal = _api_key_principal(ProjectRole.OWNER)
    owner_update = await variants_client.put(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/routing-policy",
        json={"routing_policy": "balanced"},
    )
    assert owner_update.status_code == 200
    assert owner_update.json()["routing_policy"] == "balanced"
    assert owner_update.json()["version"] == 3

    history = await variants_client.get(
        f"/api/v1/projects/{PROJECT_ID}/logical-models/{logical_model_id}/status-history"
    )
    assert history.status_code == 200
    assert history.json()["pagination"]["total"] == 1
