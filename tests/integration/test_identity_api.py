import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.errors import register_exception_handlers
from api.routes import identity
from core.config import Settings
from core.database import Database
from core.redis import RedisQueue
from models.usage import ProjectQuota, ProjectQuotaState

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def identity_client(
    database: Database,
    redis_queue: RedisQueue,
) -> AsyncIterator[AsyncClient]:
    app = FastAPI()
    app.state.database = database
    app.state.queue = redis_queue
    app.state.settings = Settings(
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        legacy_anonymous_enabled=False,
        bootstrap_enabled=True,
        bootstrap_token="test-bootstrap-token",
    )
    register_exception_handlers(app)
    app.include_router(identity.router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


def _bootstrap_payload() -> dict[str, object]:
    return {
        "user": {
            "username": "platform-owner",
            "email": "owner@example.com",
            "password": "correct horse battery staple",
        },
        "project": {"name": "Primary Project", "slug": "primary-project"},
        "api_key_name": "bootstrap",
    }


async def test_bootstrap_auth_and_identity_error_paths(
    identity_client: AsyncClient,
    database: Database,
) -> None:
    rejected = await identity_client.post(
        "/api/v1/bootstrap",
        json=_bootstrap_payload(),
        headers={"X-Bootstrap-Token": "wrong"},
    )
    assert rejected.status_code == 403

    bootstrapped = await identity_client.post(
        "/api/v1/bootstrap",
        json=_bootstrap_payload(),
        headers={"X-Bootstrap-Token": "test-bootstrap-token"},
    )
    assert bootstrapped.status_code == 201
    body = bootstrapped.json()
    token = body["api_key"]["api_key"]
    project_id = body["project"]["id"]
    assert token.startswith("mkc_")

    async with database.session() as session:
        project_uuid = uuid.UUID(project_id)
        assert await session.get(ProjectQuota, project_uuid) is not None
        assert await session.get(ProjectQuotaState, project_uuid) is not None

    auth = {"Authorization": f"Bearer {token}"}
    whoami = await identity_client.get("/api/v1/auth/whoami", headers=auth)
    assert whoami.status_code == 200
    assert whoami.json()["project_id"] == project_id

    invalid_token = await identity_client.get(
        "/api/v1/auth/whoami", headers={"Authorization": "Bearer mkc_invalid"}
    )
    assert invalid_token.status_code == 401

    repeated = await identity_client.post(
        "/api/v1/bootstrap",
        json={
            **_bootstrap_payload(),
            "user": {
                "username": "second-owner",
                "email": "second@example.com",
                "password": "another correct horse battery staple",
            },
        },
        headers={"X-Bootstrap-Token": "test-bootstrap-token"},
    )
    assert repeated.status_code == 409
    assert repeated.json()["error"]["code"] == "BOOTSTRAP_ALREADY_COMPLETED"

    created_user = await identity_client.post(
        "/api/v1/users",
        json={
            "username": "project-member",
            "email": "member@example.com",
            "password": "member correct horse battery staple",
        },
        headers=auth,
    )
    assert created_user.status_code == 201
    assert "password_hash" not in created_user.json()
    duplicate_user = await identity_client.post(
        "/api/v1/users",
        json={
            "username": "project-member",
            "email": "different@example.com",
            "password": "member correct horse battery staple",
        },
        headers=auth,
    )
    assert duplicate_user.status_code == 409

    nonmember_key = await identity_client.post(
        f"/api/v1/projects/{project_id}/api-keys",
        json={"name": "not-a-member", "user_id": created_user.json()["id"]},
        headers=auth,
    )
    assert nonmember_key.status_code == 404

    added_member = await identity_client.post(
        f"/api/v1/projects/{project_id}/members",
        json={"user_id": created_user.json()["id"], "role": "member"},
        headers=auth,
    )
    assert added_member.status_code == 201
    issued_key = await identity_client.post(
        f"/api/v1/projects/{project_id}/api-keys",
        json={"name": "member-key", "user_id": created_user.json()["id"]},
        headers=auth,
    )
    assert issued_key.status_code == 201
    assert issued_key.json()["api_key"].startswith("mkc_")
    listed_keys = await identity_client.get(f"/api/v1/projects/{project_id}/api-keys", headers=auth)
    assert listed_keys.status_code == 200
    assert all("api_key" not in item and "secret_hash" not in item for item in listed_keys.json())

    missing_member = await identity_client.post(
        f"/api/v1/projects/{project_id}/members",
        json={"user_id": "11111111-1111-1111-1111-111111111111", "role": "member"},
        headers=auth,
    )
    assert missing_member.status_code == 404

    second_project = await identity_client.post(
        "/api/v1/projects",
        json={
            "name": "Second Project",
            "slug": "second-project",
            "api_key_name": "second-project-initial",
        },
        headers=auth,
    )
    assert second_project.status_code == 201
    second_project_auth = {"Authorization": f"Bearer {second_project.json()['api_key']['api_key']}"}
    second_whoami = await identity_client.get(
        "/api/v1/auth/whoami",
        headers=second_project_auth,
    )
    assert second_whoami.status_code == 200
    assert second_whoami.json()["project_id"] == second_project.json()["id"]
    listed_projects = await identity_client.get("/api/v1/projects", headers=auth)
    assert listed_projects.status_code == 200
    assert listed_projects.json()["pagination"]["total"] == 2
    assert {item["id"] for item in listed_projects.json()["items"]} == {
        project_id,
        second_project.json()["id"],
    }
    cross_project = await identity_client.get(
        f"/api/v1/projects/{second_project.json()['id']}/api-keys",
        headers=auth,
    )
    assert cross_project.status_code == 404


async def test_duplicate_project_slug_returns_conflict(identity_client: AsyncClient) -> None:
    bootstrapped = await identity_client.post(
        "/api/v1/bootstrap",
        json=_bootstrap_payload(),
        headers={"X-Bootstrap-Token": "test-bootstrap-token"},
    )
    token = bootstrapped.json()["api_key"]["api_key"]
    auth = {"Authorization": f"Bearer {token}"}

    duplicate = await identity_client.post(
        "/api/v1/projects",
        json={"name": "Duplicate", "slug": "primary-project"},
        headers=auth,
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "PROJECT_SLUG_ALREADY_EXISTS"


async def test_admin_cannot_escalate_to_owner(identity_client: AsyncClient) -> None:
    bootstrapped = await identity_client.post(
        "/api/v1/bootstrap",
        json=_bootstrap_payload(),
        headers={"X-Bootstrap-Token": "test-bootstrap-token"},
    )
    body = bootstrapped.json()
    project_id = body["project"]["id"]
    owner_id = body["user"]["id"]
    owner_auth = {"Authorization": f"Bearer {body['api_key']['api_key']}"}
    created_admin = await identity_client.post(
        "/api/v1/users",
        json={
            "username": "project-admin",
            "email": "admin@example.com",
            "password": "admin correct horse battery staple",
        },
        headers=owner_auth,
    )
    admin_id = created_admin.json()["id"]
    await identity_client.post(
        f"/api/v1/projects/{project_id}/members",
        json={"user_id": admin_id, "role": "admin"},
        headers=owner_auth,
    )
    admin_key = await identity_client.post(
        f"/api/v1/projects/{project_id}/api-keys",
        json={"name": "admin-key", "user_id": admin_id},
        headers=owner_auth,
    )
    admin_auth = {"Authorization": f"Bearer {admin_key.json()['api_key']}"}

    self_promotion = await identity_client.patch(
        f"/api/v1/projects/{project_id}/members/{admin_id}",
        json={"role": "owner"},
        headers=admin_auth,
    )
    escalated = await identity_client.post(
        f"/api/v1/projects/{project_id}/api-keys",
        json={"name": "forged-owner", "user_id": owner_id},
        headers=admin_auth,
    )
    last_owner_demotion = await identity_client.patch(
        f"/api/v1/projects/{project_id}/members/{owner_id}",
        json={"role": "admin"},
        headers=owner_auth,
    )

    assert self_promotion.status_code == 403
    assert self_promotion.json()["error"]["code"] == "PERMISSION_DENIED"
    assert escalated.status_code == 403
    assert escalated.json()["error"]["code"] == "PERMISSION_DENIED"
    assert last_owner_demotion.status_code == 409
    assert last_owner_demotion.json()["error"]["code"] == "LAST_PROJECT_OWNER"
