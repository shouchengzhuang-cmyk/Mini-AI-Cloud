import hashlib
import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from api.main import create_app
from core.artifacts import ArtifactState
from core.config import Settings
from core.database import Database
from core.image_policy import ImagePolicyAction, ImageRule
from core.redis import RedisQueue
from models.artifact import Artifact
from models.usage import ProjectQuotaState
from repositories.registry import ImagePolicyRepository

pytestmark = pytest.mark.integration

DIGEST = "sha256:" + "a" * 64


async def test_authenticated_task_api_declares_input_and_output_artifacts(
    database: Database,
    redis_queue: RedisQueue,
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        database_url=str(database.engine.url),
        redis_url="redis://unused.invalid/0",
        control_plane_enabled=False,
        legacy_anonymous_enabled=False,
        bootstrap_enabled=True,
        bootstrap_token="artifact-bootstrap-token",
        artifact_local_root=str(tmp_path / "objects"),
        artifact_workspace_root=str(tmp_path / "workspaces"),
        artifact_max_bytes=4096,
        api_request_max_bytes=1024,
    )
    app = create_app(
        settings=settings,
        database=database,
        queue=redis_queue,
        start_control_plane=False,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        bootstrap = await client.post(
            "/api/v1/bootstrap",
            headers={"X-Bootstrap-Token": "artifact-bootstrap-token"},
            json={
                "user": {
                    "username": "artifact-owner",
                    "email": "artifact-owner@example.com",
                    "password": "correct horse battery staple",
                },
                "project": {"name": "Artifact Project", "slug": "artifact-project"},
                "api_key_name": "artifact-tests",
            },
        )
        assert bootstrap.status_code == 201
        body = bootstrap.json()
        project_id = uuid.UUID(body["project"]["id"])
        auth = {"Authorization": f"Bearer {body['api_key']['api_key']}"}
        content = b"task input"
        artifact_id = uuid.uuid4()

        async with database.session() as session, session.begin():
            await ImagePolicyRepository.replace(
                session,
                project_id=project_id,
                default_action=ImagePolicyAction.DENY,
                require_digest=True,
                rules=[
                    ImageRule(
                        action=ImagePolicyAction.ALLOW,
                        registry="docker.io",
                        repository_glob="library/python",
                        digest=DIGEST,
                    )
                ],
            )
            session.add(
                Artifact(
                    id=artifact_id,
                    project_id=project_id,
                    name="training.jsonl",
                    state=ArtifactState.READY.value,
                    backend="local",
                    object_key=(f"projects/{project_id.hex}/artifacts/{artifact_id.hex}/content"),
                    content_type="application/jsonl",
                    size_bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                )
            )
            quota_state = await session.get(ProjectQuotaState, project_id)
            assert quota_state is not None
            quota_state.artifact_bytes = len(content)

        created = await client.post(
            "/api/v1/tasks",
            headers=auth,
            json={
                "image": f"python@{DIGEST}",
                "command": ["python", "-c", "print('train')"],
                "inputs": [{"artifact_id": str(artifact_id)}],
                "artifacts": [
                    {
                        "name": "model",
                        "path": "/output/model.bin",
                        "required": True,
                    }
                ],
            },
        )
        assert created.status_code == 201, created.text

        listed = await client.get(
            f"/api/v1/tasks/{created.json()['id']}/artifacts",
            headers=auth,
        )

        streamed_content = b"s" * 2048
        streamed_checksum = hashlib.sha256(streamed_content).hexdigest()
        pending = await client.post(
            "/api/v1/artifacts",
            headers=auth,
            json={
                "name": "streamed.bin",
                "size_bytes": len(streamed_content),
                "sha256": streamed_checksum,
            },
        )
        assert pending.status_code == 201, pending.text
        uploaded = await client.put(
            f"/api/v1/artifacts/{pending.json()['id']}/content",
            headers={**auth, "X-Content-SHA256": streamed_checksum},
            content=streamed_content,
        )
        assert uploaded.status_code == 200, uploaded.text

    assert listed.status_code == 200
    bindings = listed.json()
    assert [item["direction"] for item in bindings] == ["input", "output"]
    assert bindings[0]["name"].startswith("input-000-")
    assert bindings[1]["name"] == "model"
    assert bindings[0]["artifact_id"] == str(artifact_id)
    assert bindings[0]["mount_path"].startswith("/workspace/inputs/")
    assert bindings[1]["artifact_id"] is None
    assert bindings[1]["mount_path"] == "/output/model.bin"
