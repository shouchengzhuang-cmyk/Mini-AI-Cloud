import uuid

from httpx import AsyncClient

from core.artifacts import ArtifactState
from core.database import Database
from models.artifact import Artifact
from models.usage import ProjectQuota, ProjectQuotaState

PROJECT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def _ready_artifact(database: Database, *, name: str) -> uuid.UUID:
    artifact = Artifact(
        project_id=PROJECT_ID,
        name=name,
        state=ArtifactState.READY.value,
        backend="local",
        object_key=f"projects/{PROJECT_ID.hex}/artifacts/{uuid.uuid4().hex}/content",
        content_type="application/octet-stream",
        size_bytes=4,
        sha256="a" * 64,
    )
    async with database.session() as session, session.begin():
        quota = await session.get(ProjectQuota, PROJECT_ID, with_for_update=True)
        quota_state = await session.get(ProjectQuotaState, PROJECT_ID, with_for_update=True)
        if quota is None:
            session.add(ProjectQuota(project_id=PROJECT_ID, max_artifact_bytes=4096))
        if quota_state is None:
            quota_state = ProjectQuotaState(project_id=PROJECT_ID, artifact_bytes=0)
            session.add(quota_state)
        quota_state.artifact_bytes += artifact.size_bytes or 0
        session.add(artifact)
        await session.flush()
        return artifact.id


async def test_dataset_versions_are_project_scoped_and_protect_artifacts(
    api_client: AsyncClient,
    database: Database,
) -> None:
    first_artifact_id = await _ready_artifact(database, name="train-v1.bin")
    second_artifact_id = await _ready_artifact(database, name="train-v2.bin")

    created = await api_client.post(
        f"/api/v1/projects/{PROJECT_ID}/datasets",
        json={
            "name": "training-data",
            "description": "versioned training corpus",
            "artifact_id": str(first_artifact_id),
            "metadata": {"split": "train"},
        },
    )
    assert created.status_code == 201, created.text
    dataset = created.json()
    dataset_id = dataset["id"]
    assert dataset["current_version"] == 1
    assert dataset["current_artifact_id"] == str(first_artifact_id)

    version = await api_client.post(
        f"/api/v1/projects/{PROJECT_ID}/datasets/{dataset_id}/versions",
        json={
            "artifact_id": str(second_artifact_id),
            "metadata": {"rows": 42},
        },
    )
    assert version.status_code == 201, version.text
    assert version.json()["version"] == 2
    assert version.json()["metadata"] == {"rows": 42}

    listed = await api_client.get(f"/api/v1/projects/{PROJECT_ID}/datasets")
    assert listed.status_code == 200
    assert listed.json()["pagination"]["total"] == 1
    assert listed.json()["items"][0]["current_version"] == 2

    versions = await api_client.get(f"/api/v1/projects/{PROJECT_ID}/datasets/{dataset_id}/versions")
    assert versions.status_code == 200
    assert [item["version"] for item in versions.json()] == [2, 1]

    protected = await api_client.delete(f"/api/v1/artifacts/{first_artifact_id}")
    assert protected.status_code == 409
    assert protected.json()["error"]["code"] == "ARTIFACT_REFERENCED"


async def test_dataset_rejects_non_ready_artifact(
    api_client: AsyncClient,
    database: Database,
) -> None:
    artifact_id = await _ready_artifact(database, name="pending.bin")
    async with database.session() as session, session.begin():
        artifact = await session.get(Artifact, artifact_id)
        assert artifact is not None
        artifact.state = ArtifactState.PENDING.value

    response = await api_client.post(
        f"/api/v1/projects/{PROJECT_ID}/datasets",
        json={"name": "invalid-data", "artifact_id": str(artifact_id)},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATASET_CONFLICT"
