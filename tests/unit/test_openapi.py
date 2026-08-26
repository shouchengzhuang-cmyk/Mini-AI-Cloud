from __future__ import annotations

import tomllib
from pathlib import Path

from api.main import app
from core.project_identity import DEVELOPMENT_VERSION, PROJECT_NAME, PROJECT_VERSION


def test_openapi_generation_covers_phase_i_and_phase_ii_resources() -> None:
    schema = app.openapi()

    assert schema["info"]["title"] == "Mini AI Cloud"
    assert schema["info"]["version"] == PROJECT_VERSION
    paths = schema["paths"]
    expected_paths = {
        "/api/v1/tasks",
        "/api/v1/tasks/{task_id}/artifacts",
        "/api/v1/tasks/{task_id}/timeline",
        "/api/v1/bootstrap",
        "/api/v1/projects/{project_id}/quota",
        "/api/v1/projects/{project_id}/secrets",
        "/api/v1/projects/{project_id}/datasets",
        "/api/v1/artifacts",
        "/api/v1/services",
        "/api/v1/projects/{project_id}/job-groups",
        "/api/v1/audit-events",
        "/api/v1/admin/diagnostics",
        "/v1/chat/completions",
    }
    assert expected_paths <= paths.keys()

    operation_ids: list[str] = []
    for path_item in paths.values():
        for method, operation in path_item.items():
            if method not in {"get", "post", "put", "patch", "delete"}:
                continue
            assert operation["tags"]
            assert any(status.startswith("2") for status in operation["responses"])
            operation_ids.append(operation["operationId"])
    assert len(operation_ids) == len(set(operation_ids))

    schemas = schema["components"]["schemas"]
    assert {
        "TaskCreate",
        "RetryPolicy",
        "TaskInputArtifact",
        "TaskOutputArtifact",
        "ArtifactCreate",
        "DatasetCreate",
        "ServiceCreate",
        "BootstrapRequest",
    } <= schemas.keys()


def test_package_name_and_version_match_runtime_identity() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    metadata = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]

    assert metadata["name"] == PROJECT_NAME == "mini-ai-cloud"
    assert metadata["version"] == PROJECT_VERSION == DEVELOPMENT_VERSION == "0.4.0.dev0"
