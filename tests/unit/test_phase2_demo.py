from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict

import httpx

from scripts.phase2_demo import MiniCloudDemoClient, main, run_authenticated_project_demo


@contextmanager
def _demo_client(
    handler: httpx.MockTransport,
    *,
    api_key: str = "mkc_demo-secret",
) -> Iterator[MiniCloudDemoClient]:
    with MiniCloudDemoClient(
        base_url="http://demo.test",
        api_key=api_key,
        timeout=5,
        transport=handler,
    ) as client:
        yield client


def test_authenticated_demo_checks_task_usage_timeline_and_artifact() -> None:
    project_id = str(uuid.uuid4())
    other_project_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    execution_id = str(uuid.uuid4())
    artifact_id = str(uuid.uuid4())
    marker: str | None = None
    artifact_content: bytes | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal marker, artifact_content
        path = request.url.path
        is_other_project = request.headers.get("authorization") == "Bearer mkc_other-secret"
        if path == "/readyz":
            return httpx.Response(200, json={"status": "ok"})
        if path == "/api/v1/auth/whoami":
            return httpx.Response(
                200,
                json={"project_id": other_project_id if is_other_project else project_id},
            )
        if path == f"/api/v1/projects/{project_id}/quota":
            return httpx.Response(200, json={"project_id": project_id})
        if path == f"/api/v1/projects/{project_id}/image-policy/evaluate":
            return httpx.Response(200, json={"allowed": True, "reason": "rule_allow"})
        if path == "/api/v1/tasks" and request.method == "POST":
            payload = json.loads(request.content)
            marker = str(payload["command"][2]).split("'")[1]
            return httpx.Response(201, json={"id": task_id, "status": "queued"})
        if path == f"/api/v1/tasks/{task_id}":
            if is_other_project:
                return httpx.Response(404, json={"error": {"code": "TASK_NOT_FOUND"}})
            return httpx.Response(
                200,
                json={
                    "id": task_id,
                    "status": "succeeded",
                    "execution_id": execution_id,
                    "error_message": None,
                },
            )
        if path == f"/api/v1/tasks/{task_id}/logs":
            return httpx.Response(200, json={"logs": [{"content": f"{marker}\n"}]})
        if path == f"/api/v1/tasks/{task_id}/timeline":
            return httpx.Response(200, json={"events": [{"status": "succeeded"}]})
        if path == f"/api/v1/projects/{project_id}/usage":
            return httpx.Response(
                200,
                json={"project_id": project_id, "execution_count": 1},
            )
        if path == f"/api/v1/projects/{project_id}/cost":
            return httpx.Response(200, json={"project_id": project_id, "costs": []})
        if path == "/api/v1/artifacts" and request.method == "POST":
            return httpx.Response(201, json={"id": artifact_id, "state": "pending"})
        if path == f"/api/v1/artifacts/{artifact_id}/upload-url":
            return httpx.Response(
                200,
                json={
                    "method": "PUT",
                    "url": f"http://demo.test/api/v1/artifacts/{artifact_id}/content",
                    "headers": {},
                    "authorization": "api",
                },
            )
        if path == f"/api/v1/artifacts/{artifact_id}/content" and request.method == "PUT":
            artifact_content = request.content
            return httpx.Response(200, json={"id": artifact_id, "state": "pending"})
        if path == f"/api/v1/artifacts/{artifact_id}/finalize":
            return httpx.Response(200, json={"id": artifact_id, "state": "ready"})
        if path == f"/api/v1/artifacts/{artifact_id}/download-url":
            return httpx.Response(
                200,
                json={
                    "method": "GET",
                    "url": f"http://demo.test/api/v1/artifacts/{artifact_id}/content",
                    "headers": {},
                    "authorization": "api",
                },
            )
        if path == f"/api/v1/artifacts/{artifact_id}/content" and request.method == "GET":
            return httpx.Response(200, content=artifact_content or b"")
        if path == f"/api/v1/artifacts/{artifact_id}" and request.method == "DELETE":
            return httpx.Response(200, json={"id": artifact_id, "state": "deleted"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    transport = httpx.MockTransport(handler)
    with _demo_client(transport) as client:
        with _demo_client(transport, api_key="mkc_other-secret") as other_client:
            result = run_authenticated_project_demo(
                client,
                poll_interval=0.001,
                deadline_seconds=1,
                other_project_client=other_client,
            )

    result_dict = asdict(result)
    assert result_dict["project_id"] == project_id
    assert result_dict["task_id"] == task_id
    assert result_dict["execution_id"] == execution_id
    assert result_dict["usage_executions"] == 1
    assert result_dict["artifact_id"] == artifact_id
    assert result_dict["artifact_sha256"] is not None
    assert result_dict["project_isolation_checked"] is True


def test_phase2_demo_requires_api_key() -> None:
    assert main(["--api-key", ""]) == 2
