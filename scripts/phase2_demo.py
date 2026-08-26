from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

TERMINAL_TASK_STATUSES = {"succeeded", "failed", "cancelled", "timed_out"}


class DemoFailure(RuntimeError):
    """A demo assertion or HTTP operation failed."""


@dataclass(frozen=True, slots=True)
class DemoResult:
    project_id: str
    task_id: str
    execution_id: str
    log_records: int
    timeline_events: int
    usage_executions: int
    artifact_id: str | None
    artifact_sha256: str | None
    project_isolation_checked: bool


class MiniCloudDemoClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout: float,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.api_key = api_key
        self.client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> MiniCloudDemoClient:
        self.client.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        self.client.close()

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        expected_status: int = 200,
        **kwargs: Any,
    ) -> httpx.Response:
        try:
            response = self.client.request(method, path_or_url, **kwargs)
        except httpx.HTTPError as exc:
            raise DemoFailure(f"{method} {path_or_url} failed: {exc}") from exc
        if response.status_code != expected_status:
            body = _redact(response.text[:2000], self.api_key)
            raise DemoFailure(
                f"{method} {path_or_url} returned {response.status_code}, "
                f"expected {expected_status}: {body}"
            )
        return response

    def json(
        self,
        method: str,
        path_or_url: str,
        *,
        expected_status: int = 200,
        **kwargs: Any,
    ) -> dict[str, Any]:
        response = self.request(
            method,
            path_or_url,
            expected_status=expected_status,
            **kwargs,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise DemoFailure(f"{method} {path_or_url} returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise DemoFailure(f"{method} {path_or_url} returned a non-object JSON payload")
        return payload


def run_authenticated_project_demo(
    client: MiniCloudDemoClient,
    *,
    poll_interval: float,
    deadline_seconds: float,
    image_reference: str = "python:3.12-slim",
    other_project_client: MiniCloudDemoClient | None = None,
    exercise_artifacts: bool = True,
    keep_artifact: bool = False,
) -> DemoResult:
    client.json("GET", "/readyz")
    principal = client.json("GET", "/api/v1/auth/whoami")
    project_id = _required_string(principal, "project_id", "whoami")

    quota = client.json("GET", f"/api/v1/projects/{project_id}/quota")
    if quota.get("project_id") != project_id:
        raise DemoFailure("quota response does not belong to the authenticated project")
    image_decision = client.json(
        "POST",
        f"/api/v1/projects/{project_id}/image-policy/evaluate",
        json={"image": image_reference},
    )
    if image_decision.get("allowed") is not True:
        raise DemoFailure(
            "the project image policy denied the demo image "
            f"{image_reference!r}: {image_decision.get('reason')!r}"
        )

    started_at = datetime.now(UTC) - timedelta(seconds=1)
    marker = f"phase2-auth-demo-{uuid.uuid4().hex[:12]}"
    created = client.json(
        "POST",
        "/api/v1/tasks",
        expected_status=201,
        headers={"Idempotency-Key": marker},
        json={
            "image": image_reference,
            "command": ["python", "-c", f"print('{marker}', flush=True)"],
            "timeout_seconds": 60,
            "cpu_limit": 0.25,
            "memory_limit_mb": 128,
            "network_enabled": False,
        },
    )
    task_id = _required_string(created, "id", "task create")
    task = _wait_for_task(
        client,
        task_id,
        poll_interval=poll_interval,
        deadline_seconds=deadline_seconds,
    )
    if task.get("status") != "succeeded":
        raise DemoFailure(
            f"task {task_id} ended as {task.get('status')!r}: {task.get('error_message')!r}"
        )
    execution_id = _required_string(task, "execution_id", "terminal task")

    logs = client.json("GET", f"/api/v1/tasks/{task_id}/logs?limit=500")
    records = logs.get("logs")
    if not isinstance(records, list):
        raise DemoFailure("task log response has no log list")
    combined_logs = "".join(
        str(record.get("content", "")) for record in records if isinstance(record, dict)
    )
    if marker not in combined_logs:
        raise DemoFailure("persistent task logs do not contain the demo marker")

    timeline = client.json("GET", f"/api/v1/tasks/{task_id}/timeline")
    events = timeline.get("events")
    if not isinstance(events, list) or not events:
        raise DemoFailure("task timeline is empty")

    finished_at = datetime.now(UTC) + timedelta(seconds=1)
    window = {
        "from": started_at.isoformat(),
        "to": finished_at.isoformat(),
    }
    usage = client.json("GET", f"/api/v1/projects/{project_id}/usage", params=window)
    cost = client.json("GET", f"/api/v1/projects/{project_id}/cost", params=window)
    if usage.get("project_id") != project_id or cost.get("project_id") != project_id:
        raise DemoFailure("usage or cost response crossed the authenticated project boundary")
    usage_executions = usage.get("execution_count")
    if not isinstance(usage_executions, int) or usage_executions < 1:
        raise DemoFailure("completed demo execution is absent from the usage ledger")

    isolation_checked = False
    if other_project_client is not None:
        other = other_project_client.json("GET", "/api/v1/auth/whoami")
        other_project_id = _required_string(other, "project_id", "secondary whoami")
        if other_project_id == project_id:
            raise DemoFailure("secondary API key belongs to the same project")
        response = other_project_client.client.get(f"/api/v1/tasks/{task_id}")
        if response.status_code != 404:
            raise DemoFailure(
                "cross-project task lookup did not use not-found isolation semantics: "
                f"HTTP {response.status_code}"
            )
        isolation_checked = True

    artifact_id: str | None = None
    artifact_sha256: str | None = None
    if exercise_artifacts:
        artifact_id, artifact_sha256 = _exercise_artifact(
            client,
            marker=marker,
            keep_artifact=keep_artifact,
        )

    return DemoResult(
        project_id=project_id,
        task_id=task_id,
        execution_id=execution_id,
        log_records=len(records),
        timeline_events=len(events),
        usage_executions=usage_executions,
        artifact_id=artifact_id,
        artifact_sha256=artifact_sha256,
        project_isolation_checked=isolation_checked,
    )


def _wait_for_task(
    client: MiniCloudDemoClient,
    task_id: str,
    *,
    poll_interval: float,
    deadline_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        task = client.json("GET", f"/api/v1/tasks/{task_id}")
        if task.get("status") in TERMINAL_TASK_STATUSES:
            return task
        time.sleep(poll_interval)
    raise DemoFailure(f"task {task_id} did not reach a terminal state within {deadline_seconds}s")


def _exercise_artifact(
    client: MiniCloudDemoClient,
    *,
    marker: str,
    keep_artifact: bool,
) -> tuple[str, str]:
    content = f"verified artifact for {marker}\n".encode()
    checksum = hashlib.sha256(content).hexdigest()
    created = client.json(
        "POST",
        "/api/v1/artifacts",
        expected_status=201,
        json={
            "name": f"{marker}.txt",
            "content_type": "text/plain",
            "size_bytes": len(content),
            "sha256": checksum,
        },
    )
    artifact_id = _required_string(created, "id", "artifact create")
    try:
        upload = client.json("POST", f"/api/v1/artifacts/{artifact_id}/upload-url")
        _transfer(client, upload, content=content)
        finalized = client.json(
            "POST",
            f"/api/v1/artifacts/{artifact_id}/finalize",
            json={"size_bytes": len(content), "sha256": checksum},
        )
        if finalized.get("state") != "ready":
            raise DemoFailure("artifact did not become ready after finalization")

        download = client.json("GET", f"/api/v1/artifacts/{artifact_id}/download-url")
        downloaded = _transfer(client, download)
        if downloaded != content or hashlib.sha256(downloaded).hexdigest() != checksum:
            raise DemoFailure("downloaded artifact failed byte-for-byte checksum verification")
    finally:
        if not keep_artifact:
            client.request("DELETE", f"/api/v1/artifacts/{artifact_id}")
    return artifact_id, checksum


def _transfer(
    client: MiniCloudDemoClient,
    grant: dict[str, Any],
    *,
    content: bytes | None = None,
) -> bytes:
    method = _required_string(grant, "method", "artifact transfer grant")
    url = _required_string(grant, "url", "artifact transfer grant")
    authorization = _required_string(grant, "authorization", "artifact transfer grant")
    raw_headers = grant.get("headers", {})
    if not isinstance(raw_headers, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in raw_headers.items()
    ):
        raise DemoFailure("artifact transfer grant contains invalid headers")
    headers = {str(key): str(value) for key, value in raw_headers.items()}
    kwargs: dict[str, Any] = {"headers": headers}
    if content is not None:
        kwargs["content"] = content

    if authorization == "api":
        response = client.request(method, url, **kwargs)
    elif authorization == "presigned":
        # Never forward the platform API key to an object-store presigned URL.
        try:
            response = httpx.request(method, url, timeout=client.client.timeout, **kwargs)
        except httpx.HTTPError as exc:
            raise DemoFailure(f"presigned artifact transfer failed: {exc}") from exc
        if response.status_code < 200 or response.status_code >= 300:
            raise DemoFailure(
                f"presigned artifact transfer returned HTTP {response.status_code}: "
                f"{response.text[:1000]}"
            )
    else:
        raise DemoFailure(f"unsupported artifact transfer authorization: {authorization!r}")
    return response.content


def _required_string(payload: dict[str, Any], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise DemoFailure(f"{context} response has no non-empty {key!r}")
    return value


def _redact(value: str, secret: str) -> str:
    return value.replace(secret, "[REDACTED]") if secret else value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the authenticated Phase II task, usage, timeline and artifact E2E demo "
            "against an already running local stack."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--api-key",
        default=os.getenv("MINI_CLOUD_API_KEY"),
        help="Owner/admin project API key (prefer MINI_CLOUD_API_KEY to shell history)",
    )
    parser.add_argument(
        "--other-project-api-key",
        default=os.getenv("MINI_CLOUD_OTHER_PROJECT_API_KEY"),
        help="Optional key from another project to verify not-found isolation",
    )
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument(
        "--image",
        default="python:3.12-slim",
        help="Explicit image tag or digest already allowed by the project image policy",
    )
    parser.add_argument("--skip-artifact", action="store_true")
    parser.add_argument("--keep-artifact", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.api_key:
        print(
            "phase2_demo: set MINI_CLOUD_API_KEY or pass --api-key",
            file=sys.stderr,
        )
        return 2
    if args.timeout <= 0 or args.poll_interval <= 0:
        print("phase2_demo: timeout and poll interval must be positive", file=sys.stderr)
        return 2

    try:
        with MiniCloudDemoClient(
            base_url=args.base_url,
            api_key=args.api_key,
            timeout=args.timeout,
        ) as client:
            if args.other_project_api_key:
                with MiniCloudDemoClient(
                    base_url=args.base_url,
                    api_key=args.other_project_api_key,
                    timeout=args.timeout,
                ) as other_client:
                    result = run_authenticated_project_demo(
                        client,
                        poll_interval=args.poll_interval,
                        deadline_seconds=args.timeout,
                        image_reference=args.image,
                        other_project_client=other_client,
                        exercise_artifacts=not args.skip_artifact,
                        keep_artifact=args.keep_artifact,
                    )
            else:
                result = run_authenticated_project_demo(
                    client,
                    poll_interval=args.poll_interval,
                    deadline_seconds=args.timeout,
                    image_reference=args.image,
                    exercise_artifacts=not args.skip_artifact,
                    keep_artifact=args.keep_artifact,
                )
    except DemoFailure as exc:
        print(f"phase2_demo: FAILED: {_redact(str(exc), args.api_key)}", file=sys.stderr)
        return 1

    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    print("assertions: passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
