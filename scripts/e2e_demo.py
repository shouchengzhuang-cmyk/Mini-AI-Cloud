from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from typing import Any

import httpx

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timed_out"}


def _request_json(
    client: httpx.Client,
    method: str,
    path: str,
    **kwargs: Any,
) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(
            f"{method} {path} returned {response.status_code}: {response.text[:1000]}"
        ) from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"{method} {path} returned non-JSON: {response.text[:1000]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{method} {path} returned a non-object payload")
    return payload


def _wait_for_online_worker(
    client: httpx.Client,
    *,
    deadline: float,
    poll_interval: float,
) -> dict[str, Any]:
    while time.monotonic() < deadline:
        payload = _request_json(client, "GET", "/api/v1/workers")
        items = payload.get("items")
        if isinstance(items, list):
            for item in items:
                runtime_types = item.get("runtime_types") if isinstance(item, dict) else None
                if (
                    isinstance(item, dict)
                    and item.get("status") == "online"
                    and isinstance(runtime_types, list)
                    and "docker" in runtime_types
                ):
                    return item
        time.sleep(poll_interval)
    raise RuntimeError("no online Docker worker registered before the E2E deadline")


def _wait_for_terminal_task(
    client: httpx.Client,
    task_id: str,
    *,
    deadline: float,
    poll_interval: float,
) -> dict[str, Any]:
    last_status: object = None
    while time.monotonic() < deadline:
        task = _request_json(client, "GET", f"/api/v1/tasks/{task_id}")
        last_status = task.get("status")
        if last_status in TERMINAL_STATUSES:
            return task
        time.sleep(poll_interval)
    raise RuntimeError(f"task {task_id} did not finish; last status was {last_status!r}")


def _assert_logs(
    log_payload: dict[str, Any], start_marker: str, error_marker: str, end_marker: str
) -> int:
    raw_logs = log_payload.get("logs")
    if not isinstance(raw_logs, list):
        raise AssertionError("logs response does not contain a list")
    logs = [item for item in raw_logs if isinstance(item, dict)]
    sequences = [int(item["sequence"]) for item in logs]
    if sequences != sorted(sequences):
        raise AssertionError(f"log sequence is not ordered: {sequences}")
    if len(sequences) != len(set(sequences)):
        raise AssertionError(f"log sequence contains duplicates: {sequences}")
    combined = "".join(str(item.get("content", "")) for item in logs)
    if start_marker not in combined:
        raise AssertionError(f"stdout marker {start_marker!r} was not persisted")
    if error_marker not in combined:
        raise AssertionError(f"stderr marker {error_marker!r} was not persisted")
    if end_marker not in combined:
        raise AssertionError(f"stdout marker {end_marker!r} was not persisted")
    return len(logs)


async def _follow_sse(
    base_url: str,
    task_id: str,
    *,
    deadline: float,
    request_timeout: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    logs: list[dict[str, Any]] = []
    current_event: str | None = None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError(f"task {task_id} reached the overall E2E deadline before SSE started")

    try:
        async with asyncio.timeout(remaining):
            timeout = httpx.Timeout(min(request_timeout, remaining))
            async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout) as client:
                async with client.stream("GET", f"/api/v1/tasks/{task_id}/logs/stream") as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line.startswith("event: "):
                            current_event = line[7:]
                            continue
                        if not line.startswith("data: "):
                            continue
                        payload = json.loads(line[6:])
                        if not isinstance(payload, dict):
                            raise AssertionError("SSE data must be a JSON object")
                        if current_event == "log":
                            logs.append(payload)
                        elif current_event == "end":
                            return logs, payload
                        elif current_event == "error":
                            raise AssertionError(f"SSE returned an error event: {payload}")
    except TimeoutError as exc:
        raise RuntimeError(
            f"task {task_id} SSE stream did not reach an end event before the overall E2E deadline"
        ) from exc
    raise AssertionError("SSE connection closed without an end event")


def run(args: argparse.Namespace) -> dict[str, Any]:
    run_id = uuid.uuid4().hex
    start_marker = f"E2E_START_{run_id}"
    error_marker = f"E2E_ERROR_{run_id}"
    end_marker = f"E2E_END_{run_id}"
    task_id: str | None = None
    terminal = False

    with httpx.Client(
        base_url=args.base_url.rstrip("/"),
        timeout=httpx.Timeout(args.request_timeout),
    ) as client:
        try:
            health = _request_json(client, "GET", "/health")
            if health.get("status") != "ok":
                raise RuntimeError(f"platform health is not ok: {health}")
            deadline = time.monotonic() + args.timeout
            _wait_for_online_worker(
                client,
                deadline=deadline,
                poll_interval=args.poll_interval,
            )
            create = _request_json(
                client,
                "POST",
                "/api/v1/tasks",
                headers={"Idempotency-Key": f"e2e-{run_id}"},
                json={
                    "image": args.image,
                    "command": [
                        "python",
                        "-c",
                        (
                            "import sys,time; "
                            f"print({start_marker!r}, flush=True); "
                            "time.sleep(1); "
                            f"print({error_marker!r}, file=sys.stderr, flush=True); "
                            "time.sleep(1); "
                            f"print({end_marker!r}, flush=True)"
                        ),
                    ],
                    "timeout_seconds": args.task_timeout,
                    "cpu_limit": 0.5,
                    "memory_limit_mb": 128,
                },
            )
            task_id = str(create["id"])
            streamed_logs, end_event = asyncio.run(
                _follow_sse(
                    args.base_url,
                    task_id,
                    deadline=deadline,
                    request_timeout=args.request_timeout,
                )
            )
            streamed_content = "".join(str(item.get("content", "")) for item in streamed_logs)
            streamed_streams = {str(item.get("stream")) for item in streamed_logs}
            for marker in (start_marker, error_marker, end_marker):
                if marker not in streamed_content:
                    raise AssertionError(f"SSE did not deliver marker {marker!r}")
            if not {"stdout", "stderr"}.issubset(streamed_streams):
                raise AssertionError(
                    f"SSE did not deliver both stdout and stderr: {streamed_streams}"
                )
            if end_event.get("status") != "succeeded":
                raise AssertionError(f"SSE ended with unexpected payload: {end_event}")
            task = _wait_for_terminal_task(
                client,
                task_id,
                deadline=deadline,
                poll_interval=args.poll_interval,
            )
            terminal = True
            if task.get("status") != "succeeded":
                raise AssertionError(
                    f"task {task_id} finished as {task.get('status')}: {task.get('error_message')}"
                )
            if task.get("exit_code") != 0:
                raise AssertionError(f"task {task_id} exit_code is {task.get('exit_code')!r}")
            if not task.get("execution_id"):
                raise AssertionError(f"task {task_id} has no execution_id")
            if task.get("runtime_type") != "docker":
                raise AssertionError(
                    f"task {task_id} used unexpected runtime {task.get('runtime_type')!r}"
                )
            assigned_worker_id = task.get("worker_id")
            if not isinstance(assigned_worker_id, str) or not assigned_worker_id:
                raise AssertionError(f"task {task_id} has no assigned Worker")

            logs = _request_json(client, "GET", f"/api/v1/tasks/{task_id}/logs?limit=5000")
            log_count = _assert_logs(logs, start_marker, error_marker, end_marker)
            return {
                "health": health["status"],
                "worker_id": assigned_worker_id,
                "task_id": task_id,
                "status": task["status"],
                "exit_code": task["exit_code"],
                "execution_id": task["execution_id"],
                "log_count": log_count,
                "sse_log_count": len(streamed_logs),
                "assertions": "passed",
            }
        finally:
            if task_id is not None and not terminal:
                try:
                    client.post(f"/api/v1/tasks/{task_id}/cancel")
                except httpx.HTTPError:
                    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit one task and assert its terminal state and durable logs."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--task-timeout", type=int, default=60)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.timeout <= 0 or args.task_timeout < 1 or args.poll_interval <= 0:
        raise SystemExit("timeouts and poll interval must be greater than zero")
    try:
        result = run(args)
    except (AssertionError, KeyError, RuntimeError, httpx.HTTPError) as exc:
        print(f"E2E failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
