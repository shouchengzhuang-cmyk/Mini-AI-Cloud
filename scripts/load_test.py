from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any

import httpx

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timed_out"}


@dataclass(slots=True)
class TaskSample:
    index: int
    request_started_at: float
    submitted_at: float | None = None
    completed_at: float | None = None
    task_id: str | None = None
    status: str | None = None
    submit_error: str | None = None
    poll_error: str | None = None

    @property
    def latency_seconds(self) -> float | None:
        if self.completed_at is None:
            return None
        return self.completed_at - self.request_started_at


def _dict_response(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{response.request.method} {response.request.url} returned non-JSON: "
            f"{response.text[:500]}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"{response.request.method} {response.request.url} returned a non-object payload"
        )
    return payload


async def _submit_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    *,
    index: int,
    run_id: str,
    image: str,
    task_timeout: int,
) -> TaskSample:
    async with semaphore:
        started_at = time.perf_counter()
        sample = TaskSample(index=index, request_started_at=started_at)
        payload = {
            "image": image,
            "command": ["python", "-c", f"print('load-task-{index}')"],
            "timeout_seconds": task_timeout,
            "cpu_limit": 0.25,
            "memory_limit_mb": 64,
        }
        try:
            response = await client.post(
                "/api/v1/tasks",
                json=payload,
                headers={"Idempotency-Key": f"load-{run_id}-{index}"},
            )
            response.raise_for_status()
            body = _dict_response(response)
            sample.task_id = str(body["id"])
            sample.submitted_at = time.perf_counter()
        except (httpx.HTTPError, KeyError, RuntimeError) as exc:
            sample.submit_error = str(exc)
        return sample


async def _poll_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    sample: TaskSample,
    *,
    poll_interval: float,
    completion_timeout: float,
) -> None:
    if sample.task_id is None or sample.submitted_at is None:
        return
    deadline = sample.submitted_at + completion_timeout
    while time.perf_counter() < deadline:
        try:
            async with semaphore:
                response = await client.get(f"/api/v1/tasks/{sample.task_id}")
            response.raise_for_status()
            body = _dict_response(response)
            status = str(body["status"])
            if status in TERMINAL_STATUSES:
                sample.status = status
                sample.completed_at = time.perf_counter()
                sample.poll_error = None
                return
        except (httpx.HTTPError, KeyError, RuntimeError) as exc:
            sample.poll_error = str(exc)
        await asyncio.sleep(poll_interval)
    sample.status = "client_timeout"
    if sample.poll_error is None:
        sample.poll_error = f"no terminal state after {completion_timeout:.1f}s"


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


def _summarize(
    samples: list[TaskSample],
    *,
    requested: int,
    submit_seconds: float,
) -> dict[str, Any]:
    submitted = [sample for sample in samples if sample.task_id is not None]
    terminal = [sample for sample in submitted if sample.completed_at is not None]
    latencies = [latency for sample in terminal if (latency := sample.latency_seconds) is not None]
    status_counts = Counter(sample.status or "not_submitted" for sample in samples)
    succeeded = status_counts["succeeded"]
    completion_window = 0.0
    if terminal:
        first_request = min(sample.request_started_at for sample in submitted)
        last_completion = max(sample.completed_at or first_request for sample in terminal)
        completion_window = max(0.0, last_completion - first_request)

    average = sum(latencies) / len(latencies) if latencies else None
    success_rate = (succeeded / requested * 100.0) if requested else 0.0
    return {
        "requested": requested,
        "submitted": len(submitted),
        "terminal": len(terminal),
        "statuses": dict(sorted(status_counts.items())),
        "submit_seconds": _rounded(submit_seconds),
        "submit_throughput_tasks_per_second": _rounded(
            len(submitted) / submit_seconds if submit_seconds > 0 else None
        ),
        "completion_window_seconds": _rounded(completion_window),
        "completion_throughput_tasks_per_second": _rounded(
            len(terminal) / completion_window if completion_window > 0 else None
        ),
        "latency_seconds": {
            "avg": _rounded(average),
            "p50": _rounded(_percentile(latencies, 0.50)),
            "p95": _rounded(_percentile(latencies, 0.95)),
            "p99": _rounded(_percentile(latencies, 0.99)),
        },
        "success_rate_percent": round(success_rate, 2),
        "submit_errors": [
            {"index": sample.index, "error": sample.submit_error}
            for sample in samples
            if sample.submit_error is not None
        ],
        "poll_errors": [
            {
                "index": sample.index,
                "task_id": sample.task_id,
                "error": sample.poll_error,
            }
            for sample in samples
            if sample.poll_error is not None
        ],
    }


async def _run(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    limits = httpx.Limits(
        max_connections=max(100, args.submit_concurrency + args.poll_concurrency),
        max_keepalive_connections=max(20, args.submit_concurrency),
    )
    timeout = httpx.Timeout(args.request_timeout)
    async with httpx.AsyncClient(
        base_url=args.base_url.rstrip("/"),
        timeout=timeout,
        limits=limits,
    ) as client:
        if not args.skip_health_check:
            response = await client.get("/health")
            response.raise_for_status()
            health = _dict_response(response)
            if health.get("status") != "ok":
                raise RuntimeError(f"platform health is not ok: {health}")

        run_id = uuid.uuid4().hex
        submit_semaphore = asyncio.Semaphore(args.submit_concurrency)
        submit_started = time.perf_counter()
        samples = await asyncio.gather(
            *(
                _submit_one(
                    client,
                    submit_semaphore,
                    index=index,
                    run_id=run_id,
                    image=args.image,
                    task_timeout=args.task_timeout,
                )
                for index in range(args.count)
            )
        )
        submit_seconds = time.perf_counter() - submit_started

        poll_semaphore = asyncio.Semaphore(args.poll_concurrency)
        await asyncio.gather(
            *(
                _poll_one(
                    client,
                    poll_semaphore,
                    sample,
                    poll_interval=args.poll_interval,
                    completion_timeout=args.completion_timeout,
                )
                for sample in samples
                if sample.task_id is not None
            )
        )

    summary = _summarize(samples, requested=args.count, submit_seconds=submit_seconds)
    success_rate = float(summary["success_rate_percent"])
    terminal = int(summary["terminal"])
    exit_code = 0 if terminal == args.count and success_rate >= args.min_success_rate else 1
    return summary, exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit Docker tasks concurrently and report throughput and latency."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--submit-concurrency", type=int, default=20)
    parser.add_argument("--poll-concurrency", type=int, default=50)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--completion-timeout", type=float, default=300.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--task-timeout", type=int, default=60)
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--min-success-rate", type=float, default=100.0)
    parser.add_argument("--skip-health-check", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.submit_concurrency < 1 or args.poll_concurrency < 1:
        raise SystemExit("concurrency values must be at least 1")
    if args.poll_interval <= 0 or args.completion_timeout <= 0:
        raise SystemExit("poll and timeout values must be greater than zero")
    if not 0 <= args.min_success_rate <= 100:
        raise SystemExit("--min-success-rate must be between 0 and 100")

    try:
        summary, exit_code = asyncio.run(_run(args))
    except (httpx.HTTPError, RuntimeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
