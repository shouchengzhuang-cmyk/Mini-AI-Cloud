from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

SUPPORTED_VENDORS = frozenset({"nvidia", "huawei-ascend"})


@dataclass(frozen=True, slots=True)
class PromptCase:
    id: str
    messages: list[dict[str, str]]
    expected_any: list[str]
    max_tokens: int


@dataclass(frozen=True, slots=True)
class Endpoint:
    name: str
    vendor: str
    base_url: str
    model: str
    api_key_env: str


@dataclass(frozen=True, slots=True)
class FallbackDrill:
    enabled: bool
    base_url: str
    model: str
    api_key_env: str
    expected_primary_vendor: str
    expected_fallback_vendor: str
    requests: int


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    warmup_iterations: int
    measured_iterations: int
    timeout_seconds: float
    endpoints: tuple[Endpoint, Endpoint]
    prompts: tuple[PromptCase, ...]
    fallback_drill: FallbackDrill


@dataclass(frozen=True, slots=True)
class Observation:
    phase: str
    endpoint: str
    vendor: str
    prompt_id: str
    mode: str
    iteration: int
    ok: bool
    latency_seconds: float
    time_to_first_token_seconds: float | None
    content_sha256: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    errors: tuple[str, ...]


def load_config(path: Path) -> BenchmarkConfig:
    raw = _load_object(path)
    if raw.get("schema_version") != "1.0.0":
        raise ValueError("benchmark config schema_version must be 1.0.0")
    endpoints_raw = raw.get("endpoints")
    if not isinstance(endpoints_raw, list) or len(endpoints_raw) != 2:
        raise ValueError("benchmark config requires exactly two endpoints")
    endpoints = tuple(_endpoint(item) for item in endpoints_raw)
    assert len(endpoints) == 2
    if {item.vendor for item in endpoints} != SUPPORTED_VENDORS:
        raise ValueError("endpoints must contain exactly NVIDIA and Huawei Ascend")
    if len({item.name for item in endpoints}) != 2:
        raise ValueError("endpoint names must be unique")

    prompt_path = Path(_required_string(raw, "prompt_set"))
    if not prompt_path.is_absolute():
        prompt_path = path.parent / prompt_path
    prompt_raw = _load_object(prompt_path)
    if prompt_raw.get("schema_version") != "1.0.0":
        raise ValueError("prompt set schema_version must be 1.0.0")
    prompt_items = prompt_raw.get("prompts")
    if not isinstance(prompt_items, list) or not prompt_items:
        raise ValueError("prompt set must contain prompts")
    prompts = tuple(_prompt(item) for item in prompt_items)
    if len({item.id for item in prompts}) != len(prompts):
        raise ValueError("prompt ids must be unique")

    drill_raw = raw.get("fallback_drill")
    if not isinstance(drill_raw, dict):
        raise ValueError("fallback_drill must be an object")
    drill = FallbackDrill(
        enabled=bool(drill_raw.get("enabled", False)),
        base_url=_required_string(drill_raw, "base_url"),
        model=_required_string(drill_raw, "model"),
        api_key_env=_required_string(drill_raw, "api_key_env"),
        expected_primary_vendor=_vendor(drill_raw, "expected_primary_vendor"),
        expected_fallback_vendor=_vendor(drill_raw, "expected_fallback_vendor"),
        requests=_positive_int(drill_raw, "requests"),
    )
    if drill.expected_primary_vendor == drill.expected_fallback_vendor:
        raise ValueError("fallback drill primary and fallback vendors must differ")
    return BenchmarkConfig(
        warmup_iterations=_nonnegative_int(raw, "warmup_iterations"),
        measured_iterations=_positive_int(raw, "measured_iterations"),
        timeout_seconds=_positive_number(raw, "timeout_seconds"),
        endpoints=(endpoints[0], endpoints[1]),
        prompts=prompts,
        fallback_drill=drill,
    )


async def run_benchmark(
    config: BenchmarkConfig,
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    run_id = str(uuid.uuid4())
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )
    observations: list[Observation] = []
    try:
        for phase, iterations in (
            ("warmup", config.warmup_iterations),
            ("measured", config.measured_iterations),
        ):
            for iteration in range(iterations):
                for endpoint in config.endpoints:
                    for prompt in config.prompts:
                        observations.append(
                            await _observe(
                                client,
                                endpoint=endpoint,
                                prompt=prompt,
                                phase=phase,
                                iteration=iteration,
                                stream=False,
                            )
                        )
                        if prompt.id == "stream-sentinel":
                            observations.append(
                                await _observe(
                                    client,
                                    endpoint=endpoint,
                                    prompt=prompt,
                                    phase=phase,
                                    iteration=iteration,
                                    stream=True,
                                )
                            )
            if phase == "warmup":
                await asyncio.sleep(0)
        fallback = await _run_fallback_drill(client, config)
    finally:
        if owns_client:
            await client.aclose()

    measured = [item for item in observations if item.phase == "measured"]
    complete = bool(measured) and all(item.ok for item in measured)
    if config.fallback_drill.enabled:
        complete = complete and fallback["status"] == "PASS"
    return {
        "schema_version": "1.0.0",
        "run_status": "RUN_COMPLETED_UNVERIFIED" if complete else "RUN_FAILED",
        "evidence_status": "REAL_HW_NOT_RUN",
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "config": {
            "warmup_iterations": config.warmup_iterations,
            "measured_iterations": config.measured_iterations,
            "timeout_seconds": config.timeout_seconds,
            "prompt_ids": [item.id for item in config.prompts],
            "endpoints": [
                {"name": item.name, "vendor": item.vendor, "model": item.model}
                for item in config.endpoints
            ],
        },
        "observations": [asdict(item) for item in observations],
        "measured_summary": _summaries(measured),
        "fallback_drill": fallback,
        "limitations": [
            "HTTP success does not prove the declared physical hardware identity.",
            "Results cover only this bounded prompt set and sample count.",
            "evidence_status remains REAL_HW_NOT_RUN pending diagnostics and review.",
        ],
    }


async def _observe(
    client: httpx.AsyncClient,
    *,
    endpoint: Endpoint,
    prompt: PromptCase,
    phase: str,
    iteration: int,
    stream: bool,
) -> Observation:
    started = time.monotonic()
    ttft: float | None = None
    content = ""
    usage: dict[str, int] | None = None
    errors: list[str] = []
    headers = _auth_headers(endpoint.api_key_env)
    payload: dict[str, Any] = {
        "model": endpoint.model,
        "messages": prompt.messages,
        "max_tokens": prompt.max_tokens,
        "temperature": 0,
        "stream": stream,
    }
    if stream:
        payload["stream_options"] = {"include_usage": True}
    try:
        async with client.stream(
            "POST",
            f"{endpoint.base_url.rstrip('/')}/v1/chat/completions",
            json=payload,
            headers=headers,
        ) as response:
            if response.status_code != 200:
                errors.append(f"http_status:{response.status_code}")
            content_type = response.headers.get("content-type", "").lower()
            if stream:
                if not content_type.startswith("text/event-stream"):
                    errors.append("protocol:expected_sse")
                content, usage, ttft, stream_errors = await _read_sse(response, started)
                errors.extend(stream_errors)
            else:
                if "application/json" not in content_type:
                    errors.append("protocol:expected_json")
                body = await response.aread()
                if body and ttft is None:
                    ttft = max(0.0, time.monotonic() - started)
                content, usage, body_errors = _read_buffered(body)
                errors.extend(body_errors)
    except (httpx.HTTPError, TimeoutError) as exc:
        errors.append(f"transport:{type(exc).__name__}")
    errors.extend(_semantic_errors(content, prompt))
    errors.extend(_usage_errors(usage))
    latency = max(0.0, time.monotonic() - started)
    return Observation(
        phase=phase,
        endpoint=endpoint.name,
        vendor=endpoint.vendor,
        prompt_id=prompt.id,
        mode="stream" if stream else "buffered",
        iteration=iteration,
        ok=not errors,
        latency_seconds=latency,
        time_to_first_token_seconds=ttft,
        content_sha256=(hashlib.sha256(content.encode()).hexdigest() if content else None),
        prompt_tokens=usage.get("prompt_tokens") if usage else None,
        completion_tokens=usage.get("completion_tokens") if usage else None,
        total_tokens=usage.get("total_tokens") if usage else None,
        errors=tuple(errors),
    )


async def _read_sse(
    response: httpx.Response, started: float
) -> tuple[str, dict[str, int] | None, float | None, list[str]]:
    content_parts: list[str] = []
    usage: dict[str, int] | None = None
    ttft: float | None = None
    done = False
    errors: list[str] = []
    async for line in response.aiter_lines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            done = True
            break
        if ttft is None:
            ttft = max(0.0, time.monotonic() - started)
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            errors.append("protocol:invalid_sse_json")
            continue
        if not isinstance(event, dict) or not isinstance(event.get("choices"), list):
            errors.append("protocol:invalid_sse_event")
            continue
        for choice in event["choices"]:
            if isinstance(choice, dict):
                delta = choice.get("delta")
                if isinstance(delta, dict) and isinstance(delta.get("content"), str):
                    content_parts.append(delta["content"])
        if isinstance(event.get("usage"), dict):
            usage = event["usage"]
    if not done:
        errors.append("stream:missing_done")
    return "".join(content_parts), usage, ttft, errors


def _read_buffered(body: bytes) -> tuple[str, dict[str, int] | None, list[str]]:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "", None, ["protocol:invalid_json"]
    if not isinstance(payload, dict) or not isinstance(payload.get("choices"), list):
        return "", None, ["protocol:invalid_response_object"]
    content_parts: list[str] = []
    for choice in payload["choices"]:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message")
        if isinstance(message, dict) and isinstance(message.get("content"), str):
            content_parts.append(message["content"])
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else None
    return "".join(content_parts), usage, []


def _semantic_errors(content: str, prompt: PromptCase) -> list[str]:
    normalized = " ".join(content.lower().split())
    if not normalized:
        return ["semantic:empty_content"]
    if not any(expected.lower() in normalized for expected in prompt.expected_any):
        return ["semantic:expected_sentinel_missing"]
    return []


def _usage_errors(usage: dict[str, Any] | None) -> list[str]:
    if usage is None:
        return ["usage:missing"]
    values = [usage.get(name) for name in ("prompt_tokens", "completion_tokens", "total_tokens")]
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
        return ["usage:invalid_schema"]
    prompt_tokens, completion_tokens, total_tokens = values
    assert isinstance(prompt_tokens, int)
    assert isinstance(completion_tokens, int)
    assert isinstance(total_tokens, int)
    if prompt_tokens + completion_tokens != total_tokens:
        return ["usage:inconsistent_total"]
    return []


async def _run_fallback_drill(client: httpx.AsyncClient, config: BenchmarkConfig) -> dict[str, Any]:
    drill = config.fallback_drill
    if not drill.enabled:
        return {"status": "NOT_RUN", "reason": "disabled in benchmark config"}
    prompt = config.prompts[0]
    failures: list[str] = []
    observed_vendors: list[str | None] = []
    observed_variants: list[str | None] = []
    for _ in range(drill.requests):
        try:
            response = await client.post(
                f"{drill.base_url.rstrip('/')}/v1/chat/completions",
                json={
                    "model": drill.model,
                    "messages": prompt.messages,
                    "max_tokens": prompt.max_tokens,
                    "temperature": 0,
                },
                headers=_auth_headers(drill.api_key_env),
            )
        except httpx.HTTPError as exc:
            failures.append(f"transport:{type(exc).__name__}")
            continue
        vendor = response.headers.get("x-mini-ai-accelerator-vendor")
        variant = response.headers.get("x-mini-ai-model-variant-id")
        observed_vendors.append(vendor)
        observed_variants.append(variant)
        if response.status_code != 200:
            failures.append(f"http_status:{response.status_code}")
        if vendor != drill.expected_fallback_vendor:
            failures.append("routing:unexpected_vendor")
        if not variant:
            failures.append("routing:missing_physical_variant")
    return {
        "status": "PASS" if not failures else "FAIL",
        "expected_primary_vendor": drill.expected_primary_vendor,
        "expected_fallback_vendor": drill.expected_fallback_vendor,
        "request_count": drill.requests,
        "observed_vendors": observed_vendors,
        "observed_variant_ids": observed_variants,
        "errors": failures,
        "operator_attestation_required": True,
    }


def _summaries(observations: list[Observation]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[Observation]] = {}
    for item in observations:
        groups.setdefault((item.endpoint, item.vendor, item.mode), []).append(item)
    summaries: list[dict[str, Any]] = []
    for (endpoint, vendor, mode), values in sorted(groups.items()):
        latencies = sorted(item.latency_seconds for item in values if item.ok)
        summaries.append(
            {
                "endpoint": endpoint,
                "vendor": vendor,
                "mode": mode,
                "requests": len(values),
                "successful": sum(item.ok for item in values),
                "p50_latency_seconds": statistics.median(latencies) if latencies else None,
                "p95_latency_seconds": _percentile(latencies, 0.95),
            }
        )
    return summaries


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    index = max(0, min(len(values) - 1, int((len(values) * fraction) + 0.999999) - 1))
    return values[index]


def _auth_headers(environment_name: str) -> dict[str, str]:
    value = os.environ.get(environment_name)
    return {"authorization": f"Bearer {value}"} if value else {}


def _endpoint(value: object) -> Endpoint:
    if not isinstance(value, dict):
        raise ValueError("endpoint entries must be objects")
    return Endpoint(
        name=_required_string(value, "name"),
        vendor=_vendor(value, "vendor"),
        base_url=_required_string(value, "base_url"),
        model=_required_string(value, "model"),
        api_key_env=_required_string(value, "api_key_env"),
    )


def _prompt(value: object) -> PromptCase:
    if not isinstance(value, dict):
        raise ValueError("prompt entries must be objects")
    messages = value.get("messages")
    expected = value.get("expected_any")
    if not isinstance(messages, list) or not messages:
        raise ValueError("prompt messages must be a non-empty list")
    normalized_messages: list[dict[str, str]] = []
    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("prompt messages must be objects")
        normalized_messages.append(
            {
                "role": _required_string(message, "role"),
                "content": _required_string(message, "content"),
            }
        )
    if (
        not isinstance(expected, list)
        or not expected
        or not all(isinstance(x, str) and x for x in expected)
    ):
        raise ValueError("expected_any must be a non-empty string list")
    return PromptCase(
        id=_required_string(value, "id"),
        messages=normalized_messages,
        expected_any=list(expected),
        max_tokens=_positive_int(value, "max_tokens"),
    )


def _load_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return item.strip()


def _vendor(value: dict[str, Any], key: str) -> str:
    vendor = _required_string(value, key)
    if vendor not in SUPPORTED_VENDORS:
        raise ValueError(f"{key} must be a supported vendor")
    return vendor


def _positive_int(value: dict[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 1:
        raise ValueError(f"{key} must be a positive integer")
    return item


def _nonnegative_int(value: dict[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return item


def _positive_number(value: dict[str, Any], key: str) -> float:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, (int, float)) or item <= 0:
        raise ValueError(f"{key} must be positive")
    return float(item)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark NVIDIA and Huawei Ascend backends")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    report = asyncio.run(run_benchmark(load_config(args.config.resolve())))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {report['run_status']} report to {args.output}")
    return 0 if report["run_status"] == "RUN_COMPLETED_UNVERIFIED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
