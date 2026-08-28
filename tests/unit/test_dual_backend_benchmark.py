import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from benchmarks.dual_backend import load_config, run_benchmark, run_fallback_drill

REPOSITORY_ROOT = Path(__file__).parents[2]


class SSEStream(httpx.AsyncByteStream):
    def __init__(self, content: str | list[dict[str, Any]], *, valid_usage: bool = True) -> None:
        usage = (
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            if valid_usage
            else {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 99}
        )
        events = (
            [{"choices": [{"delta": {"content": content}}]}]
            if isinstance(content, str)
            else content
        )
        self.chunks = [f"data: {json.dumps(event)}\n\n".encode() for event in events]
        self.chunks.append(f"data: {json.dumps({'choices': [], 'usage': usage})}\n\n".encode())
        self.chunks.append(b"data: [DONE]\n\n")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk


class ManualClock:
    def __init__(self, start: float = 0.0) -> None:
        self._value = start

    def __call__(self) -> float:
        return self._value

    def advance(self, seconds: float) -> None:
        self._value += seconds


class TimedSSEStream(SSEStream):
    def __init__(
        self,
        content: str | list[dict[str, Any]],
        *,
        clock: ManualClock,
        delays: list[float],
    ) -> None:
        super().__init__(content)
        if len(delays) != len(self.chunks):
            raise ValueError("each SSE chunk requires one delay")
        self.clock = clock
        self.delays = delays

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for delay, chunk in zip(self.delays, self.chunks, strict=True):
            self.clock.advance(delay)
            yield chunk


def _write_prompt_set(tmp_path: Path, prompts: list[dict[str, Any]]) -> Path:
    path = tmp_path / "prompts.json"
    path.write_text(
        json.dumps({"schema_version": "1.0.0", "prompts": prompts}, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _write_config(
    tmp_path: Path,
    *,
    fallback: bool = True,
    prompts: list[dict[str, Any]] | None = None,
    warmup_iterations: int = 1,
    measured_iterations: int = 2,
) -> Path:
    prompt_set = REPOSITORY_ROOT / "benchmarks/prompts.json"
    if prompts is not None:
        prompt_set = _write_prompt_set(tmp_path, prompts)
    config = {
        "schema_version": "1.0.0",
        "warmup_iterations": warmup_iterations,
        "measured_iterations": measured_iterations,
        "timeout_seconds": 5,
        "prompt_set": str(prompt_set),
        "endpoints": [
            {
                "name": "nvidia",
                "vendor": "nvidia",
                "base_url": "http://nvidia.test",
                "model": "physical-nvidia",
                "api_key_env": "NVIDIA_TEST_KEY",
            },
            {
                "name": "ascend",
                "vendor": "huawei-ascend",
                "base_url": "http://ascend.test",
                "model": "physical-ascend",
                "api_key_env": "ASCEND_TEST_KEY",
            },
        ],
        "fallback_drill": {
            "enabled": fallback,
            "base_url": "http://gateway.test",
            "model": "logical-chat",
            "api_key_env": "GATEWAY_TEST_KEY",
            "expected_primary_vendor": "nvidia",
            "expected_fallback_vendor": "huawei-ascend",
            "requests": 2,
        },
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def _answer(payload: dict[str, Any]) -> str:
    prompt = payload["messages"][0]["content"]
    if "Paris" in prompt:
        return "Paris"
    if "17 + 25" in prompt:
        return "42"
    return "dual-stack-ok"


async def test_dual_backend_harness_splits_phases_and_validates_fallback(
    tmp_path: Path,
) -> None:
    seen: dict[str, list[tuple[str, bool]]] = {"nvidia.test": [], "ascend.test": []}

    async def backend(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        payload = json.loads(request.content)
        if request.url.host == "gateway.test":
            return httpx.Response(
                200,
                headers={
                    "x-mini-ai-accelerator-vendor": "huawei-ascend",
                    "x-mini-ai-model-variant-id": "20000000-0000-0000-0000-000000000002",
                },
                json={"choices": []},
            )
        seen[request.url.host].append((payload["messages"][0]["content"], payload["stream"]))
        answer = _answer(payload)
        if payload["stream"]:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=SSEStream(answer),
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": answer}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    config = load_config(_write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)
        fallback_report = await run_fallback_drill(config, client=client)

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert report["evidence_status"] == "REAL_HW_NOT_RUN"
    observations = report["observations"]
    assert sum(item["phase"] == "warmup" for item in observations) == 8
    assert sum(item["phase"] == "measured" for item in observations) == 16
    assert all(item["ok"] for item in observations)
    assert report["fallback_drill"]["status"] == "NOT_RUN"
    assert fallback_report["fallback_drill"]["status"] == "PASS"
    assert fallback_report["fallback_drill"]["observed_vendors"] == [
        "huawei-ascend",
        "huawei-ascend",
    ]
    assert seen["nvidia.test"] == seen["ascend.test"]
    assert {item["mode"] for item in report["measured_summary"]} == {"buffered", "stream"}


async def test_dual_backend_baseline_phase_skips_fallback_when_not_requested(
    tmp_path: Path,
) -> None:
    async def backend(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        payload = json.loads(request.content)
        if request.url.host == "gateway.test":
            return httpx.Response(
                200,
                headers={
                    "x-mini-ai-accelerator-vendor": "huawei-ascend",
                    "x-mini-ai-model-variant-id": "20000000-0000-0000-0000-000000000002",
                },
                json={"choices": []},
            )
        answer = _answer(payload)
        if payload["stream"]:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=SSEStream(answer),
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": answer}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    config = load_config(_write_config(tmp_path, warmup_iterations=0, measured_iterations=1))
    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert report["fallback_drill"]["status"] == "NOT_RUN"
    assert report["fallback_drill"]["reason"] == (
        "fallback drill requires a separate fallback-only phase"
    )


async def test_dual_backend_baseline_success_not_tampered_by_fallback_failures(
    tmp_path: Path,
) -> None:
    async def backend(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        payload = json.loads(request.content)
        if request.url.host == "gateway.test":
            return httpx.Response(
                200,
                headers={
                    "x-mini-ai-accelerator-vendor": "nvidia",
                    "x-mini-ai-model-variant-id": "20000000-0000-0000-0000-000000000001",
                },
                json={"choices": []},
            )
        answer = _answer(payload)
        if payload["stream"]:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=SSEStream(answer),
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": answer}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    config = load_config(_write_config(tmp_path, warmup_iterations=0, measured_iterations=1))
    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)
        fallback_report = await run_fallback_drill(config, client=client)

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert fallback_report["run_status"] == "RUN_FAILED"
    assert fallback_report["fallback_drill"]["status"] == "FAIL"
    assert any(
        "routing:unexpected_vendor" in item for item in fallback_report["fallback_drill"]["errors"]
    )


async def test_dual_backend_fallback_only_mode_runs_gate_probe_phase(
    tmp_path: Path,
) -> None:
    async def backend(request: httpx.Request) -> httpx.Response:
        assert request.url.host is not None
        if request.url.host != "gateway.test":
            raise AssertionError("fallback-only mode should only call fallback endpoint")
        return httpx.Response(
            200,
            headers={
                "x-mini-ai-accelerator-vendor": "huawei-ascend",
                "x-mini-ai-model-variant-id": "20000000-0000-0000-0000-000000000002",
            },
            json={"choices": []},
        )

    config = load_config(_write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_fallback_drill(config, client=client)
    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert report["fallback_drill"]["status"] == "PASS"


async def test_dual_backend_exact_match_prevents_substring_confusion(
    tmp_path: Path,
) -> None:
    prompts = [
        {
            "id": "exact-capital",
            "match": "exact",
            "messages": [{"role": "user", "content": "Reply with exactly: Paris"}],
            "expected_any": ["paris"],
            "max_tokens": 8,
        },
        {
            "id": "exact-arithmetic",
            "match": "exact",
            "messages": [
                {"role": "user", "content": "Reply with exactly the integer result of 17 + 25."}
            ],
            "expected_any": ["42"],
            "max_tokens": 8,
        },
    ]
    config = load_config(
        _write_config(
            tmp_path,
            fallback=False,
            prompts=prompts,
            warmup_iterations=0,
            measured_iterations=1,
        )
    )

    async def backend(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        content = "Parisian" if "Paris" in payload["messages"][0]["content"] else "142"
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)

    assert report["run_status"] == "RUN_FAILED"
    assert all("semantic:exact_match_failed" in item["errors"] for item in report["observations"])


async def test_dual_backend_exact_match_normalizes_but_not_expand_meaning(
    tmp_path: Path,
) -> None:
    prompts = [
        {
            "id": "exact-arithmetic",
            "match": "exact",
            "messages": [
                {"role": "user", "content": "Reply with exactly the integer result of 17 + 25."}
            ],
            "expected_any": ["42"],
            "max_tokens": 8,
        }
    ]
    config = load_config(
        _write_config(
            tmp_path,
            fallback=False,
            prompts=prompts,
            warmup_iterations=0,
            measured_iterations=1,
        )
    )

    async def backend(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": " 42 "}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert all(item["ok"] for item in report["observations"])


async def test_dual_backend_contains_semantics_remain_default(
    tmp_path: Path,
) -> None:
    prompts = [
        {
            "id": "contains-paris",
            "messages": [{"role": "user", "content": "Tell me about Paris."}],
            "expected_any": ["paris"],
            "max_tokens": 8,
        }
    ]
    config = load_config(
        _write_config(
            tmp_path,
            fallback=False,
            prompts=prompts,
            warmup_iterations=0,
            measured_iterations=1,
        )
    )

    async def backend(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": "Parisian"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert all(item["ok"] for item in report["observations"])


async def test_dual_backend_ttft_records_first_non_empty_content_delta(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    events = [
        {"choices": [{"delta": {"role": "assistant"}}]},
        {"choices": [{"delta": {"content": ""}}]},
        {"choices": [{"delta": {"content": " "}}]},
        {"choices": [{"delta": {"content": "dual-stack-ok"}}]},
    ]
    clock = ManualClock(10.0)

    async def backend(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload["stream"]:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=TimedSSEStream(
                    events,
                    clock=clock,
                    delays=[0.125, 0.125, 0.25, 0.5, 0.0, 0.0],
                ),
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": _answer(payload)}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    config = load_config(_write_config(tmp_path, warmup_iterations=0, measured_iterations=1))
    monkeypatch.setattr("benchmarks.dual_backend.time.monotonic", clock)

    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)

    stream_observations = [item for item in report["observations"] if item["mode"] == "stream"]
    assert stream_observations
    assert all(item["time_to_first_token_seconds"] == 0.5 for item in stream_observations)


def test_prompt_matching_rejects_normalized_empty_expected_values(tmp_path: Path) -> None:
    prompts = [
        {
            "id": "empty-sentinel",
            "messages": [{"role": "user", "content": "Reply with anything."}],
            "expected_any": [" \n "],
            "max_tokens": 8,
        }
    ]

    with pytest.raises(ValueError, match="expected_any must be a non-empty string list"):
        load_config(_write_config(tmp_path, prompts=prompts))


async def test_invalid_usage_fails_run_without_upgrading_evidence(tmp_path: Path) -> None:
    async def backend(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        answer = _answer(payload)
        if payload["stream"]:
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=SSEStream(answer, valid_usage=False),
            )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={
                "choices": [{"message": {"content": answer}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 99},
            },
        )

    config = load_config(
        _write_config(
            tmp_path,
            fallback=False,
            warmup_iterations=0,
            measured_iterations=1,
        )
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as client:
        report = await run_benchmark(config, client=client)
    assert report["run_status"] == "RUN_FAILED"
    assert report["evidence_status"] == "REAL_HW_NOT_RUN"
    assert any("usage:inconsistent_total" in item["errors"] for item in report["observations"])


def test_checked_in_dual_hardware_evidence_is_conservative() -> None:
    evidence = json.loads(
        (REPOSITORY_ROOT / "evidence/m6-a11-dual-backend.json").read_text(encoding="utf-8")
    )
    assert evidence["evidence_status"] == "REAL_HW_NOT_RUN"
    assert evidence["verified_commit"] is None
    assert evidence["results"] == []
    assert evidence["fallback_drill"]["status"] == "NOT_RUN"
