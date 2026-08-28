import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx

from benchmarks.dual_backend import load_config, run_benchmark

REPOSITORY_ROOT = Path(__file__).parents[2]


class SSEStream(httpx.AsyncByteStream):
    def __init__(self, content: str, *, valid_usage: bool = True) -> None:
        usage = (
            {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            if valid_usage
            else {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 99}
        )
        self.chunks = [
            f"data: {json.dumps({'choices': [{'delta': {'content': content}}]})}\n\n".encode(),
            f"data: {json.dumps({'choices': [], 'usage': usage})}\n\n".encode(),
            b"data: [DONE]\n\n",
        ]

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk


def _write_config(tmp_path: Path, *, fallback: bool = True) -> Path:
    config = {
        "schema_version": "1.0.0",
        "warmup_iterations": 1,
        "measured_iterations": 2,
        "timeout_seconds": 5,
        "prompt_set": str(REPOSITORY_ROOT / "benchmarks/prompts.json"),
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

    assert report["run_status"] == "RUN_COMPLETED_UNVERIFIED"
    assert report["evidence_status"] == "REAL_HW_NOT_RUN"
    observations = report["observations"]
    assert sum(item["phase"] == "warmup" for item in observations) == 8
    assert sum(item["phase"] == "measured" for item in observations) == 16
    assert all(item["ok"] for item in observations)
    assert report["fallback_drill"]["status"] == "PASS"
    assert report["fallback_drill"]["observed_vendors"] == [
        "huawei-ascend",
        "huawei-ascend",
    ]
    assert seen["nvidia.test"] == seen["ascend.test"]
    assert {item["mode"] for item in report["measured_summary"]} == {"buffered", "stream"}


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

    config = load_config(_write_config(tmp_path, fallback=False))
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
