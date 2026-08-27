from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import httpx

from core.nvidia_runtime import (
    NvidiaDeviceNodeSummary,
    NvidiaRuntimeAcceptanceContract,
    load_nvidia_acceptance_contract,
    parse_nvidia_smi_csv,
    summarize_nvidia_device_nodes,
    validate_nvidia_node_labels,
)

DEFAULT_CONTRACT_PATH = Path("runtime_profiles/nvidia-vllm-k8s.acceptance.json")
NVIDIA_SMI_QUERY = (
    "--query-gpu=name,driver_version,memory.total,compute_cap",
    "--format=csv,noheader,nounits",
)


async def accept_openai_engine(
    client: httpx.AsyncClient,
    *,
    model: str,
    contract: NvidiaRuntimeAcceptanceContract,
) -> dict[str, object]:
    health = await client.get("/health")
    health.raise_for_status()

    version_response = await client.get("/version")
    version_response.raise_for_status()
    version_payload = _json_object(version_response)
    if version_payload.get("version") != contract.vllm_version:
        raise ValueError("vLLM runtime version does not match the acceptance contract")

    request = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with the word ready."}],
        "temperature": 0,
        "max_tokens": 8,
    }
    completion = await client.post("/v1/chat/completions", json={**request, "stream": False})
    completion.raise_for_status()
    completion_payload = _json_object(completion)
    choices = completion_payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("non-streaming OpenAI response omitted choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict) or not isinstance(message.get("content"), str):
        raise ValueError("non-streaming OpenAI response omitted message content")

    saw_chunk = False
    saw_done = False
    async with client.stream(
        "POST",
        "/v1/chat/completions",
        json={**request, "stream": True},
    ) as stream:
        stream.raise_for_status()
        content_type = stream.headers.get("content-type", "")
        if "text/event-stream" not in content_type:
            raise ValueError("streaming OpenAI response is not text/event-stream")
        async for line in stream.aiter_lines():
            if not line.startswith("data: "):
                continue
            data = line.removeprefix("data: ")
            if data == "[DONE]":
                saw_done = True
                continue
            payload = json.loads(data)
            if not isinstance(payload, dict) or not isinstance(payload.get("choices"), list):
                raise ValueError("streaming OpenAI event omitted choices")
            saw_chunk = True
    if not saw_chunk or not saw_done:
        raise ValueError("streaming OpenAI response omitted a data chunk or DONE marker")

    return {
        "status": "REAL_ENGINE_PASS",
        "profile_identity": contract.profile_identity,
        "vllm_version": contract.vllm_version,
        "health": "pass",
        "non_streaming": "pass",
        "sse_streaming": "pass",
    }


def collect_nvidia_diagnostic(
    *,
    contract: NvidiaRuntimeAcceptanceContract,
    nvidia_smi: str = "nvidia-smi",
    dev_root: Path = Path("/dev"),
) -> dict[str, object]:
    executable = shutil.which(nvidia_smi)
    device_paths = tuple(dev_root.glob("nvidia*"))
    wsl_dxg = dev_root / "dxg"
    if wsl_dxg.exists():
        device_paths = (*device_paths, wsl_dxg)
    device_nodes = summarize_nvidia_device_nodes(device_paths)
    if executable is None:
        return _not_run_diagnostic(contract, device_nodes, "nvidia_smi_unavailable")
    try:
        completed = subprocess.run(
            (executable, *NVIDIA_SMI_QUERY),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return _not_run_diagnostic(contract, device_nodes, "nvidia_smi_execution_failed")
    if completed.returncode != 0:
        return _not_run_diagnostic(contract, device_nodes, "nvidia_smi_nonzero_exit")
    try:
        gpus = parse_nvidia_smi_csv(completed.stdout)
    except (TypeError, ValueError):
        return _not_run_diagnostic(contract, device_nodes, "nvidia_smi_output_invalid")
    if device_nodes.indexed_device_count < len(gpus) and not device_nodes.wsl_dxg_present:
        return _not_run_diagnostic(contract, device_nodes, "device_nodes_incomplete")
    return {
        "status": "HARDWARE_OBSERVED",
        "profile_identity": contract.profile_identity,
        "resource_name": contract.resource_name,
        "allocation_authority": contract.allocation_authority.value,
        "gpus": [gpu.model_dump(mode="json") for gpu in gpus],
        "device_nodes": device_nodes.model_dump(mode="json"),
        "limitations": [
            "This diagnostic does not prove model compatibility or engine acceptance.",
            "Linux /dev/nvidia* and WSL /dev/dxg are accepted device interfaces.",
            "Physical device IDs and complete environment variables are intentionally omitted.",
        ],
    }


def _not_run_diagnostic(
    contract: NvidiaRuntimeAcceptanceContract,
    device_nodes: NvidiaDeviceNodeSummary,
    reason: str,
) -> dict[str, object]:
    return {
        "status": "REAL_HW_NOT_RUN",
        "profile_identity": contract.profile_identity,
        "resource_name": contract.resource_name,
        "reason": reason,
        "gpus": [],
        "device_nodes": device_nodes.model_dump(mode="json"),
    }


def _json_object(response: httpx.Response) -> dict[str, Any]:
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("engine response must be a JSON object")
    return payload


def _load_labels(path: Path) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in payload.items()
    ):
        raise ValueError("node labels input must be a JSON string mapping")
    return payload


def _write_result(payload: dict[str, object], output: Path | None) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")


async def _run_acceptance(
    args: argparse.Namespace, contract: NvidiaRuntimeAcceptanceContract
) -> None:
    api_key = os.getenv(args.api_key_env) if args.api_key_env else None
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    timeout = httpx.Timeout(args.timeout)
    async with httpx.AsyncClient(
        base_url=args.base_url.rstrip("/"),
        headers=headers,
        timeout=timeout,
    ) as client:
        result = await accept_openai_engine(client, model=args.model, contract=contract)
    _write_result(result, args.output)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="NVIDIA runtime diagnostics and acceptance")
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    diagnose = subparsers.add_parser("diagnose")
    diagnose.add_argument("--dev-root", type=Path, default=Path("/dev"))
    diagnose.add_argument("--nvidia-smi", default="nvidia-smi")
    diagnose.add_argument("--output", type=Path)
    diagnose.add_argument("--require-hardware", action="store_true")

    labels = subparsers.add_parser("validate-node-labels")
    labels.add_argument("--input", type=Path, required=True)
    labels.add_argument("--accelerator-count", type=int, required=True)

    accept = subparsers.add_parser("accept")
    accept.add_argument("--base-url", required=True)
    accept.add_argument("--model", required=True)
    accept.add_argument("--api-key-env")
    accept.add_argument("--timeout", type=float, default=120.0)
    accept.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    contract = load_nvidia_acceptance_contract(args.contract)
    if args.command == "diagnose":
        result = collect_nvidia_diagnostic(
            contract=contract,
            nvidia_smi=args.nvidia_smi,
            dev_root=args.dev_root,
        )
        _write_result(result, args.output)
        if args.require_hardware and result["status"] != "HARDWARE_OBSERVED":
            raise SystemExit(2)
        return
    if args.command == "validate-node-labels":
        validate_nvidia_node_labels(
            _load_labels(args.input),
            requested_count=args.accelerator_count,
            contract=contract,
        )
        print("PASS: NVIDIA GFD node labels satisfy the runtime contract.")
        return
    if args.command == "accept":
        asyncio.run(_run_acceptance(args, contract))
        return
    raise AssertionError(args.command)


if __name__ == "__main__":
    main()
