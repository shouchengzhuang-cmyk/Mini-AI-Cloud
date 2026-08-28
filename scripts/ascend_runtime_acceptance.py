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

from core.ascend_runtime import (
    AscendDeviceNodeSummary,
    AscendRuntimeAcceptanceContract,
    evaluate_ascend_cluster,
    load_ascend_acceptance_contract,
    parse_npu_smi_list,
    summarize_ascend_device_nodes,
)

DEFAULT_CONTRACT_PATH = Path("runtime_profiles/ascend-vllm-k8s.acceptance.json")


async def accept_openai_engine(
    client: httpx.AsyncClient,
    *,
    model: str,
    contract: AscendRuntimeAcceptanceContract,
) -> dict[str, object]:
    health = await client.get("/health")
    health.raise_for_status()

    version_response = await client.get("/version")
    version_response.raise_for_status()
    version_payload = _json_object(version_response)
    if version_payload.get("version") != contract.vllm_version:
        raise ValueError("vLLM runtime version does not match the Ascend acceptance contract")

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
        if "text/event-stream" not in stream.headers.get("content-type", ""):
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
        "vllm_ascend_version": contract.vllm_ascend_version,
        "health": "pass",
        "non_streaming": "pass",
        "sse_streaming": "pass",
    }


def collect_ascend_diagnostic(
    *,
    contract: AscendRuntimeAcceptanceContract,
    npu_smi: str = "npu-smi",
    dev_root: Path = Path("/dev"),
) -> dict[str, object]:
    executable = shutil.which(npu_smi)
    device_nodes = summarize_ascend_device_nodes(tuple(dev_root.glob("davinci*")))
    additional_nodes = tuple(
        path for name in ("devmm_svm", "hisi_hdc") if (path := dev_root / name).exists()
    )
    if additional_nodes:
        device_nodes = summarize_ascend_device_nodes(
            (*tuple(dev_root.glob("davinci*")), *additional_nodes)
        )
    if executable is None:
        return _not_run_diagnostic(contract, device_nodes, "npu_smi_unavailable")
    try:
        completed = subprocess.run(
            (executable, "info", "-l"),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return _not_run_diagnostic(contract, device_nodes, "npu_smi_execution_failed")
    if completed.returncode != 0:
        return _not_run_diagnostic(contract, device_nodes, "npu_smi_nonzero_exit")
    try:
        diagnostic = parse_npu_smi_list(completed.stdout)
    except (TypeError, ValueError):
        return _not_run_diagnostic(contract, device_nodes, "npu_smi_output_invalid")
    if device_nodes.indexed_device_count < diagnostic.card_count:
        return _not_run_diagnostic(contract, device_nodes, "device_nodes_incomplete")
    return {
        "status": "HARDWARE_OBSERVED",
        "profile_identity": contract.profile_identity,
        "product_generation": contract.product_generation,
        "resource_name": contract.resource_name,
        "allocation_authority": contract.allocation_authority.value,
        "npu_summary": diagnostic.model_dump(mode="json"),
        "device_nodes": device_nodes.model_dump(mode="json"),
        "limitations": [
            "This diagnostic does not prove model compatibility or engine acceptance.",
            "Physical device IDs, serial numbers, and complete environment values are omitted.",
        ],
    }


def collect_cluster_preflight(
    *,
    contract: AscendRuntimeAcceptanceContract,
    kubectl: str = "kubectl",
    kubeconfig: Path | None = None,
) -> dict[str, object]:
    executable = shutil.which(kubectl)
    if executable is None:
        raise RuntimeError("kubectl is unavailable")
    base = [executable]
    if kubeconfig is not None:
        base.extend(("--kubeconfig", str(kubeconfig)))
    runtime_class = _kubectl_json(
        (*base, "get", "runtimeclass", contract.cluster.runtime_class_name, "-o", "json")
    )
    daemonsets = _kubectl_json(
        (
            *base,
            "-n",
            "kube-system",
            "get",
            "daemonset",
            "-l",
            contract.cluster.plugin_daemonset_label_selector,
            "-o",
            "json",
        )
    )
    nodes = _kubectl_json((*base, "get", "nodes", "-o", "json"))
    return evaluate_ascend_cluster(
        runtime_class=runtime_class,
        daemonsets=daemonsets,
        nodes=nodes,
        contract=contract,
    )


def _kubectl_json(command: tuple[str, ...]) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip().splitlines()
        reason = detail[-1][:512] if detail else "kubectl returned a non-zero exit code"
        raise RuntimeError(reason)
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise ValueError("kubectl response must be a JSON object")
    return payload


def _not_run_diagnostic(
    contract: AscendRuntimeAcceptanceContract,
    device_nodes: AscendDeviceNodeSummary,
    reason: str,
) -> dict[str, object]:
    return {
        "status": "REAL_HW_NOT_RUN",
        "profile_identity": contract.profile_identity,
        "resource_name": contract.resource_name,
        "reason": reason,
        "npu_summary": None,
        "device_nodes": device_nodes.model_dump(mode="json"),
    }


def _json_object(response: httpx.Response) -> dict[str, Any]:
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("engine response must be a JSON object")
    return payload


def _write_result(payload: dict[str, object], output: Path | None) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")


async def _run_acceptance(
    args: argparse.Namespace,
    contract: AscendRuntimeAcceptanceContract,
) -> None:
    api_key = os.getenv(args.api_key_env) if args.api_key_env else None
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    async with httpx.AsyncClient(
        base_url=args.base_url.rstrip("/"),
        headers=headers,
        timeout=httpx.Timeout(args.timeout),
    ) as client:
        result = await accept_openai_engine(client, model=args.model, contract=contract)
    _write_result(result, args.output)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ascend runtime diagnostics and acceptance")
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    diagnose = subparsers.add_parser("diagnose")
    diagnose.add_argument("--dev-root", type=Path, default=Path("/dev"))
    diagnose.add_argument("--npu-smi", default="npu-smi")
    diagnose.add_argument("--output", type=Path)
    diagnose.add_argument("--require-hardware", action="store_true")

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--kubectl", default="kubectl")
    preflight.add_argument("--kubeconfig", type=Path)
    preflight.add_argument("--output", type=Path)

    accept = subparsers.add_parser("accept")
    accept.add_argument("--base-url", required=True)
    accept.add_argument("--model", required=True)
    accept.add_argument("--api-key-env")
    accept.add_argument("--timeout", type=float, default=120.0)
    accept.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    contract = load_ascend_acceptance_contract(args.contract)
    if args.command == "diagnose":
        result = collect_ascend_diagnostic(
            contract=contract,
            npu_smi=args.npu_smi,
            dev_root=args.dev_root,
        )
        _write_result(result, args.output)
        if args.require_hardware and result["status"] != "HARDWARE_OBSERVED":
            raise SystemExit(2)
        return
    if args.command == "preflight":
        _write_result(
            collect_cluster_preflight(
                contract=contract,
                kubectl=args.kubectl,
                kubeconfig=args.kubeconfig,
            ),
            args.output,
        )
        return
    if args.command == "accept":
        asyncio.run(_run_acceptance(args, contract))


if __name__ == "__main__":
    main()
