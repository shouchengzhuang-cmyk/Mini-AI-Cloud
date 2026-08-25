from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import stat
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

MANAGED_LABEL = "mini-ai-cloud/managed"
RESOURCE_KIND_LABEL = "mini-ai-cloud/resource-kind"
SERVICE_ID_LABEL = "mini-ai-cloud/service-id"
REPLICA_ID_LABEL = "mini-ai-cloud/replica-id"
PROJECT_ID_LABEL = "mini-ai-cloud/project-id"
GENERATION_LABEL = "mini-ai-cloud/generation"
EXECUTION_ID_LABEL = "mini-ai-cloud/execution-id"
CLUSTER_ID_LABEL = "mini-ai-cloud/cluster-id"
RUNTIME_LABEL = "mini-ai-cloud/runtime"
POD_RESOURCE_KIND = "serving-pod"
SERVICE_RESOURCE_KIND = "serving-service"


class KindServingE2EError(RuntimeError):
    """One mandatory Kind serving assertion failed."""


class API:
    def __init__(self, base_url: str, *, sensitive_values: Sequence[str]) -> None:
        self.client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(60.0, connect=5.0),
        )
        self.api_key: str | None = None
        self.sensitive_values = tuple(value for value in sensitive_values if value)

    async def close(self) -> None:
        await self.client.aclose()

    def auth_headers(self) -> dict[str, str]:
        if self.api_key is None:
            raise KindServingE2EError("API authentication has not been bootstrapped")
        return {"Authorization": f"Bearer {self.api_key}"}

    async def json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        expected: Sequence[int] = (200,),
    ) -> dict[str, Any]:
        merged = self.auth_headers() if self.api_key is not None else {}
        if headers:
            merged.update(headers)
        try:
            response = await self.client.request(method, path, json=payload, headers=merged)
        except httpx.RequestError as exc:
            raise KindServingE2EError(f"{method} {path} failed: {type(exc).__name__}") from exc
        if response.status_code not in expected:
            body = _redact(response.text[:2000], self.sensitive_values)
            raise KindServingE2EError(
                f"{method} {path} returned {response.status_code}, "
                f"expected {tuple(expected)}: {body}"
            )
        try:
            decoded = response.json()
        except ValueError as exc:
            raise KindServingE2EError(f"{method} {path} returned non-JSON") from exc
        if not isinstance(decoded, dict):
            raise KindServingE2EError(f"{method} {path} returned a non-object JSON value")
        return decoded

    async def chat(self, model: str, prompt: str) -> tuple[dict[str, Any], str]:
        response = await self.client.post(
            "/v1/chat/completions",
            headers=self.auth_headers(),
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            },
        )
        if response.status_code != 200:
            body = _redact(response.text[:2000], self.sensitive_values)
            raise KindServingE2EError(f"chat completion returned {response.status_code}: {body}")
        replica_id = response.headers.get("x-mini-ai-replica-id")
        if not replica_id:
            raise KindServingE2EError("Gateway response omitted x-mini-ai-replica-id")
        payload = response.json()
        if not isinstance(payload, dict):
            raise KindServingE2EError("chat completion returned a non-object JSON value")
        return payload, replica_id


class Kubectl:
    def __init__(self, kubeconfig: Path, namespace: str) -> None:
        self.kubeconfig = kubeconfig
        self.namespace = namespace

    def run(self, *arguments: str, timeout: float = 120.0) -> str:
        command = ["kubectl", "--kubeconfig", str(self.kubeconfig), *arguments]
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise KindServingE2EError(
                f"kubectl {' '.join(arguments[:3])} failed: {type(exc).__name__}"
            ) from exc
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()[:2000]
            raise KindServingE2EError(
                f"kubectl {' '.join(arguments[:3])} failed ({completed.returncode}): {detail}"
            )
        return completed.stdout

    def json(self, *arguments: str) -> dict[str, Any]:
        output = self.run(*arguments, "-o", "json")
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise KindServingE2EError("kubectl returned invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise KindServingE2EError("kubectl returned a non-object JSON value")
        return decoded

    def serving_pods(self, service_id: str) -> list[dict[str, Any]]:
        document = self.json(
            "-n",
            self.namespace,
            "get",
            "pods",
            "-l",
            f"{MANAGED_LABEL}=true,{RESOURCE_KIND_LABEL}={POD_RESOURCE_KIND}",
        )
        return _items_for_service(document, service_id)

    def serving_services(self, service_id: str) -> list[dict[str, Any]]:
        document = self.json(
            "-n",
            self.namespace,
            "get",
            "services",
            "-l",
            f"{MANAGED_LABEL}=true,{RESOURCE_KIND_LABEL}={SERVICE_RESOURCE_KIND}",
        )
        return _items_for_service(document, service_id)

    def delete_pod(self, name: str) -> None:
        self.run("-n", self.namespace, "delete", "pod", name, "--wait=true", timeout=90)

    def restart_controller(self) -> None:
        self.run(
            "-n",
            self.namespace,
            "rollout",
            "restart",
            "deployment/mini-ai-cloud-api",
        )
        self.run(
            "-n",
            self.namespace,
            "rollout",
            "status",
            "deployment/mini-ai-cloud-api",
            "--timeout=180s",
            timeout=190,
        )


async def wait_ready(base_url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    async with httpx.AsyncClient(base_url=base_url, trust_env=False, timeout=3.0) as client:
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                response = await client.get("/readyz")
                if response.status_code == 200:
                    return
                last_error = f"HTTP {response.status_code}"
            except httpx.RequestError as exc:
                last_error = type(exc).__name__
            await asyncio.sleep(1)
    raise KindServingE2EError(f"API did not become ready within {timeout_seconds}s ({last_error})")


async def run_e2e_from_environment() -> None:
    base_url = _required_env("KIND_SERVING_BASE_URL")
    kubeconfig = _resolved_path(_required_env("KIND_SERVING_KUBECONFIG"))
    namespace = _required_env("KIND_SERVING_NAMESPACE")
    image = _required_env("KIND_SERVING_APP_IMAGE")
    bootstrap_token = _required_env("KIND_SERVING_BOOTSTRAP_TOKEN")
    user_password = _required_env("KIND_SERVING_USER_PASSWORD")
    api_key_file = _resolved_path(_required_env("KIND_SERVING_API_KEY_FILE"))
    if not _is_file(kubeconfig):
        raise KindServingE2EError(f"isolated kubeconfig does not exist: {kubeconfig}")
    if image.endswith(":latest"):
        raise KindServingE2EError("Kind serving E2E refuses a latest-tagged image")

    await wait_ready(base_url, 60)
    api = API(base_url, sensitive_values=(bootstrap_token, user_password))
    kube = Kubectl(kubeconfig, namespace)
    try:
        project_id = await _authenticate(
            api,
            bootstrap_token=bootstrap_token,
            user_password=user_password,
            api_key_file=api_key_file,
        )
        await _configure_kind_image_policy(api, project_id)
        await _run_serving_scenario(api, kube, project_id=project_id, image=image)
    finally:
        await api.close()


async def _authenticate(
    api: API,
    *,
    bootstrap_token: str,
    user_password: str,
    api_key_file: Path,
) -> str:
    stored_api_key = _read_optional_text(api_key_file)
    if stored_api_key is not None:
        api_key = stored_api_key.strip()
        if not api_key:
            raise KindServingE2EError("stored Kind API key file is empty")
    else:
        response = await api.json(
            "POST",
            "/api/v1/bootstrap",
            payload={
                "user": {
                    "username": "kind-serving-owner",
                    "email": "kind-serving@example.invalid",
                    "password": user_password,
                },
                "project": {
                    "name": "Kind Serving E2E",
                    "slug": "kind-serving-e2e",
                },
                "api_key_name": "kind-serving-e2e",
            },
            headers={"X-Bootstrap-Token": bootstrap_token},
            expected=(201,),
        )
        api_key_data = response.get("api_key")
        raw_api_key = api_key_data.get("api_key") if isinstance(api_key_data, dict) else None
        if not isinstance(raw_api_key, str) or not raw_api_key:
            raise KindServingE2EError("bootstrap response omitted the one-time API key")
        api_key = raw_api_key
        _write_private_text(api_key_file, api_key)

    api.api_key = api_key
    api.sensitive_values = (*api.sensitive_values, api_key)
    principal = await api.json("GET", "/api/v1/auth/whoami")
    project_id = principal.get("project_id")
    if not isinstance(project_id, str):
        raise KindServingE2EError("authenticated principal has no project_id")
    print("PASS: authenticated a project-scoped Kind test identity")
    return project_id


async def _configure_kind_image_policy(api: API, project_id: str) -> None:
    await api.json(
        "PUT",
        f"/api/v1/projects/{project_id}/image-policy",
        payload={
            "default_action": "deny",
            "require_digest": False,
            "rules": [
                {
                    "action": "allow",
                    "registry": "docker.io",
                    "repository_glob": "library/mini-ai-cloud",
                    "tag_glob": "kind-serving-v4a",
                    "priority": 10,
                },
                {
                    "action": "allow",
                    "registry": "invalid.local",
                    "repository_glob": "mini-ai-cloud/missing",
                    "tag_glob": "kind-serving-v4a",
                    "priority": 20,
                },
            ],
        },
    )
    print("PASS: configured a project-scoped allowlist for the two fixed E2E images")


async def _run_serving_scenario(
    api: API,
    kube: Kubectl,
    *,
    project_id: str,
    image: str,
) -> None:
    suffix = secrets.token_hex(4)
    service_ids: list[str] = []
    completed = False
    model = await api.json(
        "POST",
        f"/api/v1/projects/{project_id}/models",
        payload={
            "name": f"kind-fake-{suffix}",
            "provider": "mini-ai-cloud",
            "source": f"fake/kind-{suffix}",
            "revision": "phase4a",
            "runtime": "fake",
            "default_gpu_count": 0,
            "metadata": {"purpose": "kind-serving-e2e"},
        },
        expected=(201,),
    )
    model_id = _required_string(model, "id")
    service_name = f"kind-chat-{suffix}"
    try:
        service = await api.json(
            "POST",
            "/api/v1/services",
            payload={
                "name": service_name,
                "registered_model_id": model_id,
                "runtime": "fake",
                "runtime_type": "kubernetes",
                "image": image,
                "cpu_millicores": 100,
                "memory_mb": 128,
                "gpu_count": 0,
                "tensor_parallel_size": 1,
                "replicas": 2,
            },
            expected=(201,),
        )
        service_id = _required_string(service, "id")
        service_ids.append(service_id)

        _, replicas, saw_loading = await _wait_service_ready(api, service_id, 2, timeout_seconds=90)
        if not saw_loading:
            raise KindServingE2EError(
                "no Replica was observed in starting/loading before readiness"
            )
        pods = await _wait_pods(kube, service_id, expected=2, ready=True, timeout_seconds=60)
        _assert_pod_contract(
            pods,
            project_id=project_id,
            service_id=service_id,
            image=image,
        )
        if len(kube.serving_services(service_id)) != 2:
            raise KindServingE2EError("expected one Kubernetes Service per ready Replica")
        print("PASS: two real Kubernetes serving Pods reached ready and healthy")

        models = await api.json("GET", "/v1/models")
        model_rows = models.get("data")
        if not isinstance(model_rows, list) or service_name not in {
            row.get("id") for row in model_rows if isinstance(row, dict)
        }:
            raise KindServingE2EError("/v1/models omitted the Kubernetes-backed service")
        initial_ready_ids = {_required_string(row, "id") for row in _ready_rows(replicas)}
        first_body, first_replica = await api.chat(service_name, "first Kind request")
        second_body, second_replica = await api.chat(service_name, "second Kind request")
        if {first_replica, second_replica} != initial_ready_ids:
            raise KindServingE2EError(
                "round-robin did not route exactly once to each ready Replica"
            )
        _assert_chat_body(first_body)
        _assert_chat_body(second_body)
        await _assert_sse(api, service_name)
        print("PASS: Gateway completed /v1/models, JSON, SSE, and round-robin inference")

        old_ready_ids = initial_ready_ids
        victim = pods[0]
        victim_name = _metadata_name(victim)
        victim_labels = _labels(victim)
        victim_replica_id = _required_label(victim_labels, REPLICA_ID_LABEL)
        victim_execution_id = _required_label(victim_labels, EXECUTION_ID_LABEL)
        kube.delete_pod(victim_name)
        replacements = await _wait_replacement_ready(
            api,
            service_id,
            victim_replica_id=victim_replica_id,
            previous_ready_ids=old_ready_ids,
            expected=2,
            timeout_seconds=90,
        )
        replacement_pods = await _wait_pods(
            kube, service_id, expected=2, ready=True, timeout_seconds=60
        )
        if victim_name in {_metadata_name(item) for item in replacement_pods}:
            raise KindServingE2EError("deleted serving Pod was not replaced")
        new_ready_ids = {_required_string(row, "id") for row in _ready_rows(replacements)}
        if new_ready_ids == old_ready_ids or victim_replica_id in new_ready_ids:
            raise KindServingE2EError("Pod deletion did not produce a fenced replacement Replica")
        victim_record = next(
            (row for row in replacements if row.get("id") == victim_replica_id),
            None,
        )
        if (
            not isinstance(victim_record, dict)
            or victim_record.get("execution_id") != victim_execution_id
        ):
            raise KindServingE2EError(
                "old execution identity was not preserved for fencing evidence"
            )
        if victim_record.get("status") not in {"failed", "lost", "stopped"}:
            raise KindServingE2EError("deleted Pod's old Replica did not become terminal")
        routed_after_replacement = {
            (await api.chat(service_name, f"replacement route {index}"))[1] for index in range(2)
        }
        if routed_after_replacement != new_ready_ids:
            raise KindServingE2EError("Gateway did not converge onto the replacement Replica set")
        print("PASS: manual Pod deletion was fenced, replaced, and routed through Gateway")

        await _scale(api, service_id, 4)
        _, scaled, _ = await _wait_service_ready(api, service_id, 4, timeout_seconds=90)
        scaled_pods = await _wait_pods(kube, service_id, expected=4, ready=True, timeout_seconds=60)
        if len(kube.serving_services(service_id)) != 4:
            raise KindServingE2EError("scale-up did not converge to four per-Replica Services")
        print("PASS: Kubernetes serving scaled from 2 to 4 ready Replicas")

        await _assert_scale_down_drain(
            api,
            kube,
            service_name=service_name,
            service_id=service_id,
            replicas=scaled,
            pods=scaled_pods,
        )
        print("PASS: scale 4 to 1 drained an active SSE request before Pod deletion")

        _, before_restart, _ = await _wait_service_ready(api, service_id, 1, timeout_seconds=60)
        before_pods = await _wait_pods(kube, service_id, expected=1, ready=True, timeout_seconds=30)
        before_identity = (
            _metadata_name(before_pods[0]),
            _required_label(_labels(before_pods[0]), EXECUTION_ID_LABEL),
            _required_string(_ready_rows(before_restart)[0], "id"),
        )
        kube.restart_controller()
        await wait_ready(str(api.client.base_url), 90)
        _, after_restart, _ = await _wait_service_ready(api, service_id, 1, timeout_seconds=90)
        after_pods = await _wait_pods(kube, service_id, expected=1, ready=True, timeout_seconds=60)
        after_identity = (
            _metadata_name(after_pods[0]),
            _required_label(_labels(after_pods[0]), EXECUTION_ID_LABEL),
            _required_string(_ready_rows(after_restart)[0], "id"),
        )
        if before_identity != after_identity:
            raise KindServingE2EError(
                "controller restart replaced or duplicated a healthy execution"
            )
        if len(kube.serving_services(service_id)) != 1:
            raise KindServingE2EError("controller restart left duplicate per-Replica Services")
        print("PASS: controller rollout restart adopted the existing healthy Replica")

        bad_service_id = await _assert_bad_image_backoff(
            api,
            kube,
            model_id=model_id,
            suffix=suffix,
            created_service_ids=service_ids,
        )
        if bad_service_id not in service_ids:  # Defensive: helper records before assertions.
            service_ids.append(bad_service_id)
        print("PASS: loading failure persisted a bounded error and respected replacement backoff")
        completed = True
    finally:
        cleanup_errors = await _cleanup_services(api, kube, service_ids)
        if cleanup_errors:
            detail = "; ".join(cleanup_errors)
            if completed:
                raise KindServingE2EError(f"Kind serving cleanup failed: {detail}")
            print(
                f"WARN: best-effort Kind serving cleanup was incomplete: {detail}", file=sys.stderr
            )


async def _wait_service_ready(
    api: API,
    service_id: str,
    expected: int,
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
    deadline = time.monotonic() + timeout_seconds
    last_summary = "unavailable"
    saw_loading = False
    while time.monotonic() < deadline:
        service = await api.json("GET", f"/api/v1/services/{service_id}")
        replica_document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        replicas = _object_items(replica_document)
        saw_loading = saw_loading or any(
            row.get("status") in {"starting", "loading"} for row in replicas
        )
        ready = _ready_rows(replicas)
        last_summary = (
            f"status={service.get('status')}, desired={service.get('desired_replicas')}, "
            f"healthy={service.get('healthy_replicas')}, ready_rows={len(ready)}"
        )
        if (
            service.get("desired_replicas") == expected
            and service.get("healthy_replicas") == expected
            and len(ready) == expected
        ):
            return service, replicas, saw_loading
        await asyncio.sleep(0.25)
    raise KindServingE2EError(
        f"service {service_id} did not reach {expected} ready Replicas ({last_summary})"
    )


async def _wait_pods(
    kube: Kubectl,
    service_id: str,
    *,
    expected: int,
    ready: bool,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    last: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last = kube.serving_pods(service_id)
        if len(last) == expected and (not ready or all(_pod_ready(item) for item in last)):
            return last
        await asyncio.sleep(0.5)
    states = [
        (_metadata_name(item), _nested(item, "status", "phase"), _pod_ready(item)) for item in last
    ]
    raise KindServingE2EError(
        f"expected {expected} serving Pods (ready={ready}); observed {states}"
    )


async def _wait_replacement_ready(
    api: API,
    service_id: str,
    *,
    victim_replica_id: str,
    previous_ready_ids: set[str],
    expected: int,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    """Wait past the transient DB snapshot that still reports the deleted Pod ready."""

    deadline = time.monotonic() + timeout_seconds
    last_summary = "unavailable"
    while time.monotonic() < deadline:
        service = await api.json("GET", f"/api/v1/services/{service_id}")
        replica_document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        replicas = _object_items(replica_document)
        ready_ids = {_required_string(row, "id") for row in _ready_rows(replicas)}
        last_summary = (
            f"healthy={service.get('healthy_replicas')}, ready_ids={sorted(ready_ids)}, "
            f"victim={victim_replica_id}"
        )
        if (
            service.get("desired_replicas") == expected
            and service.get("healthy_replicas") == expected
            and len(ready_ids) == expected
            and victim_replica_id not in ready_ids
            and ready_ids != previous_ready_ids
        ):
            return replicas
        await asyncio.sleep(0.25)
    raise KindServingE2EError(
        f"deleted Pod did not converge to a replacement Replica ({last_summary})"
    )


async def _scale(api: API, service_id: str, replicas: int) -> None:
    await api.json(
        "POST",
        f"/api/v1/services/{service_id}/scale",
        payload={"replicas": replicas},
    )


async def _assert_sse(api: API, model: str) -> None:
    chunks: list[bytes] = []
    async with api.client.stream(
        "POST",
        "/v1/chat/completions",
        headers=api.auth_headers(),
        json={
            "model": model,
            "messages": [{"role": "user", "content": "stream from a real Kind Pod"}],
            "stream": True,
        },
    ) as response:
        if response.status_code != 200:
            raise KindServingE2EError(f"SSE completion returned {response.status_code}")
        if not response.headers.get("content-type", "").startswith("text/event-stream"):
            raise KindServingE2EError("SSE completion returned the wrong content type")
        async for chunk in response.aiter_bytes():
            if chunk:
                chunks.append(chunk)
    body = b"".join(chunks)
    if len(chunks) < 2 or body.count(b"data:") < 3 or not body.endswith(b"data: [DONE]\n\n"):
        raise KindServingE2EError("Gateway did not preserve the multi-event SSE stream")


async def _assert_scale_down_drain(
    api: API,
    kube: Kubectl,
    *,
    service_name: str,
    service_id: str,
    replicas: list[dict[str, Any]],
    pods: list[dict[str, Any]],
) -> None:
    ready = sorted(_ready_rows(replicas), key=lambda row: int(row.get("ordinal", -1)))
    if len(ready) != 4:
        raise KindServingE2EError("drain test requires four ready Replica records")
    expected_survivor_id = _required_string(ready[0], "id")
    target_id = _required_string(ready[-1], "id")
    sequence: list[str] = []
    for index in range(4):
        _, selected = await api.chat(service_name, f"round-robin map {index}")
        sequence.append(selected)
    if len(set(sequence)) != 4 or target_id not in sequence:
        raise KindServingE2EError("could not establish a four-Replica round-robin sequence")
    while sequence[0] != target_id:
        await api.chat(service_name, "advance round-robin cursor")
        sequence = sequence[1:] + sequence[:1]

    context: Any = api.client.stream(
        "POST",
        "/v1/chat/completions",
        headers=api.auth_headers(),
        json={
            "model": service_name,
            "messages": [{"role": "user", "content": "drain-stream-" + "x" * 1024}],
            "stream": True,
        },
    )
    response: httpx.Response = await context.__aenter__()
    iterator: AsyncIterator[bytes] = response.aiter_bytes()
    chunks: list[bytes] = []
    try:
        if response.status_code != 200:
            raise KindServingE2EError(f"drain SSE returned {response.status_code}")
        selected = response.headers.get("x-mini-ai-replica-id")
        if selected != target_id:
            raise KindServingE2EError(
                f"drain stream selected {selected!r}, expected highest ordinal {target_id!r}"
            )
        try:
            chunks.append(await anext(iterator))
        except StopAsyncIteration as exc:
            raise KindServingE2EError("drain SSE ended before scale-down") from exc

        await _scale(api, service_id, 1)
        await _wait_replica_status(api, service_id, target_id, "draining", timeout_seconds=5)
        target_pod = next(
            (item for item in pods if _labels(item).get(REPLICA_ID_LABEL) == target_id),
            None,
        )
        if target_pod is None:
            raise KindServingE2EError("could not map active stream Replica to its Pod")
        current_names = {_metadata_name(item) for item in kube.serving_pods(service_id)}
        if _metadata_name(target_pod) not in current_names:
            raise KindServingE2EError("active draining Replica Pod was deleted immediately")

        routed_while_draining = {
            (await api.chat(service_name, f"request while draining {index}"))[1]
            for index in range(3)
        }
        if target_id in routed_while_draining or len(routed_while_draining) != 1:
            raise KindServingE2EError("Gateway routed new traffic to a draining Replica")

        async for chunk in iterator:
            if chunk:
                chunks.append(chunk)
    finally:
        await context.__aexit__(None, None, None)
    body = b"".join(chunks)
    if not body.endswith(b"data: [DONE]\n\n"):
        raise KindServingE2EError("active SSE request did not complete during normal drain")
    _, final_replicas, _ = await _wait_service_ready(api, service_id, 1, timeout_seconds=60)
    final_pods = await _wait_pods(kube, service_id, expected=1, ready=True, timeout_seconds=60)
    final_ready_ids = {_required_string(row, "id") for row in _ready_rows(final_replicas)}
    final_pod_replica_id = _required_label(_labels(final_pods[0]), REPLICA_ID_LABEL)
    if final_ready_ids != {expected_survivor_id} or final_pod_replica_id != expected_survivor_id:
        raise KindServingE2EError("scale-down did not retain the expected healthy Replica")
    if len(kube.serving_services(service_id)) != 1:
        raise KindServingE2EError("scale-down left duplicate per-Replica Services")


async def _wait_replica_status(
    api: API,
    service_id: str,
    replica_id: str,
    expected: str,
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last: object = None
    while time.monotonic() < deadline:
        document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        row = next(
            (item for item in _object_items(document) if item.get("id") == replica_id),
            None,
        )
        last = row.get("status") if isinstance(row, dict) else None
        if last == expected:
            return
        await asyncio.sleep(0.1)
    raise KindServingE2EError(
        f"Replica {replica_id} did not reach {expected}; last status was {last!r}"
    )


async def _assert_bad_image_backoff(
    api: API,
    kube: Kubectl,
    *,
    model_id: str,
    suffix: str,
    created_service_ids: list[str],
) -> str:
    service = await api.json(
        "POST",
        "/api/v1/services",
        payload={
            "name": f"kind-bad-image-{suffix}",
            "registered_model_id": model_id,
            "runtime": "fake",
            "runtime_type": "kubernetes",
            "image": "invalid.local/mini-ai-cloud/missing:kind-serving-v4a",
            "cpu_millicores": 50,
            "memory_mb": 64,
            "gpu_count": 0,
            "tensor_parallel_size": 1,
            "replicas": 1,
        },
        expected=(201,),
    )
    service_id = _required_string(service, "id")
    created_service_ids.append(service_id)
    deadline = time.monotonic() + 90
    failed: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        all_rows = _object_items(document)
        failed = [row for row in all_rows if row.get("status") == "failed"]
        if failed:
            break
        await asyncio.sleep(0.5)
    if not failed:
        raise KindServingE2EError("bad-image Replica did not enter failed")
    for row in failed:
        code = row.get("error_code")
        message = row.get("error_message")
        if code != "IMAGE_PULL_FAILED":
            raise KindServingE2EError(f"bad-image Replica persisted unexpected code {code!r}")
        if not isinstance(message, str) or not message or len(message) > 4096:
            raise KindServingE2EError("bad-image Replica error message is missing or unbounded")

    service_state = await api.json("GET", f"/api/v1/services/{service_id}")
    if service_state.get("scheduling_reason") != "KUBERNETES_SERVING_BACKOFF":
        raise KindServingE2EError("bad-image failure did not persist a scheduling backoff")
    scheduling_details = service_state.get("scheduling_details")
    retry_not_before = (
        scheduling_details.get("retry_not_before") if isinstance(scheduling_details, dict) else None
    )
    try:
        retry_at = (
            datetime.fromisoformat(retry_not_before) if isinstance(retry_not_before, str) else None
        )
    except ValueError as exc:
        raise KindServingE2EError("bad-image backoff has an invalid retry timestamp") from exc
    if retry_at is not None and retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    if retry_at is None or retry_at.astimezone(UTC) <= now:
        raise KindServingE2EError("bad-image backoff was not future-bounded when observed")
    retry_at = retry_at.astimezone(UTC)
    if retry_at > now + timedelta(seconds=10):
        raise KindServingE2EError("bad-image backoff exceeded the bounded E2E retry window")

    failed_ids = {_required_string(row, "id") for row in failed}
    initial_execution_ids = {
        execution_id
        for row in all_rows
        if isinstance((execution_id := row.get("execution_id")), str)
    }
    while (retry_at - datetime.now(UTC)).total_seconds() > 0.1:
        pre_retry_document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        pre_retry_rows = _object_items(pre_retry_document)
        observed_execution_ids = {
            execution_id
            for row in pre_retry_rows
            if isinstance((execution_id := row.get("execution_id")), str)
        }
        if not observed_execution_ids.issubset(initial_execution_ids):
            raise KindServingE2EError("bad-image replacement launched before retry_not_before")
        await asyncio.sleep(0.1)

    count_before = len(all_rows)
    replacement_deadline = time.monotonic() + 15
    replacement_seen = False
    later_rows = all_rows
    while time.monotonic() < replacement_deadline:
        later_document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
        later_rows = _object_items(later_document)
        replacement_seen = any(
            row.get("id") not in failed_ids and isinstance(row.get("execution_id"), str)
            for row in later_rows
        )
        if replacement_seen:
            break
        await asyncio.sleep(0.25)
    if not replacement_seen:
        raise KindServingE2EError("bad-image failure did not launch a replacement after backoff")

    await asyncio.sleep(7)
    later_document = await api.json("GET", f"/api/v1/services/{service_id}/replicas")
    later_rows = _object_items(later_document)
    if len(later_rows) > count_before + 2 or len(later_rows) > 5:
        raise KindServingE2EError(
            f"bad image caused an unbounded replacement loop ({count_before} -> {len(later_rows)})"
        )
    active_pods = kube.serving_pods(service_id)
    if len(active_pods) > 1:
        raise KindServingE2EError("bad image backoff left multiple active Pods")
    return service_id


async def _cleanup_services(api: API, kube: Kubectl, service_ids: Sequence[str]) -> list[str]:
    errors: list[str] = []
    for service_id in reversed(service_ids):
        try:
            await api.json("POST", f"/api/v1/services/{service_id}/stop")
        except KindServingE2EError as exc:
            errors.append(f"stop {service_id}: {exc}")
            continue
        try:
            await _wait_kubernetes_resources_removed(
                kube,
                service_id,
                timeout_seconds=60,
            )
        except KindServingE2EError as exc:
            errors.append(f"resources {service_id}: {exc}")
    if not errors and service_ids:
        print("PASS: stopped test services and removed their Kubernetes resources")
    return errors


async def _wait_kubernetes_resources_removed(
    kube: Kubectl,
    service_id: str,
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        pods = kube.serving_pods(service_id)
        services = kube.serving_services(service_id)
        if not pods and not services:
            return
        await asyncio.sleep(0.5)
    raise KindServingE2EError(f"service {service_id} retained serving Pods or Services after stop")


def _assert_pod_contract(
    pods: Sequence[dict[str, Any]],
    *,
    project_id: str,
    service_id: str,
    image: str,
) -> None:
    required_labels = {
        MANAGED_LABEL,
        RESOURCE_KIND_LABEL,
        SERVICE_ID_LABEL,
        REPLICA_ID_LABEL,
        PROJECT_ID_LABEL,
        GENERATION_LABEL,
        EXECUTION_ID_LABEL,
        CLUSTER_ID_LABEL,
        RUNTIME_LABEL,
    }
    names: set[str] = set()
    for pod in pods:
        metadata = pod.get("metadata")
        spec = pod.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise KindServingE2EError("serving Pod omitted metadata or spec")
        name = _metadata_name(pod)
        if len(name) > 63 or name.lower() != name:
            raise KindServingE2EError(f"serving Pod name is not a bounded DNS label: {name!r}")
        names.add(name)
        labels = _labels(pod)
        if not required_labels <= labels.keys():
            missing = sorted(required_labels - labels.keys())
            raise KindServingE2EError(f"serving Pod omitted stable labels: {missing}")
        if (
            labels[MANAGED_LABEL] != "true"
            or labels[RESOURCE_KIND_LABEL] != POD_RESOURCE_KIND
            or labels[SERVICE_ID_LABEL] != service_id
            or labels[PROJECT_ID_LABEL] != project_id
            or labels[RUNTIME_LABEL] != "kubernetes-serving"
        ):
            raise KindServingE2EError("serving Pod labels do not match persisted ownership")
        if spec.get("automountServiceAccountToken") is not False:
            raise KindServingE2EError("inference Pod automounts a service account token")
        if any(spec.get(key) is True for key in ("hostNetwork", "hostPID", "hostIPC")):
            raise KindServingE2EError("inference Pod enables a host namespace")
        pod_security = spec.get("securityContext")
        if not isinstance(pod_security, dict):
            raise KindServingE2EError("inference Pod omitted pod securityContext")
        if pod_security.get("runAsNonRoot") is not True:
            raise KindServingE2EError("inference Pod does not require a non-root user")
        seccomp = pod_security.get("seccompProfile")
        if not isinstance(seccomp, dict) or seccomp.get("type") != "RuntimeDefault":
            raise KindServingE2EError("inference Pod does not use RuntimeDefault seccomp")
        containers = spec.get("containers")
        if not isinstance(containers, list) or len(containers) != 1:
            raise KindServingE2EError("inference Pod must contain exactly one serving container")
        container = containers[0]
        if not isinstance(container, dict):
            raise KindServingE2EError("inference Pod container spec is invalid")
        accepted_images = {image, f"docker.io/library/{image}"}
        if container.get("image") not in accepted_images or str(container.get("image")).endswith(
            ":latest"
        ):
            raise KindServingE2EError("inference Pod did not use the fixed Kind application image")
        security = container.get("securityContext")
        if not isinstance(security, dict):
            raise KindServingE2EError("inference container omitted securityContext")
        capabilities = security.get("capabilities")
        dropped = capabilities.get("drop") if isinstance(capabilities, dict) else None
        if (
            security.get("allowPrivilegeEscalation") is not False
            or security.get("privileged") is not False
            or security.get("readOnlyRootFilesystem") is not True
            or security.get("runAsNonRoot") is not True
            or dropped != ["ALL"]
        ):
            raise KindServingE2EError("inference container security boundary is incomplete")
        resources = container.get("resources")
        if not isinstance(resources, dict):
            raise KindServingE2EError("inference container omitted resources")
        requests = resources.get("requests")
        limits = resources.get("limits")
        if not isinstance(requests, dict) or not isinstance(limits, dict):
            raise KindServingE2EError("inference container omitted requests or limits")
        if not {"cpu", "memory"} <= requests.keys() or not {"cpu", "memory"} <= limits.keys():
            raise KindServingE2EError("inference container CPU/memory bounds are incomplete")
        if requests != limits:
            raise KindServingE2EError(
                "inference container requests and limits are not equally bounded"
            )
        termination_grace = spec.get("terminationGracePeriodSeconds")
        if not isinstance(termination_grace, int) or not 1 <= termination_grace <= 300:
            raise KindServingE2EError("inference Pod termination grace is missing or unbounded")
        volumes = spec.get("volumes", [])
        if not isinstance(volumes, list) or any(
            not isinstance(volume, dict)
            or "emptyDir" not in volume
            or any(
                key in volume
                for key in (
                    "hostPath",
                    "secret",
                    "projected",
                    "configMap",
                    "persistentVolumeClaim",
                )
            )
            for volume in volumes
        ):
            raise KindServingE2EError("inference Pod uses a credential or persistent host volume")
    if len(names) != len(pods):
        raise KindServingE2EError("serving Pod names collided")


def _assert_chat_body(payload: dict[str, Any]) -> None:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise KindServingE2EError("chat response omitted choices")
    first = choices[0]
    message = first.get("message") if isinstance(first, dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.startswith("fake response:"):
        raise KindServingE2EError("chat response did not come from Fake inference")


def generate_credentials(output: Path) -> None:
    if output.exists():
        raise KindServingE2EError(f"refusing to overwrite existing credentials: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    values = {
        "KIND_SERVING_POSTGRES_PASSWORD": secrets.token_hex(24),
        "KIND_SERVING_BOOTSTRAP_TOKEN": secrets.token_urlsafe(36),
        "KIND_SERVING_API_KEY_PEPPER": secrets.token_urlsafe(48),
        "KIND_SERVING_WORKER_AUTH_TOKEN": secrets.token_urlsafe(48),
        "KIND_SERVING_USER_PASSWORD": secrets.token_urlsafe(32),
    }
    content = "".join(f"{key}={value}\n" for key, value in values.items())
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(content)


def _resolved_path(value: str) -> Path:
    return Path(value).resolve()


def _is_file(path: Path) -> bool:
    return path.is_file()


def _read_optional_text(path: Path) -> str | None:
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        stat.S_IRUSR | stat.S_IWUSR,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value)


def _items_for_service(document: dict[str, Any], service_id: str) -> list[dict[str, Any]]:
    items = document.get("items")
    if not isinstance(items, list):
        raise KindServingE2EError("Kubernetes list response omitted items")
    result: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict) and _labels(item).get(SERVICE_ID_LABEL) == service_id:
            result.append(item)
    return result


def _object_items(document: dict[str, Any]) -> list[dict[str, Any]]:
    raw = document.get("items")
    if not isinstance(raw, list):
        raise KindServingE2EError("API list response omitted items")
    return [item for item in raw if isinstance(item, dict)]


def _ready_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("status") == "running"
        and row.get("health") == "healthy"
        and isinstance(row.get("endpoint_url"), str)
    ]


def _pod_ready(pod: dict[str, Any]) -> bool:
    conditions = _nested(pod, "status", "conditions")
    return isinstance(conditions, list) and any(
        isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
        for item in conditions
    )


def _metadata_name(resource: dict[str, Any]) -> str:
    name = _nested(resource, "metadata", "name")
    if not isinstance(name, str) or not name:
        raise KindServingE2EError("Kubernetes resource omitted metadata.name")
    return name


def _labels(resource: dict[str, Any]) -> dict[str, str]:
    labels = _nested(resource, "metadata", "labels")
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


def _required_label(labels: dict[str, str], key: str) -> str:
    try:
        value = labels[key]
    except KeyError as exc:
        raise KindServingE2EError(f"Kubernetes resource omitted label {key}") from exc
    if not value:
        raise KindServingE2EError(f"Kubernetes resource has blank label {key}")
    return value


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise KindServingE2EError(f"response omitted string field {key}")
    return value


def _nested(payload: dict[str, Any], *keys: str) -> object:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise KindServingE2EError(f"NOT RUN: required environment variable {name} is missing")
    return value


def _redact(value: str, sensitive_values: Sequence[str]) -> str:
    redacted = value
    for sensitive in sensitive_values:
        if sensitive:
            redacted = redacted.replace(sensitive, "[REDACTED]")
    return redacted


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase IV-A Kind serving E2E helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    credentials = subparsers.add_parser("generate-credentials")
    credentials.add_argument("--output", type=Path, required=True)
    ready = subparsers.add_parser("wait-ready")
    ready.add_argument("--base-url", required=True)
    ready.add_argument("--timeout", type=float, default=120.0)
    subparsers.add_parser("run")
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        if args.command == "generate-credentials":
            generate_credentials(args.output)
        elif args.command == "wait-ready":
            asyncio.run(wait_ready(args.base_url, args.timeout))
        elif args.command == "run":
            asyncio.run(run_e2e_from_environment())
        else:  # pragma: no cover - argparse enforces the command choices.
            raise AssertionError(args.command)
    except KindServingE2EError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
