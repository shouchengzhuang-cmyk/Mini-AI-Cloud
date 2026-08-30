from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from core.image_policy import ImageReferenceError, canonicalize_image_reference
from scripts.kind_serving_e2e import (
    API,
    KindServingE2EError,
    wait_ready,
)

MANAGED_LABEL = "mini-ai-cloud/managed"
RESOURCE_KIND_LABEL = "mini-ai-cloud/resource-kind"
TASK_ID_LABEL = "mini-ai-cloud/task-id"
PROJECT_ID_LABEL = "mini-ai-cloud/project-id"
EXECUTION_ID_LABEL = "mini-ai-cloud/execution-id"
WORKER_ID_LABEL = "mini-ai-cloud/worker-id"
WORKER_SESSION_ID_LABEL = "mini-ai-cloud/worker-session-id"
CLUSTER_ID_LABEL = "mini-ai-cloud/cluster-id"
SPEC_HASH_LABEL = "mini-ai-cloud/spec-hash"
RUNTIME_PROFILE_DIGEST_LABEL = "mini-ai-cloud/runtime-profile-digest"
CONTROLLER_SESSION_ANNOTATION = "mini-ai-cloud/controller-session-id"
BATCH_JOB_RESOURCE_KIND = "batch-job"
NETWORK_POLICY_RESOURCE_KIND = "task-deny-all"
TASK_CONTAINER_NAME = "task"

FINAL_STATUSES = frozenset({"succeeded", "failed", "cancelled", "timed_out", "preempted"})
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")


class KindBatchE2EError(RuntimeError):
    """One mandatory real Kind batch assertion failed."""


class JSONAPI(Protocol):
    async def json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        expected: Sequence[int] = (200,),
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class Credentials:
    bootstrap_token: str
    user_password: str

    @property
    def sensitive_values(self) -> tuple[str, str]:
        return self.bootstrap_token, self.user_password


class Kubectl:
    def __init__(
        self,
        kubeconfig: Path,
        *,
        workload_namespace: str,
        system_namespace: str,
        worker_deployment: str,
        sensitive_values: Sequence[str],
    ) -> None:
        self.kubeconfig = kubeconfig
        self.workload_namespace = workload_namespace
        self.system_namespace = system_namespace
        self.worker_deployment = worker_deployment
        self.sensitive_values = tuple(value for value in sensitive_values if value)

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
            raise KindBatchE2EError(
                f"kubectl {' '.join(arguments[:4])} failed: {type(exc).__name__}"
            ) from exc
        if completed.returncode != 0:
            detail = _redact(
                (completed.stderr or completed.stdout).strip()[:2000],
                self.sensitive_values,
            )
            raise KindBatchE2EError(
                f"kubectl {' '.join(arguments[:4])} failed ({completed.returncode}): {detail}"
            )
        return completed.stdout

    def json(self, *arguments: str) -> dict[str, Any]:
        output = self.run(*arguments, "-o", "json")
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise KindBatchE2EError("kubectl returned invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise KindBatchE2EError("kubectl returned a non-object JSON value")
        return decoded

    def jobs_for_task(self, task_id: str) -> list[dict[str, Any]]:
        document = self.json(
            "-n",
            self.workload_namespace,
            "get",
            "jobs",
            "-l",
            f"{MANAGED_LABEL}=true,{RESOURCE_KIND_LABEL}={BATCH_JOB_RESOURCE_KIND},"
            f"{TASK_ID_LABEL}={task_id}",
        )
        return _resource_items(document, "Job")

    def pods_for_task(self, task_id: str) -> list[dict[str, Any]]:
        document = self.json(
            "-n",
            self.workload_namespace,
            "get",
            "pods",
            "-l",
            f"{MANAGED_LABEL}=true,{RESOURCE_KIND_LABEL}={BATCH_JOB_RESOURCE_KIND},"
            f"{TASK_ID_LABEL}={task_id}",
        )
        return _resource_items(document, "Pod")

    def network_policies_for_task(self, task_id: str) -> list[dict[str, Any]]:
        document = self.json(
            "-n",
            self.workload_namespace,
            "get",
            "networkpolicies",
            "-l",
            f"{MANAGED_LABEL}=true,{RESOURCE_KIND_LABEL}={NETWORK_POLICY_RESOURCE_KIND},"
            f"{TASK_ID_LABEL}={task_id}",
        )
        return _resource_items(document, "NetworkPolicy")

    def job(self, name: str) -> dict[str, Any] | None:
        output = self.run(
            "-n",
            self.workload_namespace,
            "get",
            "job",
            name,
            "--ignore-not-found",
            "-o",
            "json",
        )
        if not output.strip():
            return None
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise KindBatchE2EError("kubectl returned invalid Job JSON") from exc
        if not isinstance(decoded, dict):
            raise KindBatchE2EError("kubectl returned a non-object Job")
        return decoded

    def restart_worker(self) -> None:
        target = f"deployment/{self.worker_deployment}"
        self.run(
            "-n",
            self.system_namespace,
            "rollout",
            "restart",
            target,
        )
        self.run(
            "-n",
            self.system_namespace,
            "rollout",
            "status",
            target,
            "--timeout=240s",
            timeout=250,
        )

    def delete_exact(self, kind: str, resource: dict[str, Any]) -> None:
        name = _metadata_string(resource, "name")
        uid = _metadata_string(resource, "uid")
        current = self.json(
            "-n",
            self.workload_namespace,
            "get",
            kind,
            name,
        )
        if _metadata_string(current, "uid") != uid:
            raise KindBatchE2EError(
                f"refusing cleanup of recreated {kind} {name}: UID fence mismatch"
            )
        self.run(
            "-n",
            self.workload_namespace,
            "delete",
            kind,
            name,
            "--cascade=foreground",
            "--wait=true",
            timeout=90,
        )


async def run_e2e_from_environment() -> None:
    base_url = _required_env("KIND_BATCH_BASE_URL")
    kubeconfig = _resolved_path(_required_env("KIND_BATCH_KUBECONFIG"))
    workload_namespace = _dns_env("KIND_BATCH_WORKLOAD_NAMESPACE")
    system_namespace = _dns_env("KIND_BATCH_SYSTEM_NAMESPACE")
    worker_deployment = _dns_env("KIND_BATCH_WORKER_DEPLOYMENT")
    image = _required_env("KIND_BATCH_APP_IMAGE")
    api_key_file = _resolved_path(_required_env("KIND_BATCH_API_KEY_FILE"))
    credentials_file = _resolved_path(_required_env("KIND_BATCH_CREDENTIALS_FILE"))
    if workload_namespace == system_namespace:
        raise KindBatchE2EError("system and workload namespaces must be isolated")
    if not kubeconfig.is_file():
        raise KindBatchE2EError(f"isolated kubeconfig does not exist: {kubeconfig}")
    credentials = _read_credentials(credentials_file)
    try:
        reference = canonicalize_image_reference(image)
    except ImageReferenceError as exc:
        raise KindBatchE2EError(f"fixed Kind app image is invalid: {exc}") from exc

    await wait_ready(base_url, 90)
    api = API(base_url, sensitive_values=credentials.sensitive_values)
    kube = Kubectl(
        kubeconfig,
        workload_namespace=workload_namespace,
        system_namespace=system_namespace,
        worker_deployment=worker_deployment,
        sensitive_values=credentials.sensitive_values,
    )
    try:
        project_id = await _authenticate(
            api,
            credentials=credentials,
            api_key_file=api_key_file,
        )
        await _configure_image_policy(api, project_id, image=image)
        await _run_batch_scenarios(
            api,
            kube,
            project_id=project_id,
            image=reference.canonical,
        )
    finally:
        await api.close()


async def _authenticate(
    api: API,
    *,
    credentials: Credentials,
    api_key_file: Path,
) -> str:
    stored = _read_optional_private_text(api_key_file)
    if stored is None:
        response = await api.json(
            "POST",
            "/api/v1/bootstrap",
            payload={
                "user": {
                    "username": "kind-batch-owner",
                    "email": "kind-batch@example.invalid",
                    "password": credentials.user_password,
                },
                "project": {
                    "name": "Kind Batch E2E",
                    "slug": "kind-batch-e2e",
                },
                "api_key_name": "kind-batch-e2e",
            },
            headers={"X-Bootstrap-Token": credentials.bootstrap_token},
            expected=(201,),
        )
        raw = response.get("api_key")
        api_key = raw.get("api_key") if isinstance(raw, dict) else None
        if not isinstance(api_key, str) or not api_key:
            raise KindBatchE2EError("bootstrap response omitted the one-time API key")
        _write_private_text(api_key_file, api_key)
    else:
        api_key = stored.strip()
        if not api_key:
            raise KindBatchE2EError("stored Kind batch API key is empty")
    api.api_key = api_key
    api.sensitive_values = (*api.sensitive_values, api_key)
    principal = await api.json("GET", "/api/v1/auth/whoami")
    project_id = principal.get("project_id")
    if not isinstance(project_id, str) or not project_id:
        raise KindBatchE2EError("authenticated principal has no project_id")
    print("PASS: authenticated a reusable project-scoped Kind batch identity")
    return project_id


async def _configure_image_policy(api: JSONAPI, project_id: str, *, image: str) -> None:
    reference = canonicalize_image_reference(image)
    rule: dict[str, object] = {
        "action": "allow",
        "registry": reference.registry,
        "repository_glob": reference.repository,
        "priority": 10,
    }
    if reference.digest is not None:
        rule["digest"] = reference.digest
    else:
        rule["tag_glob"] = reference.tag
    await api.json(
        "PUT",
        f"/api/v1/projects/{project_id}/image-policy",
        payload={
            "default_action": "deny",
            "require_digest": reference.digest is not None,
            "rules": [rule],
        },
    )
    print("PASS: configured an exact project allow rule for the fixed Kind app image")


def _task_payload(
    *,
    image: str,
    scenario: str,
    command: Sequence[str],
    timeout_seconds: int,
) -> dict[str, object]:
    if not scenario or not command or timeout_seconds < 1:
        raise ValueError("batch payload requires scenario, command, and positive timeout")
    reference = canonicalize_image_reference(image)
    return {
        "workload_type": "batch_job",
        "runtime_type": "kubernetes",
        "image": reference.canonical,
        "command": list(command),
        "environment": {"KIND_BATCH_SCENARIO": scenario},
        "timeout_seconds": timeout_seconds,
        "max_retries": 0,
        "cpu_limit": 0.25,
        "memory_limit_mb": 128,
        "labels": {},
        "network_enabled": False,
        "gpu_count": 0,
    }


async def _run_batch_scenarios(
    api: JSONAPI,
    kube: Kubectl,
    *,
    project_id: str,
    image: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    task_ids: list[str] = []
    scenario_error: Exception | None = None
    try:
        success_id = await _submit_task(
            api,
            _task_payload(
                image=image,
                scenario="success",
                command=(
                    "python",
                    "-c",
                    "import time; print('KIND_BATCH_SUCCESS', flush=True); time.sleep(5)",
                ),
                timeout_seconds=30,
            ),
            idempotency_key=f"kind-batch-{suffix}-success",
        )
        task_ids.append(success_id)
        success_job, success_execution, _ = await _observe_runtime_contract(
            api,
            kube,
            task_id=success_id,
            project_id=project_id,
            image=image,
            timeout_seconds=30,
        )
        succeeded = await _wait_task_status(
            api,
            success_id,
            {"succeeded"},
            timeout_seconds=90,
        )
        _assert_execution_identity(succeeded, success_execution)
        if succeeded.get("exit_code") != 0:
            raise KindBatchE2EError("successful Kind Job did not persist exit_code=0")
        logs = await api.json("GET", f"/api/v1/tasks/{success_id}/logs")
        if "KIND_BATCH_SUCCESS" not in _joined_logs(logs):
            raise KindBatchE2EError("successful Kind Job logs omitted the stdout marker")
        _assert_job_identity(success_job, success_id, success_execution)
        print("PASS: real CPU batch Job succeeded and persisted stdout logs")

        failure_id = await _submit_task(
            api,
            _task_payload(
                image=image,
                scenario="failure",
                command=(
                    "python",
                    "-c",
                    "import time; print('KIND_BATCH_FAILURE', flush=True); "
                    "time.sleep(5); raise SystemExit(17)",
                ),
                timeout_seconds=30,
            ),
            idempotency_key=f"kind-batch-{suffix}-failure",
        )
        task_ids.append(failure_id)
        _, failure_execution, _ = await _observe_runtime_contract(
            api,
            kube,
            task_id=failure_id,
            project_id=project_id,
            image=image,
            timeout_seconds=30,
        )
        failed = await _wait_task_status(api, failure_id, {"failed"}, timeout_seconds=90)
        _assert_execution_identity(failed, failure_execution)
        if failed.get("exit_code") != 17 or failed.get("error_category") != "USER_ERROR":
            raise KindBatchE2EError("nonzero Kind Job failure taxonomy was not persisted")
        print("PASS: real CPU batch Job persisted a bounded nonzero failure")

        timeout_id = await _submit_task(
            api,
            _task_payload(
                image=image,
                scenario="timeout",
                command=("python", "-c", "import time; time.sleep(60)"),
                timeout_seconds=15,
            ),
            idempotency_key=f"kind-batch-{suffix}-timeout",
        )
        task_ids.append(timeout_id)
        _, timeout_execution, _ = await _observe_runtime_contract(
            api,
            kube,
            task_id=timeout_id,
            project_id=project_id,
            image=image,
            timeout_seconds=15,
        )
        timed_out = await _wait_task_status(
            api,
            timeout_id,
            {"timed_out"},
            timeout_seconds=90,
        )
        _assert_execution_identity(timed_out, timeout_execution)
        if timed_out.get("error_category") != "TIMEOUT":
            raise KindBatchE2EError("timed-out Kind Job omitted TIMEOUT taxonomy")
        print("PASS: real CPU batch Job reached the bounded timed_out state")

        restart_id = await _submit_task(
            api,
            _task_payload(
                image=image,
                scenario="restart-cancel",
                command=(
                    "python",
                    "-c",
                    "import time; print('KIND_BATCH_RESTART', flush=True); time.sleep(240)",
                ),
                timeout_seconds=300,
            ),
            idempotency_key=f"kind-batch-{suffix}-restart",
        )
        task_ids.append(restart_id)
        restart_job, restart_execution, restart_pod = await _observe_runtime_contract(
            api,
            kube,
            task_id=restart_id,
            project_id=project_id,
            image=image,
            timeout_seconds=300,
        )
        await _wait_task_status(api, restart_id, {"running"}, timeout_seconds=60)
        before_uid = _metadata_string(restart_job, "uid")
        before_name = _metadata_string(restart_job, "name")
        before_controller = _controller_session(restart_job)
        before_pod_uid = _metadata_string(restart_pod, "uid")
        kube.restart_worker()
        adopted = await _wait_worker_adoption(
            api,
            kube,
            task_id=restart_id,
            job_name=before_name,
            job_uid=before_uid,
            execution_id=restart_execution,
            previous_controller_session=before_controller,
            pod_uid=before_pod_uid,
            timeout_seconds=120,
        )
        _assert_job_identity(adopted, restart_id, restart_execution)
        cancelled = await api.json(
            "POST",
            f"/api/v1/tasks/{restart_id}/cancel",
            expected=(200,),
        )
        if cancelled.get("cancel_requested") is not True:
            raise KindBatchE2EError("cancel API did not persist cancel_requested=true")
        final_cancel = await _wait_task_status(
            api,
            restart_id,
            {"cancelled"},
            timeout_seconds=90,
        )
        _assert_execution_identity(final_cancel, restart_execution)
        print("PASS: Worker rollout adopted the same Job UID/execution, then cancelled it")
    except Exception as exc:
        scenario_error = exc

    cleanup_errors = await _cleanup_tasks(api, kube, task_ids)
    _raise_outcome(scenario_error, cleanup_errors)
    print("PASS: every tracked batch Job, Pod, and NetworkPolicy was removed")


async def _submit_task(
    api: JSONAPI,
    payload: dict[str, object],
    *,
    idempotency_key: str,
) -> str:
    created = await api.json(
        "POST",
        "/api/v1/tasks",
        payload=payload,
        headers={"Idempotency-Key": idempotency_key},
        expected=(201,),
    )
    return _required_string(created, "id")


async def _observe_runtime_contract(
    api: JSONAPI,
    kube: Kubectl,
    *,
    task_id: str,
    project_id: str,
    image: str,
    timeout_seconds: int,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    job = await _wait_single_job(kube, task_id, timeout_seconds=60)
    execution_id = _required_label(_labels(job), EXECUTION_ID_LABEL)
    _assert_job_contract(
        job,
        project_id=project_id,
        task_id=task_id,
        execution_id=execution_id,
        image=image,
        timeout_seconds=timeout_seconds,
    )
    pod = await _wait_controlled_pod(kube, task_id, job, timeout_seconds=30)
    _assert_pod_contract(pod, job)
    # A short Job can finish before the control-plane read observes its execution.
    # The retained Job and Pod remain authoritative for the runtime contract, while
    # each scenario's subsequent status assertion still rejects an unexpected terminal state.
    await _wait_task_execution(
        api,
        task_id,
        execution_id,
        timeout_seconds=30,
    )
    return job, execution_id, pod


async def _wait_single_job(
    kube: Kubectl,
    task_id: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_count = 0
    while time.monotonic() < deadline:
        jobs = kube.jobs_for_task(task_id)
        last_count = len(jobs)
        if len(jobs) == 1:
            return jobs[0]
        if len(jobs) > 1:
            raise KindBatchE2EError(f"task {task_id} owns multiple Kubernetes Jobs")
        await asyncio.sleep(0.25)
    raise KindBatchE2EError(
        f"task {task_id} did not create exactly one Kubernetes Job (last_count={last_count})"
    )


async def _wait_controlled_pod(
    kube: Kubectl,
    task_id: str,
    job: dict[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    job_uid = _metadata_string(job, "uid")
    while time.monotonic() < deadline:
        controlled = [
            pod for pod in kube.pods_for_task(task_id) if _controlled_by_job(pod, job_uid)
        ]
        if len(controlled) == 1:
            return controlled[0]
        if len(controlled) > 1:
            raise KindBatchE2EError(f"Job {job_uid} controls multiple Pods")
        await asyncio.sleep(0.2)
    raise KindBatchE2EError(f"Job {job_uid} did not create one controlled Pod")


async def _wait_task_execution(
    api: JSONAPI,
    task_id: str,
    execution_id: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        task = await api.json("GET", f"/api/v1/tasks/{task_id}")
        if task.get("execution_id") == execution_id:
            return task
        if task.get("status") in FINAL_STATUSES:
            raise KindBatchE2EError(
                f"task {task_id} became {task.get('status')} before execution identity appeared"
            )
        await asyncio.sleep(0.2)
    raise KindBatchE2EError(f"task {task_id} did not persist execution {execution_id}")


async def _wait_task_status(
    api: JSONAPI,
    task_id: str,
    expected_statuses: set[str],
    *,
    timeout_seconds: float,
    poll_interval: float = 0.25,
) -> dict[str, Any]:
    if not expected_statuses:
        raise ValueError("expected_statuses must not be empty")
    deadline = time.monotonic() + timeout_seconds
    last_status: object = None
    while time.monotonic() < deadline:
        task = await api.json("GET", f"/api/v1/tasks/{task_id}")
        last_status = task.get("status")
        if last_status in expected_statuses:
            return task
        if last_status in FINAL_STATUSES:
            raise KindBatchE2EError(
                f"task {task_id} reached terminal status {last_status!r}, "
                f"expected {sorted(expected_statuses)}"
            )
        await asyncio.sleep(poll_interval)
    raise KindBatchE2EError(
        f"task {task_id} did not reach {sorted(expected_statuses)}; last status was {last_status!r}"
    )


async def _wait_worker_adoption(
    api: JSONAPI,
    kube: Kubectl,
    *,
    task_id: str,
    job_name: str,
    job_uid: str,
    execution_id: str,
    previous_controller_session: str,
    pod_uid: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_controller = previous_controller_session
    while time.monotonic() < deadline:
        task = await api.json("GET", f"/api/v1/tasks/{task_id}")
        if task.get("status") in FINAL_STATUSES:
            raise KindBatchE2EError(
                f"task became {task.get('status')} during Worker restart adoption"
            )
        job = kube.job(job_name)
        if job is None:
            raise KindBatchE2EError("Worker restart deleted the in-flight Kubernetes Job")
        if _metadata_string(job, "uid") != job_uid:
            raise KindBatchE2EError("Worker restart replaced the in-flight Kubernetes Job UID")
        _assert_job_identity(job, task_id, execution_id)
        if task.get("execution_id") != execution_id:
            raise KindBatchE2EError("Worker restart replaced the persisted execution identity")
        controller = _controller_session(job)
        last_controller = controller
        controlled = [
            pod for pod in kube.pods_for_task(task_id) if _controlled_by_job(pod, job_uid)
        ]
        if controlled and any(_metadata_string(pod, "uid") != pod_uid for pod in controlled):
            raise KindBatchE2EError("Worker restart replaced the controlled Pod identity")
        if controller != previous_controller_session and len(controlled) == 1:
            return job
        await asyncio.sleep(0.25)
    raise KindBatchE2EError(
        f"Worker restart did not CAS-transfer the Job controller session (last={last_controller})"
    )


async def _cleanup_tasks(
    api: JSONAPI,
    kube: Kubectl,
    task_ids: Sequence[str],
) -> list[str]:
    errors: list[str] = []
    for task_id in task_ids:
        try:
            task = await api.json("GET", f"/api/v1/tasks/{task_id}")
            if task.get("status") not in FINAL_STATUSES:
                await api.json(
                    "POST",
                    f"/api/v1/tasks/{task_id}/cancel",
                    expected=(200, 409),
                )
                await _wait_task_status(
                    api,
                    task_id,
                    set(FINAL_STATUSES),
                    timeout_seconds=60,
                )
        except Exception as exc:
            errors.append(f"task {task_id} terminal cleanup: {type(exc).__name__}: {exc}")

        try:
            for job in kube.jobs_for_task(task_id):
                kube.delete_exact("job", job)
            for policy in kube.network_policies_for_task(task_id):
                kube.delete_exact("networkpolicy", policy)
            await _wait_runtime_absent(kube, task_id, timeout_seconds=30)
        except Exception as exc:
            errors.append(f"task {task_id} Kubernetes cleanup: {type(exc).__name__}: {exc}")
    return [_redact(error, kube.sensitive_values) for error in errors]


async def _wait_runtime_absent(
    kube: Kubectl,
    task_id: str,
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last = (0, 0, 0)
    while time.monotonic() < deadline:
        last = (
            len(kube.jobs_for_task(task_id)),
            len(kube.pods_for_task(task_id)),
            len(kube.network_policies_for_task(task_id)),
        )
        if last == (0, 0, 0):
            return
        await asyncio.sleep(0.25)
    raise KindBatchE2EError(
        f"task {task_id} cleanup left jobs={last[0]}, pods={last[1]}, policies={last[2]}"
    )


def _raise_outcome(scenario_error: Exception | None, cleanup_errors: Sequence[str]) -> None:
    if cleanup_errors:
        error = KindBatchE2EError("Kind batch cleanup failed: " + "; ".join(cleanup_errors))
        if scenario_error is not None:
            raise error from scenario_error
        raise error
    if scenario_error is not None:
        raise scenario_error


def _assert_job_contract(
    job: dict[str, Any],
    *,
    project_id: str,
    task_id: str,
    execution_id: str,
    image: str,
    timeout_seconds: int,
) -> None:
    if job.get("apiVersion") != "batch/v1" or job.get("kind") != "Job":
        raise KindBatchE2EError("batch runtime did not create a batch/v1 Job")
    _metadata_string(job, "name")
    _metadata_string(job, "uid")
    _metadata_string(job, "resourceVersion")
    labels = _labels(job)
    required = {
        TASK_ID_LABEL,
        PROJECT_ID_LABEL,
        EXECUTION_ID_LABEL,
        WORKER_ID_LABEL,
        WORKER_SESSION_ID_LABEL,
        CLUSTER_ID_LABEL,
        SPEC_HASH_LABEL,
        MANAGED_LABEL,
        RESOURCE_KIND_LABEL,
        RUNTIME_PROFILE_DIGEST_LABEL,
    }
    if not required <= labels.keys():
        raise KindBatchE2EError(
            f"batch Job omitted ownership labels: {sorted(required - labels.keys())}"
        )
    if (
        labels[TASK_ID_LABEL] != task_id
        or labels[PROJECT_ID_LABEL] != project_id
        or labels[EXECUTION_ID_LABEL] != execution_id
        or labels[MANAGED_LABEL] != "true"
        or labels[RESOURCE_KIND_LABEL] != BATCH_JOB_RESOURCE_KIND
        or labels[RUNTIME_PROFILE_DIGEST_LABEL] != "none"
    ):
        raise KindBatchE2EError("batch Job ownership labels do not match the API execution")
    _controller_session(job)
    spec = _mapping(job.get("spec"), "Job spec")
    if (
        spec.get("backoffLimit") != 0
        or spec.get("completions") != 1
        or spec.get("parallelism") != 1
        or spec.get("activeDeadlineSeconds") != timeout_seconds
    ):
        raise KindBatchE2EError("batch Job retry/deadline contract is incorrect")
    template = _mapping(spec.get("template"), "Job Pod template")
    template_labels = _labels(template)
    if any(template_labels.get(key) != value for key, value in labels.items()):
        raise KindBatchE2EError("Job and Pod template ownership labels differ")
    pod_spec = _mapping(template.get("spec"), "Job Pod spec")
    _assert_task_pod_spec(pod_spec, image=image)


def _assert_pod_contract(pod: dict[str, Any], job: dict[str, Any]) -> None:
    if pod.get("apiVersion") != "v1" or pod.get("kind") != "Pod":
        raise KindBatchE2EError("batch Job did not create a core/v1 Pod")
    pod_uid = _metadata_string(pod, "uid")
    if not pod_uid:
        raise KindBatchE2EError("controlled Pod omitted UID")
    job_uid = _metadata_string(job, "uid")
    if not _controlled_by_job(pod, job_uid):
        raise KindBatchE2EError("batch Pod is not controlled by the fenced Job UID")
    job_labels = _labels(job)
    pod_labels = _labels(pod)
    if any(pod_labels.get(key) != value for key, value in job_labels.items()):
        raise KindBatchE2EError("controlled Pod ownership labels differ from its Job")
    image = _required_container_image(job)
    _assert_task_pod_spec(
        _mapping(pod.get("spec"), "controlled Pod spec"),
        image=image,
        allow_scheduled_node=True,
    )


def _assert_task_pod_spec(
    spec: Mapping[str, Any],
    *,
    image: str,
    allow_scheduled_node: bool = False,
) -> None:
    if spec.get("restartPolicy") != "Never":
        raise KindBatchE2EError("batch Pod restartPolicy is not Never")
    if not allow_scheduled_node and spec.get("nodeName") not in {None, ""}:
        raise KindBatchE2EError("batch Pod hardcodes nodeName")
    if spec.get("automountServiceAccountToken") is not False:
        raise KindBatchE2EError("batch Pod automounts a service account token")
    if any(spec.get(key) is True for key in ("hostNetwork", "hostPID", "hostIPC")):
        raise KindBatchE2EError("batch Pod enables a host namespace")
    pod_security = _mapping(spec.get("securityContext"), "Pod securityContext")
    if pod_security.get("runAsNonRoot") is not True:
        raise KindBatchE2EError("batch Pod does not enforce runAsNonRoot")
    seccomp = _mapping(pod_security.get("seccompProfile"), "Pod seccompProfile")
    if seccomp.get("type") != "RuntimeDefault":
        raise KindBatchE2EError("batch Pod does not use RuntimeDefault seccomp")
    containers = spec.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise KindBatchE2EError("batch Pod must contain exactly one task container")
    container = _mapping(containers[0], "task container")
    if container.get("name") != TASK_CONTAINER_NAME or container.get("image") != image:
        raise KindBatchE2EError("task container name or fixed image is incorrect")
    security = _mapping(container.get("securityContext"), "container securityContext")
    capabilities = _mapping(security.get("capabilities"), "container capabilities")
    if (
        security.get("allowPrivilegeEscalation") is not False
        or security.get("privileged") is not False
        or security.get("readOnlyRootFilesystem") is not True
        or capabilities.get("drop") != ["ALL"]
    ):
        raise KindBatchE2EError("task container security boundary is incomplete")
    resources = _mapping(container.get("resources"), "container resources")
    requests = _mapping(resources.get("requests"), "container requests")
    limits = _mapping(resources.get("limits"), "container limits")
    expected = {"cpu": "250m", "memory": "128Mi"}
    if requests != expected or limits != expected:
        raise KindBatchE2EError("task container requests/limits are not exact and equal")
    if any("/" in str(key) for key in requests):
        raise KindBatchE2EError("CPU batch Job contains a hardcoded extended resource")
    volumes = spec.get("volumes", [])
    if not isinstance(volumes, list) or any(
        not isinstance(volume, dict) or "hostPath" in volume for volume in volumes
    ):
        raise KindBatchE2EError("batch Pod uses a hostPath volume")
    if any(_contains_service_account_token(volume) for volume in volumes):
        raise KindBatchE2EError("batch Pod contains a projected service account token")


def _contains_service_account_token(value: object) -> bool:
    if isinstance(value, dict):
        if "serviceAccountToken" in value:
            return True
        return any(_contains_service_account_token(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_service_account_token(item) for item in value)
    return False


def _assert_job_identity(job: dict[str, Any], task_id: str, execution_id: str) -> None:
    labels = _labels(job)
    if labels.get(TASK_ID_LABEL) != task_id or labels.get(EXECUTION_ID_LABEL) != execution_id:
        raise KindBatchE2EError("Kubernetes Job identity differs from the persisted execution")


def _assert_execution_identity(task: dict[str, Any], execution_id: str) -> None:
    if task.get("execution_id") != execution_id:
        raise KindBatchE2EError("terminal task replaced its execution identity")


def _required_container_image(job: dict[str, Any]) -> str:
    containers = _nested(job, "spec", "template", "spec", "containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise KindBatchE2EError("Job template omitted its task container")
    container = containers[0]
    image = container.get("image") if isinstance(container, dict) else None
    if not isinstance(image, str) or not image:
        raise KindBatchE2EError("Job task container omitted image")
    return image


def _controller_session(job: dict[str, Any]) -> str:
    annotations = _nested(job, "metadata", "annotations")
    value = (
        annotations.get(CONTROLLER_SESSION_ANNOTATION) if isinstance(annotations, dict) else None
    )
    if not isinstance(value, str):
        raise KindBatchE2EError("Job omitted the mutable controller-session annotation")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise KindBatchE2EError("Job controller-session annotation is not a UUID") from exc


def _controlled_by_job(pod: dict[str, Any], job_uid: str) -> bool:
    references = _nested(pod, "metadata", "ownerReferences")
    return isinstance(references, list) and any(
        isinstance(reference, dict)
        and reference.get("controller") is True
        and reference.get("kind") == "Job"
        and reference.get("uid") == job_uid
        for reference in references
    )


def _joined_logs(document: dict[str, Any]) -> str:
    logs = document.get("logs")
    if not isinstance(logs, list):
        raise KindBatchE2EError("task log response omitted logs")
    return "".join(str(item.get("content", "")) for item in logs if isinstance(item, dict))


def _resource_items(document: dict[str, Any], kind: str) -> list[dict[str, Any]]:
    items = document.get("items")
    if not isinstance(items, list):
        raise KindBatchE2EError(f"Kubernetes {kind} list omitted items")
    return [item for item in items if isinstance(item, dict)]


def _labels(resource: dict[str, Any]) -> dict[str, str]:
    labels = _nested(resource, "metadata", "labels")
    if not isinstance(labels, dict):
        return {}
    return {str(key): str(value) for key, value in labels.items()}


def _metadata_string(resource: dict[str, Any], key: str) -> str:
    value = _nested(resource, "metadata", key)
    if not isinstance(value, str) or not value:
        raise KindBatchE2EError(f"Kubernetes resource omitted metadata.{key}")
    return value


def _required_label(labels: dict[str, str], key: str) -> str:
    value = labels.get(key)
    if not value:
        raise KindBatchE2EError(f"Kubernetes resource omitted label {key}")
    return value


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise KindBatchE2EError(f"response omitted string field {key}")
    return value


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise KindBatchE2EError(f"{label} is missing or invalid")
    return {str(key): item for key, item in value.items()}


def _nested(payload: dict[str, Any], *keys: str) -> object:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _read_credentials(path: Path) -> Credentials:
    if not path.is_file():
        raise KindBatchE2EError(f"Kind batch credentials file does not exist: {path}")
    if path.stat().st_size > 65_536:
        raise KindBatchE2EError("Kind batch credentials file is unexpectedly large")
    text = path.read_text(encoding="utf-8")
    values: dict[str, object]
    if text.lstrip().startswith("{"):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise KindBatchE2EError("Kind batch credentials JSON is invalid") from exc
        if not isinstance(decoded, dict):
            raise KindBatchE2EError("Kind batch credentials JSON must be an object")
        values = {str(key): value for key, value in decoded.items()}
    else:
        values = {}
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            if not separator or not key.strip():
                raise KindBatchE2EError("Kind batch credentials line is malformed")
            values[key.strip()] = value.strip()
    bootstrap = _credential_value(
        values,
        "KIND_BATCH_BOOTSTRAP_TOKEN",
        "BOOTSTRAP_TOKEN",
        "bootstrap_token",
        "bootstrap-token",
    )
    password = _credential_value(
        values,
        "KIND_BATCH_USER_PASSWORD",
        "USER_PASSWORD",
        "user_password",
        "user-password",
    )
    return Credentials(bootstrap_token=bootstrap, user_password=password)


def _credential_value(values: Mapping[str, object], *keys: str) -> str:
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value:
            return value
    raise KindBatchE2EError(f"Kind batch credentials omitted {keys[0]}")


def _read_optional_private_text(path: Path) -> str | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise KindBatchE2EError(f"Kind batch API key path is not a file: {path}")
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


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise KindBatchE2EError(f"NOT RUN: required environment variable {name} is missing")
    return value


def _dns_env(name: str) -> str:
    value = _required_env(name)
    if not _DNS_LABEL.fullmatch(value):
        raise KindBatchE2EError(f"{name} must be a DNS-1123 label")
    return value


def _resolved_path(value: str) -> Path:
    return Path(value).resolve()


def _redact(value: str, sensitive_values: Sequence[str]) -> str:
    redacted = value
    for sensitive in sorted((item for item in sensitive_values if item), key=len, reverse=True):
        redacted = redacted.replace(sensitive, "[REDACTED]")
    return redacted


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="M7 real Kind Kubernetes batch E2E helper")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run")
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        if args.command == "run":
            asyncio.run(run_e2e_from_environment())
        else:  # pragma: no cover - argparse enforces the command choices.
            raise AssertionError(args.command)
    except (KindBatchE2EError, KindServingE2EError) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
