from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.kind_batch_e2e import (
    BATCH_JOB_RESOURCE_KIND,
    CLUSTER_ID_LABEL,
    CONTROLLER_SESSION_ANNOTATION,
    EXECUTION_ID_LABEL,
    FINAL_STATUSES,
    MANAGED_LABEL,
    PROJECT_ID_LABEL,
    RESOURCE_KIND_LABEL,
    RUNTIME_PROFILE_DIGEST_LABEL,
    SPEC_HASH_LABEL,
    TASK_ID_LABEL,
    WORKER_ID_LABEL,
    WORKER_SESSION_ID_LABEL,
    Credentials,
    KindBatchE2EError,
    _assert_job_contract,
    _assert_pod_contract,
    _authenticate,
    _configure_image_policy,
    _observe_runtime_contract,
    _raise_outcome,
    _redact,
    _task_payload,
    _wait_task_status,
    main,
)
from scripts.kind_serving_e2e import API

PROJECT_ID = "11111111-1111-4111-8111-111111111111"
TASK_ID = "22222222-2222-4222-8222-222222222222"
EXECUTION_ID = "33333333-3333-4333-8333-333333333333"
JOB_UID = "44444444-4444-4444-8444-444444444444"
POD_UID = "55555555-5555-4555-8555-555555555555"
CONTROLLER_SESSION = "66666666-6666-4666-8666-666666666666"
IMAGE = "docker.io/library/mini-ai-cloud:kind-batch"


def _labels() -> dict[str, str]:
    return {
        TASK_ID_LABEL: TASK_ID,
        PROJECT_ID_LABEL: PROJECT_ID,
        EXECUTION_ID_LABEL: EXECUTION_ID,
        WORKER_ID_LABEL: "worker-kind-1",
        WORKER_SESSION_ID_LABEL: "77777777-7777-4777-8777-777777777777",
        CLUSTER_ID_LABEL: "kind-m7-p4",
        SPEC_HASH_LABEL: "0123456789abcdef",
        MANAGED_LABEL: "true",
        RESOURCE_KIND_LABEL: BATCH_JOB_RESOURCE_KIND,
        RUNTIME_PROFILE_DIGEST_LABEL: "none",
    }


def _pod_spec(*, scheduled: bool = False) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "securityContext": {
            "runAsNonRoot": True,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [
            {
                "name": "task",
                "image": IMAGE,
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "privileged": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "resources": {
                    "requests": {"cpu": "250m", "memory": "128Mi"},
                    "limits": {"cpu": "250m", "memory": "128Mi"},
                },
            }
        ],
    }
    if scheduled:
        # The API server defaults the name and the scheduler binds the live Pod.
        # Neither field is present in the immutable Job Pod template.
        spec["serviceAccountName"] = "default"
        spec["nodeName"] = "kind-worker"
    return spec


def _job() -> dict[str, Any]:
    labels = _labels()
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "mini-ai-job-222222222222-333333333333",
            "uid": JOB_UID,
            "resourceVersion": "101",
            "labels": labels,
            "annotations": {CONTROLLER_SESSION_ANNOTATION: CONTROLLER_SESSION},
        },
        "spec": {
            "activeDeadlineSeconds": 30,
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "template": {
                "metadata": {"labels": dict(labels)},
                "spec": _pod_spec(),
            },
        },
    }


def _pod(job: dict[str, Any]) -> dict[str, Any]:
    labels = dict(cast(dict[str, str], job["metadata"]["labels"]))
    labels["batch.kubernetes.io/job-name"] = str(job["metadata"]["name"])
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{job['metadata']['name']}-abcde",
            "uid": POD_UID,
            "labels": labels,
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": job["metadata"]["name"],
                    "uid": JOB_UID,
                    "controller": True,
                }
            ],
        },
        "spec": _pod_spec(scheduled=True),
    }


def test_task_payload_is_a_fixed_cpu_batch_contract() -> None:
    payload = _task_payload(
        image="mini-ai-cloud:kind-batch",
        scenario="success",
        command=("python", "-c", "print('ok')"),
        timeout_seconds=30,
    )

    assert payload == {
        "workload_type": "batch_job",
        "runtime_type": "kubernetes",
        "image": IMAGE,
        "command": ["python", "-c", "print('ok')"],
        "environment": {"KIND_BATCH_SCENARIO": "success"},
        "timeout_seconds": 30,
        "max_retries": 0,
        "cpu_limit": 0.25,
        "memory_limit_mb": 128,
        "labels": {},
        "network_enabled": False,
        "gpu_count": 0,
    }


def test_job_and_live_pod_contract_accepts_scheduler_binding() -> None:
    job = _job()

    _assert_job_contract(
        job,
        project_id=PROJECT_ID,
        task_id=TASK_ID,
        execution_id=EXECUTION_ID,
        image=IMAGE,
        timeout_seconds=30,
    )
    _assert_pod_contract(_pod(job), job)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("apiVersion",), "v1", "batch/v1"),
        (("spec", "backoffLimit"), 1, "retry/deadline"),
        (("spec", "template", "spec", "restartPolicy"), "Always", "restartPolicy"),
        (("spec", "template", "spec", "nodeName"), "kind-worker", "nodeName"),
        (
            ("spec", "template", "spec", "automountServiceAccountToken"),
            True,
            "service account token",
        ),
        (
            ("spec", "template", "spec", "containers", 0, "securityContext", "privileged"),
            True,
            "security boundary",
        ),
        (
            ("spec", "template", "spec", "containers", 0, "resources", "limits", "cpu"),
            "500m",
            "requests/limits",
        ),
    ],
)
def test_job_contract_rejects_unsafe_or_nondeterministic_fields(
    path: tuple[str | int, ...],
    value: object,
    message: str,
) -> None:
    job = _job()
    _set_nested(job, path, value)

    with pytest.raises(KindBatchE2EError, match=message):
        _assert_job_contract(
            job,
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            execution_id=EXECUTION_ID,
            image=IMAGE,
            timeout_seconds=30,
        )


def test_job_contract_rejects_missing_ownership_label() -> None:
    job = _job()
    del job["metadata"]["labels"][SPEC_HASH_LABEL]

    with pytest.raises(KindBatchE2EError, match="ownership labels"):
        _assert_job_contract(
            job,
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            execution_id=EXECUTION_ID,
            image=IMAGE,
            timeout_seconds=30,
        )


@pytest.mark.parametrize(
    "volume",
    [
        {"name": "host", "hostPath": {"path": "/tmp"}},
        {
            "name": "token",
            "projected": {
                "sources": [{"serviceAccountToken": {"path": "token"}}],
            },
        },
    ],
)
def test_job_contract_rejects_host_or_service_account_token_volume(
    volume: dict[str, Any],
) -> None:
    job = _job()
    job["spec"]["template"]["spec"]["volumes"] = [volume]

    with pytest.raises(KindBatchE2EError, match=r"hostPath|service account token"):
        _assert_job_contract(
            job,
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            execution_id=EXECUTION_ID,
            image=IMAGE,
            timeout_seconds=30,
        )


class SequenceAPI:
    def __init__(self, responses: Sequence[dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []
        self.payloads: list[dict[str, object] | None] = []

    async def json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        expected: Sequence[int] = (200,),
    ) -> dict[str, Any]:
        del headers, expected
        self.calls.append((method, path))
        self.payloads.append(payload)
        if not self.responses:
            raise AssertionError("unexpected API poll")
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_wait_task_status_polls_until_expected_terminal_state() -> None:
    api = SequenceAPI(
        [
            {"status": "assigned"},
            {"status": "running"},
            {"status": "succeeded", "execution_id": EXECUTION_ID},
        ]
    )

    result = await _wait_task_status(
        api,
        TASK_ID,
        {"succeeded"},
        timeout_seconds=1,
        poll_interval=0,
    )

    assert result["execution_id"] == EXECUTION_ID
    assert api.calls == [("GET", f"/api/v1/tasks/{TASK_ID}")] * 3


@pytest.mark.asyncio
async def test_wait_task_status_fails_closed_on_wrong_terminal_state() -> None:
    api = SequenceAPI([{"status": "failed"}])

    with pytest.raises(KindBatchE2EError, match="terminal status 'failed'"):
        await _wait_task_status(
            api,
            TASK_ID,
            {"succeeded"},
            timeout_seconds=1,
            poll_interval=0,
        )


@pytest.mark.asyncio
async def test_runtime_contract_accepts_fast_terminal_task_with_retained_objects() -> None:
    job = _job()
    pod = _pod(job)
    api = SequenceAPI([{"status": "succeeded", "execution_id": EXECUTION_ID}])

    class RetainedRuntimeKubectl:
        def jobs_for_task(self, task_id: str) -> list[dict[str, Any]]:
            assert task_id == TASK_ID
            return [job]

        def pods_for_task(self, task_id: str) -> list[dict[str, Any]]:
            assert task_id == TASK_ID
            return [pod]

    observed_job, execution_id, observed_pod = await _observe_runtime_contract(
        api,
        cast(Any, RetainedRuntimeKubectl()),
        task_id=TASK_ID,
        project_id=PROJECT_ID,
        image=IMAGE,
        timeout_seconds=30,
    )

    assert observed_job is job
    assert observed_pod is pod
    assert execution_id == EXECUTION_ID
    assert api.calls == [("GET", f"/api/v1/tasks/{TASK_ID}")]


@pytest.mark.asyncio
async def test_image_policy_is_exact_and_deny_by_default() -> None:
    api = SequenceAPI([{}])

    await _configure_image_policy(api, PROJECT_ID, image=IMAGE)

    assert api.calls == [("PUT", f"/api/v1/projects/{PROJECT_ID}/image-policy")]
    assert api.payloads == [
        {
            "default_action": "deny",
            "require_digest": False,
            "rules": [
                {
                    "action": "allow",
                    "registry": "docker.io",
                    "repository_glob": "library/mini-ai-cloud",
                    "priority": 10,
                    "tag_glob": "kind-batch",
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_existing_shared_api_key_skips_bootstrap(tmp_path: Path) -> None:
    api_key_file = tmp_path / "shared-api-key"
    api_key_file.write_text("shared-secret-key", encoding="utf-8")

    class ExistingKeyAPI:
        def __init__(self) -> None:
            self.api_key: str | None = None
            self.sensitive_values: tuple[str, ...] = ("bootstrap-secret", "password-secret")
            self.calls: list[tuple[str, str]] = []

        async def json(self, method: str, path: str, **kwargs: object) -> dict[str, Any]:
            del kwargs
            self.calls.append((method, path))
            return {"project_id": PROJECT_ID}

    api = ExistingKeyAPI()
    project_id = await _authenticate(
        cast(API, api),
        credentials=Credentials("bootstrap-secret", "password-secret"),
        api_key_file=api_key_file,
    )

    assert project_id == PROJECT_ID
    assert api.api_key == "shared-secret-key"
    assert "shared-secret-key" in api.sensitive_values
    assert api.calls == [("GET", "/api/v1/auth/whoami")]


def test_redaction_removes_every_secret_and_cleanup_failure_wins() -> None:
    message = "bootstrap-secret password-secret shared-secret-key"
    redacted = _redact(
        message,
        ("bootstrap-secret", "password-secret", "shared-secret-key"),
    )

    assert redacted == "[REDACTED] [REDACTED] [REDACTED]"
    with pytest.raises(KindBatchE2EError, match="cleanup failed"):
        _raise_outcome(RuntimeError("scenario failed"), ["delete failed"])


def test_cli_reports_not_run_without_required_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in (
        "KIND_BATCH_BASE_URL",
        "KIND_BATCH_KUBECONFIG",
        "KIND_BATCH_WORKLOAD_NAMESPACE",
        "KIND_BATCH_SYSTEM_NAMESPACE",
        "KIND_BATCH_WORKER_DEPLOYMENT",
        "KIND_BATCH_APP_IMAGE",
        "KIND_BATCH_API_KEY_FILE",
        "KIND_BATCH_CREDENTIALS_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(sys, "argv", ["kind_batch_e2e.py", "run"])

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "FAILED: NOT RUN: required environment variable KIND_BATCH_BASE_URL is missing\n"
    )


def test_final_status_contract_includes_every_cleanup_terminal_state() -> None:
    assert FINAL_STATUSES == {
        "succeeded",
        "failed",
        "cancelled",
        "timed_out",
        "preempted",
    }


def _set_nested(payload: dict[str, Any], path: tuple[str | int, ...], value: object) -> None:
    current: object = payload
    for key in path[:-1]:
        if isinstance(key, int):
            assert isinstance(current, list)
            current = current[key]
        else:
            assert isinstance(current, dict)
            current = current[key]
    final = path[-1]
    if isinstance(final, int):
        assert isinstance(current, list)
        current[final] = value
    else:
        assert isinstance(current, dict)
        current[final] = value
