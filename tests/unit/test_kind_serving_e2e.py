from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.kind_serving_e2e import (
    API,
    DEFAULT_CONTROLLER_DEPLOYMENT,
    DEFAULT_IMAGE_POLICY_REPOSITORY,
    DEFAULT_IMAGE_POLICY_TAG,
    KindServingE2EError,
    KindServingEnvironment,
    Kubectl,
    _bad_image_reference,
    _bounded_backoff_window,
    _configure_kind_image_policy,
    _image_references_match,
)


def _set_required_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    kubeconfig = tmp_path / "private" / "kubeconfig"
    kubeconfig.parent.mkdir()
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    values = {
        "KIND_SERVING_BASE_URL": "http://127.0.0.1:18080",
        "KIND_SERVING_KUBECONFIG": str(kubeconfig),
        "KIND_SERVING_NAMESPACE": "mini-ai-cloud-serving",
        "KIND_SERVING_APP_IMAGE": "mini-ai-cloud:kind-serving-v4a",
        "KIND_SERVING_BOOTSTRAP_TOKEN": "bootstrap-token",
        "KIND_SERVING_USER_PASSWORD": "user-password",
        "KIND_SERVING_API_KEY_FILE": str(tmp_path / "private" / "api-key"),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return kubeconfig.resolve()


def test_bad_image_backoff_uses_server_timestamps() -> None:
    persisted_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    retry_at, retry_delay = _bounded_backoff_window(
        updated_at=persisted_at.isoformat(),
        retry_not_before=(persisted_at + timedelta(seconds=5)).isoformat(),
    )

    assert retry_at == persisted_at + timedelta(seconds=5)
    assert retry_delay == timedelta(seconds=5)


@pytest.mark.parametrize("delay_seconds", [0, 11])
def test_bad_image_backoff_rejects_unbounded_server_window(delay_seconds: int) -> None:
    persisted_at = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)

    with pytest.raises(KindServingE2EError, match="bounded"):
        _bounded_backoff_window(
            updated_at=persisted_at.isoformat(),
            retry_not_before=(persisted_at + timedelta(seconds=delay_seconds)).isoformat(),
        )


def test_environment_preserves_legacy_serving_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    kubeconfig = _set_required_environment(monkeypatch, tmp_path)
    for name in (
        "KIND_SERVING_WORKLOAD_NAMESPACE",
        "KIND_SERVING_CONTROLLER_DEPLOYMENT",
        "KIND_SERVING_IMAGE_POLICY_REPOSITORY",
        "KIND_SERVING_IMAGE_POLICY_TAG",
    ):
        monkeypatch.delenv(name, raising=False)

    environment = KindServingEnvironment.from_process_environment()

    assert environment.kubeconfig == kubeconfig
    assert environment.controller_namespace == "mini-ai-cloud-serving"
    assert environment.workload_namespace == "mini-ai-cloud-serving"
    assert environment.controller_deployment == DEFAULT_CONTROLLER_DEPLOYMENT
    assert environment.image == "mini-ai-cloud:kind-serving-v4a"
    assert environment.image_policy_repository == DEFAULT_IMAGE_POLICY_REPOSITORY
    assert environment.image_policy_tag == DEFAULT_IMAGE_POLICY_TAG


def test_environment_accepts_chart_release_and_workload_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_required_environment(monkeypatch, tmp_path)
    kubeconfig = tmp_path / "run-1234" / "kubeconfig"
    kubeconfig.parent.mkdir()
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    overrides = {
        "KIND_SERVING_KUBECONFIG": str(kubeconfig),
        "KIND_SERVING_NAMESPACE": "mac-system-1234",
        "KIND_SERVING_WORKLOAD_NAMESPACE": "mac-workload-1234",
        "KIND_SERVING_CONTROLLER_DEPLOYMENT": ("mac-p4-1234-mini-ai-cloud-control-plane"),
        "KIND_SERVING_APP_IMAGE": "mini-ai-cloud:m7-p4-1234",
        "KIND_SERVING_IMAGE_POLICY_REPOSITORY": "library/mini-ai-cloud",
        "KIND_SERVING_IMAGE_POLICY_TAG": "m7-p4-1234",
    }
    for name, value in overrides.items():
        monkeypatch.setenv(name, value)

    environment = KindServingEnvironment.from_process_environment()

    assert environment.kubeconfig == kubeconfig.resolve()
    assert environment.controller_namespace == "mac-system-1234"
    assert environment.workload_namespace == "mac-workload-1234"
    assert environment.controller_deployment == ("mac-p4-1234-mini-ai-cloud-control-plane")
    assert environment.image == "mini-ai-cloud:m7-p4-1234"
    assert environment.image_policy_repository == "library/mini-ai-cloud"
    assert environment.image_policy_tag == "m7-p4-1234"


def test_controller_restart_targets_chart_control_plane(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    kube = Kubectl(
        tmp_path / "kubeconfig",
        "mac-workload-1234",
        controller_namespace="mac-system-1234",
        controller_deployment="mac-p4-1234-mini-ai-cloud-control-plane",
    )
    calls: list[tuple[tuple[str, ...], float]] = []

    def fake_run(*arguments: str, timeout: float = 120.0) -> str:
        calls.append((arguments, timeout))
        return ""

    monkeypatch.setattr(kube, "run", fake_run)

    kube.restart_controller()

    deployment = "deployment/mac-p4-1234-mini-ai-cloud-control-plane"
    assert calls == [
        (("-n", "mac-system-1234", "rollout", "restart", deployment), 120.0),
        (
            (
                "-n",
                "mac-system-1234",
                "rollout",
                "status",
                deployment,
                "--timeout=180s",
            ),
            190,
        ),
    ]


@pytest.mark.asyncio
async def test_image_policy_pins_app_digest_and_bad_image_tag() -> None:
    calls: list[dict[str, object]] = []

    class PolicyAPI:
        async def json(self, method: str, path: str, **kwargs: object) -> dict[str, Any]:
            calls.append({"method": method, "path": path, **kwargs})
            return {}

    await _configure_kind_image_policy(
        cast(API, PolicyAPI()),
        "project-1234",
        image=f"docker.io/library/mini-ai-cloud:m7-p4-1234@sha256:{'a' * 64}",
        repository="library/mini-ai-cloud",
        tag="m7-p4-1234",
    )

    payload = calls[0]["payload"]
    assert isinstance(payload, dict)
    rules = payload["rules"]
    assert isinstance(rules, list)
    assert rules[0]["repository_glob"] == "library/mini-ai-cloud"
    assert rules[0]["digest"] == f"sha256:{'a' * 64}"
    assert "tag_glob" not in rules[0]
    assert rules[1]["tag_glob"] == "m7-p4-1234"
    assert _bad_image_reference("m7-p4-1234") == ("invalid.local/mini-ai-cloud/missing:m7-p4-1234")


def test_pod_image_contract_accepts_server_canonical_digest_reference() -> None:
    digest = f"sha256:{'a' * 64}"

    assert _image_references_match(
        f"docker.io/library/mini-ai-cloud@{digest}",
        f"docker.io/library/mini-ai-cloud:m7-p4-1234@{digest}",
    )


@pytest.mark.parametrize(
    "observed_image",
    [
        f"docker.io/library/mini-ai-cloud@sha256:{'b' * 64}",
        "docker.io/library/mini-ai-cloud:latest",
        None,
    ],
)
def test_pod_image_contract_rejects_different_or_malformed_reference(
    observed_image: object,
) -> None:
    expected = f"docker.io/library/mini-ai-cloud:m7-p4-1234@sha256:{'a' * 64}"

    assert not _image_references_match(observed_image, expected)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is required")
def test_fake_device_plugin_uses_isolated_overrides_and_kind_only_evidence(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    kubeconfig = tmp_path / "private" / "kubeconfig"
    kubeconfig.parent.mkdir()
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    log_path = tmp_path / "kubectl.jsonl"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_kubectl = bin_dir / "kubectl"
    fake_kubectl.write_text(
        f"#!{sys.executable}\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "stdin = sys.stdin.read() if args[-2:] == ['-f', '-'] else ''\n"
        "path = Path(os.environ['KUBECTL_LOG'])\n"
        "with path.open('a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps({'argv': args, 'stdin': stdin}) + '\\n')\n"
        "if 'get' in args and 'nodes' in args:\n"
        "    print('2')\n"
        "elif 'get' in args and 'pod' in args:\n"
        "    print('kind-control-plane', end='')\n",
        encoding="utf-8",
    )
    fake_kubectl.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "KUBECTL_LOG": str(log_path),
            "KIND_SERVING_KUBECONFIG": str(kubeconfig),
            "KIND_SERVING_WORKLOAD_NAMESPACE": "mac-workload-1234",
            "KIND_SERVING_FAKE_PLUGIN_NAME": "mac-fake-plugin-1234",
            "KIND_SERVING_FAKE_ALLOCATION_NAME": "mac-fake-allocation-1234",
        }
    )

    completed = subprocess.run(
        ["bash", str(repository_root / "scripts" / "nvidia_fake_device_plugin.sh"), "test"],
        cwd=repository_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert records
    assert all(record["argv"][:2] == ["--kubeconfig", str(kubeconfig)] for record in records)
    applied = "\n".join(record["stdin"] for record in records if record["stdin"])
    commands = "\n".join(" ".join(record["argv"]) for record in records)
    assert "name: mac-fake-plugin-1234" in applied
    assert "name: mac-fake-allocation-1234" in applied
    assert "namespace: mac-workload-1234" in applied
    assert "daemonset mac-fake-plugin-1234" in commands
    assert "pod mac-fake-allocation-1234" in commands
    assert "KIND_CONTRACT_VALIDATED" in completed.stdout
    assert "REAL_HW_NOT_RUN" in completed.stdout
