from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from scripts.soak import CLUSTER_NAME, CommandOutcome, SoakError, run_soak

ROOT = Path(__file__).parents[2]
GIT_SHA = "a" * 40


class FakeRunner:
    def __init__(
        self,
        *,
        existing_cluster: bool = False,
        fail_label_fragment: str | None = None,
        leak_resource: bool = False,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.existing_cluster = existing_cluster
        self.fail_label_fragment = fail_label_fragment
        self.leak_resource = leak_resource

    def __call__(self, argv: tuple[str, ...], _cwd: Path, _timeout: float) -> CommandOutcome:
        self.calls.append(argv)
        joined = " ".join(argv)
        if argv == ("git", "rev-parse", "HEAD"):
            return CommandOutcome(0, f"{GIT_SHA}\n")
        if argv == ("kind", "get", "clusters"):
            existing = self.existing_cluster and self.calls.count(argv) == 1
            return CommandOutcome(0, f"{CLUSTER_NAME}\n" if existing else "")
        if self.fail_label_fragment and self.fail_label_fragment in joined:
            return CommandOutcome(
                1,
                "KIND_SERVING_BOOTSTRAP_TOKEN=do-not-store\n",
                f"mkc_{'a' * 16}_{'B' * 43}",
            )
        if " psql " in f" {joined} ":
            return CommandOutcome(0, "0|0|0\n")
        if "get pods -l mini-ai-cloud/managed=true" in joined and self.leak_resource:
            return CommandOutcome(0, "pod/leaked\n")
        return CommandOutcome(0, "")


def _tool_lookup(name: str) -> str:
    return f"/usr/bin/{name}"


def test_soak_requires_explicit_confirmation(tmp_path: Path) -> None:
    with pytest.raises(SoakError, match="CONFIRM_SOAK"):
        run_soak(2, tmp_path, confirmed=False, repository_root=ROOT)
    assert list(tmp_path.iterdir()) == []


def test_soak_runs_fixed_rounds_checks_leaks_and_removes_owned_cluster(tmp_path: Path) -> None:
    fake = FakeRunner()
    result = run_soak(
        2,
        tmp_path,
        confirmed=True,
        repository_root=ROOT,
        kubeconfig=tmp_path / "private" / "kubeconfig",
        runner=fake,
        tool_lookup=_tool_lookup,
    )

    assert result.status == "PASS"
    assert fake.calls.count(("make", "kind-serving-up")) == 1
    assert fake.calls.count(("make", "test-kind-serving")) == 2
    assert fake.calls.count(("make", "kind-serving-down")) == 1
    assert (
        sum("tests/integration/test_worker_session_fencing.py" in call for call in fake.calls) == 2
    )
    assert (
        sum("rollout" in call and "deployment/mini-ai-cloud-api" in call for call in fake.calls)
        == 4
    )
    assert sum("delete" in call and "pod" in call for call in fake.calls) == 2
    summary = json.loads((result.artifact_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["git_sha"] == GIT_SHA
    assert summary["rounds_completed"] == 2
    assert summary["cleanup"] == "PASS"
    assert all(snapshot["active_requests"] == 0 for snapshot in summary["snapshots"])


def test_soak_refuses_preexisting_cluster_without_deleting_it(tmp_path: Path) -> None:
    fake = FakeRunner(existing_cluster=True)
    with pytest.raises(SoakError, match="refused existing"):
        run_soak(
            1,
            tmp_path,
            confirmed=True,
            repository_root=ROOT,
            runner=fake,
            tool_lookup=_tool_lookup,
        )
    assert ("make", "kind-serving-up") not in fake.calls
    assert ("make", "kind-serving-down") not in fake.calls


def test_failure_retains_redacted_diagnostics_and_still_cleans_up(tmp_path: Path) -> None:
    fake = FakeRunner(fail_label_fragment="test-kind-serving")
    with pytest.raises(SoakError) as raised:
        run_soak(
            1,
            tmp_path,
            confirmed=True,
            repository_root=ROOT,
            kubeconfig=tmp_path / "private" / "kubeconfig",
            runner=fake,
            tool_lookup=_tool_lookup,
        )
    artifact_dir = raised.value.artifact_dir
    assert artifact_dir is not None
    combined = "\n".join(
        path.read_text(encoding="utf-8") for path in (artifact_dir / "logs").glob("*.log")
    )
    assert "do-not-store" not in combined
    assert f"mkc_{'a' * 16}_{'B' * 43}" not in combined
    assert "[REDACTED]" in combined
    assert ("bash", "scripts/kind_serving.sh", "diagnostics") in fake.calls
    assert ("make", "kind-serving-down") in fake.calls


def test_resource_leak_fails_the_round_and_cleans_cluster(tmp_path: Path) -> None:
    fake = FakeRunner(leak_resource=True)
    with pytest.raises(SoakError, match="Pods leaked"):
        run_soak(
            1,
            tmp_path,
            confirmed=True,
            repository_root=ROOT,
            kubeconfig=tmp_path / "private" / "kubeconfig",
            runner=fake,
            tool_lookup=_tool_lookup,
        )
    assert ("make", "kind-serving-down") in fake.calls


def test_soak_workflow_is_valid_yaml() -> None:
    document = yaml.safe_load((ROOT / ".github/workflows/soak.yml").read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    assert "jobs" in document
