from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli import __main__ as cli
from cli.hero_demo import (
    KIND_CLUSTER_NAME,
    CommandOutcome,
    HeroDemoError,
    run_hero_scenarios,
)

REVISION = "1" * 40
ADOPTION_MARKER = "PASS: controller rollout restart adopted the existing healthy Replica"
DRAIN_MARKER = "PASS: scale 4 to 1 drained an active SSE request before Pod deletion"


class FakeCommands:
    def __init__(self, *, fail_label: str | None = None, existing_cluster: bool = False) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_label = fail_label
        self.existing_cluster = existing_cluster

    def __call__(self, argv: tuple[str, ...], _cwd: Path) -> CommandOutcome:
        self.calls.append(argv)
        if argv == ("git", "rev-parse", "HEAD"):
            return CommandOutcome(0, stdout=f"{REVISION}\n")
        if argv == ("kind", "get", "clusters"):
            clusters = f"{KIND_CLUSTER_NAME}\n" if self.existing_cluster else ""
            return CommandOutcome(0, stdout=clusters)
        labels: dict[tuple[str, ...], str] = {
            ("make", "kind-serving-up"): "kind-up",
            ("make", "test-kind-serving"): "kind-test",
            ("make", "kind-serving-down"): "kind-down",
        }
        label = labels.get(argv)
        if self.fail_label is not None and label == self.fail_label:
            return CommandOutcome(
                1,
                stdout="KIND_SERVING_BOOTSTRAP_TOKEN=do-not-store\n",
                stderr=f"mkc_{'a' * 16}_{'B' * 43}",
            )
        if label == "kind-test":
            return CommandOutcome(0, stdout=f"{DRAIN_MARKER}\n{ADOPTION_MARKER}\n")
        return CommandOutcome(0, stdout="ok\n")


def _tool_lookup(name: str) -> str:
    return f"/usr/bin/{name}"


def test_demo_all_reuses_one_kind_run_cleans_up_and_is_repeatable(tmp_path: Path) -> None:
    fake = FakeCommands()

    first = run_hero_scenarios(
        ("fencing", "controller-adoption", "sse-drain"),
        tmp_path,
        repository_root=Path(__file__).parents[2],
        command_runner=fake,
        tool_lookup=_tool_lookup,
    )
    second = run_hero_scenarios(
        ("fencing", "controller-adoption", "sse-drain"),
        tmp_path,
        repository_root=Path(__file__).parents[2],
        command_runner=fake,
        tool_lookup=_tool_lookup,
    )

    assert first.status == second.status == "PASS"
    assert first.artifact_dir != second.artifact_dir
    assert fake.calls.count(("make", "kind-serving-up")) == 2
    assert fake.calls.count(("make", "test-kind-serving")) == 2
    assert fake.calls.count(("make", "kind-serving-down")) == 2
    summary = json.loads((first.artifact_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "PASS"
    assert [item["status"] for item in summary["scenarios"]] == ["PASS"] * 3
    assert [item["cleanup"] for item in summary["scenarios"]] == [
        "not required",
        "PASS",
        "PASS",
    ]


def test_kind_failure_retains_redacted_diagnostics_and_runs_cleanup(tmp_path: Path) -> None:
    fake = FakeCommands(fail_label="kind-test")

    with pytest.raises(HeroDemoError) as raised:
        run_hero_scenarios(
            ("controller-adoption",),
            tmp_path,
            repository_root=Path(__file__).parents[2],
            command_runner=fake,
            tool_lookup=_tool_lookup,
        )

    assert fake.calls.count(("bash", "scripts/kind_serving.sh", "diagnostics")) == 1
    assert fake.calls.count(("make", "kind-serving-down")) == 1
    artifact_dir = raised.value.artifact_dir
    assert artifact_dir is not None
    combined_logs = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((artifact_dir / "logs").glob("*.log"))
    )
    assert "do-not-store" not in combined_logs
    assert f"mkc_{'a' * 16}_{'B' * 43}" not in combined_logs
    assert "[REDACTED]" in combined_logs
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "FAIL"
    assert summary["scenarios"][0]["cleanup"] == "PASS"


def test_preflight_refuses_to_delete_a_preexisting_dedicated_cluster(tmp_path: Path) -> None:
    fake = FakeCommands(existing_cluster=True)

    with pytest.raises(HeroDemoError, match="refused existing dedicated cluster"):
        run_hero_scenarios(
            ("sse-drain",),
            tmp_path,
            repository_root=Path(__file__).parents[2],
            command_runner=fake,
            tool_lookup=_tool_lookup,
        )

    assert ("make", "kind-serving-up") not in fake.calls
    assert ("make", "kind-serving-down") not in fake.calls


def test_demo_cli_dispatches_all_scenarios(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[tuple[str, ...], Path]] = []

    def run(names: tuple[str, ...], output_dir: Path) -> None:
        calls.append((names, output_dir))

    monkeypatch.setattr(cli, "run_hero_scenarios", run)
    result = CliRunner().invoke(cli.app, ["demo", "all", "--output-dir", str(tmp_path)])

    assert result.exit_code == 0
    assert calls == [(("fencing", "controller-adoption", "sse-drain"), tmp_path)]
