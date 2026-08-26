from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli import __main__ as cli
from cli.evidence import (
    CommandResult,
    CommandRunner,
    EvidenceBundle,
    EvidenceCollectionError,
    collect_evidence,
    redact_text,
)

REPOSITORY_ROOT = Path(__file__).parents[2]
GIT_SHA = "a" * 40


def _runner(*, dirty: bool = False, leaked_secret: str | None = None) -> CommandRunner:
    def run(argv: tuple[str, ...], _cwd: Path, label: str) -> CommandResult:
        if label == "git-revision":
            return CommandResult(label, argv, "PASS", 0, f"{GIT_SHA}\n", "")
        if label == "git-status":
            output = " M tracked.py\n" if dirty else ""
            return CommandResult(label, argv, "PASS", 0, output, "")
        output = leaked_secret or f"{label}-1.0"
        return CommandResult(label, argv, "PASS", 0, output, "")

    return run


def test_bundle_is_commit_bound_consistent_and_summary_matches_json(tmp_path: Path) -> None:
    timestamp = datetime(2026, 8, 26, 1, 2, 3, tzinfo=UTC)
    first = collect_evidence(
        tmp_path / "first",
        repository_root=REPOSITORY_ROOT,
        runner=_runner(),
        started_at=timestamp,
    )
    second = collect_evidence(
        tmp_path / "second",
        repository_root=REPOSITORY_ROOT,
        runner=_runner(),
        started_at=timestamp,
    )

    assert first.path.name == GIT_SHA
    assert (first.path / "claims.json").read_bytes() == (second.path / "claims.json").read_bytes()
    assert (first.path / "environment.json").read_bytes() == (
        second.path / "environment.json"
    ).read_bytes()
    manifest = json.loads((first.path / "manifest.json").read_text(encoding="utf-8"))
    claims = json.loads((first.path / "claims.json").read_text(encoding="utf-8"))["claims"]
    summary = (first.path / "summary.md").read_text(encoding="utf-8")
    assert manifest["git"] == {"allow_dirty": False, "dirty": False, "sha": GIT_SHA}
    assert manifest["deployment_status"] == "NOT_DEPLOYED"
    assert len(claims) == 10
    for status in ("PASS", "PENDING", "NOT_RUN", "FAIL"):
        assert f"{status}={sum(claim['status'] == status for claim in claims)}" in summary
    assert "hashes.sha256" not in manifest["artifact_hashes"]
    assert (first.path / "hashes.sha256").is_file()


def test_dirty_tree_is_refused_unless_explicitly_allowed(tmp_path: Path) -> None:
    with pytest.raises(EvidenceCollectionError, match="dirty"):
        collect_evidence(
            tmp_path / "refused",
            repository_root=REPOSITORY_ROOT,
            runner=_runner(dirty=True),
        )

    bundle = collect_evidence(
        tmp_path / "allowed",
        repository_root=REPOSITORY_ROOT,
        runner=_runner(dirty=True),
        allow_dirty=True,
    )
    manifest = json.loads((bundle.path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["git"]["dirty"] is True
    assert manifest["git"]["allow_dirty"] is True


def test_command_output_is_redacted_and_environment_is_not_captured(tmp_path: Path) -> None:
    secret = "ghp_1234567890ABCDEF"
    bundle = collect_evidence(
        tmp_path / "redacted",
        repository_root=REPOSITORY_ROOT,
        runner=_runner(leaked_secret=f"Authorization: Bearer {secret}"),
    )
    combined = "\n".join(
        path.read_text(encoding="utf-8") for path in bundle.path.rglob("*") if path.is_file()
    )
    assert secret not in combined
    assert "[REDACTED]" in combined
    environment = json.loads((bundle.path / "environment.json").read_text(encoding="utf-8"))
    assert environment["environment_variables_collected"] is False


@pytest.mark.parametrize(
    ("value", "forbidden"),
    [
        ("password=hunter2", "hunter2"),
        ("https://user:pass@example.test", "user:pass"),
        ("mkc_abcdefghijklmnop_abcdefghijklmnopqrstuvwxyz0123456789", "mkc_"),
    ],
)
def test_redact_text_covers_common_credentials(value: str, forbidden: str) -> None:
    assert forbidden not in redact_text(value)


def test_evidence_cli_dispatches_collection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = EvidenceBundle(tmp_path / GIT_SHA, GIT_SHA, False)
    calls: list[tuple[Path, bool, str]] = []

    def fake_collect(
        output_dir: Path, *, allow_dirty: bool, deployment_status: str
    ) -> EvidenceBundle:
        calls.append((output_dir, allow_dirty, deployment_status))
        return expected

    monkeypatch.setattr(cli, "collect_evidence", fake_collect)
    result = CliRunner().invoke(
        cli.app,
        [
            "evidence",
            "collect",
            "--output-dir",
            str(tmp_path),
            "--allow-dirty",
            "--deployment-status",
            "UNKNOWN",
        ],
    )

    assert result.exit_code == 0
    assert calls == [(tmp_path, True, "UNKNOWN")]
    assert str(expected.path) in result.stdout
