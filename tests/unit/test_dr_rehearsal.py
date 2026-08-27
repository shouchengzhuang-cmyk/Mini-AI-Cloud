from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.dr_marker import MARKER_BYTES, marker_ids, marker_path
from scripts.dr_rehearsal import (
    PROJECT_LABEL,
    VOLUME_LABEL,
    CommandOutcome,
    DRRehearsalError,
    run_dr_rehearsal,
    validate_volume_identity,
)

ROOT = Path(__file__).parents[2]
SHA = "b" * 40


def _tool_lookup(name: str) -> str:
    return f"/usr/bin/{name}"


class FakeDRCommands:
    def __init__(self, *, wrong_label: bool = False) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.deleted: set[str] = set()
        self.wrong_label = wrong_label

    def __call__(self, argv: tuple[str, ...], _cwd: Path, _timeout: float) -> CommandOutcome:
        self.calls.append(argv)
        if argv == ("git", "rev-parse", "HEAD"):
            return CommandOutcome(0, f"{SHA}\n")
        if len(argv) >= 3 and argv[:3] == ("docker", "volume", "ls"):
            logical = next(
                (
                    item.rsplit("=", 1)[-1]
                    for item in argv
                    if item.startswith(f"label={VOLUME_LABEL}=")
                ),
                None,
            )
            if logical is None:
                return CommandOutcome(0, "")
            project = next(
                item.rsplit("=", 1)[-1]
                for item in argv
                if item.startswith(f"label={PROJECT_LABEL}=")
            )
            name = f"{project}_{logical}"
            return CommandOutcome(0, "" if name in self.deleted else f"{name}\n")
        if len(argv) == 4 and argv[:3] == ("docker", "volume", "inspect"):
            name = argv[3]
            if name in self.deleted:
                return CommandOutcome(1, stderr="not found")
            project, logical = name.rsplit("_", 1)
            labels = {
                PROJECT_LABEL: "wrong-project" if self.wrong_label else project,
                VOLUME_LABEL: logical,
            }
            return CommandOutcome(0, json.dumps([{"Name": name, "Labels": labels}]))
        if len(argv) >= 4 and argv[:3] == ("docker", "volume", "rm"):
            self.deleted.update(argv[3:])
            return CommandOutcome(0, "\n".join(argv[3:]))
        if argv[:2] == ("bash", "scripts/backup.sh"):
            output_root = Path(argv[argv.index("--output-dir") + 1])
            project = argv[argv.index("--project-name") + 1]
            backup = output_root / f"{project}-20260826T000000Z"
            backup.mkdir(parents=True)
            (backup / "manifest.json").write_text("{}\n", encoding="utf-8")
            (backup / "postgres.dump").write_bytes(b"dump")
            (backup / "SHA256SUMS").write_text(
                f"{'0' * 64}  manifest.json\n{'1' * 64}  postgres.dump\n",
                encoding="utf-8",
            )
            return CommandOutcome(0, f"Backup complete: {backup}\n")
        if "scripts/dr_marker.py" in argv:
            run_id = argv[argv.index("--run-id") + 1]
            marker = {
                "run_id": run_id,
                "task_status": "succeeded",
                "timeline_events": 1,
                "usage_rows": 1,
                "artifact_sha256": "2" * 64,
                "artifact_size": len(MARKER_BYTES),
                "schema_version": "0015_phase4a_hardening",
                "active_reservations": 0,
            }
            return CommandOutcome(0, json.dumps(marker) + "\n")
        return CommandOutcome(0, "")


def test_volume_identity_requires_exact_name_and_compose_labels() -> None:
    labels = {
        PROJECT_LABEL: "mini-ai-cloud-local-dr-abc",
        VOLUME_LABEL: "postgres-data",
    }
    payload = [
        {
            "Name": "mini-ai-cloud-local-dr-abc_postgres-data",
            "Labels": labels,
        }
    ]
    identity = validate_volume_identity(
        "mini-ai-cloud-local-dr-abc_postgres-data",
        payload,
        expected_project="mini-ai-cloud-local-dr-abc",
        expected_logical_name="postgres-data",
    )
    assert identity.logical_name == "postgres-data"

    labels[PROJECT_LABEL] = "mini-ai-cloud"
    with pytest.raises(DRRehearsalError, match="project label"):
        validate_volume_identity(
            identity.name,
            payload,
            expected_project=identity.project,
            expected_logical_name=identity.logical_name,
        )


def test_dr_requires_explicit_confirmation(tmp_path: Path) -> None:
    with pytest.raises(DRRehearsalError, match="CONFIRM_DR"):
        run_dr_rehearsal(tmp_path, confirmed=False, repository_root=ROOT)
    assert list(tmp_path.iterdir()) == []


def test_restore_waits_for_final_queryable_postgres_process() -> None:
    restore_script = (ROOT / "scripts" / "restore.sh").read_text(encoding="utf-8")

    assert "read -r pid_one_comm </proc/1/comm" in restore_script
    assert '[ "$pid_one_comm" = postgres ]' in restore_script
    assert '--command "SELECT 1"' in restore_script
    assert "if postgres_ready >/dev/null 2>&1; then" in restore_script


def test_isolated_rehearsal_deletes_only_validated_volumes_and_cleans_project(
    tmp_path: Path,
) -> None:
    fake = FakeDRCommands()
    result = run_dr_rehearsal(
        tmp_path,
        confirmed=True,
        repository_root=ROOT,
        runner=fake,
        tool_lookup=_tool_lookup,
    )

    summary = json.loads((result.artifact_dir / "summary.json").read_text(encoding="utf-8"))
    assert result.status == "PASS"
    assert summary["git_sha"] == SHA
    assert summary["seeded_marker"] == summary["restored_marker"]
    assert summary["cleanup"] == "PASS"
    assert len(summary["deleted_volumes"]) == 2
    assert (result.artifact_dir / "backup-manifest.json").is_file()
    assert (result.artifact_dir / "backup-SHA256SUMS").is_file()
    assert not (result.artifact_dir / "backup-work").exists()
    removal = next(call for call in fake.calls if call[:3] == ("docker", "volume", "rm"))
    assert set(removal[3:]) == set(summary["deleted_volumes"])


def test_label_mismatch_refuses_volume_deletion_but_cleans_project(tmp_path: Path) -> None:
    fake = FakeDRCommands(wrong_label=True)
    with pytest.raises(DRRehearsalError, match="project label"):
        run_dr_rehearsal(
            tmp_path,
            confirmed=True,
            repository_root=ROOT,
            runner=fake,
            tool_lookup=_tool_lookup,
        )
    assert not any(call[:3] == ("docker", "volume", "rm") for call in fake.calls)
    assert any("down" in call and "--volumes" in call for call in fake.calls)


def test_marker_path_is_deterministic_and_stays_below_artifact_root(tmp_path: Path) -> None:
    first = marker_ids("dr-20260826-abcdef")
    second = marker_ids("dr-20260826-abcdef")
    path = marker_path(tmp_path, "dr-20260826-abcdef")
    assert first == second
    assert tmp_path.resolve() in path.parents
    with pytest.raises(ValueError, match="run id"):
        marker_path(tmp_path, "../escape")
