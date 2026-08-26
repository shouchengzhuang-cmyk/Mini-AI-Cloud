from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_PREFIX = "mini-ai-cloud-local-dr-"
COMPOSE_FILE = Path("deploy/dr-rehearsal.compose.yml")
VOLUME_LABEL = "com.docker.compose.volume"
PROJECT_LABEL = "com.docker.compose.project"
LOGICAL_VOLUMES = ("postgres-data", "artifact-data")


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class VolumeIdentity:
    name: str
    project: str
    logical_name: str


@dataclass(frozen=True, slots=True)
class DRRun:
    artifact_dir: Path
    project_name: str
    status: str


CommandRunner = Callable[[tuple[str, ...], Path, float], CommandOutcome]
ToolLookup = Callable[[str], str | None]


class DRRehearsalError(RuntimeError):
    def __init__(self, message: str, *, artifact_dir: Path | None = None) -> None:
        super().__init__(message)
        self.artifact_dir = artifact_dir


def _redact(value: str) -> str:
    redacted = re.sub(r"mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}", "[REDACTED_API_KEY]", value)
    redacted = re.sub(r"(?i)(authorization:\s*bearer\s+)\S+", r"\1[REDACTED]", redacted)
    redacted = re.sub(
        r"(?i)((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+",
        r"\1[REDACTED]",
        redacted,
    )
    return redacted


def _run_command(argv: tuple[str, ...], cwd: Path, timeout_seconds: float) -> CommandOutcome:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return CommandOutcome(
            124,
            stderr=f"{type(exc).__name__}: command did not complete safely",
            duration_seconds=time.monotonic() - started,
        )
    return CommandOutcome(
        completed.returncode,
        completed.stdout,
        completed.stderr,
        time.monotonic() - started,
    )


def _record_command(
    label: str,
    argv: tuple[str, ...],
    root: Path,
    timeout_seconds: float,
    runner: CommandRunner,
    commands: list[dict[str, object]],
    log_dir: Path,
) -> CommandOutcome:
    outcome = runner(argv, root, timeout_seconds)
    log = _redact(f"$ {shlex.join(argv)}\n\nSTDOUT\n{outcome.stdout}\n\nSTDERR\n{outcome.stderr}")
    log_path = log_dir / f"{len(commands) + 1:03d}-{label}.log"
    log_path.write_text(log, encoding="utf-8", newline="\n")
    commands.append(
        {
            "label": label,
            "argv": list(argv),
            "returncode": outcome.returncode,
            "duration_seconds": round(outcome.duration_seconds, 6),
            "log": str(log_path.relative_to(log_dir.parent)),
        }
    )
    return outcome


def _require_success(outcome: CommandOutcome, message: str) -> None:
    if outcome.returncode != 0:
        raise DRRehearsalError(message)


def validate_volume_identity(
    name: str,
    inspect_payload: object,
    *,
    expected_project: str,
    expected_logical_name: str,
) -> VolumeIdentity:
    if not name or "\n" in name or "\r" in name:
        raise DRRehearsalError("volume name is empty or contains a newline")
    if not isinstance(inspect_payload, list) or len(inspect_payload) != 1:
        raise DRRehearsalError("volume inspect must return exactly one object")
    item = inspect_payload[0]
    if not isinstance(item, dict) or item.get("Name") != name:
        raise DRRehearsalError("volume inspect name does not match the selected target")
    labels = item.get("Labels")
    if not isinstance(labels, dict):
        raise DRRehearsalError("volume has no Compose ownership labels")
    project = labels.get(PROJECT_LABEL)
    logical_name = labels.get(VOLUME_LABEL)
    if project != expected_project:
        raise DRRehearsalError("volume project label does not match the isolated DR project")
    if logical_name != expected_logical_name:
        raise DRRehearsalError("volume logical-name label does not match the deletion target")
    return VolumeIdentity(name, expected_project, expected_logical_name)


def _parse_json(value: str, context: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise DRRehearsalError(f"{context} returned invalid JSON") from exc


def _parse_marker(value: str, context: str) -> dict[str, object]:
    lines = [line for line in value.splitlines() if line.strip().startswith("{")]
    if not lines:
        raise DRRehearsalError(f"{context} did not emit marker JSON")
    payload = _parse_json(lines[-1], context)
    if not isinstance(payload, dict):
        raise DRRehearsalError(f"{context} marker must be an object")
    return payload


def _compose(project_name: str, *arguments: str) -> tuple[str, ...]:
    return (
        "docker",
        "compose",
        "--file",
        str(COMPOSE_FILE),
        "--project-name",
        project_name,
        *arguments,
    )


def _owned_volume(
    project_name: str,
    logical_name: str,
    record: Callable[[str, tuple[str, ...]], CommandOutcome],
) -> VolumeIdentity:
    listed = record(
        f"list-{logical_name}",
        (
            "docker",
            "volume",
            "ls",
            "--filter",
            f"label={PROJECT_LABEL}={project_name}",
            "--filter",
            f"label={VOLUME_LABEL}={logical_name}",
            "--format",
            "{{.Name}}",
        ),
    )
    _require_success(listed, f"cannot list {logical_name} volume")
    names = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
    if len(names) != 1:
        raise DRRehearsalError(
            f"expected exactly one {logical_name} volume for the isolated project, "
            f"found {len(names)}"
        )
    inspected = record(f"inspect-{logical_name}", ("docker", "volume", "inspect", names[0]))
    _require_success(inspected, f"cannot inspect {logical_name} volume")
    return validate_volume_identity(
        names[0],
        _parse_json(inspected.stdout, f"inspect {logical_name}"),
        expected_project=project_name,
        expected_logical_name=logical_name,
    )


def _backup_path(output: str, backup_root: Path) -> Path:
    prefix = "Backup complete: "
    matches = [
        line[len(prefix) :].strip() for line in output.splitlines() if line.startswith(prefix)
    ]
    if len(matches) != 1:
        raise DRRehearsalError("backup command did not report exactly one destination")
    path = Path(matches[0]).resolve()
    root = backup_root.resolve()
    if root not in path.parents or not path.is_dir() or path.is_symlink():
        raise DRRehearsalError("backup destination escaped its isolated output root")
    return path


def run_dr_rehearsal(
    output_root: Path,
    *,
    confirmed: bool,
    repository_root: Path | None = None,
    command_timeout_seconds: float = 900,
    runner: CommandRunner = _run_command,
    tool_lookup: ToolLookup = shutil.which,
) -> DRRun:
    if not confirmed:
        raise DRRehearsalError("CONFIRM_DR=YES is required")
    if command_timeout_seconds <= 0:
        raise DRRehearsalError("command timeout must be positive")
    root = (repository_root or Path.cwd()).resolve()
    if not (root / COMPOSE_FILE).is_file():
        raise DRRehearsalError(f"missing isolated compose file: {COMPOSE_FILE}")

    suffix = uuid.uuid4().hex[:12]
    run_id = datetime.now(UTC).strftime("dr-%Y%m%d%H%M%S-") + suffix[:8]
    project_name = PROJECT_PREFIX + suffix
    artifact_dir = output_root.resolve() / run_id
    log_dir = artifact_dir / "logs"
    backup_root = artifact_dir / "backup-work"
    log_dir.mkdir(parents=True, exist_ok=False)
    backup_root.mkdir()
    commands: list[dict[str, object]] = []
    selected_volumes: list[VolumeIdentity] = []
    deleted_volumes: list[str] = []
    git_sha: str | None = None
    seeded: dict[str, object] | None = None
    restored: dict[str, object] | None = None
    backup_dir: Path | None = None
    project_owned = False
    failure: str | None = None

    def record(label: str, argv: tuple[str, ...]) -> CommandOutcome:
        return _record_command(
            label,
            argv,
            root,
            command_timeout_seconds,
            runner,
            commands,
            log_dir,
        )

    try:
        missing = [name for name in ("bash", "docker", "git") if not tool_lookup(name)]
        if missing:
            raise DRRehearsalError(f"preflight missing commands: {', '.join(missing)}")
        _require_success(record("docker-info", ("docker", "info")), "Docker Engine is unreachable")
        revision = record("git-revision", ("git", "rev-parse", "HEAD"))
        _require_success(revision, "cannot resolve Git revision")
        revision_value = revision.stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}", revision_value):
            raise DRRehearsalError("Git revision is not a full lowercase SHA")
        git_sha = revision_value
        existing = record(
            "preflight-project-containers",
            (
                "docker",
                "ps",
                "--all",
                "--filter",
                f"label={PROJECT_LABEL}={project_name}",
                "--format",
                "{{.ID}}",
            ),
        )
        _require_success(existing, "cannot inspect existing Compose containers")
        if existing.stdout.strip():
            raise DRRehearsalError("unique DR project unexpectedly already exists")

        project_owned = True
        _require_success(
            record("build-dr-image", _compose(project_name, "build", "migrate", "dr-tool")),
            "DR image build failed",
        )
        _require_success(
            record("postgres-up", _compose(project_name, "up", "--detach", "postgres")),
            "isolated PostgreSQL did not start",
        )
        _require_success(
            record("migrate", _compose(project_name, "run", "--rm", "migrate")),
            "database migration failed",
        )
        seed = record(
            "seed-marker",
            _compose(
                project_name,
                "run",
                "--rm",
                "dr-tool",
                "python",
                "scripts/dr_marker.py",
                "seed",
                "--run-id",
                run_id,
            ),
        )
        _require_success(seed, "DR marker seed failed")
        seeded = _parse_marker(seed.stdout, "seed")

        _require_success(
            record("stop-before-backup", _compose(project_name, "stop", "--timeout", "20")),
            "cannot stop isolated writers before backup",
        )
        _require_success(
            record("postgres-for-backup", _compose(project_name, "up", "--detach", "postgres")),
            "cannot start isolated PostgreSQL for backup",
        )
        backup = record(
            "backup",
            (
                "bash",
                "scripts/backup.sh",
                "--output-dir",
                str(backup_root),
                "--project-name",
                project_name,
                "--compose-file",
                str(COMPOSE_FILE),
                "--local-stack",
            ),
        )
        _require_success(backup, "backup failed")
        backup_dir = _backup_path(backup.stdout, backup_root)
        shutil.copy2(backup_dir / "manifest.json", artifact_dir / "backup-manifest.json")
        shutil.copy2(backup_dir / "SHA256SUMS", artifact_dir / "backup-SHA256SUMS")

        selected_volumes = [
            _owned_volume(project_name, logical_name, record) for logical_name in LOGICAL_VOLUMES
        ]
        _require_success(
            record("stop-before-destruction", _compose(project_name, "stop", "--timeout", "20")),
            "cannot stop isolated stack before destructive step",
        )
        _require_success(
            record(
                "remove-containers-before-destruction",
                _compose(
                    project_name,
                    "--profile",
                    "dr-tools",
                    "rm",
                    "--force",
                    "--stop",
                ),
            ),
            "cannot remove isolated containers before destructive step",
        )
        # Re-inspect immediately before deletion so a stale or substituted target fails closed.
        selected_volumes = [
            _owned_volume(project_name, identity.logical_name, record)
            for identity in selected_volumes
        ]
        removed = record(
            "delete-validated-volumes",
            ("docker", "volume", "rm", *(identity.name for identity in selected_volumes)),
        )
        _require_success(removed, "validated isolated volume deletion failed")
        deleted_volumes = [identity.name for identity in selected_volumes]
        for identity in selected_volumes:
            absent = record(
                f"confirm-deleted-{identity.logical_name}",
                ("docker", "volume", "inspect", identity.name),
            )
            if absent.returncode == 0:
                raise DRRehearsalError(f"deleted volume still exists: {identity.name}")

        restore = record(
            "restore",
            (
                "bash",
                "scripts/restore.sh",
                "--backup-dir",
                str(backup_dir),
                "--project-name",
                project_name,
                "--compose-file",
                str(COMPOSE_FILE),
                "--local-stack",
                "--confirm-overwrite",
            ),
        )
        _require_success(restore, "restore failed")
        _require_success(
            record("postgres-after-restore", _compose(project_name, "up", "--detach", "postgres")),
            "restored PostgreSQL did not start",
        )
        verify = record(
            "verify-restored-marker",
            _compose(
                project_name,
                "run",
                "--rm",
                "dr-tool",
                "python",
                "scripts/dr_marker.py",
                "verify",
                "--run-id",
                run_id,
            ),
        )
        _require_success(verify, "restored marker verification failed")
        restored = _parse_marker(verify.stdout, "restore verification")
        if seeded != restored:
            raise DRRehearsalError("restored marker does not match the pre-backup marker")
    except DRRehearsalError as exc:
        failure = str(exc)
        if project_owned:
            record(
                "compose-ps-diagnostics",
                _compose(project_name, "--profile", "dr-tools", "ps", "--all"),
            )
            record(
                "compose-logs-diagnostics",
                _compose(project_name, "--profile", "dr-tools", "logs", "--no-color"),
            )
    finally:
        cleanup = "NOT_REQUIRED"
        if project_owned:
            down = record(
                "cleanup-project",
                _compose(
                    project_name,
                    "--profile",
                    "dr-tools",
                    "down",
                    "--volumes",
                    "--remove-orphans",
                    "--timeout",
                    "20",
                ),
            )
            cleanup = "PASS" if down.returncode == 0 else "FAIL"
            remaining_containers = record(
                "remaining-project-containers",
                (
                    "docker",
                    "ps",
                    "--all",
                    "--filter",
                    f"label={PROJECT_LABEL}={project_name}",
                    "--format",
                    "{{.ID}}",
                ),
            )
            remaining_volumes = record(
                "remaining-project-volumes",
                (
                    "docker",
                    "volume",
                    "ls",
                    "--filter",
                    f"label={PROJECT_LABEL}={project_name}",
                    "--format",
                    "{{.Name}}",
                ),
            )
            if (
                down.returncode != 0
                or remaining_containers.returncode != 0
                or remaining_volumes.returncode != 0
                or remaining_containers.stdout.strip()
                or remaining_volumes.stdout.strip()
            ):
                cleanup = "FAIL"
                failure = failure or "isolated DR project cleanup was incomplete"
        if backup_dir is not None and backup_dir.exists():
            shutil.rmtree(backup_dir)
        if backup_root.exists():
            try:
                backup_root.rmdir()
            except OSError:
                pass

        status = "PASS" if failure is None else "FAIL"
        summary = {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "git_sha": git_sha,
            "status": status,
            "project_name": project_name,
            "compose_file": str(COMPOSE_FILE),
            "deleted_volumes": deleted_volumes,
            "seeded_marker": seeded,
            "restored_marker": restored,
            "cleanup": cleanup,
            "backup_manifest": "backup-manifest.json"
            if (artifact_dir / "backup-manifest.json").is_file()
            else None,
            "backup_checksums": "backup-SHA256SUMS"
            if (artifact_dir / "backup-SHA256SUMS").is_file()
            else None,
            "commands": commands,
            "failure": failure,
            "evidence_boundary": (
                "This rehearsal destroys only volumes whose exact Compose project and logical "
                "volume labels match the unique disposable project. It is not production HA."
            ),
        }
        (artifact_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    if failure is not None:
        raise DRRehearsalError(failure, artifact_dir=artifact_dir)
    return DRRun(artifact_dir, project_name, "PASS")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run isolated destructive backup/restore rehearsal"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("build/dr-rehearsal"))
    parser.add_argument("--command-timeout", type=float, default=900)
    args = parser.parse_args()
    try:
        result = run_dr_rehearsal(
            args.output_dir,
            confirmed=os.getenv("CONFIRM_DR") == "YES",
            command_timeout_seconds=args.command_timeout,
        )
    except DRRehearsalError as exc:
        if exc.artifact_dir is not None:
            print(f"DR diagnostics: {exc.artifact_dir}")
        raise SystemExit(f"DR rehearsal failed: {exc}") from exc
    print(f"DR rehearsal PASS: {result.artifact_dir}")


if __name__ == "__main__":
    main()
