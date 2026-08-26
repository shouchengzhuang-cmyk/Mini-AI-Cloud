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

CLUSTER_NAME = "mini-ai-cloud-serving-v4a"
NAMESPACE = "mini-ai-cloud-serving"
MANAGED_SELECTOR = "mini-ai-cloud/managed=true"
REDIS_SELECTOR = "app.kubernetes.io/name=redis"
LEAK_QUERY = (
    "SELECT (SELECT count(*) FROM resource_reservations WHERE released_at IS NULL)::text"
    " || '|' || (SELECT count(*) FROM service_replicas WHERE status IN "
    "('pending','starting','loading','running','draining','stopping'))::text"
    " || '|' || (SELECT count(*) FROM service_replicas WHERE active_requests <> 0)::text;"
)


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class SoakRun:
    artifact_dir: Path
    status: str
    rounds: int


CommandRunner = Callable[[tuple[str, ...], Path, float], CommandOutcome]
ToolLookup = Callable[[str], str | None]


class SoakError(RuntimeError):
    def __init__(self, message: str, *, artifact_dir: Path | None = None) -> None:
        super().__init__(message)
        self.artifact_dir = artifact_dir


def _redact(value: str) -> str:
    redacted = re.sub(r"mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}", "[REDACTED_API_KEY]", value)
    redacted = re.sub(r"(?i)(authorization:\s*bearer\s+)\S+", r"\1[REDACTED]", redacted)
    redacted = re.sub(r"(?m)^(KIND_SERVING_[A-Z0-9_]+)=.*$", r"\1=[REDACTED]", redacted)
    redacted = re.sub(
        r"(?i)((?:password|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+",
        r"\1[REDACTED]",
        redacted,
    )
    return redacted


def _default_kubeconfig() -> Path:
    state_root = (
        os.getenv("XDG_RUNTIME_DIR") or os.getenv("RUNNER_TEMP") or os.getenv("TMPDIR") or "/tmp"
    )
    user_id = getattr(os, "getuid", lambda: 0)()
    state_dir = Path(
        os.getenv("KIND_SERVING_STATE_DIR")
        or Path(state_root) / f"mini-ai-cloud-kind-serving-{user_id}"
    )
    return state_dir / "kubeconfig"


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
        raise SoakError(message)


def _kubectl(kubeconfig: Path, *arguments: str) -> tuple[str, ...]:
    return ("kubectl", "--kubeconfig", str(kubeconfig), "-n", NAMESPACE, *arguments)


def _check_round_leaks(
    round_number: int,
    root: Path,
    kubeconfig: Path,
    timeout_seconds: float,
    runner: CommandRunner,
    commands: list[dict[str, object]],
    log_dir: Path,
) -> dict[str, object]:
    pods = _record_command(
        f"round-{round_number}-managed-pods",
        _kubectl(kubeconfig, "get", "pods", "-l", MANAGED_SELECTOR, "-o", "name"),
        root,
        timeout_seconds,
        runner,
        commands,
        log_dir,
    )
    services = _record_command(
        f"round-{round_number}-managed-services",
        _kubectl(kubeconfig, "get", "services", "-l", MANAGED_SELECTOR, "-o", "name"),
        root,
        timeout_seconds,
        runner,
        commands,
        log_dir,
    )
    database = _record_command(
        f"round-{round_number}-database-invariants",
        _kubectl(
            kubeconfig,
            "exec",
            "deployment/postgres",
            "--",
            "psql",
            "-U",
            "task",
            "-d",
            "task_platform",
            "-Atc",
            LEAK_QUERY,
        ),
        root,
        timeout_seconds,
        runner,
        commands,
        log_dir,
    )
    _require_success(pods, f"round {round_number}: managed Pod leak check failed")
    _require_success(services, f"round {round_number}: managed Service leak check failed")
    _require_success(database, f"round {round_number}: database invariant query failed")
    if pods.stdout.strip():
        raise SoakError(f"round {round_number}: managed serving Pods leaked after cleanup")
    if services.stdout.strip():
        raise SoakError(f"round {round_number}: managed serving Services leaked after cleanup")
    counts = database.stdout.strip().splitlines()[-1] if database.stdout.strip() else ""
    if counts != "0|0|0":
        raise SoakError(
            f"round {round_number}: reservation/replica/active-request leak counts were {counts!r}"
        )
    return {
        "round": round_number,
        "active_reservations": 0,
        "active_replicas": 0,
        "active_requests": 0,
        "managed_pods": 0,
        "managed_services": 0,
    }


def run_soak(
    rounds: int,
    output_root: Path,
    *,
    confirmed: bool,
    repository_root: Path | None = None,
    kubeconfig: Path | None = None,
    command_timeout_seconds: float = 600,
    total_timeout_seconds: float = 7200,
    runner: CommandRunner = _run_command,
    tool_lookup: ToolLookup = shutil.which,
) -> SoakRun:
    if not confirmed:
        raise SoakError("CONFIRM_SOAK=YES is required")
    if not 1 <= rounds <= 10:
        raise SoakError("rounds must be between 1 and 10")
    if command_timeout_seconds <= 0 or total_timeout_seconds <= 0:
        raise SoakError("timeouts must be positive")

    root = (repository_root or Path.cwd()).resolve()
    resolved_kubeconfig = (kubeconfig or _default_kubeconfig()).resolve()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-{uuid.uuid4().hex[:8]}"
    artifact_dir = output_root.resolve() / run_id
    log_dir = artifact_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=False)
    commands: list[dict[str, object]] = []
    snapshots: list[dict[str, object]] = []
    started = time.monotonic()
    cluster_owned = False
    failure: str | None = None

    def record(label: str, argv: tuple[str, ...]) -> CommandOutcome:
        if time.monotonic() - started >= total_timeout_seconds:
            raise SoakError("total soak timeout exceeded")
        return _record_command(
            label,
            argv,
            root,
            min(command_timeout_seconds, total_timeout_seconds - (time.monotonic() - started)),
            runner,
            commands,
            log_dir,
        )

    try:
        missing = [
            name for name in ("docker", "kind", "kubectl", "make", "uv") if not tool_lookup(name)
        ]
        if missing:
            raise SoakError(f"preflight missing commands: {', '.join(missing)}")
        _require_success(record("docker-info", ("docker", "info")), "Docker Engine is unreachable")
        clusters = record("kind-clusters-before", ("kind", "get", "clusters"))
        _require_success(clusters, "cannot list Kind clusters")
        if CLUSTER_NAME in {line.strip() for line in clusters.stdout.splitlines()}:
            raise SoakError(f"refused existing dedicated cluster {CLUSTER_NAME!r}")

        cluster_owned = True
        _require_success(record("kind-up", ("make", "kind-serving-up")), "Kind setup failed")
        for round_number in range(1, rounds + 1):
            _require_success(
                record(
                    f"round-{round_number}-worker-fencing",
                    (
                        "uv",
                        "run",
                        "pytest",
                        "-q",
                        "tests/integration/test_worker_session_fencing.py",
                    ),
                ),
                f"round {round_number}: worker session fencing failed",
            )
            _require_success(
                record(f"round-{round_number}-kind-e2e", ("make", "test-kind-serving")),
                f"round {round_number}: Kind serving lifecycle failed",
            )
            _require_success(
                record(
                    f"round-{round_number}-api-controller-restart",
                    _kubectl(
                        resolved_kubeconfig,
                        "rollout",
                        "restart",
                        "deployment/mini-ai-cloud-api",
                    ),
                ),
                f"round {round_number}: API/controller restart failed",
            )
            _require_success(
                record(
                    f"round-{round_number}-api-controller-ready",
                    _kubectl(
                        resolved_kubeconfig,
                        "rollout",
                        "status",
                        "deployment/mini-ai-cloud-api",
                        "--timeout=180s",
                    ),
                ),
                f"round {round_number}: API/controller did not become ready",
            )
            _require_success(
                record(
                    f"round-{round_number}-redis-interruption",
                    _kubectl(
                        resolved_kubeconfig,
                        "delete",
                        "pod",
                        "-l",
                        REDIS_SELECTOR,
                        "--wait=true",
                    ),
                ),
                f"round {round_number}: Redis interruption failed",
            )
            _require_success(
                record(
                    f"round-{round_number}-redis-ready",
                    _kubectl(
                        resolved_kubeconfig,
                        "rollout",
                        "status",
                        "deployment/redis",
                        "--timeout=120s",
                    ),
                ),
                f"round {round_number}: Redis did not recover",
            )
            _require_success(
                record(
                    f"round-{round_number}-api-ready-after-redis",
                    (
                        "uv",
                        "run",
                        "python",
                        "scripts/kind_serving_e2e.py",
                        "wait-ready",
                        "--base-url",
                        "http://127.0.0.1:18080",
                        "--timeout",
                        "120",
                    ),
                ),
                f"round {round_number}: API did not recover after Redis interruption",
            )
            snapshot = _check_round_leaks(
                round_number,
                root,
                resolved_kubeconfig,
                command_timeout_seconds,
                runner,
                commands,
                log_dir,
            )
            snapshots.append(snapshot)
            (artifact_dir / f"round-{round_number:02d}.json").write_text(
                json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
                newline="\n",
            )
    except SoakError as exc:
        failure = str(exc)
        if cluster_owned:
            _record_command(
                "kind-diagnostics",
                ("bash", "scripts/kind_serving.sh", "diagnostics"),
                root,
                command_timeout_seconds,
                runner,
                commands,
                log_dir,
            )
    finally:
        cleanup_status = "NOT_REQUIRED"
        if cluster_owned:
            down = _record_command(
                "kind-down",
                ("make", "kind-serving-down"),
                root,
                command_timeout_seconds,
                runner,
                commands,
                log_dir,
            )
            cleanup_status = "PASS" if down.returncode == 0 else "FAIL"
            if down.returncode != 0:
                failure = failure or "Kind cleanup failed"
            clusters_after = _record_command(
                "kind-clusters-after",
                ("kind", "get", "clusters"),
                root,
                command_timeout_seconds,
                runner,
                commands,
                log_dir,
            )
            if clusters_after.returncode != 0 or CLUSTER_NAME in {
                line.strip() for line in clusters_after.stdout.splitlines()
            }:
                cleanup_status = "FAIL"
                failure = failure or "dedicated Kind cluster remained after cleanup"

        status = "PASS" if failure is None else "FAIL"
        summary = {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "status": status,
            "rounds_requested": rounds,
            "rounds_completed": len(snapshots),
            "command_timeout_seconds": command_timeout_seconds,
            "total_timeout_seconds": total_timeout_seconds,
            "cleanup": cleanup_status,
            "snapshots": snapshots,
            "commands": commands,
            "failure": failure,
            "evidence_boundary": (
                "Kind is single-host and uses fake inference. The worker fencing test uses "
                "isolated test fixtures; this is not a production SLO or real GPU result."
            ),
        }
        (artifact_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    if failure is not None:
        raise SoakError(failure, artifact_dir=artifact_dir)
    return SoakRun(artifact_dir, "PASS", rounds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded restart and fencing soak")
    parser.add_argument("--rounds", type=int, default=int(os.getenv("SOAK_ROUNDS", "3")))
    parser.add_argument("--output-dir", type=Path, default=Path("build/soak"))
    parser.add_argument("--command-timeout", type=float, default=600)
    parser.add_argument("--total-timeout", type=float, default=7200)
    args = parser.parse_args()
    try:
        result = run_soak(
            args.rounds,
            args.output_dir,
            confirmed=os.getenv("CONFIRM_SOAK") == "YES",
            command_timeout_seconds=args.command_timeout,
            total_timeout_seconds=args.total_timeout,
        )
    except SoakError as exc:
        if exc.artifact_dir is not None:
            print(f"Soak diagnostics: {exc.artifact_dir}")
        raise SystemExit(f"soak failed: {exc}") from exc
    print(f"Soak PASS: {result.rounds} rounds; evidence: {result.artifact_dir}")


if __name__ == "__main__":
    main()
