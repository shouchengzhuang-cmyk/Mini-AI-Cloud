from __future__ import annotations

import json
import re
import shlex
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

ScenarioName = Literal["fencing", "controller-adoption", "sse-drain"]
CommandRunner = Callable[[tuple[str, ...], Path], "CommandOutcome"]
ToolLookup = Callable[[str], str | None]

KIND_CLUSTER_NAME = "mini-ai-cloud-serving-v4a"
KIND_SCENARIOS: tuple[ScenarioName, ...] = ("controller-adoption", "sse-drain")


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    name: ScenarioName
    goal: str
    steps: tuple[str, ...]
    identity_change: str
    evidence_level: str
    success_marker: str | None = None


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class DemoRun:
    artifact_dir: Path
    scenarios: tuple[ScenarioName, ...]
    status: str


class HeroDemoError(RuntimeError):
    def __init__(self, message: str, *, artifact_dir: Path | None = None) -> None:
        super().__init__(message)
        self.artifact_dir = artifact_dir


SPECS: dict[ScenarioName, ScenarioSpec] = {
    "fencing": ScenarioSpec(
        name="fencing",
        goal="Prove a stale worker session cannot mutate or complete replacement-owned work.",
        steps=(
            "Create task and worker session A identities in an isolated test database.",
            "Re-register the same worker as session B and attempt stale writes from A.",
            "Verify stale heartbeat, lease, log, artifact, secret, and completion paths "
            "fail closed.",
        ),
        identity_change=(
            "session A becomes stale; session B remains authoritative while active reservations "
            "and the new execution identity remain consistent"
        ),
        evidence_level="INTEGRATION_SQLITE",
    ),
    "controller-adoption": ScenarioSpec(
        name="controller-adoption",
        goal="Prove a restarted controller adopts an exact healthy Kubernetes workload.",
        steps=(
            "Create the isolated Kind serving stack and one healthy service Replica.",
            "Record Pod name, execution ID, and Replica ID, then restart the API/controller.",
            "Verify the same identity tuple remains and no duplicate per-Replica Service appears.",
        ),
        identity_change=(
            "Pod name, execution ID, and Replica ID remain unchanged across controller restart"
        ),
        evidence_level="KIND",
        success_marker="PASS: controller rollout restart adopted the existing healthy Replica",
    ),
    "sse-drain": ScenarioSpec(
        name="sse-drain",
        goal="Prove scale-down preserves an active SSE request before deleting its Replica.",
        steps=(
            "Scale the isolated Kind service from two to four ready Replicas.",
            "Hold an SSE request active on the selected Replica and scale from four to one.",
            "Verify draining blocks new routing, the stream completes, and deletion happens after.",
        ),
        identity_change=(
            "the selected Replica transitions ready to draining, stops receiving new requests, "
            "then is deleted only after its active request completes"
        ),
        evidence_level="KIND",
        success_marker="PASS: scale 4 to 1 drained an active SSE request before Pod deletion",
    ),
}


def run_hero_scenarios(
    scenarios: Sequence[ScenarioName],
    output_root: Path,
    *,
    repository_root: Path | None = None,
    command_runner: CommandRunner | None = None,
    tool_lookup: ToolLookup = shutil.which,
) -> DemoRun:
    selected = _normalize_scenarios(scenarios)
    root = (repository_root or Path.cwd()).resolve()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"-{uuid.uuid4().hex[:8]}"
    artifact_dir = output_root.resolve() / run_id
    log_dir = artifact_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=False)
    runner = command_runner or _run_command
    commands: list[dict[str, object]] = []
    results: dict[ScenarioName, dict[str, object]] = {
        name: {
            "name": name,
            "goal": SPECS[name].goal,
            "steps": list(SPECS[name].steps),
            "identity_or_status_change": SPECS[name].identity_change,
            "evidence_level": SPECS[name].evidence_level,
            "status": "PENDING",
            "verified_marker": None,
            "cleanup": "not required" if name == "fencing" else "not started",
        }
        for name in selected
    }
    failure: str | None = None
    kind_selected = tuple(name for name in selected if name in KIND_SCENARIOS)

    print(f"Hero demo artifacts: {artifact_dir}")
    for name in selected:
        print(f"[{name}] goal: {SPECS[name].goal}")
        for index, step in enumerate(SPECS[name].steps, start=1):
            print(f"[{name}] step {index}: {step}")

    try:
        _preflight(
            root,
            require_kind=bool(kind_selected),
            runner=runner,
            tool_lookup=tool_lookup,
            commands=commands,
            log_dir=log_dir,
        )
        if "fencing" in selected:
            outcome = _record_command(
                "fencing",
                ("uv", "run", "pytest", "-q", "tests/integration/test_worker_session_fencing.py"),
                root,
                runner,
                commands,
                log_dir,
            )
            if outcome.returncode != 0:
                raise HeroDemoError("fencing test command failed")
            results["fencing"]["status"] = "PASS"
            print(f"[fencing] PASS: {SPECS['fencing'].identity_change}")
            print("[fencing] cleanup: not required (isolated pytest fixtures removed)")

        if kind_selected:
            _run_kind_suite(
                kind_selected,
                root=root,
                runner=runner,
                commands=commands,
                log_dir=log_dir,
                results=results,
            )
    except (HeroDemoError, OSError) as exc:
        failure = str(exc)
        for name in selected:
            if results[name]["status"] == "PENDING":
                results[name]["status"] = "FAIL"
        print(f"Hero demo FAILED: {failure}")
    finally:
        revision = _revision(root, runner)
        for name in selected:
            _write_json(artifact_dir / f"{name}.json", {"git_sha": revision, **results[name]})
        status = "PASS" if failure is None else "FAIL"
        _write_json(
            artifact_dir / "summary.json",
            {
                "schema_version": "1.0.0",
                "run_id": run_id,
                "git_sha": revision,
                "status": status,
                "scenarios": [results[name] for name in selected],
                "commands": commands,
                "failure": failure,
            },
        )
        for name in selected:
            print(f"[{name}] cleanup result: {results[name]['cleanup']}")
        print(f"Hero demo result: {status}")
        print(f"Evidence artifact: {artifact_dir / 'summary.json'}")

    if failure is not None:
        raise HeroDemoError(failure, artifact_dir=artifact_dir)
    return DemoRun(artifact_dir=artifact_dir, scenarios=selected, status="PASS")


def _run_kind_suite(
    selected: tuple[ScenarioName, ...],
    *,
    root: Path,
    runner: CommandRunner,
    commands: list[dict[str, object]],
    log_dir: Path,
    results: dict[ScenarioName, dict[str, object]],
) -> None:
    up_attempted = False
    failure: HeroDemoError | None = None
    test_output = ""
    try:
        up_attempted = True
        up = _record_command(
            "kind-up",
            ("make", "kind-serving-up"),
            root,
            runner,
            commands,
            log_dir,
        )
        if up.returncode != 0:
            _record_diagnostics(root, runner, commands, log_dir)
            raise HeroDemoError("Kind serving setup failed")
        tested = _record_command(
            "kind-test",
            ("make", "test-kind-serving"),
            root,
            runner,
            commands,
            log_dir,
        )
        test_output = f"{tested.stdout}\n{tested.stderr}"
        if tested.returncode != 0:
            _record_diagnostics(root, runner, commands, log_dir)
            raise HeroDemoError("Kind serving E2E failed")
        for name in selected:
            marker = SPECS[name].success_marker
            if marker is None or marker not in test_output:
                _record_diagnostics(root, runner, commands, log_dir)
                raise HeroDemoError(f"Kind output omitted success marker for {name}")
            results[name]["status"] = "PASS"
            results[name]["verified_marker"] = marker
            print(f"[{name}] PASS: {SPECS[name].identity_change}")
    except HeroDemoError as exc:
        failure = exc
    finally:
        if up_attempted:
            down = _record_command(
                "kind-down",
                ("make", "kind-serving-down"),
                root,
                runner,
                commands,
                log_dir,
            )
            cleanup = "PASS" if down.returncode == 0 else "FAIL"
            for name in selected:
                results[name]["cleanup"] = cleanup
            print(f"[kind] cleanup: {cleanup}")
            if down.returncode != 0:
                for name in selected:
                    results[name]["status"] = "FAIL"
                _record_diagnostics(root, runner, commands, log_dir)
                failure = failure or HeroDemoError("Kind serving cleanup failed")
    if failure is not None:
        raise failure


def _preflight(
    root: Path,
    *,
    require_kind: bool,
    runner: CommandRunner,
    tool_lookup: ToolLookup,
    commands: list[dict[str, object]],
    log_dir: Path,
) -> None:
    required_files = [root / "pyproject.toml"]
    tools = ["uv"]
    if require_kind:
        required_files.extend(
            [root / "scripts" / "kind_serving.sh", root / "scripts" / "kind_serving_e2e.py"]
        )
        tools.extend(["docker", "kind", "kubectl", "make"])
    missing_files = [str(path) for path in required_files if not path.is_file()]
    missing_tools = [tool for tool in tools if tool_lookup(tool) is None]
    if missing_files or missing_tools:
        details = []
        if missing_files:
            details.append(f"missing files: {', '.join(missing_files)}")
        if missing_tools:
            details.append(f"missing commands: {', '.join(missing_tools)}")
        raise HeroDemoError("preflight failed: " + "; ".join(details))
    if not require_kind:
        print("Preflight PASS: repository and uv are available")
        return
    docker_info = _record_command(
        "docker-info", ("docker", "info"), root, runner, commands, log_dir
    )
    if docker_info.returncode != 0:
        raise HeroDemoError("preflight failed: Docker Engine is unreachable")
    clusters = _record_command(
        "kind-clusters", ("kind", "get", "clusters"), root, runner, commands, log_dir
    )
    if clusters.returncode != 0:
        raise HeroDemoError("preflight failed: kind cluster listing failed")
    if KIND_CLUSTER_NAME in {line.strip() for line in clusters.stdout.splitlines()}:
        raise HeroDemoError(
            f"preflight refused existing dedicated cluster {KIND_CLUSTER_NAME!r}; "
            "clean it explicitly before running the demo"
        )
    print("Preflight PASS: Docker, kind, kubectl, make, and uv are available")


def _record_diagnostics(
    root: Path,
    runner: CommandRunner,
    commands: list[dict[str, object]],
    log_dir: Path,
) -> None:
    _record_command(
        "kind-diagnostics",
        ("bash", "scripts/kind_serving.sh", "diagnostics"),
        root,
        runner,
        commands,
        log_dir,
    )


def _record_command(
    label: str,
    argv: tuple[str, ...],
    root: Path,
    runner: CommandRunner,
    commands: list[dict[str, object]],
    log_dir: Path,
) -> CommandOutcome:
    outcome = runner(argv, root)
    safe_output = _redact(
        f"$ {shlex.join(argv)}\n\nSTDOUT\n{outcome.stdout}\n\nSTDERR\n{outcome.stderr}"
    )
    log_path = log_dir / f"{len(commands) + 1:02d}-{label}.log"
    log_path.write_text(safe_output, encoding="utf-8")
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


def _run_command(argv: tuple[str, ...], cwd: Path) -> CommandOutcome:
    started = time.monotonic()
    completed = subprocess.run(
        argv,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    return CommandOutcome(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_seconds=time.monotonic() - started,
    )


def _revision(root: Path, runner: CommandRunner) -> str | None:
    outcome = runner(("git", "rev-parse", "HEAD"), root)
    value = outcome.stdout.strip()
    if outcome.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", value):
        return value
    return None


def _normalize_scenarios(scenarios: Sequence[ScenarioName]) -> tuple[ScenarioName, ...]:
    selected = tuple(dict.fromkeys(scenarios))
    if not selected:
        raise HeroDemoError("at least one hero scenario is required")
    unknown = sorted(set(selected) - set(SPECS))
    if unknown:
        raise HeroDemoError(f"unknown hero scenarios: {', '.join(unknown)}")
    return selected


def _redact(value: str) -> str:
    value = re.sub(r"mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}", "[REDACTED_API_KEY]", value)
    value = re.sub(r"(?i)(authorization:\s*bearer\s+)\S+", r"\1[REDACTED]", value)
    value = re.sub(r"(?m)^(KIND_SERVING_[A-Z0-9_]+)=.*$", r"\1=[REDACTED]", value)
    return re.sub(
        r"(postgresql(?:\+asyncpg)?://[^:\s]+:)[^@\s]+(@)",
        r"\1[REDACTED]\2",
        value,
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
