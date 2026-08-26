from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]

DeploymentStatus = Literal["NOT_DEPLOYED", "UNKNOWN"]

_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s]+"),
    re.compile(r"(?i)((?:api[_-]?key|token|password|secret)\s*[=:]\s*)[^\s,;]+"),
    re.compile(r"\b(?:ghp|github_pat)_[A-Za-z0-9_]{8,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{12,}\b"),
    re.compile(r"\bmkc_[A-Za-z0-9_-]{12,}\b"),
)
_URL_CREDENTIALS = re.compile(r"://[^/\s:@]+:[^@\s/]+@")


@dataclass(frozen=True, slots=True)
class CommandResult:
    label: str
    argv: tuple[str, ...]
    status: str
    exit_code: int | None
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    path: Path
    git_sha: str
    dirty: bool


CommandRunner = Callable[[tuple[str, ...], Path, str], CommandResult]


class EvidenceCollectionError(RuntimeError):
    pass


def redact_text(value: str) -> str:
    redacted = _URL_CREDENTIALS.sub("://[REDACTED]@", value)
    for pattern in _SECRET_PATTERNS:
        if pattern.groups:
            redacted = pattern.sub(r"\1[REDACTED]", redacted)
        else:
            redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


def _run_command(argv: tuple[str, ...], cwd: Path, label: str) -> CommandResult:
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except FileNotFoundError:
        return CommandResult(label, argv, "NOT_AVAILABLE", None, "", "tool not installed")
    except subprocess.TimeoutExpired:
        return CommandResult(label, argv, "TIMEOUT", None, "", "command timed out")
    return CommandResult(
        label=label,
        argv=argv,
        status="PASS" if completed.returncode == 0 else "FAIL",
        exit_code=completed.returncode,
        stdout=redact_text(completed.stdout[:8192]),
        stderr=redact_text(completed.stderr[:8192]),
    )


def _required_git_result(result: CommandResult, purpose: str) -> str:
    if result.exit_code != 0:
        raise EvidenceCollectionError(f"cannot {purpose}: {result.stderr or result.status}")
    value = result.stdout.strip()
    if not value:
        raise EvidenceCollectionError(f"cannot {purpose}: empty git output")
    return value


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise EvidenceCollectionError(f"{path} must contain a mapping")
    return raw


def _claim_status(records: list[dict[str, Any]], git_sha: str) -> str:
    statuses: list[str] = []
    for record in records:
        status = str(record.get("status", "PENDING"))
        verified_commit = record.get("verified_commit")
        if status == "PASS" and verified_commit != git_sha:
            status = "STALE"
        statuses.append(status)
    if statuses and all(status == "PASS" for status in statuses):
        return "PASS"
    if any(status in {"FAIL", "STALE"} for status in statuses):
        return "FAIL"
    if statuses and all(status == "NOT_RUN" for status in statuses):
        return "NOT_RUN"
    return "PENDING"


def _claim_payload(repository_root: Path, git_sha: str) -> tuple[list[dict[str, Any]], list[str]]:
    raw = _load_yaml_mapping(repository_root / "evidence" / "claims.yaml")
    source_claims = raw.get("claims")
    if not isinstance(source_claims, list):
        raise EvidenceCollectionError("evidence/claims.yaml must contain a claims list")
    claims: list[dict[str, Any]] = []
    limitations: set[str] = set()
    for source in source_claims:
        if not isinstance(source, dict):
            raise EvidenceCollectionError("each claim must be a mapping")
        records = source.get("evidence", [])
        if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
            raise EvidenceCollectionError("claim evidence must be a list of mappings")
        known = source.get("known_limitations", [])
        if not isinstance(known, list) or any(not isinstance(item, str) for item in known):
            raise EvidenceCollectionError("known limitations must be strings")
        limitations.update(known)
        claims.append(
            {
                "id": source.get("id"),
                "description": source.get("description"),
                "status": _claim_status(records, git_sha),
                "required_environment_ids": source.get("required_environment_ids", []),
                "invariant_ids": source.get("invariant_ids", []),
                "evidence": records,
                "known_limitations": known,
            }
        )
    return sorted(claims, key=lambda item: str(item["id"])), sorted(limitations)


def _json_text(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"


def _write_json(path: Path, value: object) -> None:
    path.write_text(_json_text(value), encoding="utf-8", newline="\n")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _contract_hashes(repository_root: Path) -> dict[str, str]:
    names = ("claims.yaml", "environments.yaml", "invariants.yaml", "schema.json", "matrix.md")
    return {name: _sha256(repository_root / "evidence" / name) for name in names}


def _probe_commands(repository_root: Path, runner: CommandRunner) -> list[CommandResult]:
    probes: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("python-version", (sys.executable, "--version")),
        ("docker-version", ("docker", "version", "--format", "{{.Client.Version}}")),
        ("kind-version", ("kind", "version")),
        ("kubectl-version", ("kubectl", "version", "--client=true", "--output=json")),
    )
    return [runner(argv, repository_root, label) for label, argv in probes]


def _summary_markdown(
    git_sha: str,
    dirty: bool,
    deployment_status: DeploymentStatus,
    claims: list[dict[str, Any]],
    limitations: list[str],
) -> str:
    counts = {
        status: sum(claim["status"] == status for claim in claims)
        for status in ("PASS", "PENDING", "NOT_RUN", "FAIL")
    }
    lines = [
        "# Mini AI Cloud evidence summary",
        "",
        f"- Git SHA: `{git_sha}`",
        f"- Dirty tree: `{'YES' if dirty else 'NO'}`",
        f"- Deployment status: `{deployment_status}`",
        f"- Claims: PASS={counts['PASS']}, PENDING={counts['PENDING']}, "
        f"NOT_RUN={counts['NOT_RUN']}, FAIL={counts['FAIL']}",
        "",
        "## Claim status",
        "",
        "| Claim | Status |",
        "|---|---|",
        *[f"| `{claim['id']}` | `{claim['status']}` |" for claim in claims],
        "",
        "## Known limitations",
        "",
        *[f"- {item}" for item in limitations],
        "",
        "This bundle reports only registered evidence. PENDING and NOT_RUN are not PASS.",
        "",
    ]
    return "\n".join(lines)


def collect_evidence(
    output_root: Path = Path("build/evidence"),
    *,
    repository_root: Path | None = None,
    allow_dirty: bool = False,
    deployment_status: DeploymentStatus = "NOT_DEPLOYED",
    runner: CommandRunner = _run_command,
    started_at: datetime | None = None,
) -> EvidenceBundle:
    root = (repository_root or Path.cwd()).resolve()
    revision_result = runner(("git", "rev-parse", "HEAD"), root, "git-revision")
    git_sha = _required_git_result(revision_result, "resolve HEAD")
    status_result = runner(("git", "status", "--porcelain=v1"), root, "git-status")
    if status_result.exit_code != 0:
        raise EvidenceCollectionError(f"cannot inspect git status: {status_result.stderr}")
    dirty = bool(status_result.stdout.strip())
    if dirty and not allow_dirty:
        raise EvidenceCollectionError(
            "working tree is dirty; commit changes or pass --allow-dirty for non-release evidence"
        )

    bundle = output_root.resolve() / git_sha
    if bundle.exists():
        raise EvidenceCollectionError(f"evidence bundle already exists: {bundle}")
    (bundle / "test-results").mkdir(parents=True)
    (bundle / "diagnostics").mkdir()

    claims, limitations = _claim_payload(root, git_sha)
    probes = _probe_commands(root, runner)
    commands = [revision_result, status_result, *probes]
    command_payload = [
        {
            **asdict(result),
            "argv": list(result.argv),
            "stdout": redact_text(result.stdout),
            "stderr": redact_text(result.stderr),
        }
        for result in commands
    ]
    _write_json(bundle / "claims.json", {"schema_version": "1.0.0", "claims": claims})
    _write_json(bundle / "commands.json", {"schema_version": "1.0.0", "commands": command_payload})
    _write_json(
        bundle / "environment.json",
        {
            "schema_version": "1.0.0",
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "operating_system": platform.system(),
            "machine": platform.machine(),
            "tools": {
                result.label: {
                    "status": result.status,
                    "exit_code": result.exit_code,
                    "version": redact_text(result.stdout.strip()),
                }
                for result in probes
            },
            "environment_variables_collected": False,
        },
    )
    (bundle / "test-results" / "README.md").write_text(
        "No test result was inferred from file presence. Claim status comes from the committed "
        "evidence contract.\n",
        encoding="utf-8",
        newline="\n",
    )
    (bundle / "diagnostics" / "README.md").write_text(
        "Version probe output is redacted and recorded in ../commands.json. No environment "
        "variables, kubeconfig, credentials, or database contents were collected.\n",
        encoding="utf-8",
        newline="\n",
    )
    (bundle / "summary.md").write_text(
        _summary_markdown(git_sha, dirty, deployment_status, claims, limitations),
        encoding="utf-8",
        newline="\n",
    )

    artifact_paths = sorted(
        path for path in bundle.rglob("*") if path.is_file() and path.name != "manifest.json"
    )
    artifact_hashes = {
        path.relative_to(bundle).as_posix(): _sha256(path) for path in artifact_paths
    }
    timestamp = (started_at or datetime.now(UTC)).astimezone(UTC).isoformat()
    manifest = {
        "schema_version": "1.0.0",
        "git": {"sha": git_sha, "dirty": dirty, "allow_dirty": allow_dirty},
        "execution": {"started_at": timestamp, "collector": "mini-cloud evidence collect"},
        "claim_status": {str(claim["id"]): claim["status"] for claim in claims},
        "contract_hashes": _contract_hashes(root),
        "artifact_hashes": artifact_hashes,
        "known_limitations": limitations,
        "deployment_status": deployment_status,
        "evidence_boundary": (
            "Only commands and committed statuses recorded in this bundle are evidence; "
            "missing tools and NOT_RUN/PENDING claims are not upgraded to PASS."
        ),
    }
    _write_json(bundle / "manifest.json", manifest)
    hash_lines = [
        f"{_sha256(path)}  {path.relative_to(bundle).as_posix()}"
        for path in sorted(path for path in bundle.rglob("*") if path.is_file())
        if path.name != "hashes.sha256"
    ]
    (bundle / "hashes.sha256").write_text(
        "\n".join(hash_lines) + "\n", encoding="utf-8", newline="\n"
    )
    return EvidenceBundle(bundle, git_sha, dirty)
