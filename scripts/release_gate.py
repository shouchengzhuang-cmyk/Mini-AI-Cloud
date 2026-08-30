from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from typer._click.core import Command
from typer.core import TyperGroup, TyperOption
from typer.main import get_command

RELEASE_VERSION = "0.6.0"
# Pin this alongside RELEASE_VERSION so retries cannot discover a different predecessor.
PREVIOUS_RELEASE_TAG: str | None = "v0.5.0"
ACTION_SHA = re.compile(r"^[0-9a-f]{40}$")
PINNED_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
SECRET_PATTERNS = {
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "github-token": re.compile(r"\b(?:github_pat_[A-Za-z0-9_]{20,}|ghp_[A-Za-z0-9]{20,})\b"),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "mini-cloud-api-key": re.compile(r"\bmkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}\b"),
}
SNAPSHOT_DIR = Path("contracts/release")
READINESS_CONTRACT = SNAPSHOT_DIR / "v0.6-readiness.json"
CHART_ROOT = Path("deploy/helm/mini-ai-cloud")
KIND_VERSION = "v0.27.0"
KUBERNETES_VERSION = "v1.32.2"
KIND_NODE_IMAGE = (
    "kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f"
)
P4_EVIDENCE_ENV = "P4_EVIDENCE_BUNDLE"
P4_EVIDENCE_DIR = "p4-kind-evidence"
P4_REQUIRED_FILES = (
    "manifest.json",
    "environment.json",
    "commands.jsonl",
    "claims.json",
    "kubernetes-summary.json",
    "cleanup.json",
    "limitations.md",
    "checksums.txt",
)
P4_REQUIRED_CLAIMS = (
    "helm-render",
    "helm-install",
    "migration",
    "control-plane-readiness",
    "worker-readiness",
    "batch-lifecycle",
    "serving-lifecycle",
    "accelerator-contract",
    "security-contract",
    "upgrade-smoke",
    "uninstall-cleanup",
)


class ReleaseGateError(RuntimeError):
    pass


def canonical_json(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def openapi_contract() -> dict[str, object]:
    from api.main import app

    schema = app.openapi()
    return {
        "schema_version": "1.0.0",
        "release_version": RELEASE_VERSION,
        "openapi": schema,
    }


def cli_contract() -> dict[str, object]:
    from cli.__main__ import app

    root = get_command(app)
    return {
        "schema_version": "1.0.0",
        "release_version": RELEASE_VERSION,
        "cli": _click_command(root),
    }


def _click_command(command: Command) -> dict[str, object]:
    parameters = []
    for parameter in command.params:
        options = list(parameter.opts) if isinstance(parameter, TyperOption) else []
        secondary = list(parameter.secondary_opts) if isinstance(parameter, TyperOption) else []
        parameters.append(
            {
                "name": parameter.name,
                "kind": type(parameter).__name__,
                "required": parameter.required,
                "nargs": parameter.nargs,
                "type": parameter.type.name,
                "options": sorted([*options, *secondary]),
            }
        )
    children = (
        {
            name: _click_command(child)
            for name, child in sorted(command.commands.items())
            if not child.hidden
        }
        if isinstance(command, TyperGroup)
        else {}
    )
    return {
        "name": command.name,
        "deprecated": bool(command.deprecated),
        "parameters": sorted(parameters, key=lambda item: str(item["name"])),
        "commands": children,
    }


def write_or_check_contracts(root: Path, *, write: bool) -> tuple[Path, Path]:
    paths = (root / SNAPSHOT_DIR / "openapi-v1.json", root / SNAPSHOT_DIR / "cli-v1.json")
    payloads = (canonical_json(openapi_contract()), canonical_json(cli_contract()))
    for path, payload in zip(paths, payloads, strict=True):
        if write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        elif not path.is_file() or path.read_bytes() != payload:
            raise ReleaseGateError(
                f"compatibility snapshot drifted: {path.relative_to(root)}; "
                "review the change and run release_gate.py contracts --write"
            )
    return paths


def validate_versions(root: Path) -> None:
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    version = project.get("project", {}).get("version")
    if version != RELEASE_VERSION:
        raise ReleaseGateError(f"pyproject version must be {RELEASE_VERSION}, found {version}")
    identity = (root / "core/project_identity.py").read_text(encoding="utf-8")
    if f'DEVELOPMENT_VERSION = "{RELEASE_VERSION}"' not in identity:
        raise ReleaseGateError("core project identity does not match the release version")
    readme = (root / "README.md").read_text(encoding="utf-8")
    if f"当前准备版本为 `{RELEASE_VERSION}`" not in readme:
        raise ReleaseGateError("README does not state the prepared release version")
    for link in (
        "docs/kubernetes-adaptation-v0.6.md",
        "docs/v0.6-release-readiness.md",
    ):
        if link not in readme:
            raise ReleaseGateError(f"README does not link the v0.6 release document: {link}")
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    if f"## [{RELEASE_VERSION}]" not in changelog:
        raise ReleaseGateError("CHANGELOG has no prepared release section")
    chart = _read_yaml_object(root / CHART_ROOT / "Chart.yaml")
    if chart.get("version") != RELEASE_VERSION or chart.get("appVersion") != RELEASE_VERSION:
        raise ReleaseGateError("Helm Chart version and appVersion must match the release version")
    values = _read_yaml_object(root / CHART_ROOT / "values.yaml")
    image = values.get("image")
    if not isinstance(image, dict) or image.get("tag") != RELEASE_VERSION:
        raise ReleaseGateError("Helm default application image tag must match the release version")
    workflow = (root / ".github/workflows/publish-release.yml").read_text(encoding="utf-8")
    if f"RELEASE_VERSION: {RELEASE_VERSION}" not in workflow:
        raise ReleaseGateError("release publication workflow version does not match")
    _validate_release_readiness(root)


def _validate_release_readiness(root: Path) -> None:
    contract = _read_json_object(root / READINESS_CONTRACT)
    expected = {
        "version": RELEASE_VERSION,
        "authorization_status": "READY_FOR_OWNER_AUTHORIZATION",
        "tag_status": "NOT_TAGGED",
        "release_status": "NOT_RELEASED",
        "deployment_status": "NOT_DEPLOYED",
        "real_hardware_status": "REAL_HW_NOT_RUN",
    }
    for field, value in expected.items():
        if contract.get(field) != value:
            raise ReleaseGateError(f"v0.6 readiness contract field {field} must be {value}")
    kind = contract.get("kind_evidence")
    if not isinstance(kind, dict) or kind != {
        "required_claim": "KIND_K8S_PASS",
        "status": "PENDING_FINAL_P4_EVIDENCE",
    }:
        raise ReleaseGateError("v0.6 readiness contract must keep final P4 evidence pending")
    matrix = contract.get("support_matrix")
    if not isinstance(matrix, list) or not matrix:
        raise ReleaseGateError("v0.6 readiness support matrix is empty")
    capabilities: set[str] = set()
    for item in matrix:
        if not isinstance(item, dict):
            raise ReleaseGateError("v0.6 readiness support matrix entry must be an object")
        capability = item.get("capability")
        if not isinstance(capability, str) or not capability or capability in capabilities:
            raise ReleaseGateError("v0.6 readiness capabilities must be non-empty and unique")
        capabilities.add(capability)
        if item.get("implementation") != "IMPLEMENTED":
            raise ReleaseGateError("v0.6 readiness matrix implementation state drifted")
        if item.get("kind_evidence") != "PENDING_FINAL_P4_EVIDENCE":
            raise ReleaseGateError("v0.6 readiness matrix cannot claim final P4 evidence yet")
        real_evidence = item.get("real_evidence")
        if real_evidence not in {"E1_NOT_RUN", "REAL_HW_NOT_RUN"}:
            raise ReleaseGateError("v0.6 readiness matrix cannot claim unrun real evidence")
        if capability == "runtime-profile-serving" and real_evidence != "REAL_HW_NOT_RUN":
            raise ReleaseGateError("runtime-profile serving must retain REAL_HW_NOT_RUN")
    if capabilities != {
        "helm-single-cluster-install",
        "fenced-kubernetes-batch-jobs",
        "runtime-profile-serving",
        "upgrade-uninstall-cleanup",
    }:
        raise ReleaseGateError("v0.6 readiness support matrix capabilities drifted")

    adaptation = (root / "docs/kubernetes-adaptation-v0.6.md").read_text(encoding="utf-8")
    readiness = (root / "docs/v0.6-release-readiness.md").read_text(encoding="utf-8")
    required_adaptation_tokens = (
        "external PostgreSQL",
        "namespaces.workload",
        "controlPlane.replicas",
        "batch/v1",
        "REAL_HW_NOT_RUN",
        "schedulerName: volcano",
        "/v1/chat/completions",
        "make test-kind-kubernetes-adaptation",
        "helm uninstall",
        "Optional E1-E3 evidence",
    )
    for token in required_adaptation_tokens:
        if token not in adaptation:
            raise ReleaseGateError(f"Kubernetes adaptation document is missing: {token}")
    for token in (
        "READY_FOR_OWNER_AUTHORIZATION",
        "PENDING_FINAL_P4_EVIDENCE",
        "REAL_HW_NOT_RUN",
        "NOT_TAGGED",
        "NOT_RELEASED",
        "NOT_DEPLOYED",
    ):
        if token not in readiness:
            raise ReleaseGateError(f"v0.6 release readiness document is missing: {token}")


def validate_action_pins(root: Path) -> int:
    workflows = sorted((root / ".github/workflows").glob("*.yml"))
    uses_count = 0
    failures: list[str] = []
    pattern = re.compile(r"^\s*-?\s*uses:\s*([^\s@]+)@([^\s#]+)")
    for workflow in workflows:
        for line_number, line in enumerate(workflow.read_text(encoding="utf-8").splitlines(), 1):
            match = pattern.match(line)
            if match is None:
                continue
            uses_count += 1
            action, revision = match.groups()
            if action.startswith("./"):
                continue
            if ACTION_SHA.fullmatch(revision) is None:
                failures.append(f"{workflow.relative_to(root)}:{line_number}: {action}@{revision}")
    if failures:
        raise ReleaseGateError("GitHub Actions must use full commit SHAs:\n" + "\n".join(failures))
    if uses_count == 0:
        raise ReleaseGateError("no GitHub Actions dependencies were found")
    return uses_count


def validate_locked_dependencies(root: Path) -> list[dict[str, str]]:
    lock = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))
    packages = lock.get("package")
    if not isinstance(packages, list) or not packages:
        raise ReleaseGateError("uv.lock contains no packages")
    components: list[dict[str, str]] = []
    failures: list[str] = []
    for raw in packages:
        if not isinstance(raw, dict):
            failures.append("uv.lock package entry is not a table")
            continue
        name = raw.get("name")
        version = raw.get("version")
        source = raw.get("source")
        if not isinstance(name, str) or not isinstance(version, str):
            failures.append(f"unversioned package entry: {raw}")
            continue
        if not isinstance(source, dict):
            failures.append(f"package {name} has no source")
            continue
        if "git" in source or "url" in source:
            failures.append(f"package {name} uses an unapproved direct dependency source")
        registry = source.get("registry")
        editable = source.get("editable")
        if registry not in {None, "https://pypi.org/simple"}:
            failures.append(f"package {name} uses unexpected registry {registry}")
        if registry is None and editable != ".":
            failures.append(f"package {name} has unsupported source {source}")
        components.append({"name": name, "version": version})
    if failures:
        raise ReleaseGateError("dependency lock validation failed:\n" + "\n".join(failures))
    return sorted(components, key=lambda item: (item["name"], item["version"]))


def scan_secret_text(value: str) -> tuple[str, ...]:
    return tuple(name for name, pattern in SECRET_PATTERNS.items() if pattern.search(value))


def scan_tracked_secrets(root: Path) -> int:
    result = _run(("git", "ls-files", "-z"), root)
    paths = [Path(item) for item in result.stdout.split("\0") if item]
    findings: list[str] = []
    scanned = 0
    for relative in paths:
        path = root / relative
        if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
            continue
        try:
            value = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        scanned += 1
        matches = scan_secret_text(value)
        if matches:
            findings.append(f"{relative.as_posix()}: {', '.join(matches)}")
    if findings:
        raise ReleaseGateError("tracked secret patterns detected:\n" + "\n".join(findings))
    return scanned


def validate_container_baseline(root: Path) -> None:
    dockerfile = (root / "docker/Dockerfile").read_text(encoding="utf-8")
    if "USER 10001:10001" not in dockerfile:
        raise ReleaseGateError("runtime image must use the non-root 10001:10001 user")
    if dockerfile.count("python -m pip uninstall --yes pip") != 2:
        raise ReleaseGateError("builder and runtime images must remove pip before release")
    if re.search(r"^FROM\s+\S+:latest(?:\s|$)", dockerfile, re.MULTILINE):
        raise ReleaseGateError("Dockerfile must not use a latest base tag")
    if re.search(
        r"^(?:ARG|ENV)\s+\S*(?:TOKEN|PASSWORD|SECRET|KEY)\s*=\s*\S+",
        dockerfile,
        re.MULTILINE | re.IGNORECASE,
    ):
        raise ReleaseGateError("Dockerfile must not bake credential values")


def chart_directory_digest(chart_root: Path) -> str:
    try:
        resolved = chart_root.resolve(strict=True)
    except OSError as exc:
        raise ReleaseGateError("Helm Chart directory is missing") from exc
    if not resolved.is_dir():
        raise ReleaseGateError("Helm Chart path is not a directory")
    files = sorted(path for path in resolved.rglob("*") if path.is_file())
    if not files:
        raise ReleaseGateError("Helm Chart directory contains no files")
    digest = hashlib.sha256()
    for path in files:
        if path.is_symlink():
            raise ReleaseGateError("Helm Chart directory must not contain symbolic links")
        relative = path.relative_to(resolved).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def validate_p4_evidence(
    root: Path,
    bundle_root: Path,
    *,
    expected_git_sha: str | None = None,
) -> dict[str, str]:
    if bundle_root.is_symlink():
        raise ReleaseGateError("P4 evidence bundle must not be a symbolic link")
    try:
        bundle = bundle_root.resolve(strict=True)
    except OSError as exc:
        raise ReleaseGateError(f"P4 evidence bundle is missing: {bundle_root}") from exc
    if not bundle.is_dir():
        raise ReleaseGateError("P4 evidence bundle path is not a directory")
    for path in bundle.rglob("*"):
        if path.is_symlink():
            raise ReleaseGateError("P4 evidence bundle must not contain symbolic links")
    for name in P4_REQUIRED_FILES:
        if not (bundle / name).is_file():
            raise ReleaseGateError(f"required P4 evidence file is missing: {name}")

    manifest = _read_json_object(bundle / "manifest.json")
    environment = _read_json_object(bundle / "environment.json")
    claims = _read_json_object(bundle / "claims.json")
    summary = _read_json_object(bundle / "kubernetes-summary.json")
    cleanup = _read_json_object(bundle / "cleanup.json")
    for filename, payload in (
        ("manifest.json", manifest),
        ("environment.json", environment),
        ("claims.json", claims),
        ("kubernetes-summary.json", summary),
        ("cleanup.json", cleanup),
    ):
        if payload.get("schema_version") != "1.0.0":
            raise ReleaseGateError(f"P4 evidence schema drifted: {filename}")

    if manifest.get("status") != "KIND_K8S_PASS":
        raise ReleaseGateError("P4 evidence must have status KIND_K8S_PASS; NOT_RUN is not PASS")
    if manifest.get("real_hardware_status") != "REAL_HW_NOT_RUN":
        raise ReleaseGateError("P4 evidence must preserve the REAL_HW_NOT_RUN boundary")
    if (
        manifest.get("kind_version") != KIND_VERSION
        or manifest.get("kubernetes_version") != KUBERNETES_VERSION
        or manifest.get("kind_node_image") != KIND_NODE_IMAGE
    ):
        raise ReleaseGateError("P4 evidence Kind or Kubernetes pins drifted")
    evidence_files = manifest.get("evidence_files")
    if not isinstance(evidence_files, list) or evidence_files != list(P4_REQUIRED_FILES):
        raise ReleaseGateError("P4 evidence manifest file contract drifted")
    run_id = manifest.get("run_id")
    identities = manifest.get("identities")
    started_at = manifest.get("started_at")
    ended_at = manifest.get("ended_at")
    if (
        not isinstance(run_id, str)
        or not run_id
        or not isinstance(identities, dict)
        or identities.get("run_id") != run_id
        or not isinstance(started_at, str)
        or not isinstance(ended_at, str)
    ):
        raise ReleaseGateError("P4 evidence run identity is invalid")
    started = _parse_utc_timestamp(started_at, description="P4 started_at")
    ended = _parse_utc_timestamp(ended_at, description="P4 ended_at")
    recorded_at = environment.get("recorded_at")
    if not isinstance(recorded_at, str):
        raise ReleaseGateError("P4 environment recorded_at is missing")
    recorded = _parse_utc_timestamp(recorded_at, description="P4 recorded_at")
    if ended < started or recorded < started or recorded > ended:
        raise ReleaseGateError("P4 evidence timestamps are out of order")
    identity_values = tuple(value for value in identities.values() if isinstance(value, str))
    expected_identity_fields = {
        "run_id",
        "cluster_name",
        "system_namespace",
        "workload_namespace",
        "release_name",
        "external_secret_name",
        "postgres_name",
        "redis_name",
    }
    if (
        set(identities) != expected_identity_fields
        or len(identity_values) != len(identities)
        or any(not value for value in identity_values)
        or len(identity_values) != len(set(identity_values))
    ):
        raise ReleaseGateError("P4 evidence identities must be non-empty unique strings")

    expected_sha = expected_git_sha or _git_sha(root)
    if environment.get("git_sha") != expected_sha:
        raise ReleaseGateError("P4 evidence Git SHA does not match the release commit")
    if environment.get("git_dirty") is not False:
        raise ReleaseGateError("P4 evidence must come from a clean worktree")
    if (
        environment.get("chart_version") != RELEASE_VERSION
        or environment.get("chart_app_version") != RELEASE_VERSION
    ):
        raise ReleaseGateError("P4 evidence Chart identity does not match v0.6.0")
    expected_chart_digest = chart_directory_digest(root / CHART_ROOT)
    if environment.get("chart_digest") != expected_chart_digest:
        raise ReleaseGateError("P4 evidence Chart digest does not match the release Chart")
    if (
        environment.get("kind_version") != KIND_VERSION
        or environment.get("kind_node_image") != KIND_NODE_IMAGE
        or environment.get("kubernetes_server_version") != KUBERNETES_VERSION
    ):
        raise ReleaseGateError("P4 environment Kind or Kubernetes pins drifted")
    images = environment.get("image_references")
    if not isinstance(images, dict) or not {"application", "postgres", "redis"}.issubset(images):
        raise ReleaseGateError("P4 evidence is missing required pinned image references")
    if any(
        not isinstance(value, str) or PINNED_IMAGE.fullmatch(value) is None
        for value in images.values()
    ):
        raise ReleaseGateError("P4 evidence images must use exact sha256 references")
    tools = environment.get("tool_versions")
    if not isinstance(tools, dict) or tools.get("kind") != KIND_VERSION:
        raise ReleaseGateError("P4 evidence tool versions do not bind the pinned Kind version")

    if claims.get("status") != "KIND_K8S_PASS":
        raise ReleaseGateError("P4 claim ledger is not KIND_K8S_PASS")
    if claims.get("real_hardware_status") != "REAL_HW_NOT_RUN":
        raise ReleaseGateError("P4 claim ledger must preserve REAL_HW_NOT_RUN")
    command_records = _read_p4_command_records(bundle / "commands.jsonl")
    claim_items = claims.get("claims")
    if not isinstance(claim_items, list):
        raise ReleaseGateError("P4 claim ledger has no claims")
    observed_claims: set[str] = set()
    for item in claim_items:
        if not isinstance(item, dict):
            raise ReleaseGateError("P4 claim entry must be an object")
        claim_id = item.get("id")
        command_ids = item.get("command_ids")
        if (
            not isinstance(claim_id, str)
            or claim_id not in P4_REQUIRED_CLAIMS
            or claim_id in observed_claims
            or item.get("status") != "PASS"
            or not isinstance(command_ids, list)
            or not command_ids
            or len(command_ids) != len(set(command_ids))
        ):
            raise ReleaseGateError("P4 claims must be unique PASS entries with command evidence")
        observed_claims.add(claim_id)
        for command_id in command_ids:
            record = command_records.get(command_id) if isinstance(command_id, str) else None
            if (
                record is None
                or record.get("claim_id") != claim_id
                or record.get("returncode") != 0
            ):
                raise ReleaseGateError(
                    "P4 claim references missing, failed, or cross-claim command evidence"
                )
    if observed_claims != set(P4_REQUIRED_CLAIMS):
        raise ReleaseGateError("P4 evidence does not contain every required claim")

    resources = summary.get("resources")
    resource_count = summary.get("resource_count")
    if (
        summary.get("status") != "PASS"
        or not isinstance(resource_count, int)
        or resource_count <= 0
        or not isinstance(resources, list)
        or len(resources) != resource_count
        or any(
            not isinstance(item, dict)
            or not isinstance(item.get("kind"), str)
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("uid"), str)
            for item in resources
        )
    ):
        raise ReleaseGateError("P4 Kubernetes summary is not PASS")
    if (
        cleanup.get("status") != "PASS"
        or cleanup.get("release_owned_remaining") != 0
        or cleanup.get("external_secret_preserved_after_uninstall") is not True
        or cleanup.get("external_namespaces_preserved_after_uninstall") is not True
        or cleanup.get("cluster_deleted") is not True
        or cleanup.get("default_kubeconfig_unchanged") is not True
        or cleanup.get("temporary_state_deleted") is not True
    ):
        raise ReleaseGateError("P4 evidence does not prove complete scoped cleanup")
    limitations = (bundle / "limitations.md").read_text(encoding="utf-8")
    if "REAL_HW_NOT_RUN" not in limitations:
        raise ReleaseGateError("P4 limitations must preserve REAL_HW_NOT_RUN")
    _validate_p4_checksums(bundle)

    return {
        "status": "KIND_K8S_PASS",
        "run_id": run_id,
        "git_sha": expected_sha,
        "chart_digest": expected_chart_digest,
        "checksums_sha256": f"sha256:{_sha256(bundle / 'checksums.txt')}",
    }


def _read_p4_command_records(path: Path) -> dict[str, dict[str, Any]]:
    bundle = path.parent
    records: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ReleaseGateError("P4 commands.jsonl contains invalid JSON") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != "1.0.0":
            raise ReleaseGateError("P4 command record schema is invalid")
        command_id = payload.get("command_id")
        claim_id = payload.get("claim_id")
        if (
            not isinstance(command_id, str)
            or not command_id
            or command_id in records
            or claim_id not in P4_REQUIRED_CLAIMS
        ):
            raise ReleaseGateError("P4 command IDs and claim bindings must be unique and valid")
        for field in ("stdout_log", "stderr_log"):
            relative = payload.get(field)
            if not isinstance(relative, str) or not relative:
                raise ReleaseGateError("P4 command record is missing a redacted log path")
            relative_path = Path(*relative.split("/"))
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise ReleaseGateError("P4 command log path is unsafe")
            log_path = bundle / relative_path
            try:
                resolved_log = log_path.resolve(strict=True)
            except OSError as exc:
                raise ReleaseGateError("P4 command record references a missing log") from exc
            if bundle not in resolved_log.parents or not resolved_log.is_file():
                raise ReleaseGateError("P4 command log path escapes the evidence bundle")
        records[command_id] = payload
    if not records:
        raise ReleaseGateError("P4 commands.jsonl is empty")
    return records


def _validate_p4_checksums(bundle: Path) -> None:
    checksum_path = bundle / "checksums.txt"
    lines = checksum_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ReleaseGateError("P4 evidence checksums are empty")
    observed: set[str] = set()
    for line in lines:
        digest, separator, relative = line.partition("  ")
        if (
            not separator
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or not relative
            or relative in observed
        ):
            raise ReleaseGateError("P4 evidence checksum line is invalid")
        relative_path = Path(*relative.split("/"))
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ReleaseGateError("P4 evidence checksum path is unsafe")
        path = bundle / relative_path
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise ReleaseGateError("checksummed P4 evidence file is missing") from exc
        if bundle not in resolved.parents or not resolved.is_file() or resolved.is_symlink():
            raise ReleaseGateError("P4 evidence checksum path escapes the bundle")
        if _sha256(resolved) != digest:
            raise ReleaseGateError(f"P4 evidence checksum mismatch: {relative}")
        observed.add(relative)
    expected = {
        path.relative_to(bundle).as_posix()
        for path in bundle.rglob("*")
        if path.is_file() and path != checksum_path
    }
    if observed != expected:
        raise ReleaseGateError("P4 checksums do not cover every evidence file exactly once")


def cyclonedx_sbom(components: list[dict[str, str]], git_sha: str) -> dict[str, object]:
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "serialNumber": f"urn:uuid:{_stable_uuid(git_sha)}",
        "version": 1,
        "metadata": {
            "component": {
                "type": "application",
                "name": "mini-ai-cloud",
                "version": RELEASE_VERSION,
                "bom-ref": f"pkg:pypi/mini-ai-cloud@{RELEASE_VERSION}",
                "purl": f"pkg:pypi/mini-ai-cloud@{RELEASE_VERSION}",
            },
            "properties": [
                {"name": "mini-ai-cloud:git-sha", "value": git_sha},
                {"name": "mini-ai-cloud:evidence", "value": "dependency lock snapshot"},
            ],
        },
        "components": [
            {
                "type": "library",
                "name": component["name"],
                "version": component["version"],
                "bom-ref": f"pkg:pypi/{component['name']}@{component['version']}",
                "purl": f"pkg:pypi/{component['name']}@{component['version']}",
            }
            for component in components
            if component["name"] != "mini-ai-cloud"
        ],
    }


def _stable_uuid(value: str) -> str:
    digest = hashlib.sha256(value.encode()).hexdigest()[:32]
    return f"{digest[:8]}-{digest[8:12]}-5{digest[13:16]}-a{digest[17:20]}-{digest[20:32]}"


def validate_release_inputs(
    root: Path,
    *,
    p4_evidence: Path | None = None,
    expected_git_sha: str | None = None,
) -> dict[str, object]:
    validate_versions(root)
    actions = validate_action_pins(root)
    dependencies = validate_locked_dependencies(root)
    files = scan_tracked_secrets(root)
    validate_container_baseline(root)
    write_or_check_contracts(root, write=False)
    result: dict[str, object] = {
        "action_dependencies": actions,
        "locked_packages": len(dependencies),
        "secret_scanned_files": files,
        "authorization_status": "READY_FOR_OWNER_AUTHORIZATION",
        "tag_status": "NOT_TAGGED",
        "release_status": "NOT_RELEASED",
        "deployment_status": "NOT_DEPLOYED",
        "real_hardware_status": "REAL_HW_NOT_RUN",
    }
    result["p4_evidence"] = (
        validate_p4_evidence(root, p4_evidence, expected_git_sha=expected_git_sha)
        if p4_evidence is not None
        else {"status": "PENDING_FINAL_P4_EVIDENCE"}
    )
    return result


def prepare_release_bundle(root: Path, output_root: Path, *, p4_evidence: Path | None) -> Path:
    git_sha = _git_sha(root)
    if p4_evidence is None:
        raise ReleaseGateError(
            "release preparation requires --p4-evidence with a real KIND_K8S_PASS bundle"
        )
    validation = validate_release_inputs(
        root,
        p4_evidence=p4_evidence,
        expected_git_sha=git_sha,
    )
    if _run(("git", "status", "--porcelain=v1"), root).stdout.strip():
        raise ReleaseGateError("release preparation requires a clean worktree")
    evidence = root / "build/evidence" / git_sha
    if not (evidence / "manifest.json").is_file():
        raise ReleaseGateError(f"commit-bound evidence bundle is missing: {evidence}")
    destination = output_root.resolve() / git_sha
    if destination.exists():
        raise ReleaseGateError(f"release preparation bundle already exists: {destination}")
    destination.mkdir(parents=True)
    dependencies = validate_locked_dependencies(root)
    _write_json(destination / "cyclonedx-sbom.json", cyclonedx_sbom(dependencies, git_sha))
    (destination / "release-notes.md").write_text(
        _release_notes(root, git_sha, validation),
        encoding="utf-8",
        newline="\n",
    )
    copied_p4 = destination / P4_EVIDENCE_DIR
    shutil.copytree(p4_evidence.resolve(strict=True), copied_p4)
    manifest = {
        "schema_version": "1.0.0",
        "version": RELEASE_VERSION,
        "git_sha": git_sha,
        "dirty_tree": False,
        "evidence_bundle": evidence.relative_to(root).as_posix(),
        "p4_evidence_bundle": P4_EVIDENCE_DIR,
        "p4_evidence": validation["p4_evidence"],
        "authorization_status": "READY_FOR_OWNER_AUTHORIZATION",
        "tag_status": "NOT_TAGGED",
        "release_status": "NOT_RELEASED",
        "github_release": "NOT_CREATED",
        "deployment_status": "NOT_DEPLOYED",
        "real_gpu": "REAL_HW_NOT_RUN",
        "real_non_kind_kubernetes": "E1_NOT_RUN",
        "real_nvidia_hardware": "REAL_HW_NOT_RUN",
        "real_huawei_ascend_hardware": "REAL_HW_NOT_RUN",
        "validation": validation,
        "limitations": [
            "No real NVIDIA GPU/vLLM acceptance was run.",
            "No real Huawei Ascend/vLLM-Ascend acceptance was run.",
            "No non-Kind Kubernetes E1 acceptance was run.",
            "No production HA, multi-physical-node, SLA, universal hardware compatibility, "
            "or complete Kubernetes-native platform claim is made.",
            "Preparing this bundle does not create a GitHub Release or deploy services.",
        ],
    }
    _write_json(destination / "release-preparation.json", manifest)
    files = sorted(path for path in destination.rglob("*") if path.is_file())
    (destination / "SHA256SUMS").write_text(
        "\n".join(f"{_sha256(path)}  {path.relative_to(destination).as_posix()}" for path in files)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return destination


def _release_notes(root: Path, git_sha: str, validation: dict[str, object]) -> str:
    previous_tag = PREVIOUS_RELEASE_TAG
    revision_range = f"{previous_tag}..HEAD" if previous_tag else "HEAD"
    commits = _run(
        ("git", "log", "--format=- `%h` %s", "--no-merges", revision_range),
        root,
    ).stdout.strip()
    return f"""# Mini AI Cloud {RELEASE_VERSION} release preparation

This file prepares release notes; it does **not** create a GitHub Release or deploy anything.

## Identity

- Version: `{RELEASE_VERSION}`
- Git SHA: `{git_sha}`
- Previous tag: `{previous_tag or "NONE"}`
- Evidence: `build/evidence/{git_sha}`
- Authorization state: **READY_FOR_OWNER_AUTHORIZATION**
- Tag state: **NOT_TAGGED**
- GitHub Release state: **NOT_RELEASED**
- Deployment state: **NOT_DEPLOYED**

## Automated gates

- Immutable GitHub Action dependencies checked: {validation["action_dependencies"]}
- Locked Python packages checked: {validation["locked_packages"]}
- Tracked text files scanned for high-confidence credential patterns:
  {validation["secret_scanned_files"]}
- OpenAPI and CLI compatibility snapshots match the reviewed v1 contracts.
- P4 Kind evidence: **{_p4_status(validation)}**.
- Wheel, container, SBOM, dependency, secret, and container scans are separate gate steps.

## Included commits

{commits or "- No commits found for the selected range."}

## Evidence boundary and limitations

- NVIDIA and Huawei Ascend runtime profiles, admission, routing, fallback, circuit breaking,
  and dual-backend benchmark contracts are included.
- Real non-Kind Kubernetes acceptance (E1): **NOT_RUN**.
- Real NVIDIA GPU/vLLM acceptance: **REAL_HW_NOT_RUN**.
- Real Huawei Ascend/vLLM-Ascend acceptance: **REAL_HW_NOT_RUN**.
- Production deployment: **NOT_DEPLOYED**.
- Git tag: **NOT_TAGGED**.
- GitHub Release: **NOT_RELEASED**.
- Production HA, multiple physical Kubernetes nodes, SLA, universal hardware compatibility,
  and a complete Kubernetes-native platform remain outside the verified scope.
"""


def _p4_status(validation: dict[str, object]) -> str:
    p4 = validation.get("p4_evidence")
    if isinstance(p4, dict) and isinstance(p4.get("status"), str):
        return p4["status"]
    return "PENDING_FINAL_P4_EVIDENCE"


def wheel_smoke(root: Path, dist_dir: Path) -> None:
    wheels = sorted(dist_dir.resolve().glob("mini_ai_cloud-*.whl"))
    if len(wheels) != 1:
        raise ReleaseGateError(f"expected exactly one Mini AI Cloud wheel, found {len(wheels)}")
    with tempfile.TemporaryDirectory(prefix="mini-ai-cloud-wheel-smoke-") as temporary:
        environment = Path(temporary) / "venv"
        _run(("uv", "venv", "--python", sys.executable, str(environment)), root)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        cli = environment / ("Scripts/mini-cloud.exe" if os.name == "nt" else "bin/mini-cloud")
        _run(("uv", "pip", "install", "--python", str(python), str(wheels[0])), root)
        _run((str(cli), "--help"), root)
        result = _run(
            (
                str(python),
                "-c",
                "import importlib.metadata; print(importlib.metadata.version('mini-ai-cloud'))",
            ),
            root,
        )
        if result.stdout.strip() != RELEASE_VERSION:
            raise ReleaseGateError("installed wheel version does not match release version")


def _git_sha(root: Path) -> str:
    value = _run(("git", "rev-parse", "HEAD"), root).stdout.strip()
    if ACTION_SHA.fullmatch(value) is None:
        raise ReleaseGateError("cannot resolve a full Git SHA")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.write_bytes(canonical_json(payload))


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseGateError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise ReleaseGateError(f"JSON document must be an object: {path}")
    return payload


def _read_yaml_object(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ReleaseGateError(f"cannot read YAML object: {path}") from exc
    if not isinstance(payload, dict):
        raise ReleaseGateError(f"YAML document must be an object: {path}")
    return payload


def _parse_utc_timestamp(value: str, *, description: str) -> datetime:
    if not value or not value.endswith("Z"):
        raise ReleaseGateError(f"{description} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ReleaseGateError(f"{description} must be an RFC3339 UTC timestamp") from exc
    if parsed.tzinfo != UTC:
        raise ReleaseGateError(f"{description} must be an RFC3339 UTC timestamp")
    return parsed


def _run(argv: Iterable[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            tuple(argv),
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=900,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        command = " ".join(argv)
        raise ReleaseGateError(f"command failed: {command}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and prepare Mini AI Cloud release gates")
    subparsers = parser.add_subparsers(dest="command", required=True)
    contracts = subparsers.add_parser("contracts")
    contracts.add_argument("--write", action="store_true")
    validate = subparsers.add_parser("validate")
    validate.add_argument("--p4-evidence", type=Path)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output-dir", type=Path, default=Path("build/release"))
    prepare.add_argument("--p4-evidence", type=Path)
    wheel = subparsers.add_parser("wheel-smoke")
    wheel.add_argument("--dist-dir", type=Path, default=Path("dist"))
    args = parser.parse_args()
    root = Path.cwd().resolve()
    try:
        if args.command == "contracts":
            paths = write_or_check_contracts(root, write=bool(args.write))
            print("Compatibility contracts: " + ", ".join(str(path) for path in paths))
        elif args.command == "validate":
            p4_evidence = args.p4_evidence or _p4_evidence_from_environment()
            print(
                json.dumps(
                    validate_release_inputs(root, p4_evidence=p4_evidence),
                    sort_keys=True,
                )
            )
        elif args.command == "prepare":
            p4_evidence = args.p4_evidence or _p4_evidence_from_environment()
            prepared = prepare_release_bundle(
                root,
                args.output_dir,
                p4_evidence=p4_evidence,
            )
            print(f"Release preparation bundle: {prepared}")
        else:
            wheel_smoke(root, args.dist_dir)
            print("Wheel smoke: PASS")
    except ReleaseGateError as exc:
        parser.exit(1, f"release gate failed: {exc}\n")


def _p4_evidence_from_environment() -> Path | None:
    value = os.environ.get(P4_EVIDENCE_ENV, "").strip()
    return Path(value) if value else None


if __name__ == "__main__":
    main()
