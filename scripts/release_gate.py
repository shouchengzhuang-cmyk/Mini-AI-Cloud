from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Iterable
from pathlib import Path

from typer._click.core import Command
from typer.core import TyperGroup, TyperOption
from typer.main import get_command

RELEASE_VERSION = "0.4.0"
ACTION_SHA = re.compile(r"^[0-9a-f]{40}$")
SECRET_PATTERNS = {
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "github-token": re.compile(r"\b(?:github_pat_[A-Za-z0-9_]{20,}|ghp_[A-Za-z0-9]{20,})\b"),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "mini-cloud-api-key": re.compile(r"\bmkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}\b"),
}
SNAPSHOT_DIR = Path("contracts/release")


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
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    if f"## [{RELEASE_VERSION}]" not in changelog:
        raise ReleaseGateError("CHANGELOG has no prepared release section")


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


def validate_release_inputs(root: Path) -> dict[str, int]:
    validate_versions(root)
    actions = validate_action_pins(root)
    dependencies = validate_locked_dependencies(root)
    files = scan_tracked_secrets(root)
    validate_container_baseline(root)
    write_or_check_contracts(root, write=False)
    return {
        "action_dependencies": actions,
        "locked_packages": len(dependencies),
        "secret_scanned_files": files,
    }


def prepare_release_bundle(root: Path, output_root: Path) -> Path:
    validation = validate_release_inputs(root)
    git_sha = _git_sha(root)
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
    manifest = {
        "schema_version": "1.0.0",
        "version": RELEASE_VERSION,
        "git_sha": git_sha,
        "dirty_tree": False,
        "evidence_bundle": evidence.relative_to(root).as_posix(),
        "github_release": "NOT_CREATED",
        "deployment_status": "NOT_DEPLOYED",
        "real_gpu": "NOT_RUN",
        "validation": validation,
        "limitations": [
            "No real NVIDIA GPU acceptance was run.",
            "No production HA or multi-physical-node claim is made.",
            "Preparing this bundle does not create a GitHub Release or deploy services.",
        ],
    }
    _write_json(destination / "release-preparation.json", manifest)
    files = sorted(path for path in destination.iterdir() if path.is_file())
    (destination / "SHA256SUMS").write_text(
        "\n".join(f"{_sha256(path)}  {path.name}" for path in files) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return destination


def _release_notes(root: Path, git_sha: str, validation: dict[str, int]) -> str:
    latest_tag = _latest_tag(root)
    revision_range = f"{latest_tag}..HEAD" if latest_tag else "HEAD"
    commits = _run(
        ("git", "log", "--format=- `%h` %s", "--no-merges", revision_range),
        root,
    ).stdout.strip()
    return f"""# Mini AI Cloud {RELEASE_VERSION} release preparation

This file prepares release notes; it does **not** create a GitHub Release or deploy anything.

## Identity

- Version: `{RELEASE_VERSION}`
- Git SHA: `{git_sha}`
- Previous tag: `{latest_tag or "NONE"}`
- Evidence: `build/evidence/{git_sha}`

## Automated gates

- Immutable GitHub Action dependencies checked: {validation["action_dependencies"]}
- Locked Python packages checked: {validation["locked_packages"]}
- Tracked text files scanned for high-confidence credential patterns:
  {validation["secret_scanned_files"]}
- OpenAPI and CLI compatibility snapshots match the reviewed v1 contracts.
- Wheel, container, Kind, SBOM, dependency, secret, and container scans are separate gate steps.

## Included commits

{commits or "- No commits found for the selected range."}

## Evidence boundary and limitations

- Real NVIDIA/vLLM GPU acceptance: **NOT RUN**.
- Production deployment: **NOT DEPLOYED**.
- GitHub Release: **NOT CREATED**.
- Production HA and multiple physical Kubernetes nodes remain outside the verified scope.
"""


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


def _latest_tag(root: Path) -> str | None:
    result = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


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
    subparsers.add_parser("validate")
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output-dir", type=Path, default=Path("build/release"))
    wheel = subparsers.add_parser("wheel-smoke")
    wheel.add_argument("--dist-dir", type=Path, default=Path("dist"))
    args = parser.parse_args()
    root = Path.cwd().resolve()
    try:
        if args.command == "contracts":
            paths = write_or_check_contracts(root, write=bool(args.write))
            print("Compatibility contracts: " + ", ".join(str(path) for path in paths))
        elif args.command == "validate":
            print(json.dumps(validate_release_inputs(root), sort_keys=True))
        elif args.command == "prepare":
            print(f"Release preparation bundle: {prepare_release_bundle(root, args.output_dir)}")
        else:
            wheel_smoke(root, args.dist_dir)
            print("Wheel smoke: PASS")
    except ReleaseGateError as exc:
        parser.exit(1, f"release gate failed: {exc}\n")


if __name__ == "__main__":
    main()
