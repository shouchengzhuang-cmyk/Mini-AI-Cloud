from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

import yaml  # type: ignore[import-untyped]

KIND_VERSION = "v0.27.0"
KUBERNETES_VERSION = "v1.32.2"
KIND_NODE_IMAGE = (
    "kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f"
)
RUN_ID_LABEL = "mini-ai-cloud/run-id"
HARNESS_OWNED_LABEL = "mini-ai-cloud/harness-owned"
NAMESPACE_ROLE_LABEL = "mini-ai-cloud/namespace-role"
REQUIRED_EVIDENCE_FILES = (
    "manifest.json",
    "environment.json",
    "commands.jsonl",
    "claims.json",
    "kubernetes-summary.json",
    "cleanup.json",
    "limitations.md",
    "checksums.txt",
)
REQUIRED_CLAIMS = (
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
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_RUN_ID = re.compile(r"^m7-[0-9]{14}-[0-9a-f]{8}$")
_SHA256_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_FULL_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_UID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_SAFE_LABEL_KEYS = {
    RUN_ID_LABEL,
    HARNESS_OWNED_LABEL,
    NAMESPACE_ROLE_LABEL,
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app.kubernetes.io/component",
    "mini-ai-cloud/resource-kind",
}
_CREDENTIAL_ASSIGNMENT = re.compile(
    r"(?i)(password|token|api[_-]?key|private[_-]?key|secret)=([^\s,;]+)"
)


class KindEvidenceError(RuntimeError):
    """The harness cannot create trustworthy, safely scoped Kind evidence."""


class ClaimStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_RUN = "NOT_RUN"


@dataclass(frozen=True, slots=True)
class RunIdentity:
    run_id: str
    cluster_name: str
    system_namespace: str
    workload_namespace: str
    release_name: str
    external_secret_name: str
    postgres_name: str
    redis_name: str

    @classmethod
    def create(
        cls,
        *,
        now: datetime | None = None,
        random_hex: str | None = None,
    ) -> RunIdentity:
        timestamp = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%d%H%M%S")
        suffix = (random_hex or uuid.uuid4().hex)[:8].lower()
        if not re.fullmatch(r"[0-9a-f]{8}", suffix):
            raise ValueError("random_hex must provide at least eight lowercase hexadecimal digits")
        run_id = f"m7-{timestamp}-{suffix}"
        identity = cls(
            run_id=run_id,
            cluster_name=f"mac-m7-{suffix}",
            system_namespace=f"mac-system-{suffix}",
            workload_namespace=f"mac-workload-{suffix}",
            release_name=f"mac-{suffix}",
            external_secret_name=f"mac-external-{suffix}",
            postgres_name=f"postgres-{suffix}",
            redis_name=f"redis-{suffix}",
        )
        identity.validate()
        return identity

    def validate(self) -> None:
        if not _RUN_ID.fullmatch(self.run_id):
            raise ValueError("run_id is not a canonical M7 harness identifier")
        names = (
            self.cluster_name,
            self.system_namespace,
            self.workload_namespace,
            self.release_name,
            self.external_secret_name,
            self.postgres_name,
            self.redis_name,
        )
        if len(set(names)) != len(names):
            raise ValueError("harness identities must be unique")
        if any(not _DNS_LABEL.fullmatch(name) for name in names):
            raise ValueError("harness identities must be DNS-1123 labels")
        if self.system_namespace == self.workload_namespace:
            raise ValueError("system and workload namespaces must remain isolated")

    def manifest_payload(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "cluster_name": self.cluster_name,
            "system_namespace": self.system_namespace,
            "workload_namespace": self.workload_namespace,
            "release_name": self.release_name,
            "external_secret_name": self.external_secret_name,
            "postgres_name": self.postgres_name,
            "redis_name": self.redis_name,
        }


@dataclass(frozen=True, slots=True)
class HarnessCredentials:
    postgres_password: str
    bootstrap_token: str
    api_key_pepper: str
    worker_auth_token: str
    secret_master_key: str

    @classmethod
    def generate(cls) -> HarnessCredentials:
        return cls(
            postgres_password=secrets.token_urlsafe(32),
            bootstrap_token=secrets.token_urlsafe(32),
            api_key_pepper=secrets.token_urlsafe(32),
            worker_auth_token=secrets.token_urlsafe(32),
            secret_master_key=("kind:" + base64.b64encode(secrets.token_bytes(32)).decode("ascii")),
        )

    def sensitive_values(self, identity: RunIdentity) -> tuple[str, ...]:
        return (
            self.postgres_password,
            self.bootstrap_token,
            self.api_key_pepper,
            self.worker_auth_token,
            self.secret_master_key,
            self.database_url(identity),
            self.redis_url(identity),
        )

    def database_url(self, identity: RunIdentity) -> str:
        return (
            f"postgresql+asyncpg://task:{self.postgres_password}@"
            f"{identity.postgres_name}.{identity.system_namespace}.svc:5432/task_platform"
        )

    def redis_url(self, identity: RunIdentity) -> str:
        return f"redis://{identity.redis_name}.{identity.system_namespace}.svc:6379/0"

    def secret_string_data(self, identity: RunIdentity) -> dict[str, str]:
        return {
            "database-url": self.database_url(identity),
            "redis-url": self.redis_url(identity),
            "postgres-password": self.postgres_password,
            "bootstrap-token": self.bootstrap_token,
            "api-key-pepper": self.api_key_pepper,
            "worker-auth-token": self.worker_auth_token,
            "secret-master-key": self.secret_master_key,
        }


@dataclass(frozen=True, slots=True)
class GitState:
    sha: str
    dirty: bool


@dataclass(frozen=True, slots=True)
class ChartState:
    version: str
    app_version: str
    digest: str


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    command_id: str
    claim_id: str
    label: str
    returncode: int
    started_at: str
    ended_at: str
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)


class CommandRunner(Protocol):
    def run(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        input_text: str | None,
        environment: Mapping[str, str] | None,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]: ...


class SubprocessCommandRunner:
    def run(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        input_text: str | None,
        environment: Mapping[str, str] | None,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        process_environment = None
        if environment is not None:
            process_environment = {**os.environ, **environment}
        return subprocess.run(
            argv,
            cwd=cwd,
            input=input_text,
            env=process_environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )


class CommandRecorder:
    def __init__(
        self,
        evidence_root: Path,
        repository_root: Path,
        *,
        runner: CommandRunner | None = None,
    ) -> None:
        self.evidence_root = evidence_root.resolve()
        self.repository_root = repository_root.resolve()
        self.runner = runner or SubprocessCommandRunner()
        self.commands_path = self.evidence_root / "commands.jsonl"
        self.logs_root = self.evidence_root / "logs"
        self.logs_root.mkdir(mode=0o700, exist_ok=True)
        self.commands_path.touch(mode=0o600, exist_ok=True)
        self._sequence = _jsonl_line_count(self.commands_path)
        self._sensitive_values: list[str] = []

    def register_sensitive_values(self, values: Sequence[str]) -> None:
        for value in values:
            if value and value not in self._sensitive_values:
                self._sensitive_values.append(value)
        self._sensitive_values.sort(key=len, reverse=True)

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        return tuple(self._sensitive_values)

    def record(
        self,
        label: str,
        argv: Sequence[str],
        *,
        claim_id: str,
        input_text: str | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 900,
    ) -> CommandOutcome:
        if not argv or any(not isinstance(item, str) or not item for item in argv):
            raise ValueError("recorded commands require non-empty string arguments")
        if claim_id not in REQUIRED_CLAIMS:
            raise ValueError(f"unknown Kind evidence claim: {claim_id}")
        if timeout_seconds <= 0:
            raise ValueError("command timeout must be positive")
        self._sequence += 1
        command_id = f"cmd-{self._sequence:04d}"
        safe_label = _safe_label(label)
        started = _utc_now()
        try:
            completed = self.runner.run(
                tuple(argv),
                cwd=self.repository_root,
                input_text=input_text,
                environment=environment,
                timeout_seconds=timeout_seconds,
            )
            returncode = completed.returncode
            stdout = completed.stdout
            stderr = completed.stderr
        except (OSError, subprocess.SubprocessError) as error:
            returncode = 127
            stdout = ""
            stderr = f"{type(error).__name__}: command execution failed"
        ended = _utc_now()

        stdout_name = f"{command_id}-{safe_label}.stdout.log"
        stderr_name = f"{command_id}-{safe_label}.stderr.log"
        _write_private_text(self.logs_root / stdout_name, self.redact(stdout))
        _write_private_text(self.logs_root / stderr_name, self.redact(stderr))
        record = {
            "schema_version": "1.0.0",
            "command_id": command_id,
            "claim_id": claim_id,
            "sequence": self._sequence,
            "label": safe_label,
            "argv": self._redacted_argv(tuple(argv)),
            "environment_override_keys": sorted(environment) if environment else [],
            "stdin_provided": input_text is not None,
            "started_at": started,
            "ended_at": ended,
            "returncode": returncode,
            "stdout_log": f"logs/{stdout_name}",
            "stderr_log": f"logs/{stderr_name}",
        }
        with self.commands_path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        return CommandOutcome(
            command_id=command_id,
            claim_id=claim_id,
            label=safe_label,
            returncode=returncode,
            started_at=started,
            ended_at=ended,
            stdout=stdout,
            stderr=stderr,
        )

    def require_success(self, outcome: CommandOutcome) -> None:
        if outcome.returncode != 0:
            raise KindEvidenceError(
                f"recorded command {outcome.command_id} failed with exit code "
                f"{outcome.returncode}; inspect its redacted evidence log"
            )

    def redact(self, value: str) -> str:
        redacted = value
        for sensitive in self._sensitive_values:
            redacted = redacted.replace(sensitive, "[REDACTED]")
        return _CREDENTIAL_ASSIGNMENT.sub(r"\1=[REDACTED]", redacted)

    def _redacted_argv(self, argv: tuple[str, ...]) -> list[str]:
        result: list[str] = []
        redact_next = False
        for argument in argv:
            if redact_next:
                result.append("[PRIVATE_KUBECONFIG]")
                redact_next = False
                continue
            if argument.startswith("--kubeconfig="):
                result.append("--kubeconfig=[PRIVATE_KUBECONFIG]")
                continue
            result.append(self.redact(argument))
            if argument == "--kubeconfig":
                redact_next = True
        return result


@dataclass(slots=True)
class _ClaimRecord:
    claim_id: str
    status: ClaimStatus = ClaimStatus.NOT_RUN
    command_ids: list[str] = field(default_factory=list)
    detail: str = "Not executed."


class ClaimLedger:
    def __init__(self, claim_ids: Sequence[str] = REQUIRED_CLAIMS) -> None:
        if not claim_ids or len(set(claim_ids)) != len(claim_ids):
            raise ValueError("claim_ids must be non-empty and unique")
        self._claims = {claim_id: _ClaimRecord(claim_id) for claim_id in claim_ids}

    def mark_pass(self, claim_id: str, outcomes: Sequence[CommandOutcome], *, detail: str) -> None:
        claim = self._claim(claim_id)
        if not outcomes or any(outcome.returncode != 0 for outcome in outcomes):
            raise KindEvidenceError("PASS claims require at least one successful command outcome")
        if any(outcome.claim_id != claim_id for outcome in outcomes):
            raise KindEvidenceError("PASS claims require outcomes bound to the same claim id")
        if not detail.strip():
            raise ValueError("claim detail must not be empty")
        claim.status = ClaimStatus.PASS
        claim.command_ids = [outcome.command_id for outcome in outcomes]
        claim.detail = detail.strip()

    def mark_fail(
        self,
        claim_id: str,
        *,
        detail: str,
        outcomes: Sequence[CommandOutcome] = (),
    ) -> None:
        claim = self._claim(claim_id)
        if not detail.strip():
            raise ValueError("claim failure detail must not be empty")
        if any(outcome.claim_id != claim_id for outcome in outcomes):
            raise KindEvidenceError("FAIL claims require outcomes bound to the same claim id")
        claim.status = ClaimStatus.FAIL
        claim.command_ids = [outcome.command_id for outcome in outcomes]
        claim.detail = detail.strip()

    def overall_status(self) -> str:
        statuses = {claim.status for claim in self._claims.values()}
        if ClaimStatus.FAIL in statuses:
            return ClaimStatus.FAIL.value
        if ClaimStatus.NOT_RUN in statuses:
            return ClaimStatus.NOT_RUN.value
        return "KIND_K8S_PASS"

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": "1.0.0",
            "status": self.overall_status(),
            "real_hardware_status": "REAL_HW_NOT_RUN",
            "claims": [
                {
                    "id": claim.claim_id,
                    "status": claim.status.value,
                    "command_ids": claim.command_ids,
                    "detail": claim.detail,
                }
                for claim in self._claims.values()
            ],
        }

    def _claim(self, claim_id: str) -> _ClaimRecord:
        try:
            return self._claims[claim_id]
        except KeyError as error:
            raise ValueError(f"unknown Kind evidence claim: {claim_id}") from error


class EvidenceBundle:
    def __init__(
        self,
        output_root: Path,
        identity: RunIdentity,
        *,
        started_at: str | None = None,
    ) -> None:
        identity.validate()
        resolved_output = output_root.resolve()
        resolved_output.mkdir(parents=True, exist_ok=True)
        self.root = resolved_output / identity.run_id
        self.root.mkdir(mode=0o700, exist_ok=False)
        os.chmod(self.root, 0o700)
        self.identity = identity
        self.started_at = started_at or _utc_now()
        _write_private_text(self.root / "commands.jsonl", "")

    def finalize(
        self,
        *,
        ledger: ClaimLedger,
        environment: Mapping[str, object],
        kubernetes_summary: Mapping[str, object],
        cleanup: Mapping[str, object],
        limitations: Sequence[str],
        sensitive_values: Sequence[str] = (),
        ended_at: str | None = None,
    ) -> Path:
        if not limitations or any(not item.strip() for item in limitations):
            raise ValueError("evidence limitations must be non-empty")
        final_time = ended_at or _utc_now()
        claims = ledger.payload()
        status = str(claims["status"])
        cleanup_status = cleanup.get("status")
        if cleanup_status == ClaimStatus.FAIL.value:
            status = ClaimStatus.FAIL.value
            claims["status"] = status
        normalized_environment = dict(environment)
        _validate_environment_payload(normalized_environment, evidence_status=status)
        _validate_cleanup_payload(cleanup, evidence_status=status)
        manifest = {
            "schema_version": "1.0.0",
            "run_id": self.identity.run_id,
            "status": status,
            "real_hardware_status": "REAL_HW_NOT_RUN",
            "started_at": self.started_at,
            "ended_at": final_time,
            "identities": self.identity.manifest_payload(),
            "kind_version": KIND_VERSION,
            "kubernetes_version": KUBERNETES_VERSION,
            "kind_node_image": KIND_NODE_IMAGE,
            "evidence_files": list(REQUIRED_EVIDENCE_FILES),
        }
        _write_private_json(self.root / "manifest.json", manifest)
        _write_private_json(self.root / "environment.json", normalized_environment)
        _write_private_json(self.root / "claims.json", claims)
        _write_private_json(self.root / "kubernetes-summary.json", dict(kubernetes_summary))
        _write_private_json(self.root / "cleanup.json", dict(cleanup))
        limitations_text = "# Limitations\n\n" + "".join(
            f"- {item.strip()}\n" for item in limitations
        )
        _write_private_text(self.root / "limitations.md", limitations_text)
        _assert_bundle_has_no_sensitive_values(self.root, sensitive_values)
        write_checksums(self.root)
        validate_evidence_bundle(self.root)
        return self.root


def capture_git_state(repository_root: Path) -> GitState:
    revision = _capture(("git", "rev-parse", "HEAD"), repository_root)
    sha = revision.stdout.strip()
    if revision.returncode != 0 or not _FULL_GIT_SHA.fullmatch(sha):
        raise KindEvidenceError("cannot resolve a full lowercase Git SHA")
    status = _capture(
        ("git", "status", "--porcelain=v1", "--untracked-files=normal"),
        repository_root,
    )
    if status.returncode != 0:
        raise KindEvidenceError("cannot determine Git dirty state")
    return GitState(sha=sha, dirty=bool(status.stdout.strip()))


def capture_chart_state(chart_root: Path) -> ChartState:
    resolved = chart_root.resolve(strict=True)
    chart_file = resolved / "Chart.yaml"
    if not chart_file.is_file():
        raise KindEvidenceError("Chart.yaml is missing")
    payload = yaml.safe_load(chart_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise KindEvidenceError("Chart.yaml is invalid")
    version = payload.get("version")
    app_version = payload.get("appVersion")
    if not isinstance(version, str) or not version.strip():
        raise KindEvidenceError("Chart version is missing")
    if not isinstance(app_version, str) or not app_version.strip():
        raise KindEvidenceError("Chart appVersion is missing")
    return ChartState(
        version=version,
        app_version=app_version,
        digest=directory_digest(resolved),
    )


def build_environment_payload(
    *,
    git_state: GitState,
    chart_state: ChartState,
    image_references: Mapping[str, str],
    tool_versions: Mapping[str, str],
    kubernetes_server_version: str | None,
    recorded_at: str | None = None,
) -> dict[str, object]:
    images = {
        name: validate_pinned_image(reference, description=f"{name} image")
        for name, reference in sorted(image_references.items())
    }
    return {
        "schema_version": "1.0.0",
        "recorded_at": recorded_at or _utc_now(),
        "git_sha": git_state.sha,
        "git_dirty": git_state.dirty,
        "chart_version": chart_state.version,
        "chart_app_version": chart_state.app_version,
        "chart_digest": chart_state.digest,
        "image_references": images,
        "kind_version": KIND_VERSION,
        "kind_node_image": KIND_NODE_IMAGE,
        "kubernetes_server_version": kubernetes_server_version,
        "tool_versions": dict(sorted(tool_versions.items())),
    }


def directory_digest(root: Path) -> str:
    resolved = root.resolve(strict=True)
    if not resolved.is_dir():
        raise KindEvidenceError("digest root must be a directory")
    digest = hashlib.sha256()
    files = sorted(path for path in resolved.rglob("*") if path.is_file())
    if not files:
        raise KindEvidenceError("digest root contains no files")
    for path in files:
        if path.is_symlink():
            raise KindEvidenceError("digest root must not contain symbolic links")
        relative = path.relative_to(resolved).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def validate_kind_version(output: str) -> str:
    match = re.search(r"\bkind (v[0-9]+\.[0-9]+\.[0-9]+)\b", output)
    if match is None or match.group(1) != KIND_VERSION:
        raise KindEvidenceError(f"Kind must be exactly {KIND_VERSION}")
    return match.group(1)


def validate_pinned_image(reference: str, *, description: str) -> str:
    if not _SHA256_IMAGE.fullmatch(reference):
        raise KindEvidenceError(f"{description} must be pinned by an exact sha256 digest")
    return reference


def build_namespace_manifests(identity: RunIdentity) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            _namespace_manifest(identity.system_namespace, identity.run_id, "system"),
            _namespace_manifest(identity.workload_namespace, identity.run_id, "workload"),
        ],
    }


def build_external_secret_manifest(
    identity: RunIdentity,
    credentials: HarnessCredentials,
) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": identity.external_secret_name,
            "namespace": identity.system_namespace,
            "labels": _owned_labels(identity.run_id, component="external-secret"),
        },
        "type": "Opaque",
        "stringData": credentials.secret_string_data(identity),
    }


def build_external_data_store_manifests(
    identity: RunIdentity,
    *,
    postgres_image: str,
    redis_image: str,
) -> dict[str, object]:
    validate_pinned_image(postgres_image, description="PostgreSQL image")
    validate_pinned_image(redis_image, description="Redis image")
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            _service(identity, identity.postgres_name, 5432, "postgres"),
            _deployment(
                identity,
                name=identity.postgres_name,
                image=postgres_image,
                component="postgres",
                port=5432,
                env=(
                    {"name": "POSTGRES_DB", "value": "task_platform"},
                    {"name": "POSTGRES_USER", "value": "task"},
                    {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
                    {
                        "name": "POSTGRES_PASSWORD",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": identity.external_secret_name,
                                "key": "postgres-password",
                            }
                        },
                    },
                ),
                run_as_user=70,
                run_as_group=70,
                fs_group=70,
                volume_mounts=(
                    {"name": "postgres-data", "mountPath": "/var/lib/postgresql/data"},
                    {"name": "postgres-run", "mountPath": "/var/run/postgresql"},
                    {"name": "postgres-tmp", "mountPath": "/tmp"},
                ),
                volumes=(
                    {"name": "postgres-data", "emptyDir": {"sizeLimit": "512Mi"}},
                    {"name": "postgres-run", "emptyDir": {"sizeLimit": "32Mi"}},
                    {"name": "postgres-tmp", "emptyDir": {"sizeLimit": "64Mi"}},
                ),
            ),
            _service(identity, identity.redis_name, 6379, "redis"),
            _deployment(
                identity,
                name=identity.redis_name,
                image=redis_image,
                component="redis",
                port=6379,
                env=(),
                args=("redis-server", "--appendonly", "no", "--save", ""),
                run_as_user=999,
                run_as_group=1000,
                fs_group=1000,
                volume_mounts=({"name": "redis-data", "mountPath": "/data"},),
                volumes=({"name": "redis-data", "emptyDir": {"sizeLimit": "256Mi"}},),
            ),
        ],
    }


@dataclass(frozen=True, slots=True)
class OwnedNamespace:
    name: str
    uid: str
    run_id: str
    role: str


def validate_owned_namespace_for_cleanup(
    payload: Mapping[str, Any],
    *,
    expected_name: str,
    expected_run_id: str,
    expected_role: str,
) -> OwnedNamespace:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise KindEvidenceError("namespace cleanup target has no metadata")
    name = metadata.get("name")
    uid = metadata.get("uid")
    labels = metadata.get("labels")
    if name != expected_name:
        raise KindEvidenceError("namespace cleanup target name drifted")
    if not isinstance(uid, str) or not _UID.fullmatch(uid):
        raise KindEvidenceError("namespace cleanup target has no trustworthy UID")
    if not isinstance(labels, Mapping):
        raise KindEvidenceError("namespace cleanup target has no labels")
    if (
        labels.get(RUN_ID_LABEL) != expected_run_id
        or labels.get(HARNESS_OWNED_LABEL) != "true"
        or labels.get(NAMESPACE_ROLE_LABEL) != expected_role
    ):
        raise KindEvidenceError("namespace cleanup target ownership labels drifted")
    return OwnedNamespace(
        name=expected_name,
        uid=uid,
        run_id=expected_run_id,
        role=expected_role,
    )


def namespace_uid_delete_request(target: OwnedNamespace) -> tuple[str, str]:
    path = f"/api/v1/namespaces/{target.name}"
    body = json.dumps(
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {"uid": target.uid},
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return path, body


def summarize_kubernetes_resources(payloads: Sequence[Mapping[str, Any]]) -> dict[str, object]:
    resources: list[dict[str, object]] = []
    for payload in payloads:
        metadata = payload.get("metadata")
        if not isinstance(metadata, Mapping):
            raise KindEvidenceError("Kubernetes evidence object has no metadata")
        kind = payload.get("kind")
        name = metadata.get("name")
        uid = metadata.get("uid")
        if not isinstance(kind, str) or not isinstance(name, str) or not isinstance(uid, str):
            raise KindEvidenceError("Kubernetes evidence object identity is incomplete")
        labels = metadata.get("labels")
        safe_labels = (
            {
                str(key): str(value)
                for key, value in labels.items()
                if isinstance(labels, Mapping)
                and key in _SAFE_LABEL_KEYS
                and isinstance(key, str)
                and isinstance(value, str)
            }
            if isinstance(labels, Mapping)
            else {}
        )
        item: dict[str, object] = {
            "api_version": str(payload.get("apiVersion") or ""),
            "kind": kind,
            "name": name,
            "namespace": str(metadata.get("namespace") or ""),
            "uid": uid,
            "labels": safe_labels,
        }
        status = payload.get("status")
        if isinstance(status, Mapping):
            item["status"] = _safe_status_summary(kind, status)
        resources.append(item)
    resources.sort(key=lambda item: (str(item["namespace"]), str(item["kind"]), str(item["name"])))
    return {
        "schema_version": "1.0.0",
        "resource_count": len(resources),
        "resources": resources,
    }


def write_checksums(bundle_root: Path) -> None:
    checksum_path = bundle_root / "checksums.txt"
    checksum_path.unlink(missing_ok=True)
    files = sorted(
        path for path in bundle_root.rglob("*") if path.is_file() and path != checksum_path
    )
    lines = []
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        relative = path.relative_to(bundle_root).as_posix()
        lines.append(f"{digest}  {relative}")
    _write_private_text(checksum_path, "\n".join(lines) + "\n")


def validate_evidence_bundle(bundle_root: Path) -> None:
    for name in REQUIRED_EVIDENCE_FILES:
        path = bundle_root / name
        if not path.is_file() or path.is_symlink():
            raise KindEvidenceError(f"required evidence file is missing: {name}")
    manifest = _read_json_object(bundle_root / "manifest.json")
    claims = _read_json_object(bundle_root / "claims.json")
    if manifest.get("status") != claims.get("status"):
        raise KindEvidenceError("manifest and claim status disagree")
    status = manifest.get("status")
    if status not in {"KIND_K8S_PASS", ClaimStatus.FAIL.value, ClaimStatus.NOT_RUN.value}:
        raise KindEvidenceError("evidence manifest has an invalid status")
    if status == "KIND_K8S_PASS":
        claim_items = claims.get("claims")
        if not isinstance(claim_items, list) or any(
            not isinstance(item, Mapping)
            or item.get("status") != ClaimStatus.PASS.value
            or not item.get("command_ids")
            for item in claim_items
        ):
            raise KindEvidenceError("KIND_K8S_PASS requires successful command-bound claims")
        claim_ids = {item.get("id") for item in claim_items if isinstance(item, Mapping)}
        if claim_ids != set(REQUIRED_CLAIMS):
            raise KindEvidenceError("KIND_K8S_PASS requires every P4 claim")
        command_records = _read_command_records(bundle_root / "commands.jsonl")
        for item in claim_items:
            assert isinstance(item, Mapping)
            claim_id = item.get("id")
            command_ids = item.get("command_ids")
            assert isinstance(command_ids, list)
            if any(
                command_id not in command_records
                or command_records[command_id].get("returncode") != 0
                or command_records[command_id].get("claim_id") != claim_id
                for command_id in command_ids
            ):
                raise KindEvidenceError(
                    "KIND_K8S_PASS claim references missing, failed, or "
                    "cross-claim command evidence"
                )
    _validate_checksums(bundle_root)


def _namespace_manifest(name: str, run_id: str, role: str) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                RUN_ID_LABEL: run_id,
                HARNESS_OWNED_LABEL: "true",
                NAMESPACE_ROLE_LABEL: role,
            },
        },
    }


def _owned_labels(run_id: str, *, component: str) -> dict[str, str]:
    return {
        RUN_ID_LABEL: run_id,
        HARNESS_OWNED_LABEL: "true",
        "app.kubernetes.io/name": "mini-ai-cloud-kind-harness",
        "app.kubernetes.io/component": component,
    }


def _service(identity: RunIdentity, name: str, port: int, component: str) -> dict[str, object]:
    labels = _owned_labels(identity.run_id, component=component)
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": identity.system_namespace,
            "labels": labels,
        },
        "spec": {
            "selector": labels,
            "ports": [{"name": component, "port": port, "targetPort": port}],
        },
    }


def _deployment(
    identity: RunIdentity,
    *,
    name: str,
    image: str,
    component: str,
    port: int,
    env: Sequence[Mapping[str, object]],
    run_as_user: int,
    run_as_group: int,
    fs_group: int,
    volume_mounts: Sequence[Mapping[str, object]],
    volumes: Sequence[Mapping[str, object]],
    args: Sequence[str] = (),
) -> dict[str, object]:
    labels = _owned_labels(identity.run_id, component=component)
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": identity.system_namespace,
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": run_as_user,
                        "runAsGroup": run_as_group,
                        "fsGroup": fs_group,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": component,
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": list(args),
                            "ports": [{"name": component, "containerPort": port}],
                            "env": list(env),
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                                "privileged": False,
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                            },
                            "volumeMounts": list(volume_mounts),
                            "readinessProbe": {
                                "tcpSocket": {"port": component},
                                "periodSeconds": 2,
                                "timeoutSeconds": 1,
                                "failureThreshold": 30,
                            },
                        }
                    ],
                    "volumes": list(volumes),
                },
            },
        },
    }


def _safe_status_summary(kind: str, status: Mapping[str, Any]) -> dict[str, object]:
    if kind == "Pod":
        ready = any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in status.get("conditions", [])
            if isinstance(status.get("conditions"), list)
        )
        return {"phase": str(status.get("phase") or "Unknown"), "ready": ready}
    if kind == "Job":
        return {
            "active": int(status.get("active") or 0),
            "succeeded": int(status.get("succeeded") or 0),
            "failed": int(status.get("failed") or 0),
        }
    if kind in {"Deployment", "DaemonSet", "StatefulSet"}:
        return {
            "desired": int(status.get("replicas") or status.get("desiredNumberScheduled") or 0),
            "ready": int(status.get("readyReplicas") or status.get("numberReady") or 0),
        }
    return {}


def _capture(argv: tuple[str, ...], cwd: Path) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            argv,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise KindEvidenceError("local evidence preflight command failed") from error


def _validate_environment_payload(
    payload: Mapping[str, object],
    *,
    evidence_status: str,
) -> None:
    if payload.get("schema_version") != "1.0.0":
        raise KindEvidenceError("environment evidence schema version is invalid")
    if not isinstance(payload.get("recorded_at"), str):
        raise KindEvidenceError("environment evidence timestamp is missing")
    git_sha = payload.get("git_sha")
    if not isinstance(git_sha, str) or not _FULL_GIT_SHA.fullmatch(git_sha):
        raise KindEvidenceError("environment evidence Git SHA is invalid")
    if not isinstance(payload.get("git_dirty"), bool):
        raise KindEvidenceError("environment evidence dirty state is missing")
    if not isinstance(payload.get("chart_version"), str) or not payload.get("chart_version"):
        raise KindEvidenceError("environment evidence Chart version is missing")
    if not isinstance(payload.get("chart_app_version"), str) or not payload.get(
        "chart_app_version"
    ):
        raise KindEvidenceError("environment evidence Chart appVersion is missing")
    chart_digest = payload.get("chart_digest")
    if not isinstance(chart_digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", chart_digest):
        raise KindEvidenceError("environment evidence Chart digest is invalid")
    if payload.get("kind_version") != KIND_VERSION or payload.get("kind_node_image") != (
        KIND_NODE_IMAGE
    ):
        raise KindEvidenceError("environment evidence Kind pins drifted")
    images = payload.get("image_references")
    if not isinstance(images, Mapping):
        raise KindEvidenceError("environment evidence image references are invalid")
    for name, reference in images.items():
        if not isinstance(name, str) or not isinstance(reference, str):
            raise KindEvidenceError("environment evidence image reference is invalid")
        validate_pinned_image(reference, description=f"{name} image")
    tools = payload.get("tool_versions")
    if not isinstance(tools, Mapping):
        raise KindEvidenceError("environment evidence tool versions are invalid")
    if evidence_status == "KIND_K8S_PASS":
        if not images:
            raise KindEvidenceError("KIND_K8S_PASS requires pinned image references")
        if payload.get("git_dirty") is not False:
            raise KindEvidenceError("KIND_K8S_PASS requires a clean Git worktree")
        if payload.get("kubernetes_server_version") != KUBERNETES_VERSION:
            raise KindEvidenceError("KIND_K8S_PASS requires the pinned Kubernetes server version")
        if tools.get("kind") != KIND_VERSION:
            raise KindEvidenceError("KIND_K8S_PASS requires the pinned Kind version")


def _validate_cleanup_payload(
    payload: Mapping[str, object],
    *,
    evidence_status: str,
) -> None:
    status = payload.get("status")
    if status not in {item.value for item in ClaimStatus}:
        raise KindEvidenceError("cleanup evidence status is invalid")
    if evidence_status == "KIND_K8S_PASS":
        if (
            status != ClaimStatus.PASS.value
            or payload.get("release_owned_remaining") != 0
            or payload.get("external_secret_preserved_after_uninstall") is not True
            or payload.get("external_namespaces_preserved_after_uninstall") is not True
            or payload.get("cluster_deleted") is not True
            or payload.get("default_kubeconfig_unchanged") is not True
            or payload.get("temporary_state_deleted") is not True
        ):
            raise KindEvidenceError("KIND_K8S_PASS requires complete scoped cleanup evidence")


def _assert_bundle_has_no_sensitive_values(root: Path, values: Sequence[str]) -> None:
    sensitive = tuple(value.encode("utf-8") for value in values if value)
    if not sensitive:
        return
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        if any(value in payload for value in sensitive):
            raise KindEvidenceError(
                f"sensitive value detected in evidence file: {path.relative_to(root).as_posix()}"
            )


def _validate_checksums(bundle_root: Path) -> None:
    lines = (bundle_root / "checksums.txt").read_text(encoding="utf-8").splitlines()
    if not lines:
        raise KindEvidenceError("evidence checksums are empty")
    for line in lines:
        digest, separator, relative = line.partition("  ")
        if not separator or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise KindEvidenceError("evidence checksum line is invalid")
        path = bundle_root / Path(*relative.split("/"))
        try:
            resolved = path.resolve(strict=True)
        except OSError as error:
            raise KindEvidenceError("checksummed evidence file is missing") from error
        if bundle_root.resolve() not in resolved.parents or not resolved.is_file():
            raise KindEvidenceError("checksum path escapes the evidence bundle")
        if hashlib.sha256(resolved.read_bytes()).hexdigest() != digest:
            raise KindEvidenceError(f"evidence checksum mismatch: {relative}")


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise KindEvidenceError(f"evidence JSON must be an object: {path.name}")
    return payload


def _read_command_records(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise KindEvidenceError("commands.jsonl contains invalid JSON") from error
        if not isinstance(payload, dict):
            raise KindEvidenceError("commands.jsonl records must be objects")
        command_id = payload.get("command_id")
        if not isinstance(command_id, str) or command_id in records:
            raise KindEvidenceError("commands.jsonl command IDs must be unique strings")
        if payload.get("claim_id") not in REQUIRED_CLAIMS:
            raise KindEvidenceError("commands.jsonl records must bind a known claim id")
        records[command_id] = payload
    return records


def _write_private_json(path: Path, payload: Mapping[str, object]) -> None:
    _write_private_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_private_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8", newline="\n")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def _safe_label(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]+", "-", value.strip().lower()).strip("-")
    if not normalized:
        raise ValueError("command label must contain a safe character")
    return normalized[:80]


def _jsonl_line_count(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
