from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import stat
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from scripts.kind_evidence import (
    HARNESS_OWNED_LABEL,
    KIND_NODE_IMAGE,
    KUBERNETES_VERSION,
    RUN_ID_LABEL,
    ChartState,
    ClaimLedger,
    ClaimStatus,
    CommandOutcome,
    CommandRecorder,
    EvidenceBundle,
    GitState,
    HarnessCredentials,
    KindEvidenceError,
    RunIdentity,
    build_environment_payload,
    build_external_data_store_manifests,
    build_external_secret_manifest,
    build_namespace_manifests,
    capture_chart_state,
    capture_git_state,
    summarize_kubernetes_resources,
    validate_evidence_bundle,
    validate_kind_version,
    validate_owned_namespace_for_cleanup,
    validate_pinned_image,
)

DEFAULT_POSTGRES_IMAGE = (
    "docker.io/library/postgres@sha256:"
    "cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
)
DEFAULT_REDIS_IMAGE = (
    "docker.io/library/redis@sha256:"
    "ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf"
)
FAKE_PLUGIN_IMAGE = (
    "registry.k8s.io/e2e-test-images/sample-device-plugin@sha256:"
    "2227c6949af186356919c2e63c35d68f07f4722d810e55cd97c7850e368d080d"
)
FAKE_ALLOCATION_IMAGE = (
    "registry.k8s.io/e2e-test-images/agnhost@sha256:"
    "541cafada1867e8684b25d24f0cb1132e76aff093401b5987490b654fbd79c0a"
)
APP_REPOSITORY = "docker.io/library/mini-ai-cloud"
NODE_PORT = 30080
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_VERSION = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")


@dataclass(frozen=True, slots=True)
class ToolPaths:
    helm: str
    kind: str
    kubectl: str
    docker: str
    uv: str


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    repository_root: Path
    evidence_root: Path
    chart_root: Path
    kind_config_template: Path
    tools: ToolPaths
    postgres_image: str = DEFAULT_POSTGRES_IMAGE
    redis_image: str = DEFAULT_REDIS_IMAGE


@dataclass(frozen=True, slots=True)
class PinnedImageAlias:
    component: str
    digest_reference: str
    local_tag: str
    containerd_tag: str


@dataclass(slots=True)
class PhaseFailure(RuntimeError):
    detail: str
    outcomes: list[CommandOutcome] = field(default_factory=list)

    def __str__(self) -> str:
        return self.detail


def render_kind_config(template: str, host_port: int) -> str:
    if template.count("__HOST_PORT__") != 1:
        raise KindEvidenceError("Kind config must contain exactly one __HOST_PORT__ placeholder")
    if not 1024 <= host_port <= 65535:
        raise KindEvidenceError("Kind host port must be an unprivileged TCP port")
    rendered = template.replace("__HOST_PORT__", str(host_port))
    payload = yaml.safe_load(rendered)
    if not isinstance(payload, dict) or payload.get("kind") != "Cluster":
        raise KindEvidenceError("rendered Kind config is invalid")
    return rendered


def parse_build_digest(payload: Mapping[str, object]) -> str:
    for key in ("containerimage.digest", "containerimage.descriptor.digest"):
        value = payload.get(key)
        if isinstance(value, str) and _DIGEST.fullmatch(value):
            return value
    raise KindEvidenceError("Docker build metadata omitted an exact application manifest digest")


def application_reference(tag: str, digest: str) -> str:
    if not re.fullmatch(r"m7-[0-9a-f]{8}", tag) or not _DIGEST.fullmatch(digest):
        raise KindEvidenceError("application tag or digest is not safely bounded")
    return f"{APP_REPOSITORY}:{tag}@{digest}"


def chart_fullname(release_name: str) -> str:
    return release_name if "mini-ai-cloud" in release_name else f"{release_name}-mini-ai-cloud"


def pinned_image_aliases(
    identity: RunIdentity,
    *,
    postgres_image: str,
    redis_image: str,
) -> tuple[PinnedImageAlias, ...]:
    suffix = identity.run_id[-8:]
    sources = (
        ("postgres", postgres_image),
        ("redis", redis_image),
        ("fake-plugin", FAKE_PLUGIN_IMAGE),
        ("fake-allocation", FAKE_ALLOCATION_IMAGE),
    )
    aliases: list[PinnedImageAlias] = []
    for component, source in sources:
        validate_pinned_image(source, description=f"{component} image")
        local_tag = f"mini-ai-cloud-p4-{component}:m7-{suffix}"
        aliases.append(
            PinnedImageAlias(
                component=component,
                digest_reference=source,
                local_tag=local_tag,
                containerd_tag=f"docker.io/library/{local_tag}",
            )
        )
    return tuple(aliases)


def local_image_cleanup_argv(docker: str, tags: Sequence[str]) -> tuple[str, ...]:
    unique_tags = tuple(dict.fromkeys(tags))
    if not unique_tags:
        raise KindEvidenceError("local image cleanup requires at least one run-specific tag")
    if any(
        not (
            re.fullmatch(r"mini-ai-cloud:m7-[0-9a-f]{8}", tag)
            or re.fullmatch(r"mini-ai-cloud-p4-[a-z-]+:m7-[0-9a-f]{8}", tag)
        )
        for tag in unique_tags
    ):
        raise KindEvidenceError("refusing to remove an image outside the run-specific tag contract")
    return (docker, "image", "rm", "--force", *unique_tags)


def image_archive_path(temp_root: Path, component: str) -> Path:
    if not re.fullmatch(r"[a-z]+(?:-[a-z]+)*", component):
        raise KindEvidenceError("image archive component is not safely bounded")
    resolved_temp = temp_root.resolve(strict=True)
    archive_root = (resolved_temp / "image-archives").resolve()
    if archive_root.parent != resolved_temp:
        raise KindEvidenceError("image archive root escaped the private harness directory")
    archive = (archive_root / f"{component}.tar").resolve()
    if archive.parent != archive_root:
        raise KindEvidenceError("image archive path escaped the private archive directory")
    return archive


def docker_image_save_argv(
    docker: str,
    alias: PinnedImageAlias,
    archive: Path,
) -> tuple[str, ...]:
    if archive.name != f"{alias.component}.tar" or archive.suffix != ".tar":
        raise KindEvidenceError("image archive name does not match its pinned component")
    return (
        docker,
        "image",
        "save",
        "--platform",
        "linux/amd64",
        "--output",
        str(archive),
        alias.local_tag,
    )


def kind_image_archive_load_argv(
    kind: str,
    cluster_name: str,
    archive: Path,
) -> tuple[str, ...]:
    if not archive.is_absolute() or archive.suffix != ".tar":
        raise KindEvidenceError("Kind image archive must be an absolute tar path")
    return (
        kind,
        "load",
        "image-archive",
        "--name",
        cluster_name,
        str(archive),
    )


def build_upgrade_sentinels(identity: RunIdentity, app_image: str) -> dict[str, object]:
    validate_pinned_image(app_image, description="upgrade sentinel application image")
    labels = {
        RUN_ID_LABEL: identity.run_id,
        HARNESS_OWNED_LABEL: "true",
        "app.kubernetes.io/name": "mini-ai-cloud-kind-harness",
        "app.kubernetes.io/component": "upgrade-sentinel",
    }
    security = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "privileged": False,
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
    }
    pod_security = {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "fsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    container = {
        "image": app_image,
        "imagePullPolicy": "Never",
        "env": [
            {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
            {"name": "TMPDIR", "value": "/tmp"},
        ],
        "resources": {
            "requests": {"cpu": "50m", "memory": "64Mi"},
            "limits": {"cpu": "50m", "memory": "64Mi"},
        },
        "securityContext": security,
        "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
    }
    job_container = {
        **container,
        "name": "batch-sentinel",
        "command": ["python", "-c", "import time; time.sleep(600)"],
    }
    serving_container = {
        **container,
        "name": "serving-sentinel",
        "command": [
            "python",
            "-m",
            "scripts.fake_inference",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--model",
            "upgrade-sentinel",
            "--replica-id",
            "upgrade-sentinel",
            "--execution-id",
            "upgrade-sentinel",
            "--startup-delay-seconds",
            "0",
            "--chunk-delay-seconds",
            "0",
        ],
        "ports": [{"name": "http", "containerPort": 8000}],
        "readinessProbe": {
            "httpGet": {"path": "/health", "port": "http"},
            "periodSeconds": 2,
            "timeoutSeconds": 1,
            "failureThreshold": 30,
        },
    }
    pod_spec = {
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "securityContext": pod_security,
        "restartPolicy": "Never",
        "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}}],
    }
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": f"upgrade-sentinel-{identity.run_id[-8:]}",
                    "namespace": identity.workload_namespace,
                    "labels": labels,
                },
                "data": {"owner": identity.run_id},
            },
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": f"upgrade-job-{identity.run_id[-8:]}",
                    "namespace": identity.workload_namespace,
                    "labels": labels,
                },
                "spec": {
                    "backoffLimit": 0,
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {**pod_spec, "containers": [job_container]},
                    },
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": f"upgrade-serving-{identity.run_id[-8:]}",
                    "namespace": identity.workload_namespace,
                    "labels": labels,
                },
                "spec": {**pod_spec, "containers": [serving_container]},
            },
        ],
    }


def validate_pod_security(document: Mapping[str, object], *, workload_namespace: str) -> None:
    items = document.get("items")
    if not isinstance(items, list):
        raise KindEvidenceError("Pod security query did not return an item list")
    for item in items:
        if not isinstance(item, Mapping):
            raise KindEvidenceError("Pod security query returned an invalid item")
        metadata = item.get("metadata")
        spec = item.get("spec")
        if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
            raise KindEvidenceError("Pod security query returned an incomplete Pod")
        namespace = metadata.get("namespace")
        if (
            namespace == workload_namespace
            and spec.get("automountServiceAccountToken") is not False
        ):
            raise KindEvidenceError("a workload Pod automounts a service account token")
        if any(spec.get(key) is True for key in ("hostNetwork", "hostPID", "hostIPC")):
            raise KindEvidenceError("a harness Pod enables a host namespace")
        volumes = spec.get("volumes", [])
        if not isinstance(volumes, list) or any(
            isinstance(volume, Mapping) and "hostPath" in volume for volume in volumes
        ):
            raise KindEvidenceError("a harness Pod uses hostPath")
        containers = spec.get("containers")
        if not isinstance(containers, list) or not containers:
            raise KindEvidenceError("a harness Pod has no containers")
        for container in containers:
            security = container.get("securityContext") if isinstance(container, Mapping) else None
            if not isinstance(security, Mapping):
                raise KindEvidenceError("a harness container omitted securityContext")
            capabilities = security.get("capabilities")
            dropped = capabilities.get("drop") if isinstance(capabilities, Mapping) else None
            if (
                security.get("allowPrivilegeEscalation") is not False
                or security.get("privileged") is True
                or security.get("readOnlyRootFilesystem") is not True
                or dropped != ["ALL"]
            ):
                raise KindEvidenceError("a harness container violates the security contract")


def _private_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)


def _fingerprint(path: Path) -> str | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise KindEvidenceError("default kubeconfig path is not a regular file")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _free_host_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        port = int(listener.getsockname()[1])
    if port < 1024:
        raise KindEvidenceError("operating system selected a privileged host port")
    return port


def _json_object(text: str, *, description: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise KindEvidenceError(f"{description} did not return JSON") from error
    if not isinstance(payload, dict):
        raise KindEvidenceError(f"{description} did not return a JSON object")
    return payload


def _items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("items")
    if not isinstance(raw, list):
        raise KindEvidenceError("Kubernetes list response omitted items")
    return [item for item in raw if isinstance(item, dict)]


class KindAdaptationHarness:
    def __init__(
        self,
        config: HarnessConfig,
        *,
        identity: RunIdentity | None = None,
    ) -> None:
        self.config = config
        self.identity = identity or RunIdentity.create()
        self.identity.validate()
        self.credentials = HarnessCredentials.generate()
        self.user_password = secrets.token_urlsafe(32)
        self.temp_root = Path(tempfile.mkdtemp(prefix=f"mini-ai-cloud-{self.identity.run_id}-"))
        os.chmod(self.temp_root, 0o700)
        self.kubeconfig = self.temp_root / "kubeconfig"
        self.build_metadata = self.temp_root / "build-metadata.json"
        self.rendered_kind_config = self.temp_root / "kind-config.yaml"
        self.api_key_file = self.temp_root / "api-key"
        self.batch_credentials_file = self.temp_root / "batch-credentials.json"
        self.bin_root = self.temp_root / "bin"
        self.bin_root.mkdir(mode=0o700)
        self.image_archive_root = self.temp_root / "image-archives"
        self.image_archive_root.mkdir(mode=0o700)
        self._install_kubectl_shim()
        self.bundle = EvidenceBundle(config.evidence_root, self.identity)
        self.recorder = CommandRecorder(self.bundle.root, config.repository_root)
        self.recorder.register_sensitive_values(
            (*self.credentials.sensitive_values(self.identity), self.user_password)
        )
        self.ledger = ClaimLedger()
        self.git_state: GitState = capture_git_state(config.repository_root)
        self.chart_state: ChartState = capture_chart_state(config.chart_root)
        self.tool_versions: dict[str, str] = {}
        self.server_version: str | None = None
        self.host_port = _free_host_port()
        self.local_app_tag = f"mini-ai-cloud:m7-{self.identity.run_id[-8:]}"
        self.app_tag = f"m7-{self.identity.run_id[-8:]}"
        self.app_image = f"{APP_REPOSITORY}:{self.app_tag}@sha256:{'0' * 64}"
        self.image_aliases = pinned_image_aliases(
            self.identity,
            postgres_image=config.postgres_image,
            redis_image=config.redis_image,
        )
        self.created_local_image_tags: list[str] = []
        self.image_archives: dict[str, Path] = {}
        self.cluster_created = False
        self.release_installed = False
        self.cleanup_complete = False
        self.kubernetes_summary: dict[str, object] = {
            "schema_version": "1.0.0",
            "status": ClaimStatus.NOT_RUN.value,
            "resource_count": 0,
            "resources": [],
        }
        self.cleanup_payload: dict[str, object] = {
            "schema_version": "1.0.0",
            "status": ClaimStatus.NOT_RUN.value,
            "release_owned_remaining": -1,
            "external_secret_preserved_after_uninstall": False,
            "external_namespaces_preserved_after_uninstall": False,
            "cluster_deleted": False,
            "default_kubeconfig_unchanged": False,
            "temporary_state_deleted": False,
        }
        self.default_kubeconfig = Path.home() / ".kube" / "config"
        self.default_kubeconfig_before = _fingerprint(self.default_kubeconfig)

    def _install_kubectl_shim(self) -> None:
        target = self.bin_root / "kubectl"
        source = Path(self.config.tools.kubectl).expanduser()
        resolved: Path | None = None
        if source.is_absolute() or source.parent != Path("."):
            resolved = source.resolve(strict=False)
        else:
            located = shutil.which(self.config.tools.kubectl)
            if located is not None:
                resolved = Path(located).resolve()
        if resolved is not None:
            target.symlink_to(resolved)

    @property
    def fullname(self) -> str:
        return chart_fullname(self.identity.release_name)

    @property
    def control_deployment(self) -> str:
        return f"{self.fullname}-control-plane"

    @property
    def worker_deployment(self) -> str:
        return f"{self.fullname}-worker"

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.host_port}"

    def execute(self) -> tuple[int, Path | None, str | None]:
        failure: str | None = None
        failed_claim: str | None = None
        phases: tuple[tuple[str, str, Callable[[], list[CommandOutcome]]], ...] = (
            (
                "helm-render",
                "Helm lint, schema, template, and fixed tool pins passed.",
                self._helm_render,
            ),
            (
                "helm-install",
                "The pinned Kind cluster and Helm release installed.",
                self._helm_install,
            ),
            (
                "migration",
                "The Helm hook migrated PostgreSQL to the repository head.",
                self._migration,
            ),
            (
                "control-plane-readiness",
                "The control-plane Deployment and NodePort API became ready.",
                self._control_ready,
            ),
            ("worker-readiness", "The Worker Deployment became ready.", self._worker_ready),
            ("batch-lifecycle", "The real batch/v1 Job lifecycle helper passed.", self._batch),
            ("serving-lifecycle", "The real Fake serving lifecycle helper passed.", self._serving),
            (
                "accelerator-contract",
                "Fake allocation and NVIDIA/Ascend render contracts passed; "
                "real hardware was not run.",
                self._accelerator,
            ),
            (
                "security-contract",
                "Runtime and rendered security boundaries passed.",
                self._security,
            ),
            (
                "upgrade-smoke",
                "A ConfigMap upgrade preserved existing Job and serving Pod UIDs.",
                self._upgrade,
            ),
            (
                "uninstall-cleanup",
                "Uninstall preserved external objects and exact scoped cleanup "
                "removed the cluster.",
                self._uninstall_cleanup,
            ),
        )
        for claim_id, detail, action in phases:
            try:
                outcomes = action()
                self.ledger.mark_pass(claim_id, outcomes, detail=detail)
            except PhaseFailure as error:
                failure = self.recorder.redact(error.detail)
                failed_claim = claim_id
                self.ledger.mark_fail(claim_id, detail=failure, outcomes=error.outcomes)
                break
            except Exception as error:
                failure = self.recorder.redact(f"{type(error).__name__}: {error}")
                failed_claim = claim_id
                self.ledger.mark_fail(claim_id, detail=failure)
                break

        if not self.cleanup_complete:
            cleanup_outcomes, cleanup_error = self._emergency_cleanup()
            if cleanup_error is not None:
                failure = failure or cleanup_error
            if failed_claim != "uninstall-cleanup":
                self.ledger.mark_fail(
                    "uninstall-cleanup",
                    detail=(
                        "Emergency cleanup ran; the full uninstall preservation "
                        "contract was not proven."
                    ),
                    outcomes=cleanup_outcomes,
                )

        temp_error = self._remove_temporary_state()
        if temp_error is not None:
            failure = failure or temp_error
            self.cleanup_payload["status"] = ClaimStatus.FAIL.value
            self.ledger.mark_fail("uninstall-cleanup", detail=temp_error)

        bundle_path: Path | None = None
        try:
            self.kubernetes_summary["status"] = (
                ClaimStatus.PASS.value
                if self.ledger.overall_status() == "KIND_K8S_PASS"
                else ClaimStatus.FAIL.value
            )
            bundle_path = self.bundle.finalize(
                ledger=self.ledger,
                environment=self._environment_payload(),
                kubernetes_summary=self.kubernetes_summary,
                cleanup=self.cleanup_payload,
                limitations=(
                    "Single-node Kind validates Kubernetes control contracts, not production HA.",
                    "Fake accelerators do not prove NVIDIA or Ascend hardware execution.",
                    "REAL_HW_NOT_RUN remains the hardware evidence boundary.",
                ),
                sensitive_values=self.recorder.sensitive_values,
            )
        except Exception as error:
            failure = failure or self.recorder.redact(
                f"evidence finalization failed: {type(error).__name__}: {error}"
            )

        status = self.ledger.overall_status()
        succeeded = failure is None and status == "KIND_K8S_PASS" and bundle_path is not None
        return (0 if succeeded else 1), bundle_path, failure

    def _record(
        self,
        outcomes: list[CommandOutcome],
        claim_id: str,
        label: str,
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 900,
    ) -> CommandOutcome:
        outcome = self.recorder.record(
            label,
            argv,
            claim_id=claim_id,
            input_text=input_text,
            environment=environment,
            timeout_seconds=timeout_seconds,
        )
        outcomes.append(outcome)
        if outcome.returncode != 0:
            raise PhaseFailure(
                f"{label} failed with exit code {outcome.returncode}; "
                "inspect redacted evidence logs",
                outcomes.copy(),
            )
        return outcome

    def _helm_render(self) -> list[CommandOutcome]:
        claim = "helm-render"
        outcomes: list[CommandOutcome] = []
        for image, description in (
            (self.config.postgres_image, "PostgreSQL image"),
            (self.config.redis_image, "Redis image"),
            (FAKE_PLUGIN_IMAGE, "Fake Device Plugin image"),
            (FAKE_ALLOCATION_IMAGE, "Fake allocation image"),
        ):
            validate_pinned_image(image, description=description)
        kind = self._record(outcomes, claim, "kind-version", (self.config.tools.kind, "version"))
        self.tool_versions["kind"] = validate_kind_version(kind.stdout)
        helm = self._record(
            outcomes,
            claim,
            "helm-version",
            (self.config.tools.helm, "version", "--short"),
        )
        helm_match = _VERSION.search(helm.stdout)
        if helm_match is None:
            raise PhaseFailure("Helm did not report a semantic version", outcomes)
        self.tool_versions["helm"] = helm_match.group(0)
        kubectl = self._record(
            outcomes,
            claim,
            "kubectl-client-version",
            (self.config.tools.kubectl, "version", "--client", "-o", "json"),
        )
        kubectl_payload = _json_object(kubectl.stdout, description="kubectl client version")
        client = kubectl_payload.get("clientVersion")
        client_version = client.get("gitVersion") if isinstance(client, Mapping) else None
        if client_version != KUBERNETES_VERSION:
            raise PhaseFailure(f"kubectl client must be exactly {KUBERNETES_VERSION}", outcomes)
        self.tool_versions["kubectl"] = str(client_version)
        docker = self._record(
            outcomes,
            claim,
            "docker-version",
            (self.config.tools.docker, "version", "--format", "{{.Client.Version}}"),
        )
        self.tool_versions["docker"] = docker.stdout.strip()
        uv = self._record(outcomes, claim, "uv-version", (self.config.tools.uv, "--version"))
        self.tool_versions["uv"] = uv.stdout.strip()
        self._record(
            outcomes,
            claim,
            "build-application-image",
            (
                self.config.tools.docker,
                "buildx",
                "build",
                "--load",
                "--provenance=false",
                "--file",
                "docker/Dockerfile",
                "--tag",
                self.local_app_tag,
                "--metadata-file",
                str(self.build_metadata),
                ".",
            ),
            timeout_seconds=1800,
        )
        self.created_local_image_tags.append(self.local_app_tag)
        metadata = _json_object(
            self.build_metadata.read_text(encoding="utf-8"), description="Docker build metadata"
        )
        digest = parse_build_digest(metadata)
        self.app_image = application_reference(self.app_tag, digest)
        validate_pinned_image(self.app_image, description="application image")
        self._record(
            outcomes,
            claim,
            "helm-lint",
            (self.config.tools.helm, "lint", str(self.config.chart_root)),
        )
        self._record(
            outcomes,
            claim,
            "helm-template",
            (
                self.config.tools.helm,
                "template",
                self.identity.release_name,
                str(self.config.chart_root),
                "--namespace",
                self.identity.system_namespace,
                *self._helm_values(),
            ),
        )
        return outcomes

    def _helm_install(self) -> list[CommandOutcome]:
        claim = "helm-install"
        outcomes: list[CommandOutcome] = []
        kind_template = self.config.kind_config_template.read_text(encoding="utf-8")
        _private_write(self.rendered_kind_config, render_kind_config(kind_template, self.host_port))
        for alias in self.image_aliases:
            self._record(
                outcomes,
                claim,
                f"pull-{alias.component}-image",
                (
                    self.config.tools.docker,
                    "pull",
                    "--platform",
                    "linux/amd64",
                    alias.digest_reference,
                ),
                timeout_seconds=900,
            )
            self._record(
                outcomes,
                claim,
                f"tag-{alias.component}-single-platform-image",
                (
                    self.config.tools.docker,
                    "tag",
                    alias.digest_reference,
                    alias.local_tag,
                ),
            )
            self.created_local_image_tags.append(alias.local_tag)
            archive = image_archive_path(self.temp_root, alias.component)
            self._record(
                outcomes,
                claim,
                f"save-{alias.component}-single-platform-archive",
                docker_image_save_argv(self.config.tools.docker, alias, archive),
                timeout_seconds=900,
            )
            if not archive.is_file() or archive.is_symlink() or archive.stat().st_size == 0:
                raise PhaseFailure(
                    f"{alias.component} single-platform image archive is invalid", outcomes
                )
            os.chmod(archive, stat.S_IRUSR | stat.S_IWUSR)
            self.image_archives[alias.component] = archive
        self._record(
            outcomes,
            claim,
            "create-kind-cluster",
            (
                self.config.tools.kind,
                "create",
                "cluster",
                "--name",
                self.identity.cluster_name,
                "--image",
                KIND_NODE_IMAGE,
                "--config",
                str(self.rendered_kind_config),
                "--kubeconfig",
                str(self.kubeconfig),
                "--wait",
                "180s",
            ),
            timeout_seconds=600,
        )
        self.cluster_created = True
        os.chmod(self.kubeconfig, stat.S_IRUSR | stat.S_IWUSR)
        node = f"{self.identity.cluster_name}-control-plane"
        node_image = self._record(
            outcomes,
            claim,
            "verify-kind-node-image",
            (
                self.config.tools.docker,
                "inspect",
                node,
                "--format",
                "{{.Config.Image}}",
            ),
        )
        if node_image.stdout.strip() != KIND_NODE_IMAGE:
            raise PhaseFailure("Kind node container does not use the pinned image", outcomes)
        self._record(
            outcomes,
            claim,
            "load-application-image",
            (
                self.config.tools.kind,
                "load",
                "docker-image",
                "--name",
                self.identity.cluster_name,
                self.local_app_tag,
            ),
            timeout_seconds=600,
        )
        for alias in self.image_aliases:
            try:
                archive = self.image_archives[alias.component]
            except KeyError as error:
                raise PhaseFailure(
                    f"{alias.component} single-platform image archive was not prepared", outcomes
                ) from error
            self._record(
                outcomes,
                claim,
                f"load-{alias.component}-image-archive",
                kind_image_archive_load_argv(
                    self.config.tools.kind,
                    self.identity.cluster_name,
                    archive,
                ),
                timeout_seconds=600,
            )
        canonical_tag = f"{APP_REPOSITORY}:{self.app_tag}"
        self._record(
            outcomes,
            claim,
            "tag-application-digest-in-containerd",
            (
                self.config.tools.docker,
                "exec",
                node,
                "ctr",
                "--namespace",
                "k8s.io",
                "images",
                "tag",
                "--force",
                canonical_tag,
                self.app_image,
            ),
        )
        self._record(
            outcomes,
            claim,
            "verify-application-digest-in-containerd",
            (
                self.config.tools.docker,
                "exec",
                node,
                "ctr",
                "--namespace",
                "k8s.io",
                "images",
                "inspect",
                self.app_image,
            ),
        )
        for alias in self.image_aliases:
            self._record(
                outcomes,
                claim,
                f"alias-{alias.component}-digest-in-containerd",
                (
                    self.config.tools.docker,
                    "exec",
                    node,
                    "ctr",
                    "--namespace",
                    "k8s.io",
                    "images",
                    "tag",
                    "--force",
                    alias.containerd_tag,
                    alias.digest_reference,
                ),
            )
            self._record(
                outcomes,
                claim,
                f"verify-{alias.component}-digest-in-containerd",
                (
                    self.config.tools.docker,
                    "exec",
                    node,
                    "ctr",
                    "--namespace",
                    "k8s.io",
                    "images",
                    "inspect",
                    alias.digest_reference,
                ),
            )
        server = self._record(
            outcomes,
            claim,
            "kubernetes-server-version",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "version",
                "-o",
                "json",
            ),
        )
        version_payload = _json_object(server.stdout, description="Kubernetes server version")
        server_data = version_payload.get("serverVersion")
        self.server_version = (
            str(server_data.get("gitVersion")) if isinstance(server_data, Mapping) else None
        )
        if self.server_version != KUBERNETES_VERSION:
            raise PhaseFailure(f"Kubernetes server must be exactly {KUBERNETES_VERSION}", outcomes)
        namespaces = json.dumps(build_namespace_manifests(self.identity))
        self._record(
            outcomes,
            claim,
            "apply-owned-namespaces",
            (self.config.tools.kubectl, "--kubeconfig", str(self.kubeconfig), "apply", "-f", "-"),
            input_text=namespaces,
        )
        secret = json.dumps(build_external_secret_manifest(self.identity, self.credentials))
        self._record(
            outcomes,
            claim,
            "apply-external-secret",
            (self.config.tools.kubectl, "--kubeconfig", str(self.kubeconfig), "apply", "-f", "-"),
            input_text=secret,
        )
        stores = json.dumps(
            build_external_data_store_manifests(
                self.identity,
                postgres_image=self.config.postgres_image,
                redis_image=self.config.redis_image,
            )
        )
        self._record(
            outcomes,
            claim,
            "apply-external-data-stores",
            (self.config.tools.kubectl, "--kubeconfig", str(self.kubeconfig), "apply", "-f", "-"),
            input_text=stores,
        )
        for deployment in (self.identity.postgres_name, self.identity.redis_name):
            self._record(
                outcomes,
                claim,
                f"wait-{deployment}",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    self.identity.system_namespace,
                    "rollout",
                    "status",
                    f"deployment/{deployment}",
                    "--timeout=180s",
                ),
            )
        self._record(
            outcomes,
            claim,
            "helm-upgrade-install",
            tuple(self._helm_command(log_level="INFO")),
            timeout_seconds=900,
        )
        self.release_installed = True
        return outcomes

    def _migration(self) -> list[CommandOutcome]:
        claim = "migration"
        outcomes: list[CommandOutcome] = []
        local = self._record(
            outcomes,
            claim,
            "read-local-alembic-head",
            (self.config.tools.uv, "run", "alembic", "heads"),
        )
        cluster = self._record(
            outcomes,
            claim,
            "read-cluster-alembic-version",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.system_namespace,
                "exec",
                f"deployment/{self.identity.postgres_name}",
                "--",
                "psql",
                "-U",
                "task",
                "-d",
                "task_platform",
                "-tAc",
                "SELECT version_num FROM alembic_version;",
            ),
        )
        local_head = local.stdout.strip().split()[0] if local.stdout.strip() else ""
        cluster_head = cluster.stdout.strip()
        if not local_head or cluster_head != local_head:
            raise PhaseFailure(
                "cluster Alembic version does not match the repository head", outcomes
            )
        return outcomes

    def _control_ready(self) -> list[CommandOutcome]:
        claim = "control-plane-readiness"
        outcomes: list[CommandOutcome] = []
        self._rollout(outcomes, claim, self.control_deployment)
        self._record(
            outcomes,
            claim,
            "wait-nodeport-api",
            (
                self.config.tools.uv,
                "run",
                "python",
                "scripts/kind_serving_e2e.py",
                "wait-ready",
                "--base-url",
                self.base_url,
                "--timeout",
                "120",
            ),
        )
        return outcomes

    def _worker_ready(self) -> list[CommandOutcome]:
        outcomes: list[CommandOutcome] = []
        self._rollout(outcomes, "worker-readiness", self.worker_deployment)
        return outcomes

    def _batch(self) -> list[CommandOutcome]:
        claim = "batch-lifecycle"
        outcomes: list[CommandOutcome] = []
        _private_write(
            self.batch_credentials_file,
            json.dumps(
                {
                    "bootstrap_token": self.credentials.bootstrap_token,
                    "user_password": self.user_password,
                }
            ),
        )
        environment = self._helper_environment(
            {
                "KIND_BATCH_BASE_URL": self.base_url,
                "KIND_BATCH_KUBECONFIG": str(self.kubeconfig),
                "KIND_BATCH_WORKLOAD_NAMESPACE": self.identity.workload_namespace,
                "KIND_BATCH_SYSTEM_NAMESPACE": self.identity.system_namespace,
                "KIND_BATCH_WORKER_DEPLOYMENT": self.worker_deployment,
                "KIND_BATCH_APP_IMAGE": self.app_image,
                "KIND_BATCH_API_KEY_FILE": str(self.api_key_file),
                "KIND_BATCH_CREDENTIALS_FILE": str(self.batch_credentials_file),
            }
        )
        self._record(
            outcomes,
            claim,
            "run-batch-lifecycle-helper",
            (self.config.tools.uv, "run", "python", "scripts/kind_batch_e2e.py", "run"),
            environment=environment,
            timeout_seconds=1200,
        )
        self._register_api_key()
        return outcomes

    def _serving(self) -> list[CommandOutcome]:
        claim = "serving-lifecycle"
        outcomes: list[CommandOutcome] = []
        environment = self._helper_environment(
            {
                "KIND_SERVING_BASE_URL": self.base_url,
                "KIND_SERVING_KUBECONFIG": str(self.kubeconfig),
                "KIND_SERVING_NAMESPACE": self.identity.system_namespace,
                "KIND_SERVING_WORKLOAD_NAMESPACE": self.identity.workload_namespace,
                "KIND_SERVING_CONTROLLER_DEPLOYMENT": self.control_deployment,
                "KIND_SERVING_APP_IMAGE": self.app_image,
                "KIND_SERVING_IMAGE_POLICY_REPOSITORY": "library/mini-ai-cloud",
                "KIND_SERVING_IMAGE_POLICY_TAG": self.app_tag,
                "KIND_SERVING_BOOTSTRAP_TOKEN": self.credentials.bootstrap_token,
                "KIND_SERVING_USER_PASSWORD": self.user_password,
                "KIND_SERVING_API_KEY_FILE": str(self.api_key_file),
            }
        )
        self._record(
            outcomes,
            claim,
            "run-serving-lifecycle-helper",
            (self.config.tools.uv, "run", "python", "scripts/kind_serving_e2e.py", "run"),
            environment=environment,
            timeout_seconds=1200,
        )
        self._register_api_key()
        return outcomes

    def _accelerator(self) -> list[CommandOutcome]:
        claim = "accelerator-contract"
        outcomes: list[CommandOutcome] = []
        suffix = self.identity.run_id[-8:]
        plugin = f"mac-plugin-{suffix}"
        allocation = f"mac-allocation-{suffix}"
        environment = self._helper_environment(
            {
                "KIND_SERVING_KUBECONFIG": str(self.kubeconfig),
                "KIND_SERVING_WORKLOAD_NAMESPACE": self.identity.workload_namespace,
                "KIND_SERVING_FAKE_PLUGIN_NAME": plugin,
                "KIND_SERVING_FAKE_ALLOCATION_NAME": allocation,
            }
        )
        self._record(
            outcomes,
            claim,
            "run-fake-device-plugin-helper",
            ("bash", "scripts/nvidia_fake_device_plugin.sh", "test"),
            environment=environment,
            timeout_seconds=300,
        )
        self._record(
            outcomes,
            claim,
            "validate-runtime-profile-contracts",
            (
                self.config.tools.uv,
                "run",
                "pytest",
                "-q",
                "tests/unit/test_nvidia_runtime.py",
                "tests/unit/test_ascend_runtime.py",
                "tests/unit/test_kubernetes_serving_preflight.py",
            ),
            timeout_seconds=600,
        )
        for namespace, kind, name in (
            ("kube-system", "daemonset", plugin),
            (self.identity.workload_namespace, "pod", allocation),
        ):
            absent = self._record(
                outcomes,
                claim,
                f"verify-{kind}-{name}-cleaned",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    namespace,
                    "get",
                    kind,
                    name,
                    "--ignore-not-found",
                    "-o",
                    "name",
                ),
            )
            if absent.stdout.strip():
                raise PhaseFailure("Fake Device Plugin helper left a scoped resource", outcomes)
        return outcomes

    def _security(self) -> list[CommandOutcome]:
        claim = "security-contract"
        outcomes: list[CommandOutcome] = []
        self._record(
            outcomes,
            claim,
            "validate-helm-security-contract",
            (
                self.config.tools.uv,
                "run",
                "python",
                "scripts/validate_helm_render.py",
                "--helm",
                self.config.tools.helm,
            ),
        )
        pods = self._record(
            outcomes,
            claim,
            "inspect-system-and-workload-pods",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "pods",
                "--namespace",
                self.identity.system_namespace,
                "-o",
                "json",
            ),
        )
        workload = self._record(
            outcomes,
            claim,
            "inspect-workload-pods",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "get",
                "pods",
                "--namespace",
                self.identity.workload_namespace,
                "-o",
                "json",
            ),
        )
        merged = {
            "items": [
                *_items(_json_object(pods.stdout, description="system Pod query")),
                *_items(_json_object(workload.stdout, description="workload Pod query")),
            ]
        }
        validate_pod_security(merged, workload_namespace=self.identity.workload_namespace)
        control_sa = self.control_deployment
        worker_sa = self.worker_deployment
        for service_account, verb, resource in (
            (control_sa, "create", "pods"),
            (worker_sa, "create", "jobs.batch"),
        ):
            denial = self._record(
                outcomes,
                claim,
                f"deny-{service_account}-outside-allowlist",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "auth",
                    "can-i",
                    verb,
                    resource,
                    "--namespace",
                    "default",
                    "--as",
                    f"system:serviceaccount:{self.identity.system_namespace}:{service_account}",
                ),
            )
            if denial.stdout.strip() != "no":
                raise PhaseFailure("namespaced RBAC crossed the allowlist boundary", outcomes)
        return outcomes

    def _upgrade(self) -> list[CommandOutcome]:
        claim = "upgrade-smoke"
        outcomes: list[CommandOutcome] = []
        sentinels = build_upgrade_sentinels(self.identity, self.app_image)
        self._record(
            outcomes,
            claim,
            "apply-upgrade-sentinels",
            (self.config.tools.kubectl, "--kubeconfig", str(self.kubeconfig), "apply", "-f", "-"),
            input_text=json.dumps(sentinels),
        )
        suffix = self.identity.run_id[-8:]
        pod_name = f"upgrade-serving-{suffix}"
        job_name = f"upgrade-job-{suffix}"
        self._record(
            outcomes,
            claim,
            "wait-upgrade-serving-sentinel",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.workload_namespace,
                "wait",
                "--for=condition=Ready",
                f"pod/{pod_name}",
                "--timeout=120s",
            ),
        )
        before = {
            "pod": self._resource_uid(outcomes, claim, "pod", pod_name, "before"),
            "job": self._resource_uid(outcomes, claim, "job", job_name, "before"),
        }
        self._record(
            outcomes,
            claim,
            "helm-configmap-upgrade",
            tuple(self._helm_command(log_level="DEBUG")),
            timeout_seconds=900,
        )
        self._rollout(outcomes, claim, self.control_deployment)
        self._rollout(outcomes, claim, self.worker_deployment)
        config = self._record(
            outcomes,
            claim,
            "verify-upgraded-configmap",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.system_namespace,
                "get",
                "configmap",
                f"{self.fullname}-config",
                "-o",
                "jsonpath={.data.LOG_LEVEL}",
            ),
        )
        if config.stdout.strip() != "DEBUG":
            raise PhaseFailure("Helm upgrade did not update the ordinary ConfigMap", outcomes)
        after = {
            "pod": self._resource_uid(outcomes, claim, "pod", pod_name, "after"),
            "job": self._resource_uid(outcomes, claim, "job", job_name, "after"),
        }
        if before != after:
            raise PhaseFailure(
                "Helm upgrade replaced or deleted an existing workload sentinel", outcomes
            )
        self._capture_kubernetes_summary(outcomes, claim)
        return outcomes

    def _uninstall_cleanup(self) -> list[CommandOutcome]:
        claim = "uninstall-cleanup"
        outcomes: list[CommandOutcome] = []
        self._record(
            outcomes,
            claim,
            "helm-uninstall",
            (
                self.config.tools.helm,
                "uninstall",
                self.identity.release_name,
                "--namespace",
                self.identity.system_namespace,
                "--kubeconfig",
                str(self.kubeconfig),
                "--wait",
                "--timeout",
                "5m",
            ),
        )
        self.release_installed = False
        secret = self._record(
            outcomes,
            claim,
            "verify-external-secret-preserved",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.system_namespace,
                "get",
                "secret",
                self.identity.external_secret_name,
                "-o",
                "jsonpath={.metadata.uid}",
            ),
        )
        if not secret.stdout.strip():
            raise PhaseFailure("external Secret was removed by Helm uninstall", outcomes)
        for namespace in (self.identity.system_namespace, self.identity.workload_namespace):
            present = self._record(
                outcomes,
                claim,
                f"verify-{namespace}-preserved",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "get",
                    "namespace",
                    namespace,
                    "-o",
                    "name",
                ),
            )
            if present.stdout.strip() != f"namespace/{namespace}":
                raise PhaseFailure("external namespace was removed by Helm uninstall", outcomes)
        remaining_documents = self._wait_release_owned_gone(outcomes, claim)
        remaining_count = len(remaining_documents)
        self.cleanup_payload["release_owned_remaining"] = remaining_count
        self.cleanup_payload["external_secret_preserved_after_uninstall"] = True
        self.cleanup_payload["external_namespaces_preserved_after_uninstall"] = True
        if remaining_count:
            raise PhaseFailure("Helm uninstall left release-owned resources", outcomes)
        _private_write(
            self.bundle.root / "remaining-resources.json",
            json.dumps(
                {
                    "schema_version": "1.0.0",
                    "release_owned": remaining_documents,
                    "external_secret": {
                        "name": self.identity.external_secret_name,
                        "namespace": self.identity.system_namespace,
                        "preserved": True,
                    },
                    "external_namespaces": [
                        self.identity.system_namespace,
                        self.identity.workload_namespace,
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        self._delete_owned_namespaces(outcomes, claim)
        self._delete_cluster(outcomes, claim)
        self._remove_local_images(outcomes, claim)
        if _fingerprint(self.default_kubeconfig) != self.default_kubeconfig_before:
            raise PhaseFailure("the default kubeconfig changed during the isolated run", outcomes)
        self.cleanup_payload.update(
            {
                "status": ClaimStatus.PASS.value,
                "cluster_deleted": True,
                "default_kubeconfig_unchanged": True,
            }
        )
        self.cleanup_complete = True
        return outcomes

    def _wait_release_owned_gone(
        self, outcomes: list[CommandOutcome], claim: str
    ) -> list[dict[str, Any]]:
        resource_types = (
            "pod,service,deployment,replicaset,statefulset,daemonset,job,cronjob,"
            "configmap,serviceaccount,role,rolebinding"
        )
        last_remaining: list[dict[str, Any]] = []
        for attempt in range(1, 31):
            current: list[dict[str, Any]] = []
            for namespace in (self.identity.system_namespace, self.identity.workload_namespace):
                remaining = self._record(
                    outcomes,
                    claim,
                    f"list-release-owned-{namespace}-attempt-{attempt}",
                    (
                        self.config.tools.kubectl,
                        "--kubeconfig",
                        str(self.kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        resource_types,
                        "--selector",
                        f"app.kubernetes.io/instance={self.identity.release_name}",
                        "-o",
                        "json",
                    ),
                )
                document = _json_object(
                    remaining.stdout, description="release-owned resource query"
                )
                current.extend(_items(document))
            if not current:
                return []
            last_remaining = current
            if attempt < 30:
                time.sleep(1)
        return last_remaining

    def _emergency_cleanup(self) -> tuple[list[CommandOutcome], str | None]:
        claim = "uninstall-cleanup"
        outcomes: list[CommandOutcome] = []
        errors: list[str] = []

        def attempt(
            label: str, argv: Sequence[str], *, input_text: str | None = None
        ) -> CommandOutcome:
            outcome = self.recorder.record(
                label,
                argv,
                claim_id=claim,
                input_text=input_text,
                timeout_seconds=300,
            )
            outcomes.append(outcome)
            if outcome.returncode != 0:
                errors.append(f"{label} failed with exit code {outcome.returncode}")
            return outcome

        if self.cluster_created and self.release_installed:
            uninstall = attempt(
                "emergency-helm-uninstall",
                (
                    self.config.tools.helm,
                    "uninstall",
                    self.identity.release_name,
                    "--namespace",
                    self.identity.system_namespace,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--wait",
                    "--timeout",
                    "3m",
                ),
            )
            if uninstall.returncode == 0:
                self.release_installed = False
        if self.cluster_created:
            deletion = attempt(
                "emergency-kind-delete-cluster",
                (self.config.tools.kind, "delete", "cluster", "--name", self.identity.cluster_name),
            )
            if deletion.returncode == 0:
                self.cluster_created = False
            listing = attempt(
                "emergency-verify-kind-cluster-deleted",
                (self.config.tools.kind, "get", "clusters"),
            )
            if listing.returncode == 0:
                self.cluster_created = self.identity.cluster_name in listing.stdout.splitlines()
                if self.cluster_created:
                    errors.append("emergency Kind cluster remained after exact deletion")
        if self.created_local_image_tags:
            image_cleanup = attempt(
                "emergency-remove-run-specific-images",
                local_image_cleanup_argv(self.config.tools.docker, self.created_local_image_tags),
            )
            if image_cleanup.returncode == 0:
                self.created_local_image_tags.clear()
        cluster_deleted = not self.cluster_created
        self.cleanup_payload.update(
            {
                "status": ClaimStatus.FAIL.value if errors else ClaimStatus.PASS.value,
                "cluster_deleted": cluster_deleted,
                "default_kubeconfig_unchanged": (
                    _fingerprint(self.default_kubeconfig) == self.default_kubeconfig_before
                ),
            }
        )
        self.cleanup_complete = not errors
        detail = "; ".join(errors) if errors else None
        return outcomes, detail

    def _delete_owned_namespaces(self, outcomes: list[CommandOutcome], claim: str) -> None:
        for namespace, role in (
            (self.identity.system_namespace, "system"),
            (self.identity.workload_namespace, "workload"),
        ):
            query = self._record(
                outcomes,
                claim,
                f"read-owned-{role}-namespace",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "get",
                    "namespace",
                    namespace,
                    "-o",
                    "json",
                ),
            )
            owned = validate_owned_namespace_for_cleanup(
                _json_object(query.stdout, description=f"{role} namespace"),
                expected_name=namespace,
                expected_run_id=self.identity.run_id,
                expected_role=role,
            )
            self._record(
                outcomes,
                claim,
                f"delete-owned-{role}-namespace-by-uid",
                (
                    self.config.tools.uv,
                    "run",
                    "python",
                    "scripts/kind_kubernetes_adaptation.py",
                    "delete-owned-namespace",
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--name",
                    owned.name,
                    "--uid",
                    owned.uid,
                    "--run-id",
                    owned.run_id,
                    "--role",
                    owned.role,
                ),
            )
            self._record(
                outcomes,
                claim,
                f"wait-owned-{role}-namespace-deleted",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "wait",
                    "--for=delete",
                    f"namespace/{namespace}",
                    "--timeout=180s",
                ),
            )

    def _delete_cluster(self, outcomes: list[CommandOutcome], claim: str) -> None:
        self._record(
            outcomes,
            claim,
            "delete-kind-cluster",
            (self.config.tools.kind, "delete", "cluster", "--name", self.identity.cluster_name),
            timeout_seconds=300,
        )
        self.cluster_created = False
        clusters = self._record(
            outcomes,
            claim,
            "verify-kind-cluster-deleted",
            (self.config.tools.kind, "get", "clusters"),
        )
        if self.identity.cluster_name in clusters.stdout.splitlines():
            raise PhaseFailure("Kind cluster remained after exact deletion", outcomes)

    def _remove_local_images(self, outcomes: list[CommandOutcome], claim: str) -> None:
        self._record(
            outcomes,
            claim,
            "remove-run-specific-images",
            local_image_cleanup_argv(self.config.tools.docker, self.created_local_image_tags),
        )
        self.created_local_image_tags.clear()

    def _remove_temporary_state(self) -> str | None:
        try:
            shutil.rmtree(self.temp_root)
        except OSError as error:
            return f"temporary state cleanup failed: {type(error).__name__}"
        deleted = not self.temp_root.exists()
        self.cleanup_payload["temporary_state_deleted"] = deleted
        return None if deleted else "temporary state directory remained after cleanup"

    def _capture_kubernetes_summary(self, outcomes: list[CommandOutcome], claim: str) -> None:
        resources: list[dict[str, Any]] = []
        for namespace in (self.identity.system_namespace, self.identity.workload_namespace):
            result = self._record(
                outcomes,
                claim,
                f"capture-{namespace}-resources",
                (
                    self.config.tools.kubectl,
                    "--kubeconfig",
                    str(self.kubeconfig),
                    "--namespace",
                    namespace,
                    "get",
                    "pod,service,deployment,job,configmap,serviceaccount,role,rolebinding",
                    "-o",
                    "json",
                ),
            )
            resources.extend(
                _items(_json_object(result.stdout, description=f"{namespace} resources"))
            )
        self.kubernetes_summary = summarize_kubernetes_resources(resources)
        self.kubernetes_summary["status"] = ClaimStatus.PASS.value

    def _resource_uid(
        self,
        outcomes: list[CommandOutcome],
        claim: str,
        kind: str,
        name: str,
        moment: str,
    ) -> str:
        result = self._record(
            outcomes,
            claim,
            f"read-{kind}-{name}-uid-{moment}",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.workload_namespace,
                "get",
                kind,
                name,
                "-o",
                "jsonpath={.metadata.uid}",
            ),
        )
        uid = result.stdout.strip()
        if not uid:
            raise PhaseFailure(f"{kind}/{name} omitted its UID", outcomes)
        return uid

    def _rollout(
        self,
        outcomes: list[CommandOutcome],
        claim: str,
        deployment: str,
    ) -> None:
        self._record(
            outcomes,
            claim,
            f"rollout-{deployment}",
            (
                self.config.tools.kubectl,
                "--kubeconfig",
                str(self.kubeconfig),
                "--namespace",
                self.identity.system_namespace,
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=300s",
            ),
        )

    def _register_api_key(self) -> None:
        if not self.api_key_file.is_file():
            raise KindEvidenceError("lifecycle helper did not create the private API key file")
        api_key = self.api_key_file.read_text(encoding="utf-8").strip()
        if not api_key:
            raise KindEvidenceError("private API key file is empty")
        self.recorder.register_sensitive_values((api_key,))

    def _helper_environment(self, values: Mapping[str, str]) -> dict[str, str]:
        path = os.environ.get("PATH", "")
        return {**values, "PATH": f"{self.bin_root}{os.pathsep}{path}"}

    def _helm_values(self, *, log_level: str = "INFO") -> tuple[str, ...]:
        digest = self.app_image.rsplit("@", 1)[1]
        repository = f"{APP_REPOSITORY}:{self.app_tag}"
        values = (
            ("--set", "global.testMode", "true"),
            ("--set-string", "namespaces.workload", self.identity.workload_namespace),
            ("--set-string", "image.repository", repository),
            ("--set-string", "image.tag", self.app_tag),
            ("--set-string", "image.digest", digest),
            ("--set-string", "image.pullPolicy", "Never"),
            ("--set-string", "existingSecret.name", self.identity.external_secret_name),
            ("--set-string", "config.appEnvironment", "test"),
            ("--set-string", "config.clusterId", self.identity.cluster_name),
            ("--set-string", "config.servingClusterId", self.identity.cluster_name),
            ("--set-string", "config.logLevel", log_level),
            ("--set", "config.bootstrapEnabled", "true"),
            ("--set", "config.servingEnabled", "true"),
            ("--set", "config.servingFakeEnabled", "true"),
            ("--set-string", "config.servingImage", self.app_image),
            ("--set-string", "service.type", "NodePort"),
            ("--set", "service.nodePort", str(NODE_PORT)),
        )
        result: list[str] = []
        for flag, name, value in values:
            result.extend((flag, f"{name}={value}"))
        return tuple(result)

    def _helm_command(self, *, log_level: str) -> list[str]:
        return [
            self.config.tools.helm,
            "upgrade",
            "--install",
            self.identity.release_name,
            str(self.config.chart_root),
            "--namespace",
            self.identity.system_namespace,
            "--kubeconfig",
            str(self.kubeconfig),
            "--wait",
            "--timeout",
            "8m",
            "--history-max",
            "2",
            *self._helm_values(log_level=log_level),
        ]

    def _environment_payload(self) -> dict[str, object]:
        return build_environment_payload(
            git_state=self.git_state,
            chart_state=self.chart_state,
            image_references={
                "application": self.app_image,
                "postgres": self.config.postgres_image,
                "redis": self.config.redis_image,
                "fake-device-plugin": FAKE_PLUGIN_IMAGE,
                "fake-allocation": FAKE_ALLOCATION_IMAGE,
            },
            tool_versions=self.tool_versions,
            kubernetes_server_version=self.server_version,
        )


def verify_bundle(bundle: Path) -> None:
    validate_evidence_bundle(bundle)
    forbidden = (
        re.compile(rb"--from-literal(?:=|\s)"),
        re.compile(rb"(?i)(password|token|api[_-]?key|private[_-]?key)=[^\[\s][^\s]*"),
        re.compile(rb"-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----"),
    )
    for path in bundle.rglob("*"):
        if not path.is_file() or path.name == "checksums.txt":
            continue
        payload = path.read_bytes()
        if any(pattern.search(payload) for pattern in forbidden):
            raise KindEvidenceError(
                f"credential-like material found in {path.relative_to(bundle).as_posix()}"
            )


async def delete_owned_namespace(
    *,
    kubeconfig: Path,
    name: str,
    uid: str,
    run_id: str,
    role: str,
) -> None:
    from kubernetes_asyncio import client, config
    from kubernetes_asyncio.client.exceptions import ApiException

    await config.load_kube_config(config_file=str(kubeconfig))
    api_client = client.ApiClient()
    api = client.CoreV1Api(api_client=api_client)
    try:
        namespace = await api.read_namespace(name=name)
        metadata = namespace.metadata
        payload = {
            "metadata": {
                "name": getattr(metadata, "name", None),
                "uid": getattr(metadata, "uid", None),
                "labels": getattr(metadata, "labels", None),
            }
        }
        owned = validate_owned_namespace_for_cleanup(
            payload,
            expected_name=name,
            expected_run_id=run_id,
            expected_role=role,
        )
        if owned.uid != uid:
            raise KindEvidenceError("namespace UID changed before deletion")
        resource_version = getattr(metadata, "resource_version", None)
        if not isinstance(resource_version, str) or not resource_version:
            raise KindEvidenceError("namespace resourceVersion is missing")
        body = client.V1DeleteOptions(
            propagation_policy="Foreground",
            preconditions=client.V1Preconditions(
                uid=owned.uid,
                resource_version=resource_version,
            ),
        )
        await api.delete_namespace(name=owned.name, body=body)
    except ApiException as error:
        raise KindEvidenceError(
            f"namespace UID-precondition delete failed with Kubernetes status {error.status}"
        ) from error
    finally:
        await api_client.close()


def _default_tool(name: str) -> str:
    return os.environ.get(f"{name.upper()}_BIN", os.environ.get(name.upper(), name))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run isolated M7 Kubernetes adaptation evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--evidence-root", type=Path, default=Path("build/kind-evidence"))
    run.add_argument("--helm", default=_default_tool("helm"))
    run.add_argument("--kind", default=_default_tool("kind"))
    run.add_argument("--kubectl", default=_default_tool("kubectl"))
    run.add_argument("--docker", default=_default_tool("docker"))
    run.add_argument("--uv", default=_default_tool("uv"))
    run.add_argument("--postgres-image", default=DEFAULT_POSTGRES_IMAGE)
    run.add_argument("--redis-image", default=DEFAULT_REDIS_IMAGE)
    verify = subparsers.add_parser("verify-bundle")
    verify.add_argument("--bundle", type=Path, required=True)
    delete = subparsers.add_parser("delete-owned-namespace", help=argparse.SUPPRESS)
    delete.add_argument("--kubeconfig", type=Path, required=True)
    delete.add_argument("--name", required=True)
    delete.add_argument("--uid", required=True)
    delete.add_argument("--run-id", required=True)
    delete.add_argument("--role", choices=("system", "workload"), required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    repository_root = Path(__file__).resolve().parents[1]
    try:
        if args.command == "verify-bundle":
            verify_bundle(args.bundle.resolve(strict=True))
            print(f"EVIDENCE_SECRET_SCAN_PASS bundle={args.bundle.resolve()}")
            return
        if args.command == "delete-owned-namespace":
            asyncio.run(
                delete_owned_namespace(
                    kubeconfig=args.kubeconfig.resolve(strict=True),
                    name=args.name,
                    uid=args.uid,
                    run_id=args.run_id,
                    role=args.role,
                )
            )
            return
        config = HarnessConfig(
            repository_root=repository_root,
            evidence_root=(repository_root / args.evidence_root).resolve(),
            chart_root=repository_root / "deploy" / "helm" / "mini-ai-cloud",
            kind_config_template=repository_root / "deploy" / "kind-m7" / "kind-config.yaml",
            tools=ToolPaths(
                helm=args.helm,
                kind=args.kind,
                kubectl=args.kubectl,
                docker=args.docker,
                uv=args.uv,
            ),
            postgres_image=args.postgres_image,
            redis_image=args.redis_image,
        )
        harness = KindAdaptationHarness(config)
        returncode, bundle, error = harness.execute()
        if bundle is not None:
            print(f"EVIDENCE_BUNDLE={bundle}")
        if returncode == 0:
            print(f"KIND_K8S_PASS run_id={harness.identity.run_id}")
            print("REAL_HW_NOT_RUN: Kind/Fake evidence is not physical accelerator evidence.")
        else:
            print(f"FAILED: {error or 'P4 evidence did not reach KIND_K8S_PASS'}", file=sys.stderr)
        raise SystemExit(returncode)
    except (KindEvidenceError, OSError, ValueError) as error:
        print(f"FAILED: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
