from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.secrets import SecretKeyRing
from scripts.kind_evidence import (
    HARNESS_OWNED_LABEL,
    KIND_NODE_IMAGE,
    KIND_VERSION,
    KUBERNETES_VERSION,
    NAMESPACE_ROLE_LABEL,
    REQUIRED_CLAIMS,
    REQUIRED_EVIDENCE_FILES,
    RUN_ID_LABEL,
    ChartState,
    ClaimLedger,
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
    directory_digest,
    namespace_uid_delete_request,
    summarize_kubernetes_resources,
    validate_evidence_bundle,
    validate_kind_version,
    validate_owned_namespace_for_cleanup,
    validate_pinned_image,
)

PINNED_POSTGRES = "docker.io/library/postgres@sha256:" + "a" * 64
PINNED_REDIS = "docker.io/library/redis@sha256:" + "b" * 64
PINNED_APP = "ghcr.io/example/mini-ai-cloud@sha256:" + "c" * 64


class FakeRunner:
    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        input_text: str | None,
        environment: Mapping[str, str] | None,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(
            {
                "argv": argv,
                "cwd": cwd,
                "input_text": input_text,
                "environment": environment,
                "timeout_seconds": timeout_seconds,
            }
        )
        return subprocess.CompletedProcess(
            args=argv,
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


def _identity() -> RunIdentity:
    return RunIdentity.create(
        now=datetime(2026, 8, 30, 1, 2, 3, tzinfo=UTC),
        random_hex="1234abcdffffeeee",
    )


def _environment(*, server_version: str | None = None, dirty: bool = True) -> dict[str, object]:
    return build_environment_payload(
        git_state=GitState(sha="d" * 40, dirty=dirty),
        chart_state=ChartState(
            version="0.6.0",
            app_version="0.6.0",
            digest="sha256:" + "e" * 64,
        ),
        image_references={
            "application": PINNED_APP,
            "postgres": PINNED_POSTGRES,
            "redis": PINNED_REDIS,
        },
        tool_versions={
            "kind": KIND_VERSION,
            "kubectl": "v1.32.2",
            "helm": "v3.21.4",
        },
        kubernetes_server_version=server_version,
        recorded_at="2026-08-30T01:02:03Z",
    )


def _limitations() -> tuple[str, ...]:
    return (
        "Single-node Kind validates Kubernetes control contracts, not production HA.",
        "Fake accelerators do not prove NVIDIA or Ascend hardware execution.",
        "REAL_HW_NOT_RUN remains the hardware evidence boundary.",
    )


def test_run_identity_is_unique_bounded_and_separates_every_scope() -> None:
    identity = _identity()
    second = RunIdentity.create(
        now=datetime(2026, 8, 30, 1, 2, 3, tzinfo=UTC),
        random_hex="5678abcdffffeeee",
    )

    assert identity.run_id == "m7-20260830010203-1234abcd"
    assert identity.cluster_name == "mac-m7-1234abcd"
    assert identity.system_namespace == "mac-system-1234abcd"
    assert identity.workload_namespace == "mac-workload-1234abcd"
    assert identity.release_name == "mac-1234abcd"
    assert len(set(identity.manifest_payload().values())) == len(identity.manifest_payload())
    assert identity.run_id != second.run_id
    assert identity.cluster_name != second.cluster_name


def test_kind_and_image_pins_fail_closed() -> None:
    assert validate_kind_version("kind v0.27.0 go1.23.6 linux/amd64") == KIND_VERSION
    assert validate_pinned_image(PINNED_APP, description="application image") == PINNED_APP
    assert KIND_NODE_IMAGE.endswith(
        "@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f"
    )

    with pytest.raises(KindEvidenceError, match=r"exactly v0\.27\.0"):
        validate_kind_version("kind v0.26.0 go1.23 linux/amd64")
    with pytest.raises(KindEvidenceError, match="exact sha256"):
        validate_pinned_image("example/app:latest", description="application image")


def test_generated_credentials_use_application_secret_key_ring_contract() -> None:
    credentials = HarnessCredentials.generate()

    key_ring = SecretKeyRing.from_encoded(credentials.secret_master_key)

    assert key_ring.active_key_id == "kind"
    assert len(key_ring.active_key()) == 32


def test_command_recorder_redacts_credentials_kubeconfig_and_stdin(
    tmp_path: Path,
) -> None:
    identity = _identity()
    bundle = EvidenceBundle(tmp_path / "evidence", identity)
    secret = "sensitive-bootstrap-token-value"
    kubeconfig = "/tmp/private-state/kubeconfig"
    runner = FakeRunner(
        stdout=f"created token={secret}\n",
        stderr=f"password={secret}\n",
    )
    recorder = CommandRecorder(bundle.root, tmp_path, runner=runner)
    recorder.register_sensitive_values((secret,))

    outcome = recorder.record(
        "apply external secret",
        (
            "kubectl",
            "--kubeconfig",
            kubeconfig,
            "apply",
            "-f",
            "-",
            f"--from-literal=token={secret}",
        ),
        claim_id="helm-install",
        input_text=f'{{"stringData":{{"token":"{secret}"}}}}',
        environment={"BOOTSTRAP_TOKEN": secret},
    )
    recorder.require_success(outcome)

    commands = (bundle.root / "commands.jsonl").read_text(encoding="utf-8")
    stdout_log = next((bundle.root / "logs").glob("*.stdout.log")).read_text(encoding="utf-8")
    stderr_log = next((bundle.root / "logs").glob("*.stderr.log")).read_text(encoding="utf-8")
    assert secret not in commands + stdout_log + stderr_log
    assert kubeconfig not in commands
    assert "[PRIVATE_KUBECONFIG]" in commands
    assert "[REDACTED]" in commands
    assert "stringData" not in commands
    command_record = json.loads(commands)
    assert command_record["claim_id"] == "helm-install"
    assert command_record["environment_override_keys"] == ["BOOTSTRAP_TOKEN"]
    assert runner.calls[0]["input_text"] is not None


def test_command_recorder_redacts_equals_form_kubeconfig_argument(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    kubeconfig = "/tmp/private-state/equals-kubeconfig"
    recorder = CommandRecorder(bundle.root, tmp_path, runner=FakeRunner())

    recorder.record(
        "inspect cluster",
        ("kubectl", f"--kubeconfig={kubeconfig}", "version"),
        claim_id="helm-install",
    )

    commands = (bundle.root / "commands.jsonl").read_text(encoding="utf-8")
    assert kubeconfig not in commands
    assert "--kubeconfig=[PRIVATE_KUBECONFIG]" in commands


def test_failed_command_error_is_safe_and_cannot_back_a_pass_claim(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    recorder = CommandRecorder(
        bundle.root,
        tmp_path,
        runner=FakeRunner(returncode=1, stderr="token=must-not-escape"),
    )
    outcome = recorder.record("failed command", ("false",), claim_id="helm-install")

    with pytest.raises(KindEvidenceError, match=r"cmd-0001.*exit code 1") as captured:
        recorder.require_success(outcome)
    assert "must-not-escape" not in str(captured.value)
    with pytest.raises(KindEvidenceError, match="PASS claims require"):
        ClaimLedger().mark_pass("helm-render", (outcome,), detail="must not pass")


def test_pass_claim_rejects_outcome_bound_to_another_claim(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    recorder = CommandRecorder(bundle.root, tmp_path, runner=FakeRunner())
    with pytest.raises(ValueError, match="unknown Kind evidence claim"):
        recorder.record("unknown proof", ("true",), claim_id="not-a-p4-claim")
    install_outcome = recorder.record(
        "verify install",
        ("true",),
        claim_id="helm-install",
    )

    with pytest.raises(KindEvidenceError, match="same claim id"):
        ClaimLedger().mark_pass(
            "helm-render",
            (install_outcome,),
            detail="must not reuse another claim's proof",
        )

    with pytest.raises(KindEvidenceError, match="same claim id"):
        ClaimLedger().mark_fail(
            "worker-readiness",
            outcomes=(install_outcome,),
            detail="must not reuse another claim's failure proof",
        )


def test_not_run_bundle_is_complete_checksummed_and_never_claims_kind_success(
    tmp_path: Path,
) -> None:
    bundle = EvidenceBundle(
        tmp_path / "evidence",
        _identity(),
        started_at="2026-08-30T01:02:03Z",
    )
    path = bundle.finalize(
        ledger=ClaimLedger(),
        environment=_environment(),
        kubernetes_summary={"schema_version": "1.0.0", "status": "NOT_RUN", "resources": []},
        cleanup={"schema_version": "1.0.0", "status": "NOT_RUN"},
        limitations=_limitations(),
        ended_at="2026-08-30T01:03:03Z",
    )

    assert {item.name for item in path.iterdir()} >= set(REQUIRED_EVIDENCE_FILES)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    claims = json.loads((path / "claims.json").read_text(encoding="utf-8"))
    combined = "".join(
        item.read_text(encoding="utf-8") for item in path.rglob("*") if item.is_file()
    )
    assert manifest["status"] == "NOT_RUN"
    assert claims["status"] == "NOT_RUN"
    assert all(item["status"] == "NOT_RUN" for item in claims["claims"])
    assert "KIND_K8S_PASS" not in combined
    assert stat.S_IMODE(path.stat().st_mode) == 0o700
    assert all(
        stat.S_IMODE((path / name).stat().st_mode) == 0o600 for name in REQUIRED_EVIDENCE_FILES
    )
    validate_evidence_bundle(path)


def test_kind_success_requires_every_command_bound_claim_and_cleanup(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    recorder = CommandRecorder(bundle.root, tmp_path, runner=FakeRunner(stdout="ok\n"))
    ledger = ClaimLedger()
    for claim_id in REQUIRED_CLAIMS:
        outcome = recorder.record(
            f"verify {claim_id}",
            ("true",),
            claim_id=claim_id,
        )
        ledger.mark_pass(claim_id, (outcome,), detail=f"Verified {claim_id}.")

    path = bundle.finalize(
        ledger=ledger,
        environment=_environment(server_version=KUBERNETES_VERSION, dirty=False),
        kubernetes_summary={"schema_version": "1.0.0", "status": "PASS", "resources": []},
        cleanup={
            "schema_version": "1.0.0",
            "status": "PASS",
            "release_owned_remaining": 0,
            "external_secret_preserved_after_uninstall": True,
            "external_namespaces_preserved_after_uninstall": True,
            "cluster_deleted": True,
            "default_kubeconfig_unchanged": True,
            "temporary_state_deleted": True,
        },
        limitations=_limitations(),
    )

    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "KIND_K8S_PASS"
    validate_evidence_bundle(path)


def test_kind_success_rejects_missing_server_or_cleanup_proof(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence-a", _identity())
    recorder = CommandRecorder(bundle.root, tmp_path, runner=FakeRunner())
    ledger = ClaimLedger()
    for claim_id in REQUIRED_CLAIMS:
        outcome = recorder.record(
            f"verify {claim_id}",
            ("true",),
            claim_id=claim_id,
        )
        ledger.mark_pass(claim_id, (outcome,), detail=f"Verified {claim_id}.")

    with pytest.raises(KindEvidenceError, match="pinned Kubernetes server"):
        bundle.finalize(
            ledger=ledger,
            environment=_environment(server_version=None, dirty=False),
            kubernetes_summary={"status": "PASS"},
            cleanup={
                "status": "PASS",
                "release_owned_remaining": 0,
                "external_secret_preserved_after_uninstall": True,
                "external_namespaces_preserved_after_uninstall": True,
                "cluster_deleted": True,
                "default_kubeconfig_unchanged": True,
                "temporary_state_deleted": True,
            },
            limitations=_limitations(),
        )

    second = EvidenceBundle(tmp_path / "evidence-b", _identity())
    second_recorder = CommandRecorder(second.root, tmp_path, runner=FakeRunner())
    second_ledger = ClaimLedger()
    for claim_id in REQUIRED_CLAIMS:
        second_outcome = second_recorder.record(
            f"verify {claim_id}",
            ("true",),
            claim_id=claim_id,
        )
        second_ledger.mark_pass(claim_id, (second_outcome,), detail=f"Verified {claim_id}.")
    with pytest.raises(KindEvidenceError, match="complete scoped cleanup"):
        second.finalize(
            ledger=second_ledger,
            environment=_environment(server_version=KUBERNETES_VERSION, dirty=False),
            kubernetes_summary={"status": "PASS"},
            cleanup={
                "status": "PASS",
                "release_owned_remaining": 1,
                "external_secret_preserved_after_uninstall": True,
                "external_namespaces_preserved_after_uninstall": True,
                "cluster_deleted": True,
                "default_kubeconfig_unchanged": True,
                "temporary_state_deleted": True,
            },
            limitations=_limitations(),
        )


def test_bundle_refuses_registered_secret_leak(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    secret = "never-write-this-secret"
    (bundle.root / "unsafe.log").write_text(secret, encoding="utf-8")

    with pytest.raises(KindEvidenceError, match="sensitive value detected"):
        bundle.finalize(
            ledger=ClaimLedger(),
            environment=_environment(),
            kubernetes_summary={"status": "NOT_RUN"},
            cleanup={"status": "NOT_RUN"},
            limitations=_limitations(),
            sensitive_values=(secret,),
        )


def test_chart_digest_binds_version_and_every_file(tmp_path: Path) -> None:
    chart = tmp_path / "chart"
    templates = chart / "templates"
    templates.mkdir(parents=True)
    (chart / "Chart.yaml").write_text(
        "apiVersion: v2\nname: mini-ai-cloud\nversion: 0.6.0\nappVersion: '0.6.0'\n",
        encoding="utf-8",
    )
    deployment = templates / "deployment.yaml"
    deployment.write_text("kind: Deployment\n", encoding="utf-8")

    first = capture_chart_state(chart)
    deployment.write_text("kind: Deployment\nmetadata: {}\n", encoding="utf-8")
    second_digest = directory_digest(chart)

    assert first.version == "0.6.0"
    assert first.app_version == "0.6.0"
    assert first.digest.startswith("sha256:")
    assert first.digest != second_digest


def test_namespaces_external_secret_and_data_stores_are_isolated_and_secure() -> None:
    identity = _identity()
    credentials = HarnessCredentials(
        postgres_password="postgres-sensitive-value",
        bootstrap_token="bootstrap-sensitive-value",
        api_key_pepper="pepper-sensitive-value",
        worker_auth_token="worker-sensitive-value",
        secret_master_key="master-sensitive-value",
    )
    namespaces = build_namespace_manifests(identity)
    secret = build_external_secret_manifest(identity, credentials)
    stores = build_external_data_store_manifests(
        identity,
        postgres_image=PINNED_POSTGRES,
        redis_image=PINNED_REDIS,
    )

    namespace_items = namespaces["items"]
    assert isinstance(namespace_items, list)
    assert {item["metadata"]["name"] for item in namespace_items} == {
        identity.system_namespace,
        identity.workload_namespace,
    }
    assert all(
        item["metadata"]["labels"][RUN_ID_LABEL] == identity.run_id for item in namespace_items
    )
    secret_metadata = secret["metadata"]
    secret_data = secret["stringData"]
    assert isinstance(secret_metadata, dict)
    assert isinstance(secret_data, dict)
    secret_labels = secret_metadata["labels"]
    assert isinstance(secret_labels, dict)
    assert secret_metadata["namespace"] == identity.system_namespace
    assert secret_labels[HARNESS_OWNED_LABEL] == "true"
    assert secret_data["database-url"] == credentials.database_url(identity)

    store_items = stores["items"]
    assert isinstance(store_items, list)
    deployments = [item for item in store_items if item["kind"] == "Deployment"]
    assert len(deployments) == 2
    for deployment in deployments:
        pod_spec = deployment["spec"]["template"]["spec"]
        container = pod_spec["containers"][0]
        assert pod_spec["automountServiceAccountToken"] is False
        assert pod_spec["securityContext"]["runAsNonRoot"] is True
        assert container["securityContext"]["privileged"] is False
        assert container["securityContext"]["allowPrivilegeEscalation"] is False
        assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert pod_spec["volumes"]
        assert all(set(volume) == {"name", "emptyDir"} for volume in pod_spec["volumes"])
        assert all(volume["emptyDir"]["sizeLimit"] for volume in pod_spec["volumes"])
        assert "hostPath" not in json.dumps(deployment)
        assert "@sha256:" in container["image"]

    by_name = {item["metadata"]["name"]: item for item in deployments}
    postgres = by_name[identity.postgres_name]["spec"]["template"]["spec"]
    postgres_container = postgres["containers"][0]
    assert postgres["securityContext"]["runAsUser"] == 70
    assert postgres["securityContext"]["fsGroup"] == 70
    assert {mount["mountPath"] for mount in postgres_container["volumeMounts"]} == {
        "/var/lib/postgresql/data",
        "/var/run/postgresql",
        "/tmp",
    }
    assert {item["name"]: item.get("value") for item in postgres_container["env"]}[
        "PGDATA"
    ] == "/var/lib/postgresql/data/pgdata"

    redis = by_name[identity.redis_name]["spec"]["template"]["spec"]
    redis_container = redis["containers"][0]
    assert redis["securityContext"]["runAsUser"] == 999
    assert redis["securityContext"]["fsGroup"] == 1000
    assert redis_container["args"] == ["redis-server", "--appendonly", "no", "--save", ""]
    assert redis_container["volumeMounts"] == [{"name": "redis-data", "mountPath": "/data"}]


@pytest.mark.parametrize(
    "mutation",
    ["name", "run_id", "owned", "role", "uid"],
)
def test_namespace_cleanup_requires_exact_name_run_id_role_and_uid(mutation: str) -> None:
    identity = _identity()
    payload: dict[str, Any] = {
        "metadata": {
            "name": identity.workload_namespace,
            "uid": "11111111-2222-3333-4444-555555555555",
            "labels": {
                RUN_ID_LABEL: identity.run_id,
                HARNESS_OWNED_LABEL: "true",
                NAMESPACE_ROLE_LABEL: "workload",
            },
        }
    }
    metadata = payload["metadata"]
    if mutation == "name":
        metadata["name"] = "another-namespace"
    elif mutation == "run_id":
        metadata["labels"][RUN_ID_LABEL] = "m7-20260830010203-deadbeef"
    elif mutation == "owned":
        metadata["labels"][HARNESS_OWNED_LABEL] = "false"
    elif mutation == "role":
        metadata["labels"][NAMESPACE_ROLE_LABEL] = "system"
    else:
        metadata["uid"] = ""

    with pytest.raises(KindEvidenceError, match="cleanup target"):
        validate_owned_namespace_for_cleanup(
            payload,
            expected_name=identity.workload_namespace,
            expected_run_id=identity.run_id,
            expected_role="workload",
        )


def test_namespace_cleanup_uses_uid_precondition() -> None:
    identity = _identity()
    target = validate_owned_namespace_for_cleanup(
        {
            "metadata": {
                "name": identity.system_namespace,
                "uid": "11111111-2222-3333-4444-555555555555",
                "labels": {
                    RUN_ID_LABEL: identity.run_id,
                    HARNESS_OWNED_LABEL: "true",
                    NAMESPACE_ROLE_LABEL: "system",
                },
            }
        },
        expected_name=identity.system_namespace,
        expected_run_id=identity.run_id,
        expected_role="system",
    )

    path, body = namespace_uid_delete_request(target)
    delete_options = json.loads(body)
    assert path == f"/api/v1/namespaces/{identity.system_namespace}"
    assert delete_options["preconditions"]["uid"] == target.uid
    assert delete_options["propagationPolicy"] == "Foreground"


def test_kubernetes_summary_omits_secret_data_specs_and_sensitive_labels() -> None:
    secret_value = "must-not-appear"
    summary = summarize_kubernetes_resources(
        (
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "external-secret",
                    "namespace": "system",
                    "uid": "secret-uid",
                    "labels": {
                        RUN_ID_LABEL: _identity().run_id,
                        "sensitive.example/token": secret_value,
                    },
                },
                "data": {"token": secret_value},
                "stringData": {"password": secret_value},
            },
        )
    )

    rendered = json.dumps(summary, sort_keys=True)
    assert secret_value not in rendered
    assert "stringData" not in rendered
    assert "data" not in rendered
    resources = summary["resources"]
    assert isinstance(resources, list)
    first = resources[0]
    assert isinstance(first, dict)
    assert first["labels"] == {RUN_ID_LABEL: _identity().run_id}


def test_checksum_validation_detects_tampering(tmp_path: Path) -> None:
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    path = bundle.finalize(
        ledger=ClaimLedger(),
        environment=_environment(),
        kubernetes_summary={"status": "NOT_RUN"},
        cleanup={"status": "NOT_RUN"},
        limitations=_limitations(),
    )
    (path / "limitations.md").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(KindEvidenceError, match="checksum mismatch"):
        validate_evidence_bundle(path)


def test_evidence_permissions_are_private_on_posix(tmp_path: Path) -> None:
    if os.name != "posix":
        pytest.skip("POSIX permission evidence is validated in Ubuntu WSL")
    bundle = EvidenceBundle(tmp_path / "evidence", _identity())
    assert stat.S_IMODE(bundle.root.stat().st_mode) == 0o700
    assert stat.S_IMODE((bundle.root / "commands.jsonl").stat().st_mode) == 0o600
