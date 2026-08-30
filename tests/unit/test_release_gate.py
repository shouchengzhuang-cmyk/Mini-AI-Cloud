from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from scripts.release_gate import (
    CHART_ROOT,
    KIND_NODE_IMAGE,
    KIND_VERSION,
    KUBERNETES_VERSION,
    P4_REQUIRED_CLAIMS,
    P4_REQUIRED_FILES,
    PREVIOUS_RELEASE_TAG,
    RELEASE_VERSION,
    ReleaseGateError,
    _release_notes,
    _validate_release_readiness,
    chart_directory_digest,
    cyclonedx_sbom,
    main,
    prepare_release_bundle,
    scan_secret_text,
    validate_action_pins,
    validate_container_baseline,
    validate_locked_dependencies,
    validate_p4_evidence,
    validate_versions,
    write_or_check_contracts,
)

ROOT = Path(__file__).parents[2]
GIT_SHA = "a" * 40


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _refresh_p4_checksums(bundle: Path) -> None:
    checksum_path = bundle / "checksums.txt"
    checksum_path.unlink(missing_ok=True)
    files = sorted(path for path in bundle.rglob("*") if path.is_file())
    checksum_path.write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
            f"{path.relative_to(bundle).as_posix()}\n"
            for path in files
        ),
        encoding="utf-8",
    )


def _p4_bundle(tmp_path: Path, *, git_sha: str = GIT_SHA) -> Path:
    bundle = tmp_path / "p4-evidence"
    logs = bundle / "logs"
    logs.mkdir(parents=True)
    command_records: list[dict[str, object]] = []
    claim_records: list[dict[str, object]] = []
    for index, claim_id in enumerate(P4_REQUIRED_CLAIMS, 1):
        command_id = f"cmd-{index:04d}"
        stdout_log = f"logs/{command_id}.stdout.log"
        stderr_log = f"logs/{command_id}.stderr.log"
        (bundle / stdout_log).write_text("verified\n", encoding="utf-8")
        (bundle / stderr_log).write_text("", encoding="utf-8")
        command_records.append(
            {
                "schema_version": "1.0.0",
                "command_id": command_id,
                "claim_id": claim_id,
                "returncode": 0,
                "stdout_log": stdout_log,
                "stderr_log": stderr_log,
            }
        )
        claim_records.append(
            {
                "id": claim_id,
                "status": "PASS",
                "command_ids": [command_id],
                "detail": f"Verified {claim_id}.",
            }
        )
    (bundle / "commands.jsonl").write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in command_records),
        encoding="utf-8",
    )
    identities = {
        "run_id": "m7-20260830010203-1234abcd",
        "cluster_name": "mac-m7-1234abcd",
        "system_namespace": "mac-system-1234abcd",
        "workload_namespace": "mac-workload-1234abcd",
        "release_name": "mac-1234abcd",
        "external_secret_name": "mac-external-1234abcd",
        "postgres_name": "postgres-1234abcd",
        "redis_name": "redis-1234abcd",
    }
    _write_json(
        bundle / "manifest.json",
        {
            "schema_version": "1.0.0",
            "run_id": identities["run_id"],
            "status": "KIND_K8S_PASS",
            "real_hardware_status": "REAL_HW_NOT_RUN",
            "started_at": "2026-08-30T01:02:03Z",
            "ended_at": "2026-08-30T01:03:03Z",
            "identities": identities,
            "kind_version": KIND_VERSION,
            "kubernetes_version": KUBERNETES_VERSION,
            "kind_node_image": KIND_NODE_IMAGE,
            "evidence_files": list(P4_REQUIRED_FILES),
        },
    )
    _write_json(
        bundle / "environment.json",
        {
            "schema_version": "1.0.0",
            "recorded_at": "2026-08-30T01:02:03Z",
            "git_sha": git_sha,
            "git_dirty": False,
            "chart_version": RELEASE_VERSION,
            "chart_app_version": RELEASE_VERSION,
            "chart_digest": chart_directory_digest(ROOT / CHART_ROOT),
            "image_references": {
                "application": "ghcr.io/example/mini-ai-cloud@sha256:" + "b" * 64,
                "postgres": "docker.io/library/postgres@sha256:" + "c" * 64,
                "redis": "docker.io/library/redis@sha256:" + "d" * 64,
            },
            "kind_version": KIND_VERSION,
            "kind_node_image": KIND_NODE_IMAGE,
            "kubernetes_server_version": KUBERNETES_VERSION,
            "tool_versions": {
                "kind": KIND_VERSION,
                "kubectl": KUBERNETES_VERSION,
                "helm": "v3.21.4",
            },
        },
    )
    _write_json(
        bundle / "claims.json",
        {
            "schema_version": "1.0.0",
            "status": "KIND_K8S_PASS",
            "real_hardware_status": "REAL_HW_NOT_RUN",
            "claims": claim_records,
        },
    )
    _write_json(
        bundle / "kubernetes-summary.json",
        {
            "schema_version": "1.0.0",
            "status": "PASS",
            "resource_count": 1,
            "resources": [
                {
                    "api_version": "apps/v1",
                    "kind": "Deployment",
                    "name": "mac-1234abcd-mini-ai-cloud-control-plane",
                    "namespace": identities["system_namespace"],
                    "uid": "11111111-1111-1111-1111-111111111111",
                    "labels": {},
                    "status": {"desired": 1, "ready": 1},
                }
            ],
        },
    )
    _write_json(
        bundle / "cleanup.json",
        {
            "schema_version": "1.0.0",
            "status": "PASS",
            "release_owned_remaining": 0,
            "external_secret_preserved_after_uninstall": True,
            "external_namespaces_preserved_after_uninstall": True,
            "cluster_deleted": True,
            "default_kubeconfig_unchanged": True,
            "temporary_state_deleted": True,
        },
    )
    (bundle / "limitations.md").write_text(
        "# Limitations\n\n- REAL_HW_NOT_RUN remains the hardware boundary.\n",
        encoding="utf-8",
    )
    _refresh_p4_checksums(bundle)
    return bundle


def _mutate_json(
    bundle: Path,
    filename: str,
    mutation: Callable[[dict[str, object]], None],
) -> None:
    path = bundle / filename
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    mutation(payload)
    _write_json(path, payload)
    _refresh_p4_checksums(bundle)


def test_release_version_has_pinned_predecessor() -> None:
    assert RELEASE_VERSION == "0.6.0"
    assert PREVIOUS_RELEASE_TAG == "v0.5.0"


def test_release_notes_use_pinned_previous_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[tuple[str, ...]] = []

    def fake_run(command: tuple[str, ...], _root: Path) -> object:
        recorded.append(command)
        return type("Result", (), {"stdout": "- `abc1234` prior change\n"})()

    monkeypatch.setattr("scripts.release_gate.PREVIOUS_RELEASE_TAG", "v0.3.0")
    monkeypatch.setattr("scripts.release_gate._run", fake_run)

    notes = _release_notes(
        ROOT,
        "a" * 40,
        {"action_dependencies": 1, "locked_packages": 2, "secret_scanned_files": 3},
    )

    assert "- Previous tag: `v0.3.0`" in notes
    assert recorded == [("git", "log", "--format=- `%h` %s", "--no-merges", "v0.3.0..HEAD")]


def test_release_notes_keep_dual_vendor_hardware_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scripts.release_gate._run",
        lambda _command, _root: type("Result", (), {"stdout": ""})(),
    )

    notes = _release_notes(
        ROOT,
        "a" * 40,
        {"action_dependencies": 1, "locked_packages": 2, "secret_scanned_files": 3},
    )

    assert "NVIDIA and Huawei Ascend runtime profiles" in notes
    assert "Real non-Kind Kubernetes acceptance (E1): **NOT_RUN**" in notes
    assert "Real NVIDIA GPU/vLLM acceptance: **REAL_HW_NOT_RUN**" in notes
    assert "Real Huawei Ascend/vLLM-Ascend acceptance: **REAL_HW_NOT_RUN**" in notes
    assert "complete Kubernetes-native platform remain outside" in notes


def test_release_identity_actions_dependencies_container_and_contracts_are_locked() -> None:
    validate_versions(ROOT)
    assert validate_action_pins(ROOT) >= 1
    dependencies = validate_locked_dependencies(ROOT)
    assert any(item["name"] == "mini-ai-cloud" for item in dependencies)
    validate_container_baseline(ROOT)
    paths = write_or_check_contracts(ROOT, write=False)
    assert all(path.is_file() for path in paths)


def test_secret_scanner_detects_high_confidence_credentials_without_echoing_values() -> None:
    private_key = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
    mini_key = "".join(("m", "k", "c", "_", "0123", "4567", "89ab", "cdef", "_", "A" * 43))

    assert scan_secret_text(private_key) == ("private-key",)
    assert scan_secret_text(mini_key) == ("mini-cloud-api-key",)
    assert scan_secret_text("MINI_CLOUD_API_KEY is an environment variable name") == ()


def test_cyclonedx_sbom_is_versioned_and_contains_locked_components() -> None:
    payload = cyclonedx_sbom(
        [
            {"name": "mini-ai-cloud", "version": RELEASE_VERSION},
            {"name": "fastapi", "version": "0.1.0"},
        ],
        "a" * 40,
    )

    assert payload["bomFormat"] == "CycloneDX"
    assert payload["specVersion"] == "1.6"
    metadata = payload["metadata"]
    assert isinstance(metadata, dict)
    component = metadata["component"]
    assert isinstance(component, dict)
    assert component["version"] == RELEASE_VERSION
    assert payload["components"] == [
        {
            "type": "library",
            "name": "fastapi",
            "version": "0.1.0",
            "bom-ref": "pkg:pypi/fastapi@0.1.0",
            "purl": "pkg:pypi/fastapi@0.1.0",
        }
    ]
    json.dumps(payload, allow_nan=False)


def test_action_pin_validator_rejects_mutable_tags(tmp_path: Path) -> None:
    workflow = tmp_path / ".github/workflows/ci.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text("steps:\n  - uses: actions/checkout@v4\n", encoding="utf-8")

    with pytest.raises(ReleaseGateError, match="full commit SHAs"):
        validate_action_pins(tmp_path)


def test_release_security_workflow_prepares_sbom_output_directory() -> None:
    workflow = (ROOT / ".github/workflows/release-security.yml").read_text(encoding="utf-8")

    prepare = workflow.index("run: mkdir -p build/release-security")
    generate = workflow.index("name: Generate image SPDX SBOM")
    assert prepare < generate


def test_issue_comment_publisher_reuses_exact_sha_gitleaks_gate() -> None:
    publisher = (ROOT / ".github/workflows/publish-release.yml").read_text(encoding="utf-8")
    security_gate = (ROOT / ".github/workflows/release-security.yml").read_text(encoding="utf-8")

    assert 'require_success "release-security.yml"' in publisher
    assert "gitleaks/gitleaks-action@" in security_gate
    assert "gitleaks/gitleaks-action@" not in publisher


def test_p4_evidence_accepts_exact_sha_chart_claims_cleanup_and_checksums(
    tmp_path: Path,
) -> None:
    bundle = _p4_bundle(tmp_path)

    result = validate_p4_evidence(ROOT, bundle, expected_git_sha=GIT_SHA)

    assert result == {
        "status": "KIND_K8S_PASS",
        "run_id": "m7-20260830010203-1234abcd",
        "git_sha": GIT_SHA,
        "chart_digest": chart_directory_digest(ROOT / CHART_ROOT),
        "checksums_sha256": "sha256:"
        + hashlib.sha256((bundle / "checksums.txt").read_bytes()).hexdigest(),
    }


@pytest.mark.parametrize(
    ("filename", "field", "value", "error"),
    [
        ("manifest.json", "status", "NOT_RUN", "NOT_RUN is not PASS"),
        ("environment.json", "git_sha", "f" * 40, "Git SHA"),
        ("environment.json", "git_dirty", True, "clean worktree"),
        ("environment.json", "chart_digest", "sha256:" + "e" * 64, "Chart digest"),
        ("cleanup.json", "temporary_state_deleted", False, "scoped cleanup"),
    ],
)
def test_p4_evidence_rejects_semantic_drift_even_with_refreshed_checksums(
    tmp_path: Path,
    filename: str,
    field: str,
    value: object,
    error: str,
) -> None:
    bundle = _p4_bundle(tmp_path)
    _mutate_json(bundle, filename, lambda payload: payload.__setitem__(field, value))

    with pytest.raises(ReleaseGateError, match=error):
        validate_p4_evidence(ROOT, bundle, expected_git_sha=GIT_SHA)


def test_p4_evidence_rejects_unpinned_image_and_cross_claim_command(tmp_path: Path) -> None:
    image_bundle = _p4_bundle(tmp_path / "image")

    def unpin_image(payload: dict[str, object]) -> None:
        images = payload["image_references"]
        assert isinstance(images, dict)
        images["application"] = "ghcr.io/example/mini-ai-cloud:latest"

    _mutate_json(image_bundle, "environment.json", unpin_image)
    with pytest.raises(ReleaseGateError, match="exact sha256"):
        validate_p4_evidence(ROOT, image_bundle, expected_git_sha=GIT_SHA)

    claim_bundle = _p4_bundle(tmp_path / "claim")

    def cross_claim(payload: dict[str, object]) -> None:
        claims = payload["claims"]
        assert isinstance(claims, list)
        first = claims[0]
        assert isinstance(first, dict)
        first["command_ids"] = ["cmd-0002"]

    _mutate_json(claim_bundle, "claims.json", cross_claim)
    with pytest.raises(ReleaseGateError, match="cross-claim"):
        validate_p4_evidence(ROOT, claim_bundle, expected_git_sha=GIT_SHA)


def test_p4_evidence_rejects_invalid_or_reversed_utc_timestamps(tmp_path: Path) -> None:
    reversed_bundle = _p4_bundle(tmp_path / "reversed")
    _mutate_json(
        reversed_bundle,
        "manifest.json",
        lambda payload: payload.__setitem__("ended_at", "2026-08-30T01:01:03Z"),
    )
    with pytest.raises(ReleaseGateError, match="out of order"):
        validate_p4_evidence(ROOT, reversed_bundle, expected_git_sha=GIT_SHA)

    invalid_bundle = _p4_bundle(tmp_path / "invalid")
    _mutate_json(
        invalid_bundle,
        "environment.json",
        lambda payload: payload.__setitem__("recorded_at", "2026-08-30 01:02:03"),
    )
    with pytest.raises(ReleaseGateError, match="RFC3339 UTC"):
        validate_p4_evidence(ROOT, invalid_bundle, expected_git_sha=GIT_SHA)


def test_p4_evidence_rejects_checksum_drift_and_unlisted_files(tmp_path: Path) -> None:
    drifted = _p4_bundle(tmp_path / "drifted")
    (drifted / "limitations.md").write_text(
        "# Limitations\n\n- REAL_HW_NOT_RUN remains the hardware boundary.\n- tampered\n",
        encoding="utf-8",
    )
    with pytest.raises(ReleaseGateError, match="checksum mismatch"):
        validate_p4_evidence(ROOT, drifted, expected_git_sha=GIT_SHA)

    unlisted = _p4_bundle(tmp_path / "unlisted")
    (unlisted / "extra.txt").write_text("not checksummed\n", encoding="utf-8")
    with pytest.raises(ReleaseGateError, match="cover every evidence file"):
        validate_p4_evidence(ROOT, unlisted, expected_git_sha=GIT_SHA)


def test_prepare_requires_real_p4_bundle_before_writing_output(tmp_path: Path) -> None:
    with pytest.raises(ReleaseGateError, match="requires --p4-evidence"):
        prepare_release_bundle(ROOT, tmp_path / "release", p4_evidence=None)
    assert not (tmp_path / "release").exists()


def test_validate_cli_forwards_explicit_p4_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = tmp_path / "p4"
    bundle.mkdir()
    recorded: dict[str, object] = {}

    def fake_validate(
        root: Path,
        *,
        p4_evidence: Path | None = None,
        expected_git_sha: str | None = None,
    ) -> dict[str, object]:
        recorded.update(
            {
                "root": root,
                "p4_evidence": p4_evidence,
                "expected_git_sha": expected_git_sha,
            }
        )
        return {"p4_evidence": {"status": "KIND_K8S_PASS"}}

    monkeypatch.setattr("scripts.release_gate.validate_release_inputs", fake_validate)
    monkeypatch.setattr(
        sys,
        "argv",
        ["release_gate.py", "validate", "--p4-evidence", str(bundle)],
    )

    main()

    assert recorded["p4_evidence"] == bundle
    assert recorded["expected_git_sha"] is None
    assert '"status": "KIND_K8S_PASS"' in capsys.readouterr().out


def test_publication_workflow_runs_p4_and_passes_exact_bundle_to_release_gate() -> None:
    workflow = (ROOT / ".github/workflows/publish-release.yml").read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

    assert "make test-kind-kubernetes-adaptation" in workflow
    assert "EVIDENCE_BUNDLE=" in workflow
    assert "P4_EVIDENCE_BUNDLE: ${{ steps.p4-evidence.outputs.bundle }}" in workflow
    assert '"- Tag state: **NOT_TAGGED**"' in workflow
    assert '"Git tag: **NOT_TAGGED**."' in workflow
    assert "CREATED BY APPROVED WORKFLOW" in workflow
    assert "Real non-Kind Kubernetes acceptance (E1): **NOT_RUN**" in workflow
    assert "Real Huawei Ascend/vLLM-Ascend acceptance: **REAL_HW_NOT_RUN**" in workflow
    assert "Production deployment: **NOT_DEPLOYED**" in workflow
    assert "P4_EVIDENCE_BUNDLE=<real KIND_K8S_PASS bundle> is required" in makefile
    assert '--p4-evidence "$(P4_EVIDENCE_BUNDLE)"' in makefile


def test_release_readiness_rejects_unrun_real_hardware_claim(tmp_path: Path) -> None:
    contract_source = ROOT / "contracts/release/v0.6-readiness.json"
    contract_target = tmp_path / "contracts/release/v0.6-readiness.json"
    contract_target.parent.mkdir(parents=True)
    contract = json.loads(contract_source.read_text(encoding="utf-8"))
    assert isinstance(contract, dict)
    matrix = contract["support_matrix"]
    assert isinstance(matrix, list)
    runtime = next(
        item
        for item in matrix
        if isinstance(item, dict) and item.get("capability") == "runtime-profile-serving"
    )
    runtime["real_evidence"] = "REAL_NVIDIA_K8S_PASS"
    _write_json(contract_target, contract)
    docs = tmp_path / "docs"
    docs.mkdir()
    for name in ("kubernetes-adaptation-v0.6.md", "v0.6-release-readiness.md"):
        (docs / name).write_text(
            (ROOT / "docs" / name).read_text(encoding="utf-8"), encoding="utf-8"
        )

    with pytest.raises(ReleaseGateError, match="cannot claim unrun real evidence"):
        _validate_release_readiness(tmp_path)
