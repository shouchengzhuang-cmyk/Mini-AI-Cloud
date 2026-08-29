from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.release_gate import (
    PREVIOUS_RELEASE_TAG,
    RELEASE_VERSION,
    ReleaseGateError,
    _release_notes,
    cyclonedx_sbom,
    scan_secret_text,
    validate_action_pins,
    validate_container_baseline,
    validate_locked_dependencies,
    validate_versions,
    write_or_check_contracts,
)

ROOT = Path(__file__).parents[2]


def test_release_version_has_pinned_predecessor() -> None:
    assert RELEASE_VERSION == "0.5.0"
    assert PREVIOUS_RELEASE_TAG == "v0.4.0"


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
