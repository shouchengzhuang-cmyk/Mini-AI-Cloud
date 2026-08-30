from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from scripts.kind_evidence import REQUIRED_CLAIMS, CommandOutcome, RunIdentity
from scripts.kind_kubernetes_adaptation import (
    APP_REPOSITORY,
    DEFAULT_POSTGRES_IMAGE,
    DEFAULT_REDIS_IMAGE,
    FAKE_ALLOCATION_IMAGE,
    FAKE_PLUGIN_IMAGE,
    NODE_PORT,
    HarnessConfig,
    KindAdaptationHarness,
    KindEvidenceError,
    PhaseFailure,
    ToolPaths,
    application_containerd_aliases,
    application_reference,
    build_upgrade_sentinels,
    chart_fullname,
    docker_image_save_argv,
    image_archive_path,
    kind_image_archive_load_argv,
    local_image_cleanup_argv,
    parse_build_digest,
    pinned_image_aliases,
    render_kind_config,
    validate_pod_security,
    verify_bundle,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PINNED_APP = f"{APP_REPOSITORY}:m7-1234abcd@sha256:{'a' * 64}"


def _identity() -> RunIdentity:
    return RunIdentity.create(
        now=datetime(2026, 8, 30, 1, 2, 3, tzinfo=UTC),
        random_hex="1234abcdffffeeee",
    )


def _config(tmp_path: Path) -> HarnessConfig:
    return HarnessConfig(
        repository_root=REPOSITORY_ROOT,
        evidence_root=tmp_path / "evidence",
        chart_root=REPOSITORY_ROOT / "deploy" / "helm" / "mini-ai-cloud",
        kind_config_template=REPOSITORY_ROOT / "deploy" / "kind-m7" / "kind-config.yaml",
        tools=ToolPaths(
            helm="helm",
            kind="kind",
            kubectl="kubectl",
            docker="docker",
            uv="uv",
        ),
    )


def test_fixed_images_and_kind_port_contract_are_immutable() -> None:
    for image in (
        DEFAULT_POSTGRES_IMAGE,
        DEFAULT_REDIS_IMAGE,
        FAKE_PLUGIN_IMAGE,
        FAKE_ALLOCATION_IMAGE,
    ):
        name, separator, digest = image.partition("@sha256:")
        assert name
        assert separator
        assert len(digest) == 64
    assert NODE_PORT == 30080


def test_pinned_external_images_use_single_platform_archives_and_digest_aliases(
    tmp_path: Path,
) -> None:
    aliases = pinned_image_aliases(
        _identity(),
        postgres_image=DEFAULT_POSTGRES_IMAGE,
        redis_image=DEFAULT_REDIS_IMAGE,
    )

    assert [alias.component for alias in aliases] == [
        "postgres",
        "redis",
        "fake-plugin",
        "fake-allocation",
    ]
    assert len({alias.local_tag for alias in aliases}) == 4
    assert len({alias.containerd_tag for alias in aliases}) == 4
    for alias in aliases:
        assert "@sha256:" in alias.digest_reference
        assert alias.local_tag.endswith(":m7-1234abcd")
        assert alias.containerd_tag == f"docker.io/library/{alias.local_tag}"
        archive = image_archive_path(tmp_path, alias.component)
        assert archive == tmp_path / "image-archives" / f"{alias.component}.tar"
        assert docker_image_save_argv("docker", alias, archive) == (
            "docker",
            "image",
            "save",
            "--platform",
            "linux/amd64",
            "--output",
            str(archive),
            alias.local_tag,
        )
        assert kind_image_archive_load_argv("kind", "mac-m7-1234abcd", archive) == (
            "kind",
            "load",
            "image-archive",
            "--name",
            "mac-m7-1234abcd",
            str(archive),
        )

    cleanup = local_image_cleanup_argv(
        "docker",
        ("mini-ai-cloud:m7-1234abcd", *(alias.local_tag for alias in aliases)),
    )
    assert cleanup[:4] == ("docker", "image", "rm", "--force")
    assert cleanup[4:] == (
        "mini-ai-cloud:m7-1234abcd",
        *(alias.local_tag for alias in aliases),
    )
    with pytest.raises(KindEvidenceError, match="outside the run-specific tag"):
        local_image_cleanup_argv("docker", ("postgres:16-alpine",))
    with pytest.raises(KindEvidenceError, match="safely bounded"):
        image_archive_path(tmp_path, "../postgres")


def test_kind_config_renders_only_the_run_host_port() -> None:
    template = (REPOSITORY_ROOT / "deploy" / "kind-m7" / "kind-config.yaml").read_text(
        encoding="utf-8"
    )

    rendered = render_kind_config(template, 18443)

    assert "hostPort: 18443" in rendered
    assert "containerPort: 30080" in rendered
    assert 'listenAddress: "127.0.0.1"' in rendered
    assert "__HOST_PORT__" not in rendered
    with pytest.raises(KindEvidenceError, match="unprivileged"):
        render_kind_config(template, 80)


def test_application_digest_comes_from_build_metadata_and_keeps_policy_tag() -> None:
    digest = "sha256:" + "b" * 64

    assert parse_build_digest({"containerimage.digest": digest}) == digest
    assert application_reference("m7-1234abcd", digest) == (
        f"{APP_REPOSITORY}:m7-1234abcd@{digest}"
    )
    policy_reference, canonical_reference = application_containerd_aliases("m7-1234abcd", digest)
    assert policy_reference == f"{APP_REPOSITORY}:m7-1234abcd@{digest}"
    assert canonical_reference == f"{APP_REPOSITORY}@{digest}"
    assert policy_reference.rsplit("@", 1)[1] == canonical_reference.rsplit("@", 1)[1]
    with pytest.raises(KindEvidenceError, match="manifest digest"):
        parse_build_digest({"containerimage.digest": "sha256:short"})
    with pytest.raises(KindEvidenceError, match="safely bounded"):
        application_containerd_aliases("m7-nothex", digest)
    with pytest.raises(KindEvidenceError, match="safely bounded"):
        application_containerd_aliases("m7-1234abcd", "sha256:short")


def test_upgrade_sentinels_are_uid_trackable_and_hardened() -> None:
    identity = _identity()

    manifest = build_upgrade_sentinels(identity, PINNED_APP)

    items = cast(list[dict[str, Any]], manifest["items"])
    assert [item["kind"] for item in items] == ["ConfigMap", "Job", "Pod"]
    pod_specs = [
        items[1]["spec"]["template"]["spec"],
        items[2]["spec"],
    ]
    for spec in pod_specs:
        assert spec["automountServiceAccountToken"] is False
        assert "hostNetwork" not in spec
        assert all("hostPath" not in volume for volume in spec["volumes"])
        container = spec["containers"][0]
        assert container["image"] == PINNED_APP
        assert container["securityContext"]["privileged"] is False
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_pod_security_rejects_token_mount_hostpath_and_privilege() -> None:
    base: dict[str, Any] = {
        "metadata": {"namespace": "workload"},
        "spec": {
            "automountServiceAccountToken": False,
            "volumes": [{"name": "tmp", "emptyDir": {}}],
            "containers": [
                {
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "privileged": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    }
                }
            ],
        },
    }
    validate_pod_security({"items": [base]}, workload_namespace="workload")

    bad_token = json.loads(json.dumps(base))
    bad_token["spec"]["automountServiceAccountToken"] = True
    with pytest.raises(KindEvidenceError, match="service account token"):
        validate_pod_security({"items": [bad_token]}, workload_namespace="workload")

    bad_host = json.loads(json.dumps(base))
    bad_host["spec"]["volumes"] = [{"name": "host", "hostPath": {"path": "/"}}]
    with pytest.raises(KindEvidenceError, match="hostPath"):
        validate_pod_security({"items": [bad_host]}, workload_namespace="workload")

    bad_privilege = json.loads(json.dumps(base))
    bad_privilege["spec"]["containers"][0]["securityContext"]["privileged"] = True
    with pytest.raises(KindEvidenceError, match="security contract"):
        validate_pod_security({"items": [bad_privilege]}, workload_namespace="workload")


def test_helm_values_bind_unique_scopes_digest_and_typed_test_flags(tmp_path: Path) -> None:
    harness = KindAdaptationHarness(_config(tmp_path), identity=_identity())
    try:
        harness.app_image = PINNED_APP
        values = harness._helm_values(log_level="DEBUG")
        joined = " ".join(values)

        assert "--set global.testMode=true" in joined
        assert "--set config.servingFakeEnabled=true" in joined
        assert "--set service.nodePort=30080" in joined
        assert f"namespaces.workload={harness.identity.workload_namespace}" in joined
        assert f"existingSecret.name={harness.identity.external_secret_name}" in joined
        assert f"image.repository={APP_REPOSITORY}:m7-1234abcd" in joined
        assert f"config.servingImage={PINNED_APP}" in joined
        assert harness.credentials.bootstrap_token not in joined
        assert chart_fullname(harness.identity.release_name) == "mac-1234abcd-mini-ai-cloud"
    finally:
        assert harness._remove_temporary_state() is None


def test_first_phase_failure_still_finalizes_failed_bundle_and_scoped_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = KindAdaptationHarness(_config(tmp_path), identity=_identity())
    temp_root = harness.temp_root

    def fail_render() -> list[CommandOutcome]:
        raise PhaseFailure("synthetic preflight failure")

    monkeypatch.setattr(harness, "_helm_render", fail_render)

    returncode, bundle, error = harness.execute()

    assert returncode == 1
    assert bundle is not None
    assert error == "synthetic preflight failure"
    assert not temp_root.exists()
    verify_bundle(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    claims = json.loads((bundle / "claims.json").read_text(encoding="utf-8"))
    summary = json.loads((bundle / "kubernetes-summary.json").read_text(encoding="utf-8"))
    cleanup = json.loads((bundle / "cleanup.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "FAIL"
    assert claims["status"] == "FAIL"
    assert {item["id"] for item in claims["claims"]} == set(REQUIRED_CLAIMS)
    assert summary["status"] == "FAIL"
    assert cleanup["cluster_deleted"] is True
    assert cleanup["temporary_state_deleted"] is True
