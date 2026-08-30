import json
from pathlib import Path

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "helm" / "mini-ai-cloud"
FIXTURES = ROOT / "tests" / "fixtures" / "helm"


def test_chart_metadata_has_no_bundled_dependencies() -> None:
    metadata = yaml.safe_load((CHART / "Chart.yaml").read_text(encoding="utf-8"))

    assert metadata["apiVersion"] == "v2"
    assert metadata["type"] == "application"
    assert metadata["version"] == metadata["appVersion"] == "0.6.0"
    assert "dependencies" not in metadata


def test_default_values_are_single_replica_and_not_latest() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))

    assert values["controlPlane"]["replicas"] == 1
    assert values["service"]["type"] == "ClusterIP"
    assert values["global"]["testMode"] is False
    assert values["image"]["tag"] != "latest"
    assert values["existingSecret"]["name"]


def test_schema_fails_closed_for_ha_and_unknown_security_options() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text(encoding="utf-8"))

    assert schema["additionalProperties"] is False
    assert schema["properties"]["controlPlane"]["properties"]["replicas"]["const"] == 1
    assert schema["properties"]["storage"]["additionalProperties"] is False
    assert "podSecurity" not in schema["properties"]


def test_chart_never_owns_secrets_namespaces_or_cluster_rbac() -> None:
    templates = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((CHART / "templates").glob("*.yaml"))
    )

    assert "kind: Secret\n" not in templates
    assert "kind: Namespace\n" not in templates
    assert "kind: ClusterRole\n" not in templates
    assert "kind: ClusterRoleBinding\n" not in templates
    assert "hostPath:" not in templates
    assert "privileged: true" not in templates
    assert 'resources: ["services"]' in templates
    assert 'apiGroups: ["batch"]' in templates
    assert 'resources: ["jobs"]' in templates
    assert 'drop: ["ALL"]' in templates
    assert "readOnlyRootFilesystem: true" in templates


def test_render_contract_fixtures_and_snapshot_are_present() -> None:
    expected = {
        "values-positive.yaml",
        "values-replicas-two.yaml",
        "values-nodeport-production.yaml",
        "values-production-fake.yaml",
        "values-forbidden-hostpath.yaml",
        "values-forbidden-privileged.yaml",
        "no-secret.snapshot.json",
    }

    assert expected <= {path.name for path in FIXTURES.iterdir()}
    snapshot = json.loads((FIXTURES / "no-secret.snapshot.json").read_text(encoding="utf-8"))
    assert "Secret" in snapshot["forbiddenKinds"]
    assert snapshot["externalSecretName"] == "mini-ai-cloud-ci"


def test_chart_runtime_profiles_match_repository_sources() -> None:
    sources = ROOT / "runtime_profiles"
    packaged = CHART / "runtime_profiles"
    source_files = {path.name: path.read_bytes() for path in sources.iterdir() if path.is_file()}
    packaged_files = {path.name: path.read_bytes() for path in packaged.iterdir() if path.is_file()}

    assert packaged_files == source_files
    assert sum(len(content) for content in packaged_files.values()) < 900_000


def test_kind_smoke_uses_ephemeral_credentials_and_pinned_tools() -> None:
    script = (ROOT / "scripts" / "helm_kind_smoke.sh").read_text(encoding="utf-8")

    assert "umask 077" in script
    assert "secrets.token_urlsafe" in script
    assert "secrets.token_bytes(32)" in script
    assert "--from-env-file" in script
    assert "--from-literal" not in script
    assert "kindest/node:v1.32.2@sha256:" in script
    assert 'KIND_VERSION" != "v0.27.0"' in script
    assert "wait_for_release_cleanup" in script
