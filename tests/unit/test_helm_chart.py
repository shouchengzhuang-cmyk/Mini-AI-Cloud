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
    assert values["worker"]["replicas"] == 1
    assert values["service"]["type"] == "ClusterIP"
    assert values["global"]["testMode"] is False
    assert values["image"]["tag"] != "latest"
    assert values["existingSecret"]["name"]


def test_worker_uses_stable_stateful_identity_for_restart_adoption() -> None:
    template = (CHART / "templates" / "worker-deployment.yaml").read_text(encoding="utf-8")

    assert "kind: StatefulSet" in template
    assert "serviceName:" in template
    assert "- name: WORKER_ID" in template
    assert "fieldPath: metadata.name" in template
    assert "- name: KUBERNETES_WORKER_POD_NAMESPACE" in template
    assert "fieldPath: metadata.namespace" in template
    assert "- name: KUBERNETES_WORKER_STATEFULSET_NAME" in template
    assert (CHART / "templates" / "worker-headless-service.yaml").is_file()


def test_schema_fails_closed_for_ha_and_unknown_security_options() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text(encoding="utf-8"))

    assert schema["additionalProperties"] is False
    assert schema["properties"]["controlPlane"]["properties"]["replicas"]["const"] == 1
    assert schema["properties"]["storage"]["additionalProperties"] is False
    assert "podSecurity" not in schema["properties"]


def test_chart_keeps_inventory_and_workload_rbac_bounded() -> None:
    templates = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((CHART / "templates").glob("*.yaml"))
    )

    assert "kind: Secret\n" not in templates
    assert "kind: Namespace\n" not in templates
    assert templates.count("\nkind: ClusterRole\n") == 1
    assert templates.count("\nkind: ClusterRoleBinding\n") == 1
    assert 'resources: ["nodes"]\n    verbs: ["get"]' in templates
    assert 'resources: ["pods"]\n    verbs: ["list"]' in templates
    assert 'ACCELERATOR_INVENTORY_PROVIDERS: "kubernetes-node"' in templates
    assert 'resources: ["secrets"]' not in templates
    assert 'verbs: ["*"]' not in templates
    assert 'resources: ["*"]' not in templates
    assert "hostPath:" not in templates
    assert "privileged: true" not in templates
    assert 'resources: ["services"]' in templates
    assert 'apiGroups: ["batch"]' in templates
    assert 'resources: ["jobs"]' in templates
    assert 'verbs: ["create", "delete", "get", "list", "patch", "watch"]' in templates
    assert 'resources: ["statefulsets"]' in templates
    assert 'verbs: ["get"]' in templates
    assert "resourceNames:" in templates
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
