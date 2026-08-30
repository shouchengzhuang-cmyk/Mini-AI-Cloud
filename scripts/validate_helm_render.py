from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy" / "helm" / "mini-ai-cloud"
FIXTURES = ROOT / "tests" / "fixtures" / "helm"
POSITIVE_VALUES = FIXTURES / "values-positive.yaml"
KIND_VALUES = CHART / "ci" / "values-kind.yaml"
SNAPSHOT = FIXTURES / "no-secret.snapshot.json"
SOURCE_PROFILES = ROOT / "runtime_profiles"
CHART_PROFILES = CHART / "runtime_profiles"


def _run(command: list[str], *, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )
    if expect_success and completed.returncode != 0:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stdout}{completed.stderr}"
        )
    if not expect_success and completed.returncode == 0:
        raise RuntimeError(f"command unexpectedly succeeded: {' '.join(command)}")
    return completed


def _render(
    helm: str,
    *,
    release: str,
    namespace: str,
    values: Path,
) -> list[dict[str, Any]]:
    completed = _run(
        [
            helm,
            "template",
            release,
            str(CHART),
            "--namespace",
            namespace,
            "--values",
            str(values),
        ]
    )
    documents = [item for item in yaml.safe_load_all(completed.stdout) if item is not None]
    if not documents or not all(isinstance(item, dict) for item in documents):
        raise RuntimeError("Helm rendered no Kubernetes objects or a non-object document")
    return documents


def _pod_specs(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for document in documents:
        kind = document.get("kind")
        spec = document.get("spec", {})
        if kind in {"Deployment", "StatefulSet"}:
            specs.append(spec["template"]["spec"])
        elif kind == "Job":
            specs.append(spec["template"]["spec"])
    return specs


def _secret_references(value: Any) -> set[tuple[str, str]]:
    references: set[tuple[str, str]] = set()
    if isinstance(value, dict):
        secret_ref = value.get("secretKeyRef")
        if isinstance(secret_ref, dict):
            name = secret_ref.get("name")
            key = secret_ref.get("key")
            if isinstance(name, str) and isinstance(key, str):
                references.add((name, key))
        for nested in value.values():
            references.update(_secret_references(nested))
    elif isinstance(value, list):
        for nested in value:
            references.update(_secret_references(nested))
    return references


def _assert_security(documents: list[dict[str, Any]]) -> None:
    forbidden_kinds = {"ClusterRole", "ClusterRoleBinding", "Namespace", "Secret"}
    observed_kinds = {str(document.get("kind")) for document in documents}
    unexpected = observed_kinds & forbidden_kinds
    if unexpected:
        raise RuntimeError(f"Chart rendered forbidden Kubernetes kinds: {sorted(unexpected)}")

    for document in documents:
        if document.get("kind") != "Role":
            continue
        for rule in document.get("rules", []):
            if "*" in rule.get("resources", []) or "*" in rule.get("verbs", []):
                raise RuntimeError("namespaced RBAC must not contain wildcard resources or verbs")

    roles = {
        str(document["metadata"]["name"]): document
        for document in documents
        if document.get("kind") == "Role"
    }
    control_role = next(role for name, role in roles.items() if name.endswith("-control-plane"))
    worker_role = next(role for name, role in roles.items() if name.endswith("-worker"))

    def canonical_rules(role: dict[str, Any]) -> set[tuple[tuple[str, ...], ...]]:
        return {
            (
                tuple(rule.get("apiGroups", [])),
                tuple(rule.get("resources", [])),
                tuple(rule.get("verbs", [])),
            )
            for rule in role.get("rules", [])
        }

    expected_control = {
        (("",), ("pods",), ("create", "delete", "get", "list", "watch")),
        (("",), ("pods/status",), ("get",)),
        (("",), ("services",), ("create", "delete", "get", "list")),
    }
    expected_worker = {
        (("",), ("pods",), ("create", "delete", "get", "list", "watch")),
        (("",), ("pods/status", "pods/log"), ("get",)),
        (
            ("networking.k8s.io",),
            ("networkpolicies",),
            ("create", "delete", "get", "list", "watch"),
        ),
        (("batch",), ("jobs",), ("create", "delete", "get", "list", "patch", "watch")),
    }
    if canonical_rules(control_role) != expected_control:
        raise RuntimeError("control-plane Role differs from the bounded P3 contract")
    if canonical_rules(worker_role) != expected_worker:
        raise RuntimeError("worker Role differs from the bounded P2 contract")

    for pod_spec in _pod_specs(documents):
        for forbidden_field in ("hostNetwork", "hostPID", "hostIPC"):
            if pod_spec.get(forbidden_field) is True:
                raise RuntimeError(f"Pod spec enables forbidden {forbidden_field}")
        pod_security = pod_spec.get("securityContext", {})
        if pod_security.get("runAsNonRoot") is not True:
            raise RuntimeError("every rendered Pod must set runAsNonRoot=true")
        if pod_security.get("seccompProfile", {}).get("type") != "RuntimeDefault":
            raise RuntimeError("every rendered Pod must use seccompProfile RuntimeDefault")
        for volume in pod_spec.get("volumes", []):
            if "hostPath" in volume:
                raise RuntimeError("Chart must never render hostPath")
            empty_dir = volume.get("emptyDir")
            if isinstance(empty_dir, dict) and empty_dir.get("sizeLimit"):
                continue
            if isinstance(volume.get("configMap"), dict):
                continue
            if not isinstance(empty_dir, dict) or not empty_dir.get("sizeLimit"):
                raise RuntimeError("every rendered writable volume must be a bounded emptyDir")
        containers = [*pod_spec.get("initContainers", []), *pod_spec.get("containers", [])]
        for container in containers:
            security = container.get("securityContext", {})
            if security.get("allowPrivilegeEscalation") is not False:
                raise RuntimeError("every container must disable privilege escalation")
            if security.get("readOnlyRootFilesystem") is not True:
                raise RuntimeError("every container must use a read-only root filesystem")
            if security.get("runAsNonRoot") is not True:
                raise RuntimeError("every container must set runAsNonRoot=true")
            if security.get("capabilities", {}).get("drop") != ["ALL"]:
                raise RuntimeError("every container must drop all Linux capabilities")
            image = str(container.get("image", ""))
            if not image or image.endswith(":latest"):
                raise RuntimeError("every container image must be explicitly versioned or pinned")


def _assert_snapshot(documents: list[dict[str, Any]]) -> None:
    expected = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    counts = Counter(str(document["kind"]) for document in documents)
    if dict(sorted(counts.items())) != expected["objectKindCounts"]:
        raise RuntimeError(f"rendered kind snapshot changed: {dict(sorted(counts.items()))}")
    if set(counts) & set(expected["forbiddenKinds"]):
        raise RuntimeError("no-secret snapshot contains an owned Secret or cluster-scoped object")
    references = _secret_references(documents)
    names = {name for name, _key in references}
    keys = sorted({key for _name, key in references})
    if names != {expected["externalSecretName"]}:
        raise RuntimeError(f"unexpected external Secret references: {sorted(names)}")
    if keys != expected["externalSecretKeys"]:
        raise RuntimeError(f"external Secret key snapshot changed: {keys}")


def _assert_runtime_profiles(documents: list[dict[str, Any]]) -> None:
    source_files = {path.name: path for path in SOURCE_PROFILES.iterdir() if path.is_file()}
    chart_files = {path.name: path for path in CHART_PROFILES.iterdir() if path.is_file()}
    if source_files.keys() != chart_files.keys():
        raise RuntimeError("Chart runtime-profile file set differs from the repository source")
    for name, source_path in source_files.items():
        if source_path.read_bytes() != chart_files[name].read_bytes():
            raise RuntimeError(f"Chart runtime-profile copy drifted from source: {name}")
    total_bytes = sum(path.stat().st_size for path in chart_files.values())
    if total_bytes >= 900_000:
        raise RuntimeError("runtime-profile ConfigMap payload is too close to the 1 MiB limit")

    profile_maps = [
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and str(document.get("metadata", {}).get("name", "")).endswith("-runtime-profiles")
    ]
    if len(profile_maps) != 1:
        raise RuntimeError("Chart must render exactly one runtime-profile ConfigMap")
    rendered_data = profile_maps[0].get("data", {})
    expected_data = {name: path.read_text(encoding="utf-8") for name, path in source_files.items()}
    if rendered_data != expected_data:
        raise RuntimeError("rendered runtime-profile ConfigMap is not byte-for-byte source content")

    expected_paths = {f"runtime_profiles/{name}" for name in source_files}
    for document in documents:
        if document.get("kind") != "Deployment":
            continue
        pod_spec = document["spec"]["template"]["spec"]
        volume = next(item for item in pod_spec["volumes"] if item["name"] == "runtime-profiles")
        observed_paths = {item["path"] for item in volume["configMap"]["items"]}
        if observed_paths != expected_paths:
            raise RuntimeError(
                "runtime-profile volume must preserve the repository directory topology"
            )
        container = pod_spec["containers"][0]
        mount = next(
            item for item in container["volumeMounts"] if item["name"] == "runtime-profiles"
        )
        if mount != {
            "name": "runtime-profiles",
            "mountPath": "/etc/mini-ai-cloud",
            "readOnly": True,
        }:
            raise RuntimeError("runtime-profile volume must be mounted read-only at the safe root")


def validate(helm: str) -> None:
    _run([helm, "lint", str(CHART), "--values", str(POSITIVE_VALUES)])

    suffix = uuid.uuid4().hex[:8]
    release = f"m7-{suffix}"
    namespace = f"m7-system-{suffix}"
    documents = _render(
        helm,
        release=release,
        namespace=namespace,
        values=POSITIVE_VALUES,
    )
    namespaces = {document.get("metadata", {}).get("namespace") for document in documents}
    if namespaces != {namespace, "mini-ai-cloud-ci-workloads"}:
        raise RuntimeError(f"random namespace render escaped the allowlist: {sorted(namespaces)}")

    _assert_security(documents)
    _assert_snapshot(documents)
    _assert_runtime_profiles(documents)

    deployments = {
        document["metadata"]["name"]: document
        for document in documents
        if document.get("kind") == "Deployment"
    }
    control_plane = next(
        document for name, document in deployments.items() if name.endswith("-control-plane")
    )
    if control_plane["spec"]["replicas"] != 1:
        raise RuntimeError("control-plane Deployment must render exactly one replica")
    if control_plane["spec"].get("strategy", {}).get("type") != "Recreate":
        raise RuntimeError("control-plane Deployment must avoid overlapping replicas on upgrade")
    worker = next(document for document in documents if document.get("kind") == "StatefulSet")
    if worker["spec"]["replicas"] != 1:
        raise RuntimeError("default worker StatefulSet must render exactly one replica")
    if worker["spec"].get("serviceName") != f"{release}-mini-ai-cloud-worker":
        raise RuntimeError("worker StatefulSet must use its release-scoped governing Service")
    worker_env = {
        item["name"]: item for item in worker["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    worker_id_ref = worker_env.get("WORKER_ID", {}).get("valueFrom", {}).get("fieldRef", {})
    if worker_id_ref.get("fieldPath") != "metadata.name":
        raise RuntimeError("worker identity must use the stable StatefulSet Pod name")

    kind_documents = _render(
        helm,
        release="mini-ai-cloud-kind",
        namespace="mini-ai-cloud-system",
        values=KIND_VALUES,
    )
    service_types = {
        document["spec"].get("type", "ClusterIP")
        for document in kind_documents
        if document.get("kind") == "Service"
    }
    if "NodePort" not in service_types:
        raise RuntimeError("Kind values must explicitly exercise the test-only NodePort path")
    kind_config = next(
        document
        for document in kind_documents
        if document.get("kind") == "ConfigMap"
        and not str(document["metadata"]["name"]).endswith("-runtime-profiles")
    )
    if kind_config["data"]["KUBERNETES_SERVING_FAKE_ENABLED"] != "true":
        raise RuntimeError("Kind values must explicitly enable the test-only Fake serving path")
    _assert_security(kind_documents)

    for fixture_name in (
        "values-replicas-two.yaml",
        "values-nodeport-production.yaml",
        "values-production-fake.yaml",
        "values-forbidden-hostpath.yaml",
        "values-forbidden-privileged.yaml",
    ):
        _run(
            [
                helm,
                "template",
                "m7-rejected",
                str(CHART),
                "--namespace",
                "mini-ai-cloud-system",
                "--values",
                str(FIXTURES / fixture_name),
            ],
            expect_success=False,
        )

    print(
        "CHART_RENDERED: lint, schema negatives, random namespace, no-secret snapshot, "
        "RBAC, security contexts, and Kind values passed"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the Mini AI Cloud Helm Chart")
    parser.add_argument("--helm", default="helm", help="Helm binary or absolute path")
    args = parser.parse_args()
    validate(args.helm)


if __name__ == "__main__":
    main()
