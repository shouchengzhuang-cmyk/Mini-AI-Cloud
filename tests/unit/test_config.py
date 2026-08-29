from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from core.config import Settings
from core.runtime_profiles import RuntimeProfileCatalog


def test_worker_labels_are_parsed_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_LABELS", "region=local, runtime=docker,zone=lab")

    settings = Settings(_env_file=None)

    assert settings.worker_labels == {
        "region": "local",
        "runtime": "docker",
        "zone": "lab",
    }


def test_worker_labels_accept_mapping_and_normalize_values() -> None:
    settings = Settings(_env_file=None, worker_labels={"gpu": 1, "enabled": True})

    assert settings.worker_labels == {"gpu": "1", "enabled": "True"}


@pytest.mark.parametrize("cluster_id", ["", "space is invalid", "_invalid-prefix"])
def test_cluster_id_rejects_unsafe_docker_label_values(cluster_id: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, cluster_id=cluster_id)


@pytest.mark.parametrize("value", ["region", "=local", "region=local,bad"])
def test_worker_labels_reject_malformed_pairs(value: str) -> None:
    with pytest.raises(ValidationError, match="labels must use key=value pairs"):
        Settings(_env_file=None, worker_labels=value)


@pytest.mark.parametrize(
    "overrides",
    [
        {"lease_renew_interval": 30, "task_lease_seconds": 30},
        {"heartbeat_interval": 15, "worker_offline_timeout": 15},
        {"default_task_timeout": 120, "max_task_timeout": 60},
    ],
)
def test_distributed_timeout_relationships_are_validated(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **overrides)


@pytest.mark.parametrize(
    ("overrides", "expected_name"),
    [
        (
            {
                "api_key_pepper": "p" * 32,
                "worker_auth_token": "local-development-worker-token",
            },
            "WORKER_AUTH_TOKEN",
        ),
        (
            {
                "api_key_pepper": "local-development-api-key-pepper-change-me",
                "worker_auth_token": "w" * 32,
            },
            "API_KEY_PEPPER",
        ),
        (
            {
                "api_key_pepper": "p" * 32,
                "worker_auth_token": "w" * 32,
                "bootstrap_enabled": True,
                "bootstrap_token": "too-short",
            },
            "BOOTSTRAP_TOKEN",
        ),
    ],
)
def test_production_rejects_development_or_weak_credentials(
    overrides: dict[str, Any],
    expected_name: str,
) -> None:
    values: dict[str, Any] = {
        "app_env": "production",
        "legacy_anonymous_enabled": False,
        "bootstrap_enabled": False,
        **overrides,
    }
    with pytest.raises(ValidationError, match=expected_name):
        Settings(_env_file=None, **values)


def test_production_accepts_explicit_strong_credentials() -> None:
    settings = Settings(
        _env_file=None,
        app_env="production",
        legacy_anonymous_enabled=False,
        bootstrap_enabled=True,
        bootstrap_token="b" * 32,
        api_key_pepper="p" * 32,
        worker_auth_token="w" * 32,
    )

    assert settings.app_env == "production"


def test_gateway_buffer_limit_has_safe_default_and_environment_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert Settings(_env_file=None).service_proxy_max_response_bytes == 16 * 1024 * 1024

    monkeypatch.setenv("SERVICE_PROXY_MAX_RESPONSE_BYTES", "2048")
    assert Settings(_env_file=None).service_proxy_max_response_bytes == 2048

    with pytest.raises(ValidationError):
        Settings(_env_file=None, service_proxy_max_response_bytes=1023)


def test_gateway_phase_timeouts_and_drain_timeout_accept_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defaults = Settings(_env_file=None)
    assert defaults.service_proxy_connect_timeout == 5
    assert defaults.service_proxy_first_token_timeout == 30
    assert defaults.service_proxy_timeout == 120
    assert defaults.service_drain_timeout == 30

    monkeypatch.setenv("SERVICE_PROXY_CONNECT_TIMEOUT", "2")
    monkeypatch.setenv("SERVICE_PROXY_FIRST_TOKEN_TIMEOUT", "3")
    monkeypatch.setenv("SERVICE_PROXY_TIMEOUT", "4")
    monkeypatch.setenv("SERVICE_DRAIN_TIMEOUT", "0")
    configured = Settings(_env_file=None)
    assert configured.service_proxy_connect_timeout == 2
    assert configured.service_proxy_first_token_timeout == 3
    assert configured.service_proxy_timeout == 4
    assert configured.service_drain_timeout == 0

    with pytest.raises(ValidationError):
        Settings(_env_file=None, service_proxy_connect_timeout=0)


def test_optional_vllm_worker_id_accepts_compose_empty_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SERVICE_VLLM_WORKER_ID", "")

    assert Settings(_env_file=None).service_vllm_worker_id is None


def test_optional_vllm_image_accepts_empty_or_pinned_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VLLM_IMAGE", "")
    assert Settings(_env_file=None).vllm_image is None

    pinned = "example/vllm@sha256:" + "a" * 64
    monkeypatch.setenv("VLLM_IMAGE", pinned)
    assert Settings(_env_file=None).vllm_image == pinned


def test_kubernetes_serving_is_safe_by_default_and_accepts_explicit_test_configuration() -> None:
    defaults = Settings(_env_file=None)
    assert defaults.kubernetes_serving_enabled is False
    assert defaults.kubernetes_serving_fake_enabled is False

    configured = Settings(
        _env_file=None,
        app_env="test",
        kubernetes_serving_enabled=True,
        kubernetes_serving_fake_enabled=True,
        kubernetes_serving_namespace="model-serving",
        kubernetes_serving_cluster_id="kind.phase4-a",
        kubernetes_serving_image="mini-ai-cloud:kind-serving-v4a",
        kubernetes_serving_service_account_name="serving-runtime",
        kubernetes_serving_image_pull_secrets=("registry-pull, secondary-registry, registry-pull"),
        runtime_profile_manifest_path=("/etc/mini-ai-cloud/runtime_profiles/manifest.json"),
    )
    assert configured.kubernetes_serving_namespace == "model-serving"
    assert configured.kubernetes_serving_cluster_id == "kind.phase4-a"
    assert configured.kubernetes_serving_service_account_name == "serving-runtime"
    assert configured.kubernetes_serving_image_pull_secrets == (
        "registry-pull",
        "secondary-registry",
    )
    assert configured.runtime_profile_manifest_path == (
        "/etc/mini-ai-cloud/runtime_profiles/manifest.json"
    )


def test_default_runtime_profile_manifest_is_release_relative_not_cwd_relative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    settings = Settings(_env_file=None)
    manifest_path = Path(settings.runtime_profile_manifest_path)
    catalog = RuntimeProfileCatalog.from_path(manifest_path)

    assert manifest_path.is_absolute()
    assert manifest_path.name == "manifest.json"
    assert manifest_path.parent.name == "runtime_profiles"
    assert {entry.identity for entry in catalog.manifest.profiles} >= {
        "nvidia-vllm-k8s@2.0.0",
        "ascend-vllm-k8s-a2@2.0.0",
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"kubernetes_serving_fake_enabled": True},
        {
            "app_env": "production",
            "legacy_anonymous_enabled": False,
            "bootstrap_enabled": False,
            "api_key_pepper": "p" * 32,
            "worker_auth_token": "w" * 32,
            "kubernetes_serving_enabled": True,
            "kubernetes_serving_fake_enabled": True,
        },
        {"kubernetes_serving_namespace": "Uppercase"},
        {"kubernetes_serving_cluster_id": "trailing-"},
        {"kubernetes_serving_service_account_name": "Bad_Name"},
        {"kubernetes_serving_image_pull_secrets": "registry-pull,trailing-"},
        {"kubernetes_serving_lease_seconds": 6, "kubernetes_serving_probe_timeout": 3},
    ],
)
def test_kubernetes_serving_rejects_unsafe_configuration(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **overrides)


def test_kubernetes_serving_image_pull_secrets_accept_empty_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KUBERNETES_SERVING_IMAGE_PULL_SECRETS", "")

    assert Settings(_env_file=None).kubernetes_serving_image_pull_secrets == ()
