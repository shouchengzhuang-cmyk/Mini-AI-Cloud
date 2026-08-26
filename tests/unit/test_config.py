from typing import Any

import pytest
from pydantic import ValidationError

from core.config import Settings


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
