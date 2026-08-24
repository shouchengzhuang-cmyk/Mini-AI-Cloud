import json
from functools import lru_cache
from typing import Annotated

from pydantic import BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

_LOCAL_API_KEY_PEPPER = "local-development-api-key-pepper-change-me"
_LOCAL_WORKER_AUTH_TOKEN = "local-development-worker-token"
_PRODUCTION_CREDENTIAL_MIN_BYTES = 32


def _parse_labels(value: object) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    if value in (None, ""):
        return {}
    if not isinstance(value, str):
        raise ValueError("labels must be a comma-separated string or object")
    if value.lstrip().startswith("{"):
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise ValueError("labels JSON must be an object")
        return {str(key): str(item) for key, item in decoded.items()}
    result: dict[str, str] = {}
    for pair in value.split(","):
        key, separator, item = pair.partition("=")
        if not separator or not key.strip():
            raise ValueError("labels must use key=value pairs")
        result[key.strip()] = item.strip()
    return result


def _blank_to_none(value: object) -> object:
    if isinstance(value, str) and not value.strip():
        return None
    return value


Labels = Annotated[dict[str, str], NoDecode, BeforeValidator(_parse_labels)]
OptionalNonEmptyString = Annotated[str | None, BeforeValidator(_blank_to_none)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    database_url: str = "postgresql+asyncpg://task:task@localhost:5432/task_platform"
    redis_url: str = "redis://localhost:6379/0"
    app_env: str = Field(default="development", pattern=r"^(development|test|production)$")

    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    control_plane_enabled: bool = True
    legacy_anonymous_enabled: bool = True
    bootstrap_enabled: bool = True
    bootstrap_token: str = ""
    legacy_project_id: str = "00000000-0000-0000-0000-000000000001"
    api_key_pepper: str = _LOCAL_API_KEY_PEPPER
    secret_master_key: str = ""
    worker_auth_token: str = _LOCAL_WORKER_AUTH_TOKEN

    cluster_id: str = Field(
        default="mini-docker-cloud-local",
        min_length=1,
        max_length=63,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$",
    )
    worker_id: str | None = None
    worker_concurrency: int = Field(default=4, ge=1, le=128)
    worker_labels: Labels = Field(default_factory=lambda: {"runtime": "docker", "region": "local"})
    worker_runtime_types: str = "docker"
    worker_node_name: str | None = None
    fake_gpu_count: int = Field(default=0, ge=0, le=64)
    fake_gpu_model: str = "FAKE-A100"
    fake_gpu_memory_mb: int = Field(default=40_960, ge=1)
    heartbeat_interval: float = Field(default=5.0, gt=0)
    worker_offline_timeout: float = Field(default=15.0, gt=0)
    task_lease_seconds: float = Field(default=30.0, gt=5)
    lease_renew_interval: float = Field(default=5.0, gt=0)
    worker_shutdown_timeout: float = Field(default=30.0, gt=0)

    default_task_timeout: int = Field(default=60, ge=1, le=86400)
    max_task_timeout: int = Field(default=86400, ge=1)
    max_task_retries: int = Field(default=10, ge=0, le=100)
    max_recovery_attempts: int = Field(default=3, ge=0, le=100)
    recovery_cleanup_grace_seconds: float = Field(default=5.0, ge=0)
    retry_max_backoff_seconds: float = Field(default=60.0, gt=0)
    docker_stop_timeout: int = Field(default=5, ge=0, le=60)
    docker_always_pull: bool = False
    docker_pids_limit: int = Field(default=256, ge=16, le=4096)
    docker_tmpfs_size_mb: int = Field(default=64, ge=1, le=4096)
    max_task_log_bytes: int = Field(default=10 * 1024 * 1024, ge=1024)
    max_log_chunk_bytes: int = Field(default=64 * 1024, ge=1024)
    log_drain_timeout: float = Field(default=5.0, gt=0)
    orphan_reconcile_interval: float = Field(default=1.0, gt=0)

    outbox_poll_interval: float = Field(default=0.25, gt=0)
    scheduler_poll_interval: float = Field(default=1.0, gt=0)
    scheduler_mode: str = Field(default="pull", pattern=r"^(pull|global)$")
    scheduling_policy: str = Field(default="binpack", pattern=r"^(binpack|spread)$")
    scheduler_aging_interval_seconds: int = Field(default=60, ge=1)
    scheduler_preemption_enabled: bool = False
    scheduler_preemption_min_delta: int = Field(default=10, ge=1, le=100)
    reaper_interval: float = Field(default=5.0, gt=0)
    control_operation_timeout: float = Field(default=30.0, gt=0)
    control_shutdown_timeout: float = Field(default=10.0, gt=0)
    health_check_timeout: float = Field(default=3.0, gt=0)
    batch_size: int = Field(default=100, ge=1, le=1000)

    log_stream_maxlen: int = Field(default=10000, ge=100)
    log_stream_ttl_seconds: int = Field(default=86400, ge=60)
    ready_stream_maxlen: int = Field(default=100000, ge=1000)
    redis_socket_timeout: float = Field(default=5.0, gt=0)
    sse_heartbeat_seconds: float = Field(default=10.0, gt=0)
    api_request_max_bytes: int = Field(default=1_048_576, ge=1024)
    websocket_queue_size: int = Field(default=1000, ge=10, le=100_000)
    api_key_rate_limit_per_minute: int = Field(default=600, ge=1)
    rate_limit_fail_open: bool = False

    cpu_price_per_hour: float = Field(default=0.05, ge=0)
    gpu_price_per_hour: float = Field(default=1.0, ge=0)
    memory_price_per_gb_hour: float = Field(default=0.005, ge=0)

    artifact_backend: str = Field(default="local", pattern=r"^(local|s3)$")
    artifact_local_root: str = "/var/lib/mini-ai-cloud/artifacts"
    artifact_workspace_root: str = "/var/lib/mini-ai-cloud/workspaces"
    docker_artifact_workspace_volume: str = Field(
        default="",
        max_length=255,
        pattern=r"^(?:[A-Za-z0-9][A-Za-z0-9_.-]{0,254})?$",
    )
    artifact_s3_bucket: str = "mini-ai-cloud"
    artifact_s3_endpoint_url: str | None = None
    artifact_max_bytes: int = Field(default=10 * 1024 * 1024 * 1024, ge=1024)
    artifact_signed_url_ttl_seconds: int = Field(default=900, ge=60, le=86_400)

    kubernetes_namespace: str = "mini-ai-cloud"
    kubernetes_kubeconfig: str | None = None
    kubernetes_in_cluster: bool = False
    kubernetes_cleanup_grace_seconds: int = Field(default=30, ge=0, le=3600)

    service_reconcile_interval: float = Field(default=2.0, gt=0)
    service_health_interval: float = Field(default=5.0, gt=0)
    service_proxy_timeout: float = Field(default=120.0, gt=0)
    service_proxy_max_response_bytes: int = Field(
        default=16 * 1024 * 1024,
        ge=1024,
        le=1024 * 1024 * 1024,
    )
    service_endpoint_host_allowlist: str = ""
    service_autoscale_interval: float = Field(default=15.0, gt=0)
    service_scale_to_zero_enabled: bool = False
    service_vllm_docker_enabled: bool = False
    service_vllm_worker_id: OptionalNonEmptyString = Field(
        default=None, min_length=1, max_length=255
    )
    service_vllm_endpoint_host: str = Field(default="127.0.0.1", min_length=1, max_length=255)
    service_vllm_publish_address: str = Field(default="127.0.0.1", min_length=1, max_length=255)
    service_vllm_cache_volume: str = Field(
        default="mini-ai-cloud-vllm-cache",
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$",
    )
    service_vllm_ready_timeout: float = Field(default=600.0, gt=0)
    service_vllm_probe_timeout: float = Field(default=3.0, gt=0)
    service_vllm_lease_seconds: float = Field(default=900.0, gt=0)

    cleanup_interval_seconds: float = Field(default=3600.0, gt=0)
    task_retention_days: int = Field(default=30, ge=1)
    log_retention_days: int = Field(default=14, ge=1)
    audit_retention_days: int = Field(default=365, ge=1)
    artifact_retention_days: int = Field(default=30, ge=1)

    @model_validator(mode="after")
    def validate_distributed_timeouts(self) -> "Settings":
        if self.lease_renew_interval >= self.task_lease_seconds:
            raise ValueError("LEASE_RENEW_INTERVAL must be less than TASK_LEASE_SECONDS")
        if self.heartbeat_interval >= self.worker_offline_timeout:
            raise ValueError("HEARTBEAT_INTERVAL must be less than WORKER_OFFLINE_TIMEOUT")
        if self.default_task_timeout > self.max_task_timeout:
            raise ValueError("DEFAULT_TASK_TIMEOUT must not exceed MAX_TASK_TIMEOUT")
        if self.service_vllm_lease_seconds <= self.service_vllm_probe_timeout * 2:
            raise ValueError(
                "SERVICE_VLLM_LEASE_SECONDS must exceed two SERVICE_VLLM_PROBE_TIMEOUTs"
            )
        if self.fake_gpu_count and self.app_env == "production":
            raise ValueError("FAKE_GPU_COUNT must be zero in production")
        runtime_types = {
            item.strip() for item in self.worker_runtime_types.split(",") if item.strip()
        }
        if not runtime_types or not runtime_types <= {"docker", "kubernetes", "fake"}:
            raise ValueError("WORKER_RUNTIME_TYPES must contain docker, kubernetes or fake")
        if self.app_env == "production" and self.legacy_anonymous_enabled:
            raise ValueError("LEGACY_ANONYMOUS_ENABLED must be false in production")
        if self.app_env == "production":
            _validate_production_credential(
                "WORKER_AUTH_TOKEN",
                self.worker_auth_token,
                forbidden_value=_LOCAL_WORKER_AUTH_TOKEN,
            )
            _validate_production_credential(
                "API_KEY_PEPPER",
                self.api_key_pepper,
                forbidden_value=_LOCAL_API_KEY_PEPPER,
            )
            if self.bootstrap_enabled:
                _validate_production_credential("BOOTSTRAP_TOKEN", self.bootstrap_token)
        return self


def _validate_production_credential(
    name: str,
    value: str,
    *,
    forbidden_value: str | None = None,
) -> None:
    normalized = value.strip()
    if forbidden_value is not None and normalized == forbidden_value:
        raise ValueError(f"{name} must replace the local development placeholder in production")
    if len(normalized.encode("utf-8")) < _PRODUCTION_CREDENTIAL_MIN_BYTES:
        raise ValueError(
            f"{name} must contain at least {_PRODUCTION_CREDENTIAL_MIN_BYTES} bytes in production"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
