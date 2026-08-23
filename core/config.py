import json
from functools import lru_cache
from typing import Annotated

from pydantic import BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


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


Labels = Annotated[dict[str, str], NoDecode, BeforeValidator(_parse_labels)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    database_url: str = "postgresql+asyncpg://task:task@localhost:5432/task_platform"
    redis_url: str = "redis://localhost:6379/0"

    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    control_plane_enabled: bool = True

    cluster_id: str = Field(
        default="mini-docker-cloud-local",
        min_length=1,
        max_length=63,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$",
    )
    worker_id: str | None = None
    worker_concurrency: int = Field(default=4, ge=1, le=128)
    worker_labels: Labels = Field(default_factory=lambda: {"runtime": "docker", "region": "local"})
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

    cpu_price_per_hour: float = Field(default=0.05, ge=0)
    gpu_price_per_hour: float = Field(default=1.0, ge=0)

    @model_validator(mode="after")
    def validate_distributed_timeouts(self) -> "Settings":
        if self.lease_renew_interval >= self.task_lease_seconds:
            raise ValueError("LEASE_RENEW_INTERVAL must be less than TASK_LEASE_SECONDS")
        if self.heartbeat_interval >= self.worker_offline_timeout:
            raise ValueError("HEARTBEAT_INTERVAL must be less than WORKER_OFFLINE_TIMEOUT")
        if self.default_task_timeout > self.max_task_timeout:
            raise ValueError("DEFAULT_TASK_TIMEOUT must not exceed MAX_TASK_TIMEOUT")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
