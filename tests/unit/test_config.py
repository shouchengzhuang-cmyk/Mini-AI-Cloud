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
