from typing import Any

import pytest
from pydantic import ValidationError

from api.schemas.tasks import TaskCreate


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "image": "python:3.12-slim",
        "command": ["python", "-c", "print('ok')"],
        "environment": {"PRESERVE": "  exact value  "},
        "timeout_seconds": 60,
        "max_retries": 1,
        "cpu_limit": 1.0,
        "memory_limit_mb": 256,
    }
    payload.update(overrides)
    return payload


def test_task_create_preserves_argv_and_environment_values() -> None:
    request = TaskCreate.model_validate(_payload(command=["python", "-c", "print(' spaced ')", ""]))

    assert request.command[-1] == ""
    assert request.environment["PRESERVE"] == "  exact value  "
    assert request.network_enabled is False
    assert request.gpu_count == 0


@pytest.mark.parametrize("field", ["privileged", "volumes", "network_mode", "devices"])
def test_task_create_rejects_arbitrary_docker_options(field: str) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TaskCreate.model_validate(_payload(**{field: True}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout_seconds", "60"),
        ("max_retries", 1.0),
        ("network_enabled", "false"),
        ("gpu_count", "1"),
    ],
)
def test_task_create_rejects_coerced_scalar_types(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        TaskCreate.model_validate(_payload(**{field: value}))


@pytest.mark.parametrize(
    "command",
    [[], ["   "], ["echo", "bad\x00argument"], ["echo", "x" * 8193]],
)
def test_task_create_rejects_invalid_command(command: list[str]) -> None:
    with pytest.raises(ValidationError):
        TaskCreate.model_validate(_payload(command=command))


@pytest.mark.parametrize(
    "environment",
    [{"1INVALID": "value"}, {"VALID": "bad\x00value"}],
)
def test_task_create_rejects_invalid_environment(environment: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        TaskCreate.model_validate(_payload(environment=environment))


def test_task_create_rejects_image_with_whitespace() -> None:
    with pytest.raises(ValidationError, match="must not contain whitespace"):
        TaskCreate.model_validate(_payload(image="python :3.12-slim"))
