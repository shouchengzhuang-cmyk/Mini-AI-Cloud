import uuid
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


def test_task_create_accepts_only_pinned_secret_references() -> None:
    secret_id = uuid.uuid4()

    request = TaskCreate.model_validate(
        _payload(
            secret_bindings=[
                {"secret_id": str(secret_id), "version": 2, "env_name": "DATABASE_PASSWORD"}
            ]
        )
    )

    assert request.secret_bindings[0].secret_id == secret_id
    assert request.secret_bindings[0].version == 2
    dumped = request.model_dump(mode="json")
    assert dumped["secret_bindings"] == [
        {"secret_id": str(secret_id), "version": 2, "env_name": "DATABASE_PASSWORD"}
    ]


def test_task_create_accepts_structured_retry_policy_and_maps_total_attempts() -> None:
    request = TaskCreate.model_validate(
        _payload(
            max_retries=0,
            retry_policy={
                "max_attempts": 4,
                "backoff": "exponential",
                "base_seconds": 2,
                "max_seconds": 60,
                "retry_on_exit_codes": [1, 137],
            },
        )
    )

    assert request.effective_retry_policy.max_attempts == 4
    assert request.effective_retry_policy.retry_on_exit_codes == [1, 137]


def test_task_create_maps_legacy_max_retries_to_total_attempts() -> None:
    request = TaskCreate.model_validate(_payload(max_retries=3))

    assert request.retry_policy is None
    assert request.effective_retry_policy.max_attempts == 4


@pytest.mark.parametrize(
    "retry_policy",
    [
        {"max_attempts": 0},
        {"max_attempts": 2, "base_seconds": 5.0, "max_seconds": 4.0},
        {"max_attempts": 2, "retry_on_exit_codes": [1, 1]},
        {"max_attempts": 2, "retry_on_exit_codes": [256]},
        {"max_attempts": 2, "backoff": "random"},
    ],
)
def test_task_create_rejects_invalid_retry_policy(retry_policy: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TaskCreate.model_validate(_payload(max_retries=0, retry_policy=retry_policy))


def test_task_create_rejects_conflicting_legacy_and_structured_retry_limits() -> None:
    with pytest.raises(ValidationError, match="max_retries conflicts"):
        TaskCreate.model_validate(_payload(max_retries=2, retry_policy={"max_attempts": 4}))


@pytest.mark.parametrize(
    "secret_bindings",
    [
        [{"secret_id": str(uuid.uuid4()), "version": 0, "env_name": "TOKEN"}],
        [{"secret_id": str(uuid.uuid4()), "version": 1, "env_name": "bad-name"}],
        [
            {"secret_id": str(uuid.uuid4()), "version": 1, "env_name": "TOKEN"},
            {"secret_id": str(uuid.uuid4()), "version": 1, "env_name": "TOKEN"},
        ],
    ],
)
def test_task_create_rejects_invalid_secret_references(
    secret_bindings: list[dict[str, object]],
) -> None:
    with pytest.raises(ValidationError):
        TaskCreate.model_validate(_payload(secret_bindings=secret_bindings))

    with pytest.raises(ValidationError, match="must not overlap"):
        TaskCreate.model_validate(
            _payload(
                environment={"TOKEN": "public"},
                secret_bindings=[
                    {"secret_id": str(uuid.uuid4()), "version": 1, "env_name": "TOKEN"}
                ],
            )
        )
