import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from api.schemas.registry import (
    ImageEvaluationRequest,
    ImagePolicyRuleInput,
    RegisteredModelCreate,
    SecretCreate,
    SecretResponse,
)
from core.image_policy import ImagePolicyAction


def test_secret_requests_hide_plaintext_and_responses_have_no_value_fields() -> None:
    plaintext = "super-secret-value"
    request = SecretCreate(name="DATABASE_PASSWORD", value=plaintext)
    response = SecretResponse(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        name=request.name,
        description=None,
        current_version=1,
        revoked_at=None,
        created_by_user_id=uuid.uuid4(),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    assert plaintext not in repr(request)
    assert "value" not in response.model_dump()
    assert "ciphertext" not in response.model_dump()
    assert "nonce" not in response.model_dump()
    assert "key_id" not in response.model_dump()


def test_image_policy_inputs_reject_latest_and_normalize_rules() -> None:
    with pytest.raises(ValidationError):
        ImageEvaluationRequest(image="python:latest")

    digest = "sha256:" + "A" * 64
    rule = ImagePolicyRuleInput(
        action=ImagePolicyAction.ALLOW,
        registry="GHCR.IO",
        repository_glob="Example/*",
        digest=digest,
    )

    assert rule.registry == "ghcr.io"
    assert rule.repository_glob == "example/*"
    assert rule.digest == digest.casefold()


def test_registered_model_metadata_must_be_small_and_json_serializable() -> None:
    payload = RegisteredModelCreate(
        name="qwen-small",
        provider="huggingface",
        source="Qwen/Qwen2.5-0.5B-Instruct",
        metadata={"format": "safetensors"},
    )
    assert payload.metadata == {"format": "safetensors"}
    assert payload.runtime.value == "vllm"
    assert payload.runtime_defaults.tensor_parallel_size == 1

    with pytest.raises(ValidationError, match="JSON serializable"):
        RegisteredModelCreate(
            name="invalid",
            provider="local",
            source="local/model",
            metadata={"bad": object()},
        )


def test_registered_model_runtime_defaults_are_typed_and_gang_consistent() -> None:
    payload = RegisteredModelCreate(
        name="qwen-tp4",
        provider="huggingface",
        source="Qwen/Qwen2.5-7B-Instruct",
        revision="main",
        runtime="vllm",
        default_gpu_count=4,
        runtime_defaults={
            "gpu_model": "NVIDIA A100",
            "tensor_parallel_size": 4,
            "dtype": "bfloat16",
            "gpu_memory_utilization": 0.85,
            "max_model_len": 8192,
        },
    )

    assert payload.default_gpu_count == 4
    assert payload.runtime_defaults.tensor_parallel_size == 4
    assert payload.runtime_defaults.dtype == "bfloat16"

    with pytest.raises(ValidationError, match="tensor_parallel_size"):
        RegisteredModelCreate(
            name="split-tp",
            provider="huggingface",
            source="org/model",
            default_gpu_count=4,
            runtime_defaults={"tensor_parallel_size": 2},
        )

    with pytest.raises(ValidationError, match="fake registered models"):
        RegisteredModelCreate(
            name="invalid-fake",
            provider="local",
            source="fake/model",
            runtime="fake",
            default_gpu_count=1,
        )
