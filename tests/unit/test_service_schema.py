import uuid

import pytest
from pydantic import ValidationError

from api.schemas.gateway import OpenAIProxyRequest
from api.schemas.services import ServiceCreate, ServiceScale
from core.enums import RuntimeType
from models.service import ServingRuntime


def test_service_create_normalizes_safe_identifiers() -> None:
    payload = ServiceCreate.model_validate(
        {
            "name": " inference-main ",
            "model": " org/model-v1 ",
            "runtime": "vllm",
            "runtime_type": "docker",
            "image": " registry.example/vllm:latest ",
            "gpu_count": 4,
            "gpu_model": " NVIDIA A100 ",
            "replicas": 2,
        }
    )

    assert payload.name == "inference-main"
    assert payload.model == "org/model-v1"
    assert payload.runtime == ServingRuntime.VLLM
    assert payload.runtime_type == RuntimeType.DOCKER
    assert payload.image == "registry.example/vllm:latest"
    assert payload.gpu_model == "NVIDIA A100"
    assert payload.tensor_parallel_size == 4


def test_service_create_accepts_a_project_registered_model_reference() -> None:
    registered_model_id = uuid.uuid4()

    payload = ServiceCreate.model_validate(
        {
            "name": "registry-backed",
            "registered_model_id": str(registered_model_id),
        }
    )

    assert payload.model is None
    assert payload.registered_model_id == registered_model_id
    assert payload.tensor_parallel_size is None


def test_service_create_requires_a_direct_or_registered_model_reference() -> None:
    with pytest.raises(ValidationError, match="model or registered_model_id is required"):
        ServiceCreate.model_validate({"name": "missing-model"})


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "bad name", "model": "org/model"},
        {"name": "good", "model": "org /model"},
        {"name": "good", "model": "org/model", "replicas": True},
        {"name": "good", "model": "org/model", "replicas": 1001},
        {"name": "good", "model": "org/model", "privileged": True},
    ],
)
def test_service_create_rejects_unsafe_or_unknown_input(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ServiceCreate.model_validate(payload)


def test_service_scale_requires_a_bounded_strict_integer() -> None:
    assert ServiceScale(replicas=0).replicas == 0
    with pytest.raises(ValidationError):
        ServiceScale(replicas=True)
    with pytest.raises(ValidationError):
        ServiceScale(replicas=-1)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "name": "bad-vllm-runtime",
            "model": "org/model",
            "runtime": "vllm",
            "runtime_type": "kubernetes",
        },
        {
            "name": "bad-fake-runtime",
            "model": "org/model",
            "runtime": "fake",
            "runtime_type": "docker",
        },
        {
            "name": "partial-gang",
            "model": "org/model",
            "gpu_count": 4,
            "tensor_parallel_size": 2,
        },
        {
            "name": "cpu-parallel",
            "model": "org/model",
            "gpu_count": 0,
            "tensor_parallel_size": 2,
        },
        {
            "name": "unknown-dtype",
            "model": "org/model",
            "dtype": "int8",
        },
    ],
)
def test_service_create_rejects_unsupported_runtime_or_vllm_spec(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ServiceCreate.model_validate(payload)


def test_fake_service_uses_cpu_single_replica_runtime_contract() -> None:
    payload = ServiceCreate.model_validate(
        {
            "name": "fake-serving",
            "model": "fake/model",
            "runtime": "fake",
            "runtime_type": "fake",
        }
    )

    assert payload.gpu_count == 0
    assert payload.tensor_parallel_size == 1


def test_service_autoscaling_bounds_include_initial_desired_replicas() -> None:
    payload = ServiceCreate.model_validate(
        {
            "name": "autoscaled",
            "model": "org/model",
            "replicas": 2,
            "autoscaling": {
                "min_replicas": 1,
                "max_replicas": 4,
                "target_concurrency": 8,
                "cooldown_seconds": 30,
            },
        }
    )
    assert payload.autoscaling is not None
    assert payload.autoscaling.enabled is True
    assert payload.autoscaling.max_replicas == 4

    with pytest.raises(ValidationError, match="within enabled autoscaling"):
        ServiceCreate.model_validate(
            {
                "name": "outside",
                "model": "org/model",
                "replicas": 5,
                "autoscaling": {"min_replicas": 1, "max_replicas": 4},
            }
        )
    with pytest.raises(ValidationError, match="greater than or equal"):
        ServiceCreate.model_validate(
            {
                "name": "bad-bounds",
                "model": "org/model",
                "autoscaling": {"min_replicas": 3, "max_replicas": 2},
            }
        )


def test_openai_gateway_request_allows_passthrough_options_but_keeps_routing_strict() -> None:
    payload = OpenAIProxyRequest.model_validate(
        {
            "model": "chat-main",
            "stream": True,
            "temperature": 0.2,
            "messages": [{"role": "user", "content": "hello"}],
        }
    )
    upstream = payload.upstream_payload(upstream_model="org/model")
    assert upstream["model"] == "org/model"
    assert upstream["temperature"] == 0.2

    with pytest.raises(ValidationError):
        OpenAIProxyRequest.model_validate({"model": "bad model", "stream": False})
    with pytest.raises(ValidationError):
        OpenAIProxyRequest.model_validate({"model": "chat-main", "stream": 1})
