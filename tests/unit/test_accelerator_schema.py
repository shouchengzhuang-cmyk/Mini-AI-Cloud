from typing import Any

import pytest
from pydantic import ValidationError

from api.schemas.accelerators import AcceleratorRequest
from api.schemas.services import ServiceCreate
from api.schemas.tasks import TaskCreate
from core.accelerators import AcceleratorDevice
from core.enums import AcceleratorKind, AcceleratorVendor


def _task_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "image": "python:3.12-slim",
        "command": ["python", "-c", "print('ok')"],
    }
    payload.update(overrides)
    return payload


def _nvidia_request(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "count": 1,
        "memory_mb_per_device": 24_000,
        "allowed_vendors": ["nvidia"],
        "allowed_kinds": ["gpu"],
        "allowed_models": ["NVIDIA A100"],
        "selection_policy": "nvidia-only",
    }
    payload.update(overrides)
    return payload


def _ascend_request(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "count": 1,
        "memory_mb_per_device": 32_000,
        "allowed_vendors": ["huawei-ascend"],
        "allowed_kinds": ["npu"],
        "selection_policy": "ascend-only",
    }
    payload.update(overrides)
    return payload


def test_accelerator_contract_accepts_only_the_m6_vendor_kind_pairs() -> None:
    request = AcceleratorRequest.model_validate(
        {
            "count": 2,
            "memory_mb_per_device": 24_000,
            "allowed_vendors": ["nvidia", "huawei-ascend"],
            "allowed_kinds": ["gpu", "npu"],
            "required_capabilities": ["bf16", "tensor-parallel"],
            "selection_policy": "prefer-ascend",
        }
    )

    assert request.allowed_vendors == [
        AcceleratorVendor.NVIDIA,
        AcceleratorVendor.HUAWEI_ASCEND,
    ]
    assert request.allowed_kinds == [AcceleratorKind.GPU, AcceleratorKind.NPU]

    with pytest.raises(ValidationError, match="does not include the kind"):
        AcceleratorRequest.model_validate(
            {
                "count": 1,
                "allowed_vendors": ["nvidia"],
                "allowed_kinds": ["npu"],
            }
        )
    with pytest.raises(ValidationError):
        AcceleratorRequest.model_validate(
            {
                "count": 1,
                "allowed_vendors": ["cambricon"],
                "allowed_kinds": ["gpu"],
            }
        )


def test_accelerator_contract_rejects_ambiguous_or_empty_constraints() -> None:
    with pytest.raises(ValidationError, match="requires allowed_vendors"):
        AcceleratorRequest.model_validate({"count": 1})
    with pytest.raises(ValidationError, match="constraints require count"):
        AcceleratorRequest.model_validate(
            {
                "count": 0,
                "allowed_vendors": ["nvidia"],
                "allowed_kinds": ["gpu"],
            }
        )
    with pytest.raises(ValidationError, match="nvidia-only"):
        AcceleratorRequest.model_validate(
            {
                "count": 1,
                "allowed_vendors": ["nvidia", "huawei-ascend"],
                "allowed_kinds": ["gpu", "npu"],
                "selection_policy": "nvidia-only",
            }
        )


def test_accelerator_device_enforces_vendor_kind_and_memory_invariants() -> None:
    device = AcceleratorDevice(
        device_id="GPU-123",
        vendor=AcceleratorVendor.NVIDIA,
        kind=AcceleratorKind.GPU,
        model="NVIDIA A100",
        memory_total_mb=40_960,
        memory_free_mb=32_768,
        capabilities=("bf16",),
    )
    assert device.vendor == AcceleratorVendor.NVIDIA

    with pytest.raises(ValueError, match="devices must use kind"):
        AcceleratorDevice(
            device_id="GPU-123",
            vendor=AcceleratorVendor.NVIDIA,
            kind=AcceleratorKind.NPU,
            model="NVIDIA A100",
            memory_total_mb=40_960,
            memory_free_mb=32_768,
        )
    with pytest.raises(ValueError, match="between zero"):
        AcceleratorDevice(
            device_id="NPU-123",
            vendor=AcceleratorVendor.HUAWEI_ASCEND,
            kind=AcceleratorKind.NPU,
            model="Ascend 910B",
            memory_total_mb=32_000,
            memory_free_mb=33_000,
        )
    with pytest.raises(TypeError, match="AcceleratorVendor"):
        AcceleratorDevice(
            device_id="GPU-raw",
            vendor="nvidia",  # type: ignore[arg-type]
            kind=AcceleratorKind.GPU,
            model="NVIDIA A100",
            memory_total_mb=40_960,
            memory_free_mb=32_768,
        )


def test_legacy_gpu_request_maps_to_the_vendor_neutral_contract() -> None:
    request = TaskCreate.model_validate(
        _task_payload(gpu_count=1, gpu_memory_mb=24_000, gpu_model="NVIDIA A100")
    )

    assert request.effective_accelerator == AcceleratorRequest.model_validate(_nvidia_request())
    assert "accelerator" not in request.model_dump(mode="json")


def test_legacy_gpu_model_is_normalized_before_conflict_checks_and_persistence() -> None:
    request = TaskCreate.model_validate(_task_payload(gpu_count=1, gpu_model=" NVIDIA A100 "))

    assert request.gpu_model == "NVIDIA A100"
    assert request.effective_accelerator.allowed_models == ["NVIDIA A100"]


def test_equivalent_new_nvidia_request_uses_the_existing_execution_fields() -> None:
    task = TaskCreate.model_validate(_task_payload(accelerator=_nvidia_request()))
    service = ServiceCreate.model_validate(
        {"name": "nvidia-service", "model": "org/model", "accelerator": _nvidia_request()}
    )

    assert (task.gpu_count, task.gpu_memory_mb, task.gpu_model) == (
        1,
        24_000,
        "NVIDIA A100",
    )
    assert service.gpu_count == 1
    assert service.tensor_parallel_size == 1
    task.require_current_accelerator_execution_support()
    service.require_current_accelerator_execution_support()


def test_consistent_legacy_and_new_fields_are_accepted_but_conflicts_fail_closed() -> None:
    request = TaskCreate.model_validate(
        _task_payload(
            accelerator=_nvidia_request(),
            gpu_count=1,
            gpu_memory_mb=24_000,
            gpu_model="NVIDIA A100",
        )
    )
    assert request.gpu_count == 1

    with pytest.raises(ValidationError, match="conflicts with legacy"):
        TaskCreate.model_validate(_task_payload(accelerator=_nvidia_request(), gpu_count=2))
    with pytest.raises(ValidationError, match="cannot be combined"):
        TaskCreate.model_validate(_task_payload(accelerator=_ascend_request(), gpu_count=1))


def test_ascend_schema_is_available_but_execution_remains_fail_closed_in_a1() -> None:
    request = TaskCreate.model_validate(_task_payload(accelerator=_ascend_request()))

    assert request.effective_accelerator.allowed_vendors == [AcceleratorVendor.HUAWEI_ASCEND]
    assert request.gpu_count == 0
    with pytest.raises(ValueError, match="schema-ready"):
        request.require_current_accelerator_execution_support()


def test_legacy_gpu_details_require_a_positive_count() -> None:
    with pytest.raises(ValidationError, match="require gpu_count greater than zero"):
        TaskCreate.model_validate(_task_payload(gpu_memory_mb=1024))
