from pathlib import Path

import pytest
from pydantic import ValidationError

from api.schemas.model_variants import LogicalModelCreate, ModelVariantCreate
from core.enums import AcceleratorKind, AcceleratorVendor
from core.runtime_profiles import RuntimeProfileCatalog, RuntimeProfileCompatibilityError

REPOSITORY_ROOT = Path(__file__).parents[2]
ARTIFACT_DIGEST = "sha256:" + "a" * 64


def _catalog() -> RuntimeProfileCatalog:
    return RuntimeProfileCatalog.from_path(REPOSITORY_ROOT / "runtime_profiles" / "manifest.json")


def _variant_payload(**overrides: object) -> dict[str, object]:
    profile = next(
        item for item in _catalog().manifest.profiles if item.vendor is AcceleratorVendor.NVIDIA
    )
    payload: dict[str, object] = {
        "name": "qwen-small-nvidia-bf16",
        "vendor": "nvidia",
        "kind": "gpu",
        "runtime_profile_id": profile.profile_id,
        "runtime_profile_version": profile.profile_version,
        "runtime_profile_digest": profile.semantic_digest,
        "artifact_source": "modelscope/Qwen/Qwen-small",
        "artifact_revision": "revision-nvidia-1",
        "artifact_digest": ARTIFACT_DIGEST,
        "architecture": "qwen2",
        "dtype": "bfloat16",
        "quantization": None,
        "status": "ready",
    }
    payload.update(overrides)
    return payload


def test_logical_model_and_variant_contracts_are_typed() -> None:
    logical = LogicalModelCreate(name="qwen-small", public_name="Qwen Small")
    variant = ModelVariantCreate.model_validate(_variant_payload())

    assert logical.name == "qwen-small"
    assert variant.vendor is AcceleratorVendor.NVIDIA
    assert variant.kind is AcceleratorKind.GPU
    assert variant.artifact_digest == ARTIFACT_DIGEST


def test_variant_rejects_empty_digest_vendor_kind_mismatch_and_unexplained_degradation() -> None:
    with pytest.raises(ValidationError, match="artifact_digest"):
        ModelVariantCreate.model_validate(_variant_payload(artifact_digest=""))
    with pytest.raises(ValidationError, match="not compatible"):
        ModelVariantCreate.model_validate(_variant_payload(kind="npu"))
    with pytest.raises(ValidationError, match="require status_reason"):
        ModelVariantCreate.model_validate(_variant_payload(status="degraded"))


def test_manifest_catalog_binds_profile_identity_digest_and_vendor() -> None:
    catalog = _catalog()
    variant = ModelVariantCreate.model_validate(_variant_payload())

    profile = catalog.resolve_compatible(
        profile_id=variant.runtime_profile_id,
        profile_version=variant.runtime_profile_version,
        semantic_digest=variant.runtime_profile_digest,
        vendor=variant.vendor,
        kind=variant.kind,
        architecture=variant.architecture,
        dtype=variant.dtype,
    )
    assert profile.vendor is AcceleratorVendor.NVIDIA

    with pytest.raises(RuntimeProfileCompatibilityError, match="digest does not match"):
        catalog.resolve_compatible(
            profile_id=variant.runtime_profile_id,
            profile_version=variant.runtime_profile_version,
            semantic_digest="sha256:" + "f" * 64,
            vendor=variant.vendor,
            kind=variant.kind,
            architecture=variant.architecture,
            dtype=variant.dtype,
        )
    with pytest.raises(RuntimeProfileCompatibilityError, match="vendor/kind"):
        catalog.resolve_compatible(
            profile_id=variant.runtime_profile_id,
            profile_version=variant.runtime_profile_version,
            semantic_digest=variant.runtime_profile_digest,
            vendor=AcceleratorVendor.HUAWEI_ASCEND,
            kind=AcceleratorKind.NPU,
            architecture=variant.architecture,
            dtype=variant.dtype,
        )


def test_nvidia_artifact_is_not_inferred_as_an_ascend_variant() -> None:
    catalog = _catalog()
    nvidia = ModelVariantCreate.model_validate(_variant_payload())
    ascend_profile = next(
        item for item in catalog.manifest.profiles if item.vendor is AcceleratorVendor.HUAWEI_ASCEND
    )
    ascend = ModelVariantCreate.model_validate(
        _variant_payload(
            name="qwen-small-ascend-bf16",
            vendor="huawei-ascend",
            kind="npu",
            runtime_profile_id=ascend_profile.profile_id,
            runtime_profile_version=ascend_profile.profile_version,
            runtime_profile_digest=ascend_profile.semantic_digest,
            artifact_revision="revision-ascend-1",
            artifact_digest="sha256:" + "b" * 64,
        )
    )

    assert nvidia.artifact_digest != ascend.artifact_digest
    assert nvidia.runtime_profile_id != ascend.runtime_profile_id
