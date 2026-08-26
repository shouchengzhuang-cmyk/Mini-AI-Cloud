import hashlib

import pytest
from pydantic import ValidationError

from api.schemas.artifacts import ArtifactCreate, ArtifactFinalize


def test_artifact_create_normalizes_checksum_and_never_accepts_object_keys() -> None:
    checksum = hashlib.sha256(b"model").hexdigest().upper()
    payload = ArtifactCreate(
        name=" model.bin ",
        content_type="Application/Octet-Stream",
        size_bytes=5,
        sha256=checksum,
    )

    assert payload.name == "model.bin"
    assert payload.content_type == "application/octet-stream"
    assert payload.sha256 == checksum.casefold()

    with pytest.raises(ValidationError):
        ArtifactCreate.model_validate(
            {
                **payload.model_dump(),
                "object_key": "../../user-controlled",
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "../model", "size_bytes": 0, "sha256": "a" * 64},
        {
            "name": "model",
            "content_type": "text/plain; charset=utf-8",
            "size_bytes": 0,
            "sha256": "a" * 64,
        },
        {"name": "model", "size_bytes": True, "sha256": "a" * 64},
        {"name": "model", "size_bytes": 0, "sha256": "not-a-checksum"},
    ],
)
def test_artifact_create_rejects_unsafe_or_ambiguous_metadata(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ArtifactCreate.model_validate(payload)


def test_artifact_finalize_requires_strict_size_and_checksum() -> None:
    checksum = hashlib.sha256(b"").hexdigest()
    assert ArtifactFinalize(size_bytes=0, sha256=checksum).sha256 == checksum
    with pytest.raises(ValidationError):
        ArtifactFinalize(size_bytes=True, sha256=checksum)
