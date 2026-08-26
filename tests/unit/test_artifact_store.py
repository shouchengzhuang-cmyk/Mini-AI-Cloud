import hashlib
import io
import threading
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from core.artifacts import (
    ArtifactIntegrityError,
    ArtifactTooLargeError,
    InvalidArtifactKeyError,
    LocalArtifactStore,
    S3ArtifactStore,
)


async def _chunks(value: bytes, chunk_size: int = 3) -> AsyncIterator[bytes]:
    for offset in range(0, len(value), chunk_size):
        yield value[offset : offset + chunk_size]


async def test_local_store_streams_verifies_promotes_and_reads(tmp_path: Path) -> None:
    content = b"artifact-content"
    checksum = hashlib.sha256(content).hexdigest()
    store = LocalArtifactStore(tmp_path / "objects", max_bytes=1024)
    staging = "projects/a/artifacts/b/staging"
    final = "projects/a/artifacts/b/content"

    uploaded = await store.put(
        staging,
        _chunks(content),
        content_type="application/octet-stream",
        expected_size_bytes=len(content),
        expected_sha256=checksum,
    )
    finalized = await store.finalize(
        staging,
        final,
        expected_size_bytes=len(content),
        expected_sha256=checksum,
    )
    downloaded = b"".join([chunk async for chunk in store.read(final)])

    assert uploaded.sha256 == checksum
    assert finalized.size_bytes == len(content)
    assert downloaded == content
    assert not (tmp_path / "objects/projects/a/artifacts/b/staging").exists()

    # A stale upload grant can only recreate staging; it cannot replace final.
    await store.put(
        staging,
        _chunks(b"stale"),
        content_type="application/octet-stream",
        expected_size_bytes=5,
        expected_sha256=hashlib.sha256(b"stale").hexdigest(),
    )
    recovered = await store.finalize(
        staging,
        final,
        expected_size_bytes=len(content),
        expected_sha256=checksum,
    )
    assert recovered.sha256 == checksum
    assert b"".join([chunk async for chunk in store.read(final)]) == content


@pytest.mark.parametrize(
    "object_key",
    ["../escape", "/absolute", "a/../../escape", r"a\..\escape", "a//b", " a/b"],
)
async def test_local_store_rejects_path_traversal(tmp_path: Path, object_key: str) -> None:
    store = LocalArtifactStore(tmp_path / "objects", max_bytes=1024)

    with pytest.raises(InvalidArtifactKeyError):
        await store.delete(object_key)


async def test_local_store_removes_partial_file_after_limit_or_checksum_failure(
    tmp_path: Path,
) -> None:
    root = tmp_path / "objects"
    store = LocalArtifactStore(root, max_bytes=4)
    key = "projects/a/artifacts/b/staging"

    with pytest.raises(ArtifactTooLargeError):
        await store.put(
            key,
            _chunks(b"12345"),
            content_type="application/octet-stream",
            expected_size_bytes=4,
            expected_sha256=hashlib.sha256(b"1234").hexdigest(),
        )
    assert not (root / key).exists()

    with pytest.raises(ArtifactIntegrityError):
        await store.put(
            key,
            _chunks(b"1234"),
            content_type="application/octet-stream",
            expected_size_bytes=4,
            expected_sha256=hashlib.sha256(b"xxxx").hexdigest(),
        )
    assert not (root / key).exists()


async def test_artifact_stores_stop_streaming_when_declared_size_is_exceeded(
    tmp_path: Path,
) -> None:
    consumed: list[bytes] = []

    async def oversized() -> AsyncIterator[bytes]:
        for chunk in (b"123", b"456", b"must-not-be-read"):
            consumed.append(chunk)
            yield chunk

    key = "projects/a/artifacts/oversized/staging"
    checksum = hashlib.sha256(b"1234").hexdigest()
    local = LocalArtifactStore(tmp_path / "bounded", max_bytes=1024)
    with pytest.raises(ArtifactIntegrityError, match="declared size"):
        await local.put(
            key,
            oversized(),
            content_type="application/octet-stream",
            expected_size_bytes=4,
            expected_sha256=checksum,
        )
    assert consumed == [b"123", b"456"]
    assert not (tmp_path / "bounded" / key).exists()

    consumed.clear()
    client = _FakeS3Client()
    s3 = S3ArtifactStore(bucket="artifacts", max_bytes=1024, client=client)
    with pytest.raises(ArtifactIntegrityError, match="declared size"):
        await s3.put(
            key,
            oversized(),
            content_type="application/octet-stream",
            expected_size_bytes=4,
            expected_sha256=checksum,
        )
    assert consumed == [b"123", b"456"]
    assert key not in client.objects


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.calls: list[tuple[str, dict[str, object], int]] = []

    def upload_fileobj(
        self,
        source: Any,
        bucket: str,
        key: str,
        *,
        ExtraArgs: dict[str, object],
    ) -> None:
        self.calls.append(("upload", {"Bucket": bucket, "Key": key}, threading.get_ident()))
        self.objects[key] = (source.read(), str(ExtraArgs["ContentType"]))

    def head_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("head", kwargs, threading.get_ident()))
        key = str(kwargs["Key"])
        if key not in self.objects:
            raise _client_error("NoSuchKey", 404, "HeadObject")
        content, content_type = self.objects[key]
        return {
            "ContentLength": len(content),
            "ContentType": content_type,
            "ETag": _etag(content),
        }

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(("get", kwargs, threading.get_ident()))
        key = str(kwargs["Key"])
        if key not in self.objects:
            raise _client_error("NoSuchKey", 404, "GetObject")
        content, _content_type = self.objects[key]
        if_match = kwargs.get("IfMatch")
        if if_match is not None and if_match != _etag(content):
            raise _client_error("PreconditionFailed", 412, "GetObject")
        return {"Body": io.BytesIO(content)}

    def copy_object(self, **kwargs: object) -> None:
        self.calls.append(("copy", kwargs, threading.get_ident()))
        source = kwargs["CopySource"]
        assert isinstance(source, dict)
        source_key = str(source["Key"])
        content, content_type = self.objects[source_key]
        if kwargs.get("CopySourceIfMatch") != _etag(content):
            raise _client_error("PreconditionFailed", 412, "CopyObject")
        self.objects[str(kwargs["Key"])] = (content, content_type)

    def delete_object(self, **kwargs: object) -> None:
        self.calls.append(("delete", kwargs, threading.get_ident()))
        self.objects.pop(str(kwargs["Key"]), None)

    def generate_presigned_url(self, operation: str, **kwargs: object) -> str:
        details = {"operation": operation, **kwargs}
        self.calls.append(("presign", details, threading.get_ident()))
        return f"https://objects.test/{operation}"


async def test_s3_store_uses_threaded_sdk_calls_and_checksum_fenced_promotion() -> None:
    client = _FakeS3Client()
    store = S3ArtifactStore(bucket="artifacts", max_bytes=1024, client=client)
    content = b"s3-artifact"
    checksum = hashlib.sha256(content).hexdigest()
    staging = "projects/a/artifacts/b/staging"
    final = "projects/a/artifacts/b/content"
    main_thread = threading.get_ident()

    await store.put(
        staging,
        _chunks(content),
        content_type="application/octet-stream",
        expected_size_bytes=len(content),
        expected_sha256=checksum,
    )
    finalized = await store.finalize(
        staging,
        final,
        expected_size_bytes=len(content),
        expected_sha256=checksum,
    )
    upload_url = await store.signed_upload_url(
        staging,
        content_type="application/octet-stream",
        expected_size_bytes=len(content),
        expected_sha256=checksum,
        expires_seconds=300,
    )
    download_url = await store.signed_download_url(
        final,
        download_name="model.bin",
        expires_seconds=300,
    )
    downloaded = b"".join([chunk async for chunk in store.read(final)])

    assert finalized.sha256 == checksum
    assert staging not in client.objects
    assert client.objects[final][0] == content
    assert downloaded == content
    assert upload_url is not None and upload_url.method == "PUT"
    assert "x-amz-checksum-sha256" in upload_url.headers
    assert download_url is not None and download_url.method == "GET"
    assert all(thread_id != main_thread for _name, _details, thread_id in client.calls)


def _etag(content: bytes) -> str:
    return f'"{hashlib.sha256(content).hexdigest()}"'


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )
