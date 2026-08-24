import asyncio
import base64
import hashlib
import hmac
import os
import re
import tempfile
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from core.config import Settings

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_OBJECT_KEY_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_STREAM_CHUNK_BYTES = 1024 * 1024
_SPOOL_MEMORY_BYTES = 8 * 1024 * 1024


class ArtifactState(StrEnum):
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"
    DELETING = "deleting"
    DELETED = "deleted"


class ArtifactStoreError(RuntimeError):
    """An object store operation failed without exposing backend credentials."""


class InvalidArtifactKeyError(ValueError):
    pass


class ArtifactObjectNotFoundError(ArtifactStoreError):
    pass


class ArtifactObjectChangedError(ArtifactStoreError):
    pass


class ArtifactTooLargeError(ArtifactStoreError):
    def __init__(self, size_bytes: int, maximum_bytes: int) -> None:
        super().__init__(
            f"artifact size {size_bytes} exceeds the configured maximum of {maximum_bytes} bytes"
        )
        self.size_bytes = size_bytes
        self.maximum_bytes = maximum_bytes


class ArtifactIntegrityError(ArtifactStoreError):
    pass


@dataclass(frozen=True, slots=True)
class ArtifactObjectInfo:
    size_bytes: int
    sha256: str
    content_type: str | None = None
    version_token: str | None = None


@dataclass(frozen=True, slots=True)
class SignedArtifactURL:
    method: str
    url: str
    headers: dict[str, str]
    expires_at: datetime


@runtime_checkable
class ArtifactStore(Protocol):
    backend: str
    max_bytes: int

    async def put(
        self,
        object_key: str,
        chunks: AsyncIterable[bytes],
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo: ...

    async def inspect(self, object_key: str) -> ArtifactObjectInfo: ...

    async def finalize(
        self,
        staging_key: str,
        final_key: str,
        *,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo: ...

    def read(self, object_key: str) -> AsyncIterator[bytes]: ...

    async def delete(self, object_key: str) -> None: ...

    async def signed_upload_url(
        self,
        object_key: str,
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None: ...

    async def signed_download_url(
        self,
        object_key: str,
        *,
        download_name: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None: ...


class LocalArtifactStore:
    """Filesystem-backed store with bounded streaming writes and atomic promotion."""

    backend = "local"

    def __init__(self, root: str | Path, *, max_bytes: int) -> None:
        if max_bytes < 0:
            raise ValueError("max_bytes must not be negative")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        # Serializing local mutations closes the inspect/rename race inside one
        # API process. The staging/final split also keeps expired upload grants
        # from overwriting an already-finalized object.
        self._mutation_lock = asyncio.Lock()

    async def put(
        self,
        object_key: str,
        chunks: AsyncIterable[bytes],
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo:
        expected_sha256 = normalize_sha256(expected_sha256)
        _check_expected_size(expected_size_bytes, self.max_bytes)
        destination = self._object_path(object_key)
        async with self._mutation_lock:
            await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
            temporary = await asyncio.to_thread(
                tempfile.NamedTemporaryFile,
                mode="w+b",
                prefix=".artifact-upload-",
                dir=destination.parent,
                delete=False,
            )
            temporary_path = Path(temporary.name)
            size_bytes = 0
            digest = hashlib.sha256()
            try:
                async for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("artifact chunks must be bytes")
                    if not chunk:
                        continue
                    size_bytes += len(chunk)
                    if size_bytes > self.max_bytes:
                        raise ArtifactTooLargeError(size_bytes, self.max_bytes)
                    if size_bytes > expected_size_bytes:
                        raise ArtifactIntegrityError(
                            "artifact upload exceeded its declared size: "
                            f"expected {expected_size_bytes} bytes"
                        )
                    digest.update(chunk)
                    await asyncio.to_thread(temporary.write, chunk)
                await asyncio.to_thread(temporary.flush)
                await asyncio.to_thread(os.fsync, temporary.fileno())
                await asyncio.to_thread(temporary.close)
                info = ArtifactObjectInfo(
                    size_bytes=size_bytes,
                    sha256=digest.hexdigest(),
                    content_type=content_type,
                )
                _verify_object(info, expected_size_bytes, expected_sha256)
                await asyncio.to_thread(os.replace, temporary_path, destination)
                return info
            except BaseException:
                await asyncio.to_thread(temporary.close)
                await asyncio.to_thread(temporary_path.unlink, missing_ok=True)
                raise

    async def inspect(self, object_key: str) -> ArtifactObjectInfo:
        path = self._object_path(object_key)
        try:
            info = await asyncio.to_thread(_inspect_local_file, path, self.max_bytes)
        except FileNotFoundError as exc:
            raise ArtifactObjectNotFoundError(f"artifact object not found: {object_key}") from exc
        return info

    async def finalize(
        self,
        staging_key: str,
        final_key: str,
        *,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo:
        expected_sha256 = normalize_sha256(expected_sha256)
        _check_expected_size(expected_size_bytes, self.max_bytes)
        staging_path = self._object_path(staging_key)
        final_path = self._object_path(final_key)
        async with self._mutation_lock:
            return await asyncio.to_thread(
                _finalize_local_file,
                staging_path,
                final_path,
                expected_size_bytes,
                expected_sha256,
                self.max_bytes,
                staging_key,
            )

    async def read(self, object_key: str) -> AsyncIterator[bytes]:
        path = self._object_path(object_key)
        try:
            source = await asyncio.to_thread(path.open, "rb")
        except FileNotFoundError as exc:
            raise ArtifactObjectNotFoundError(f"artifact object not found: {object_key}") from exc
        total = 0
        try:
            while True:
                chunk = await asyncio.to_thread(source.read, _STREAM_CHUNK_BYTES)
                if not chunk:
                    return
                total += len(chunk)
                if total > self.max_bytes:
                    raise ArtifactTooLargeError(total, self.max_bytes)
                yield chunk
        finally:
            await asyncio.to_thread(source.close)

    async def delete(self, object_key: str) -> None:
        path = self._object_path(object_key)
        async with self._mutation_lock:
            await asyncio.to_thread(path.unlink, missing_ok=True)

    async def signed_upload_url(
        self,
        object_key: str,
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None:
        self._object_path(object_key)
        return None

    async def signed_download_url(
        self,
        object_key: str,
        *,
        download_name: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None:
        self._object_path(object_key)
        return None

    def _object_path(self, object_key: str) -> Path:
        parts = validate_object_key(object_key)
        candidate = self.root.joinpath(*parts).resolve(strict=False)
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise InvalidArtifactKeyError("artifact object key escapes the storage root") from exc
        return candidate


class S3ArtifactStore:
    """S3/MinIO store; every blocking SDK or response-body operation runs in a thread."""

    backend = "s3"

    def __init__(
        self,
        *,
        bucket: str,
        max_bytes: int,
        endpoint_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        if not bucket.strip():
            raise ValueError("bucket must not be blank")
        if max_bytes < 0:
            raise ValueError("max_bytes must not be negative")
        self.bucket = bucket.strip()
        self.max_bytes = max_bytes
        self.client = client or boto3.client("s3", endpoint_url=endpoint_url)

    async def put(
        self,
        object_key: str,
        chunks: AsyncIterable[bytes],
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo:
        key = canonical_object_key(object_key)
        expected_sha256 = normalize_sha256(expected_sha256)
        _check_expected_size(expected_size_bytes, self.max_bytes)
        spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_BYTES, mode="w+b")
        size_bytes = 0
        digest = hashlib.sha256()
        try:
            async for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise TypeError("artifact chunks must be bytes")
                if not chunk:
                    continue
                size_bytes += len(chunk)
                if size_bytes > self.max_bytes:
                    raise ArtifactTooLargeError(size_bytes, self.max_bytes)
                if size_bytes > expected_size_bytes:
                    raise ArtifactIntegrityError(
                        "artifact upload exceeded its declared size: "
                        f"expected {expected_size_bytes} bytes"
                    )
                digest.update(chunk)
                await asyncio.to_thread(spool.write, chunk)
            info = ArtifactObjectInfo(
                size_bytes=size_bytes,
                sha256=digest.hexdigest(),
                content_type=content_type,
            )
            _verify_object(info, expected_size_bytes, expected_sha256)
            await asyncio.to_thread(spool.seek, 0)
            try:
                await asyncio.to_thread(
                    self.client.upload_fileobj,
                    spool,
                    self.bucket,
                    key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "Metadata": {"sha256": expected_sha256},
                        "ChecksumAlgorithm": "SHA256",
                    },
                )
            except (BotoCoreError, ClientError) as exc:
                raise _s3_error(exc, key) from exc
            return info
        finally:
            await asyncio.to_thread(spool.close)

    async def inspect(self, object_key: str) -> ArtifactObjectInfo:
        key = canonical_object_key(object_key)
        try:
            head = await asyncio.to_thread(
                self.client.head_object,
                Bucket=self.bucket,
                Key=key,
            )
            declared_size = int(head.get("ContentLength", -1))
            if declared_size < 0:
                raise ArtifactStoreError("S3 response omitted ContentLength")
            if declared_size > self.max_bytes:
                raise ArtifactTooLargeError(declared_size, self.max_bytes)
            etag_value = head.get("ETag")
            etag = str(etag_value) if etag_value else None
            request: dict[str, object] = {"Bucket": self.bucket, "Key": key}
            if etag is not None:
                request["IfMatch"] = etag
            response = await asyncio.to_thread(self.client.get_object, **request)
            body = response["Body"]
            digest = hashlib.sha256()
            actual_size = 0
            try:
                while True:
                    chunk = await asyncio.to_thread(body.read, _STREAM_CHUNK_BYTES)
                    if not chunk:
                        break
                    actual_size += len(chunk)
                    if actual_size > self.max_bytes:
                        raise ArtifactTooLargeError(actual_size, self.max_bytes)
                    digest.update(chunk)
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    await asyncio.to_thread(close)
            if actual_size != declared_size:
                raise ArtifactIntegrityError(
                    "S3 object size changed while checksum verification was in progress"
                )
            return ArtifactObjectInfo(
                size_bytes=actual_size,
                sha256=digest.hexdigest(),
                content_type=_optional_string(head.get("ContentType")),
                version_token=etag,
            )
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc

    async def finalize(
        self,
        staging_key: str,
        final_key: str,
        *,
        expected_size_bytes: int,
        expected_sha256: str,
    ) -> ArtifactObjectInfo:
        source = canonical_object_key(staging_key)
        destination = canonical_object_key(final_key)
        expected_sha256 = normalize_sha256(expected_sha256)
        _check_expected_size(expected_size_bytes, self.max_bytes)

        try:
            existing = await self.inspect(destination)
        except ArtifactObjectNotFoundError:
            existing = None
        if existing is not None:
            _verify_object(existing, expected_size_bytes, expected_sha256)
            return existing

        source_info = await self.inspect(source)
        _verify_object(source_info, expected_size_bytes, expected_sha256)
        request: dict[str, object] = {
            "Bucket": self.bucket,
            "Key": destination,
            "CopySource": {"Bucket": self.bucket, "Key": source},
            "MetadataDirective": "COPY",
        }
        if source_info.version_token is not None:
            request["CopySourceIfMatch"] = source_info.version_token
        try:
            await asyncio.to_thread(self.client.copy_object, **request)
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, source) from exc
        final_info = await self.inspect(destination)
        _verify_object(final_info, expected_size_bytes, expected_sha256)
        await self.delete(source)
        return final_info

    async def read(self, object_key: str) -> AsyncIterator[bytes]:
        key = canonical_object_key(object_key)
        try:
            response = await asyncio.to_thread(
                self.client.get_object,
                Bucket=self.bucket,
                Key=key,
            )
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc
        body = response["Body"]
        total = 0
        try:
            while True:
                chunk = await asyncio.to_thread(body.read, _STREAM_CHUNK_BYTES)
                if not chunk:
                    return
                total += len(chunk)
                if total > self.max_bytes:
                    raise ArtifactTooLargeError(total, self.max_bytes)
                yield bytes(chunk)
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                await asyncio.to_thread(close)

    async def delete(self, object_key: str) -> None:
        key = canonical_object_key(object_key)
        try:
            await asyncio.to_thread(
                self.client.delete_object,
                Bucket=self.bucket,
                Key=key,
            )
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc

    async def signed_upload_url(
        self,
        object_key: str,
        *,
        content_type: str,
        expected_size_bytes: int,
        expected_sha256: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None:
        key = canonical_object_key(object_key)
        expected_sha256 = normalize_sha256(expected_sha256)
        _check_expected_size(expected_size_bytes, self.max_bytes)
        checksum = base64.b64encode(bytes.fromhex(expected_sha256)).decode("ascii")
        params = {
            "Bucket": self.bucket,
            "Key": key,
            "ContentType": content_type,
            "ContentLength": expected_size_bytes,
            "ChecksumSHA256": checksum,
        }
        try:
            url = await asyncio.to_thread(
                self.client.generate_presigned_url,
                "put_object",
                Params=params,
                ExpiresIn=expires_seconds,
                HttpMethod="PUT",
            )
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc
        return SignedArtifactURL(
            method="PUT",
            url=str(url),
            headers={
                "Content-Type": content_type,
                "Content-Length": str(expected_size_bytes),
                "x-amz-checksum-sha256": checksum,
            },
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_seconds),
        )

    async def signed_download_url(
        self,
        object_key: str,
        *,
        download_name: str,
        expires_seconds: int,
    ) -> SignedArtifactURL | None:
        key = canonical_object_key(object_key)
        params = {
            "Bucket": self.bucket,
            "Key": key,
            "ResponseContentDisposition": artifact_content_disposition(download_name),
        }
        try:
            url = await asyncio.to_thread(
                self.client.generate_presigned_url,
                "get_object",
                Params=params,
                ExpiresIn=expires_seconds,
                HttpMethod="GET",
            )
        except (BotoCoreError, ClientError) as exc:
            raise _s3_error(exc, key) from exc
        return SignedArtifactURL(
            method="GET",
            url=str(url),
            headers={},
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_seconds),
        )


def build_artifact_store(settings: Settings) -> ArtifactStore:
    if settings.artifact_backend == "local":
        return LocalArtifactStore(
            settings.artifact_local_root,
            max_bytes=settings.artifact_max_bytes,
        )
    return S3ArtifactStore(
        bucket=settings.artifact_s3_bucket,
        endpoint_url=settings.artifact_s3_endpoint_url,
        max_bytes=settings.artifact_max_bytes,
    )


def normalize_sha256(value: str) -> str:
    normalized = value.strip().casefold()
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError("sha256 must contain exactly 64 hexadecimal characters")
    return normalized


def validate_object_key(object_key: str) -> tuple[str, ...]:
    if (
        not object_key
        or object_key != object_key.strip()
        or "\\" in object_key
        or "\x00" in object_key
        or object_key.startswith("/")
    ):
        raise InvalidArtifactKeyError("artifact object key is not canonical")
    path = PurePosixPath(object_key)
    parts = path.parts
    if (
        not parts
        or any(part in {"", ".", ".."} for part in parts)
        or any(not _OBJECT_KEY_SEGMENT.fullmatch(part) for part in parts)
        or "/".join(parts) != object_key
    ):
        raise InvalidArtifactKeyError("artifact object key contains unsafe path segments")
    return parts


def canonical_object_key(object_key: str) -> str:
    return "/".join(validate_object_key(object_key))


def artifact_content_disposition(download_name: str) -> str:
    normalized = download_name.strip()
    if (
        not normalized
        or normalized in {".", ".."}
        or any(character in normalized for character in ("\\", "/", "\r", "\n", "\x00"))
        or any(ord(character) < 32 for character in normalized)
    ):
        raise ValueError("download name contains unsafe characters")
    fallback = "".join(
        character if 32 <= ord(character) < 127 and character not in {'"', "\\"} else "_"
        for character in normalized
    )
    encoded = quote(normalized, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


def _check_expected_size(size_bytes: int, maximum_bytes: int) -> None:
    if size_bytes < 0:
        raise ValueError("expected_size_bytes must not be negative")
    if size_bytes > maximum_bytes:
        raise ArtifactTooLargeError(size_bytes, maximum_bytes)


def _verify_object(
    info: ArtifactObjectInfo,
    expected_size_bytes: int,
    expected_sha256: str,
) -> None:
    if info.size_bytes != expected_size_bytes:
        raise ArtifactIntegrityError(
            f"artifact size mismatch: expected {expected_size_bytes}, got {info.size_bytes}"
        )
    if not hmac.compare_digest(info.sha256, expected_sha256):
        raise ArtifactIntegrityError("artifact SHA-256 checksum mismatch")


def _inspect_local_file(path: Path, maximum_bytes: int) -> ArtifactObjectInfo:
    before = path.stat()
    if before.st_size > maximum_bytes:
        raise ArtifactTooLargeError(before.st_size, maximum_bytes)
    digest = hashlib.sha256()
    size_bytes = 0
    with path.open("rb") as source:
        while chunk := source.read(_STREAM_CHUNK_BYTES):
            size_bytes += len(chunk)
            if size_bytes > maximum_bytes:
                raise ArtifactTooLargeError(size_bytes, maximum_bytes)
            digest.update(chunk)
    after = path.stat()
    before_token = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_token = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_token != after_token or size_bytes != before.st_size:
        raise ArtifactObjectChangedError("artifact object changed during checksum verification")
    return ArtifactObjectInfo(
        size_bytes=size_bytes,
        sha256=digest.hexdigest(),
        version_token=":".join(str(value) for value in after_token),
    )


def _finalize_local_file(
    staging_path: Path,
    final_path: Path,
    expected_size_bytes: int,
    expected_sha256: str,
    maximum_bytes: int,
    staging_key: str,
) -> ArtifactObjectInfo:
    if final_path.is_file():
        info = _inspect_local_file(final_path, maximum_bytes)
        _verify_object(info, expected_size_bytes, expected_sha256)
        return info
    try:
        info = _inspect_local_file(staging_path, maximum_bytes)
    except FileNotFoundError as exc:
        raise ArtifactObjectNotFoundError(f"artifact object not found: {staging_key}") from exc
    _verify_object(info, expected_size_bytes, expected_sha256)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging_path, final_path)
    return info


def _s3_error(exc: BotoCoreError | ClientError, object_key: str) -> ArtifactStoreError:
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"NoSuchKey", "NotFound", "404"} or status == 404:
            return ArtifactObjectNotFoundError(f"artifact object not found: {object_key}")
        if code in {"PreconditionFailed", "412"} or status == 412:
            return ArtifactObjectChangedError(
                f"artifact object changed during finalization: {object_key}"
            )
    return ArtifactStoreError(f"S3 artifact operation failed for object {object_key}")


def _optional_string(value: object) -> str | None:
    return str(value) if value is not None else None
