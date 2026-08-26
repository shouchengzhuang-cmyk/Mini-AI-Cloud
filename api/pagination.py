from __future__ import annotations

import base64
import binascii
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class CursorKey:
    created_at: datetime
    item_id: uuid.UUID


@dataclass(frozen=True, slots=True)
class TextCursorKey:
    created_at: datetime
    item_id: str


def encode_cursor(created_at: datetime, item_id: uuid.UUID) -> str:
    return _encode_cursor(created_at, str(item_id))


def encode_text_cursor(created_at: datetime, item_id: str) -> str:
    _validate_text_id(item_id)
    return _encode_cursor(created_at, item_id)


def _encode_cursor(created_at: datetime, item_id: str) -> str:
    normalized = _as_utc(created_at)
    payload = json.dumps(
        {"created_at": normalized.isoformat(), "id": item_id},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(value: str) -> CursorKey:
    created_at, raw_item_id = _decode_cursor(value)
    try:
        item_id = uuid.UUID(raw_item_id)
    except ValueError as exc:
        raise ValueError("cursor is invalid") from exc
    return CursorKey(created_at=created_at, item_id=item_id)


def decode_text_cursor(value: str) -> TextCursorKey:
    created_at, item_id = _decode_cursor(value)
    _validate_text_id(item_id)
    return TextCursorKey(created_at=created_at, item_id=item_id)


def _decode_cursor(value: str) -> tuple[datetime, str]:
    if not value or len(value) > 512:
        raise ValueError("cursor is empty or too long")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        payload = json.loads(decoded)
        if not isinstance(payload, dict) or set(payload) != {"created_at", "id"}:
            raise ValueError("cursor payload has unexpected fields")
        raw_created_at = payload["created_at"]
        raw_item_id = payload["id"]
        if not isinstance(raw_created_at, str) or not isinstance(raw_item_id, str):
            raise ValueError("cursor payload values must be strings")
        created_at = datetime.fromisoformat(raw_created_at)
        if created_at.tzinfo is None:
            raise ValueError("cursor timestamp must include a timezone")
        item_id = raw_item_id
    except (binascii.Error, UnicodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc
    return _as_utc(created_at), item_id


def _validate_text_id(value: str) -> None:
    if (
        not value
        or len(value) > 255
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("cursor item id is invalid")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
