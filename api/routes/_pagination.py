from api.errors import APIError
from api.pagination import CursorKey, TextCursorKey, decode_cursor, decode_text_cursor


def parse_list_cursor(*, cursor: str | None, offset: int) -> CursorKey | None:
    if cursor is not None and offset != 0:
        raise APIError(
            400,
            "INVALID_PAGINATION",
            "cursor and non-zero offset cannot be combined",
        )
    if cursor is None:
        return None
    try:
        return decode_cursor(cursor)
    except ValueError as exc:
        raise APIError(400, "INVALID_CURSOR", "The pagination cursor is invalid") from exc


def parse_text_list_cursor(*, cursor: str | None, offset: int) -> TextCursorKey | None:
    if cursor is not None and offset != 0:
        raise APIError(
            400,
            "INVALID_PAGINATION",
            "cursor and non-zero offset cannot be combined",
        )
    if cursor is None:
        return None
    try:
        return decode_text_cursor(cursor)
    except ValueError as exc:
        raise APIError(400, "INVALID_CURSOR", "The pagination cursor is invalid") from exc
