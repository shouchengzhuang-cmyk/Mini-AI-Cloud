from collections.abc import Iterable

REDACTION_MARKER = b"***REDACTED***"


class StreamingSecretRedactor:
    """Best-effort exact-value redaction that preserves matches across byte chunks."""

    def __init__(self, values: Iterable[str]) -> None:
        patterns = {value.encode("utf-8") for value in values if value}
        self._patterns = tuple(sorted(patterns, key=lambda item: (-len(item), item)))
        self._maximum_pattern_bytes = max((len(item) for item in self._patterns), default=0)
        self._pending = bytearray()

    def feed(self, content: bytes) -> bytes:
        if not self._patterns:
            return content
        self._pending.extend(content)
        return self._consume(final=False)

    def finish(self) -> bytes:
        if not self._patterns:
            return b""
        return self._consume(final=True)

    def _consume(self, *, final: bool) -> bytes:
        data = bytes(self._pending)
        output = bytearray()
        position = 0
        while position < len(data):
            remaining_bytes = len(data) - position
            matched = next(
                (pattern for pattern in self._patterns if data.startswith(pattern, position)),
                None,
            )
            could_complete = (
                not final
                and remaining_bytes < self._maximum_pattern_bytes
                and any(
                    len(pattern) > remaining_bytes
                    and _prefix_matches(pattern, data, position, remaining_bytes)
                    for pattern in self._patterns
                )
            )
            if could_complete:
                break
            if matched is None:
                output.append(data[position])
                position += 1
            else:
                output.extend(REDACTION_MARKER)
                position += len(matched)
        self._pending[:] = data[position:]
        return bytes(output)


def _prefix_matches(pattern: bytes, data: bytes, position: int, length: int) -> bool:
    return all(pattern[index] == data[position + index] for index in range(length))


def redact_text(value: str, secrets: Iterable[str]) -> str:
    redacted = value
    for secret in sorted({item for item in secrets if item}, key=lambda item: (-len(item), item)):
        redacted = redacted.replace(secret, REDACTION_MARKER.decode("ascii"))
    return redacted
