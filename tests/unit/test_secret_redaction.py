import pytest

from worker.redaction import REDACTION_MARKER, StreamingSecretRedactor, redact_text


def test_streaming_redactor_hides_complete_and_cross_chunk_values() -> None:
    secret = "ABC123XYZ"
    redactor = StreamingSecretRedactor([secret])

    chunks = [b"before ABC", b"123", b"XYZ after ", b"ABC123XYZ"]
    output = b"".join(redactor.feed(chunk) for chunk in chunks) + redactor.finish()

    assert output == b"before " + REDACTION_MARKER + b" after " + REDACTION_MARKER
    assert secret.encode() not in output


def test_streaming_redactor_keeps_stdout_and_stderr_state_independent() -> None:
    stdout = StreamingSecretRedactor(["secret-value"])
    stderr = StreamingSecretRedactor(["secret-value"])

    assert stdout.feed(b"secret-") == b""
    assert stderr.feed(b"unrelated") == b"unrelated"
    assert stdout.feed(b"value") == REDACTION_MARKER
    assert stdout.finish() == b""
    assert stderr.finish() == b""


@pytest.mark.parametrize(
    ("values", "message", "expected"),
    [
        (("token",), "failed with token", "failed with ***REDACTED***"),
        (("abc", "abcdef"), "abcdef/abc", "***REDACTED***/***REDACTED***"),
        ((), "unchanged", "unchanged"),
    ],
)
def test_redact_text_sanitizes_exception_messages(
    values: tuple[str, ...], message: str, expected: str
) -> None:
    assert redact_text(message, values) == expected
