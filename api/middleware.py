from __future__ import annotations

import uuid

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api.errors import REQUEST_ID_HEADER, error_response


class RequestBodyLimitMiddleware:
    """Bound request bodies even when Content-Length is absent or untrusted."""

    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if _is_artifact_content_upload(scope):
            # Artifact uploads have their own declared-size, checksum and
            # streaming store limits. Buffering them here would both defeat
            # streaming and incorrectly cap them at the JSON request limit.
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        content_length = headers.get("content-length")
        if content_length is not None:
            try:
                declared_size = int(content_length)
            except ValueError:
                declared_size = self.max_bytes + 1
            if declared_size < 0 or declared_size > self.max_bytes:
                await self._reject(scope, receive, send)
                return

        buffered: list[Message] = []
        total = 0
        more_body = True
        while more_body:
            message = await receive()
            buffered.append(message)
            if message["type"] == "http.disconnect":
                break
            if message["type"] != "http.request":
                continue
            total += len(message.get("body", b""))
            if total > self.max_bytes:
                await self._reject(scope, receive, send)
                return
            more_body = bool(message.get("more_body", False))

        index = 0

        async def replay() -> Message:
            nonlocal index
            if index < len(buffered):
                message = buffered[index]
                index += 1
                return message
            # Once the buffered request body has been replayed, preserve the
            # live ASGI receive channel. StreamingResponse listens here for a
            # future ``http.disconnect``; returning an immediate empty request
            # forever would create a CPU-bound busy loop.
            return await receive()

        await self.app(scope, replay, send)

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        state = scope.get("state")
        state_request_id = state.get("request_id") if isinstance(state, dict) else None
        headers = Headers(scope=scope)
        incoming = headers.get(REQUEST_ID_HEADER)
        request_id = (
            state_request_id
            if isinstance(state_request_id, str) and state_request_id
            else incoming
            if incoming and len(incoming) <= 255
            else str(uuid.uuid4())
        )
        response = error_response(
            status_code=413,
            code="REQUEST_TOO_LARGE",
            message="Request body exceeds the configured limit",
            request_id=request_id,
        )
        await response(scope, receive, send)


def _is_artifact_content_upload(scope: Scope) -> bool:
    if scope.get("method") != "PUT":
        return False
    path = scope.get("path")
    if not isinstance(path, str):
        return False
    parts = path.strip("/").split("/")
    if len(parts) != 5 or parts[:3] != ["api", "v1", "artifacts"] or parts[4] != "content":
        return False
    try:
        uuid.UUID(parts[3])
    except ValueError:
        return False
    return True
