from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


def create_app(*, model: str = "fake-model", delay_seconds: float = 0.0) -> FastAPI:
    if not model.strip():
        raise ValueError("model must not be blank")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")
    app = FastAPI(title="Mini AI Cloud Fake Inference", version="1.0")
    started_at = int(time.time())

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "model": model}

    @app.get("/v1/models")
    async def models() -> dict[str, object]:
        return {
            "object": "list",
            "data": [
                {
                    "id": model,
                    "object": "model",
                    "created": started_at,
                    "owned_by": "mini-ai-cloud-fake",
                }
            ],
        }

    @app.post("/v1/chat/completions", response_model=None)
    async def chat_completions(request: Request) -> Response:
        payload = await _json_payload(request)
        if isinstance(payload, JSONResponse):
            return payload
        requested_model = payload.get("model")
        messages = payload.get("messages")
        if not isinstance(requested_model, str) or not isinstance(messages, list):
            return _error("model must be a string and messages must be an array")
        prompt = _last_message(messages)
        content = f"fake response: {prompt}"
        request_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())
        if payload.get("stream") is True:
            return StreamingResponse(
                _chat_stream(
                    request_id=request_id,
                    created=created,
                    model=requested_model,
                    content=content,
                    usage=_usage(prompt, content),
                    delay_seconds=delay_seconds,
                ),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        return JSONResponse(
            {
                "id": request_id,
                "object": "chat.completion",
                "created": created,
                "model": requested_model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
                "usage": _usage(prompt, content),
            }
        )

    @app.post("/v1/completions", response_model=None)
    async def completions(request: Request) -> Response:
        payload = await _json_payload(request)
        if isinstance(payload, JSONResponse):
            return payload
        requested_model = payload.get("model")
        prompt_value = payload.get("prompt")
        if not isinstance(requested_model, str) or not isinstance(prompt_value, str):
            return _error("model and prompt must be strings")
        content = f"fake completion: {prompt_value}"
        request_id = f"cmpl-{uuid.uuid4().hex}"
        created = int(time.time())
        if payload.get("stream") is True:
            return StreamingResponse(
                _completion_stream(
                    request_id=request_id,
                    created=created,
                    model=requested_model,
                    content=content,
                    usage=_usage(prompt_value, content),
                    delay_seconds=delay_seconds,
                ),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        return JSONResponse(
            {
                "id": request_id,
                "object": "text_completion",
                "created": created,
                "model": requested_model,
                "choices": [{"index": 0, "text": content, "finish_reason": "stop"}],
                "usage": _usage(prompt_value, content),
            }
        )

    return app


async def _json_payload(request: Request) -> dict[str, Any] | JSONResponse:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error("request body must be valid JSON")
    if not isinstance(payload, dict):
        return _error("request body must be a JSON object")
    return payload


def _last_message(messages: list[object]) -> str:
    for message in reversed(messages):
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
    return ""


async def _chat_stream(
    *,
    request_id: str,
    created: int,
    model: str,
    content: str,
    usage: dict[str, int],
    delay_seconds: float,
) -> AsyncIterator[bytes]:
    yield _sse(
        {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        }
    )
    for piece in _pieces(content):
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        yield _sse(
            {
                "id": request_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
            }
        )
    yield _sse(
        {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
    )
    yield _sse(
        {
            "id": request_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
            "usage": usage,
        }
    )
    yield b"data: [DONE]\n\n"


async def _completion_stream(
    *,
    request_id: str,
    created: int,
    model: str,
    content: str,
    usage: dict[str, int],
    delay_seconds: float,
) -> AsyncIterator[bytes]:
    for piece in _pieces(content):
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        yield _sse(
            {
                "id": request_id,
                "object": "text_completion",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "text": piece, "finish_reason": None}],
            }
        )
    yield _sse(
        {
            "id": request_id,
            "object": "text_completion",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "text": "", "finish_reason": "stop"}],
        }
    )
    yield _sse(
        {
            "id": request_id,
            "object": "text_completion",
            "created": created,
            "model": model,
            "choices": [],
            "usage": usage,
        }
    )
    yield b"data: [DONE]\n\n"


def _pieces(content: str, *, size: int = 16) -> list[str]:
    return [content[index : index + size] for index in range(0, len(content), size)] or [""]


def _sse(payload: dict[str, object]) -> bytes:
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


def _usage(prompt: str, completion: str) -> dict[str, int]:
    prompt_tokens = max(1, len(prompt.split()))
    completion_tokens = max(1, len(completion.split()))
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }


def _error(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "param": None,
                "code": "invalid_request",
            }
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a lightweight OpenAI-compatible server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--model", default="fake-model")
    parser.add_argument("--replica-id")
    parser.add_argument("--execution-id")
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    args = parser.parse_args()
    uvicorn.run(
        create_app(model=args.model, delay_seconds=args.delay_seconds),
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
