import pytest
from httpx import ASGITransport, AsyncClient

from scripts.fake_inference import create_app


async def test_fake_inference_health_models_and_non_streaming_completion() -> None:
    app = create_app(model="fake/test-model")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://fake.test") as client:
        health = await client.get("/health")
        models = await client.get("/v1/models")
        chat = await client.post(
            "/v1/chat/completions",
            json={
                "model": "fake/test-model",
                "messages": [{"role": "user", "content": "hello world"}],
            },
        )

    assert health.json() == {"status": "ok", "model": "fake/test-model"}
    assert models.json()["data"][0]["id"] == "fake/test-model"
    assert chat.status_code == 200
    assert chat.json()["object"] == "chat.completion"
    assert chat.json()["choices"][0]["message"]["content"] == ("fake response: hello world")
    assert chat.json()["usage"]["total_tokens"] > 0


async def test_fake_inference_stream_is_openai_compatible_sse() -> None:
    app = create_app(model="fake-model")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://fake.test") as client:
        response = await client.post(
            "/v1/completions",
            json={"model": "fake-model", "prompt": "stream me", "stream": True},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert b'"object":"text_completion"' in response.content
    assert response.content.endswith(b"data: [DONE]\n\n")


async def test_fake_inference_rejects_invalid_payload() -> None:
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://fake.test") as client:
        response = await client.post("/v1/chat/completions", json={"model": "fake-model"})

    assert response.status_code == 400
    assert response.json()["error"]["type"] == "invalid_request_error"


def test_fake_inference_configuration_validation() -> None:
    with pytest.raises(ValueError):
        create_app(model=" ")
    with pytest.raises(ValueError):
        create_app(delay_seconds=-1)
