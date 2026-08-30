import re

from httpx import ASGITransport, AsyncClient
from starlette.middleware.cors import CORSMiddleware

from api.main import app


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


async def test_workbench_index_is_served_as_html() -> None:
    async with _client() as client:
        response = await client.get("/workbench")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Mini AI Cloud Workbench" in response.text
    assert response.headers["cache-control"] == "no-store"
    assert "connect-src 'self';" in response.headers["content-security-policy"]
    assert "connect-src 'self' http: https:;" not in response.headers["content-security-policy"]


async def test_workbench_core_assets_are_served() -> None:
    async with _client() as client:
        css_response = await client.get("/workbench/assets/workbench.css")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert css_response.status_code == 200
    assert css_response.headers["content-type"].startswith("text/css")
    assert js_response.status_code == 200
    assert "javascript" in js_response.headers["content-type"]


async def test_missing_workbench_asset_returns_404() -> None:
    async with _client() as client:
        response = await client.get("/workbench/assets/does-not-exist.js")

    assert response.status_code == 404


async def test_workbench_does_not_break_livez_or_openapi() -> None:
    async with _client() as client:
        livez_response = await client.get("/livez")
        openapi_response = await client.get("/openapi.json")

    assert livez_response.status_code == 200
    assert livez_response.json() == {"status": "ok", "checks": None}
    assert openapi_response.status_code == 200
    paths = openapi_response.json()["paths"]
    assert "/api/v1/tasks" in paths
    assert "/workbench" not in paths


async def test_workbench_contains_no_hard_coded_api_key_or_local_storage() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    combined = f"{index_response.text}\n{js_response.text}"
    assert re.search(r"mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}", combined) is None
    assert "localStorage" not in combined


async def test_workbench_connection_is_locked_to_same_origin() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "API origin (same origin)" in index_response.text
    assert re.search(r'<input[^>]+id="api-base"[^>]+readonly', index_response.text)
    assert "another API Base URL" not in index_response.text
    assert "url.origin !== window.location.origin" in js_response.text
    assert "return window.location.origin;" in js_response.text
    assert "mini_ai_cloud_workbench_base" not in js_response.text
    assert all(middleware.cls is not CORSMiddleware for middleware in app.user_middleware)


async def test_workbench_service_runtime_controls_restore_a_valid_pair() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert re.search(r'<option value="fake" disabled>fake</option>', index_response.text)
    assert "function syncServingRuntime(form)" in js_response.text
    assert 'runtimeType.value = "fake";' in js_response.text
    assert "runtimeType.disabled = true;" in js_response.text
    assert 'if (runtimeType.value === "fake") runtimeType.value = "docker";' in js_response.text
    assert "runtime_type: String(form.elements.runtime_type.value" in js_response.text
