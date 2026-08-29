import re

from httpx import ASGITransport, AsyncClient

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
