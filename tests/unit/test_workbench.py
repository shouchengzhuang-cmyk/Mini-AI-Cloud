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
        missing_response = await client.get("/workbench/assets/does-not-exist.js")
        index_response = await client.get("/workbench/assets/index.html")

    assert missing_response.status_code == 404
    assert index_response.status_code == 404


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


async def test_workbench_connection_attempts_do_not_clear_newer_credentials() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "connectionAttempt: 0," in js_response.text
    assert "const attempt = ++state.connectionAttempt;" in js_response.text
    assert "if (attempt !== state.connectionAttempt)" in js_response.text
    assert 'error.name = "AbortError";' in js_response.text
    assert 'if (attempt === state.connectionAttempt) state.apiKey = "";' in js_response.text
    assert "submit.disabled = true;" in js_response.text
    assert "submit.disabled = false;" in js_response.text


async def test_workbench_api_key_is_not_a_native_form_control() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert 'id="api-key"' in index_response.text
    assert 'name="api-key"' not in index_response.text
    assert 'const apiKeyInput = query("#api-key");' in js_response.text


async def test_workbench_service_runtime_controls_restore_a_valid_pair() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert re.search(r'<option value="fake" disabled>fake</option>', index_response.text)
    assert "function syncServingRuntime(form)" in js_response.text
    assert 'runtimeType.value = "fake";' in js_response.text
    assert "runtimeType.disabled = true;" in js_response.text
    assert 'if (runtimeType.value === "fake") runtimeType.value = "docker";' in js_response.text
    assert "runtime_type: runtimeType," in js_response.text


async def test_workbench_kubernetes_vllm_requires_logical_model_admission() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert 'runtimeType.value === "kubernetes"' in js_response.text
    assert "model.disabled = kubernetesVllm;" in js_response.text
    assert "logicalModelId.required = kubernetesVllm;" in js_response.text
    assert "logicalModelId.disabled = !kubernetesVllm;" in js_response.text
    assert "modelVariantId.disabled = !kubernetesVllm;" in js_response.text
    assert 'if (!kubernetesVllm) acceleratorVendor.value = "nvidia";' in js_response.text
    assert "acceleratorVendor.disabled = !kubernetesVllm;" in js_response.text
    assert "runtimeProfile.disabled = !kubernetesVllm;" in js_response.text
    assert 'acceleratorCount.min = kubernetesVllm ? "1" : "0";' in js_response.text
    assert 'acceleratorCount.value = "1";' in js_response.text
    assert (
        'throw new Error("Logical model ID is required for Kubernetes vLLM.");' in js_response.text
    )
    assert 'elements.runtime_type.addEventListener("change"' in js_response.text


async def test_workbench_idempotency_key_supports_insecure_http_contexts() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "function createIdempotencyKey()" in js_response.text
    assert 'typeof cryptoApi.randomUUID === "function"' in js_response.text
    assert "cryptoApi.getRandomValues(entropy);" in js_response.text
    assert 'headers: { "Idempotency-Key": createIdempotencyKey() }' in js_response.text
    assert "crypto.randomUUID()" not in js_response.text


async def test_workbench_tensor_parallelism_tracks_accelerator_count() -> None:
    async with _client() as client:
        index_response = await client.get("/workbench")
        js_response = await client.get("/workbench/assets/workbench.js")

    assert re.search(r'<input[^>]+name="tensor_parallel_size"[^>]+readonly', index_response.text)
    assert "function syncTensorParallelSize(form)" in js_response.text
    assert "Math.max(1, acceleratorCount)" in js_response.text
    assert 'acceleratorCount.value = "0";' in js_response.text
    assert "acceleratorCount.disabled = true;" in js_response.text
    assert 'elements.accelerator_count.addEventListener("input"' in js_response.text


async def test_workbench_task_logs_follow_sequence_cursor() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "const TASK_LOG_PAGE_SIZE = 500;" in js_response.text
    assert "const TASK_LOG_MAX_PAGES_PER_REFRESH = 2;" in js_response.text
    assert "const TASK_LOG_RETAIN_LIMIT = 1000;" in js_response.text
    assert "async function fetchTaskLogs(taskId)" in js_response.text
    assert "&offset=${offset}`" in js_response.text
    assert "state.taskLogs.offset = nextOffset;" in js_response.text
    assert "pagesFetched < TASK_LOG_MAX_PAGES_PER_REFRESH" in js_response.text
    assert "state.taskLogs.entries.length - TASK_LOG_RETAIN_LIMIT" in js_response.text
    assert "if (logs.length < TASK_LOG_PAGE_SIZE) break;" in js_response.text


async def test_workbench_visibility_changes_do_not_abort_mutations() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "function abortReadRequests()" in js_response.text
    assert 'if (channel.startsWith("action:")) continue;' in js_response.text
    assert "state.refreshTimer = null;\n        abortReadRequests();" in js_response.text


async def test_workbench_resource_lists_follow_api_cursors() -> None:
    async with _client() as client:
        js_response = await client.get("/workbench/assets/workbench.js")

    assert "const LIST_PAGE_SIZE = 100;" in js_response.text
    assert "function listCursorSuffix(page)" in js_response.text
    assert "function resetListPages()" in js_response.text
    assert "pagination.next_cursor || null" in js_response.text
    assert "page.history.push(page.cursor);" in js_response.text
    assert 'listPagination("tasks"' in js_response.text
    assert 'listPagination("services"' in js_response.text
    assert 'listPagination("workers"' in js_response.text
    assert '? listPagination("tasks", payload.pagination || {}, 0, renderTasks)' in js_response.text
