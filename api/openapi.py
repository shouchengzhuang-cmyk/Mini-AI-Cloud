from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from api.schemas.common import ErrorResponse

_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "options", "head"}
_COMMON_ERRORS = {
    "400": "Malformed or unsupported request",
    "401": "Authentication required",
    "403": "Insufficient permission",
    "404": "Resource not found in the authenticated scope",
    "409": "State or idempotency conflict",
    "413": "Request body exceeds the configured limit",
    "429": "Rate limit exceeded",
    "500": "Unexpected server error",
    "503": "Required backend is unavailable",
}


def install_openapi_contract(app: FastAPI) -> None:
    """Install a deterministic OpenAPI contract including uniform API errors."""

    def build_schema() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=(
                "Project-scoped distributed compute jobs, artifacts, datasets and model "
                "services. API failures use one machine-readable error envelope."
            ),
            routes=app.routes,
        )
        components = schema.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        security_schemes = components.setdefault("securitySchemes", {})
        security_schemes.update(
            {
                "BearerApiKey": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "mkc_ API key",
                    "description": "Project-scoped API key issued once at creation time.",
                },
                "ApiKeyHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key",
                    "description": "Alternative header for a project-scoped API key.",
                },
                "BootstrapToken": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-Bootstrap-Token",
                    "description": "One-time operator token for initial platform bootstrap.",
                },
            }
        )
        error_schema = ErrorResponse.model_json_schema(ref_template="#/components/schemas/{model}")
        schemas.update(error_schema.pop("$defs", {}))
        schemas["ErrorResponse"] = error_schema
        example = {
            "error": {
                "code": "RESOURCE_NOT_FOUND",
                "message": "Resource not found",
                "request_id": "018f6f32-babc-7d00-8000-000000000001",
            }
        }
        for path, path_item in schema.get("paths", {}).items():
            if not isinstance(path_item, dict):
                continue
            for method, operation in path_item.items():
                if method not in _HTTP_METHODS or not isinstance(operation, dict):
                    continue
                operation.setdefault(
                    "description",
                    operation.get("summary") or f"{method.upper()} {path}",
                )
                if path == "/api/v1/bootstrap":
                    operation.setdefault("security", [{"BootstrapToken": []}])
                elif path.startswith(("/api/v1/", "/v1/")):
                    operation.setdefault(
                        "security",
                        [{"BearerApiKey": []}, {"ApiKeyHeader": []}],
                    )
                responses = operation.setdefault("responses", {})
                for status_code, description in _COMMON_ERRORS.items():
                    responses.setdefault(
                        status_code,
                        {
                            "description": description,
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                                    "example": example,
                                }
                            },
                        },
                    )
        app.openapi_schema = schema
        return schema

    app.openapi = build_schema  # type: ignore[method-assign]
