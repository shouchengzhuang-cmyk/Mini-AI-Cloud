from fastapi import FastAPI

from api.errors import register_exception_handlers
from api.openapi import install_openapi_contract
from api.routes import artifacts, datasets, services, tasks


def test_openapi_generation_includes_descriptions_examples_and_error_envelope() -> None:
    app = FastAPI(title="contract-test", version="0")
    register_exception_handlers(app)
    app.include_router(tasks.router)
    app.include_router(artifacts.router)
    app.include_router(datasets.router)
    app.include_router(services.router)
    install_openapi_contract(app)

    schema = app.openapi()

    assert "ErrorResponse" in schema["components"]["schemas"]
    assert schema["components"]["securitySchemes"] == {
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
    assert schema["components"]["schemas"]["ErrorResponse"]["required"] == ["error"]
    operations = [
        operation
        for path, item in schema["paths"].items()
        if path.startswith("/api/v1/")
        for method, operation in item.items()
        if method in {"get", "post", "put", "patch", "delete"}
    ]
    assert operations
    for operation in operations:
        assert operation["description"]
        assert operation["security"] == [{"BearerApiKey": []}, {"ApiKeyHeader": []}]
        error = operation["responses"]["500"]["content"]["application/json"]
        assert error["schema"]["$ref"] == "#/components/schemas/ErrorResponse"
        assert error["example"]["error"]["request_id"]
