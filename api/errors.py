import uuid
from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.schemas.common import ErrorDetail, ErrorResponse, ValidationIssue
from core.logging import get_logger

REQUEST_ID_HEADER = "X-Request-ID"
LOGGER = get_logger("api_errors")


class APIError(Exception):
    """An expected API failure with a stable machine-readable error code."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        details: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        if not 400 <= status_code <= 599:
            raise ValueError("APIError status_code must be between 400 and 599")
        if not code or not code.strip():
            raise ValueError("APIError code must not be blank")
        if not message or not message.strip():
            raise ValueError("APIError message must not be blank")
        self.status_code = status_code
        self.code = code.strip()
        self.message = message.strip()
        self.details = details
        self.headers = dict(headers or {})


class NotFoundError(APIError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(404, code, message)


class ConflictError(APIError):
    def __init__(self, code: str, message: str, *, details: Any | None = None) -> None:
        super().__init__(409, code, message, details=details)


class ServiceUnavailableError(APIError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(503, code, message)


def request_id_from(request: Request) -> str:
    request_id = getattr(request.state, "request_id", None)
    if isinstance(request_id, str) and request_id:
        return request_id
    header_request_id = request.headers.get(REQUEST_ID_HEADER)
    if header_request_id:
        return header_request_id
    return str(uuid.uuid4())


def error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    request_id: str,
    details: Any | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    response_headers = dict(headers or {})
    response_headers[REQUEST_ID_HEADER] = request_id
    payload = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
            request_id=request_id,
            details=details,
        )
    )
    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(mode="json", exclude_none=True),
        headers=response_headers,
    )


async def api_error_handler(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, APIError):
        return await unhandled_exception_handler(request, exc)
    return error_response(
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        request_id=request_id_from(request),
        details=exc.details,
        headers=exc.headers,
    )


async def request_validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):
        return await unhandled_exception_handler(request, exc)
    issues = [
        ValidationIssue(
            location=[item if isinstance(item, int) else str(item) for item in error["loc"]],
            message=error["msg"],
            type=error["type"],
        ).model_dump(mode="json")
        for error in exc.errors()
    ]
    return error_response(
        status_code=422,
        code="VALIDATION_ERROR",
        message="Request validation failed",
        request_id=request_id_from(request),
        details=issues,
    )


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, StarletteHTTPException):
        return await unhandled_exception_handler(request, exc)
    message = exc.detail if isinstance(exc.detail, str) else "HTTP request failed"
    details = None if isinstance(exc.detail, str) else exc.detail
    return error_response(
        status_code=exc.status_code,
        code="HTTP_ERROR",
        message=message,
        request_id=request_id_from(request),
        details=details,
        headers=exc.headers,
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    LOGGER.error(
        "unhandled API exception",
        request_id=request_id_from(request),
        error=str(exc),
        exc_info=exc,
    )
    return error_response(
        status_code=500,
        code="INTERNAL_SERVER_ERROR",
        message="An unexpected error occurred",
        request_id=request_id_from(request),
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(APIError, api_error_handler)
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
