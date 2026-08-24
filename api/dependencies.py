import time
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Annotated

from fastapi import Depends, Request, WebSocket, WebSocketException
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import HTTPConnection

from api.errors import APIError
from core.config import Settings
from core.database import Database
from core.rbac import Permission, Principal, PrincipalKind, require_permission
from core.redis import RedisQueue
from repositories.identity import ApiKeyRepository


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


def get_app_settings(request: Request) -> Settings:
    return request.app.state.settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    database: Database = request.app.state.database
    async with database.session() as session:
        yield session


async def get_principal(request: Request) -> Principal:
    return await authenticate_connection(request)


async def get_websocket_principal(websocket: WebSocket) -> Principal:
    try:
        return await authenticate_connection(websocket)
    except APIError as exc:
        code = 4401 if exc.status_code == 401 else 4403
        raise WebSocketException(code=code, reason=exc.code) from exc


async def authenticate_connection(connection: HTTPConnection) -> Principal:
    settings: Settings = connection.app.state.settings
    authorization = connection.headers.get("Authorization")
    x_api_key = connection.headers.get("X-API-Key")
    if authorization and x_api_key:
        from api.errors import APIError

        raise APIError(400, "MULTIPLE_AUTH_METHODS", "Use one API key authentication header")
    token: str | None = None
    if authorization is not None:
        scheme, separator, credentials = authorization.partition(" ")
        if not separator or scheme.casefold() != "bearer" or not credentials:
            from api.errors import APIError

            raise APIError(
                401,
                "INVALID_AUTHORIZATION",
                "Authorization must use Bearer API key authentication",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = credentials
    elif x_api_key is not None:
        token = x_api_key

    if token is None:
        if not settings.legacy_anonymous_enabled:
            from api.errors import APIError

            raise APIError(
                401,
                "AUTHENTICATION_REQUIRED",
                "An API key is required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        principal = Principal(
            kind=PrincipalKind.LEGACY,
            project_id=uuid.UUID(settings.legacy_project_id),
        )
        connection.state.principal = principal
        return principal

    database: Database = connection.app.state.database
    pepper = settings.api_key_pepper.encode("utf-8")
    async with database.session() as session:
        authenticated = await ApiKeyRepository.authenticate(
            session,
            token,
            resolve_hmac_key=lambda key_id: pepper if key_id == "v1" else None,
        )
    if authenticated is None:
        from api.errors import APIError

        raise APIError(
            401,
            "INVALID_API_KEY",
            "The API key is invalid, expired or revoked",
            headers={"WWW-Authenticate": "Bearer"},
        )
    await _enforce_api_key_rate_limit(connection, authenticated, settings)
    connection.state.principal = authenticated
    return authenticated


async def _enforce_api_key_rate_limit(
    connection: HTTPConnection, principal: Principal, settings: Settings
) -> None:
    if principal.api_key_id is None:
        return
    queue: RedisQueue = connection.app.state.queue
    bucket = int(time.time() // 60)
    key = f"ratelimit:api-key:{principal.api_key_id}:{bucket}"
    try:
        async with queue.client.pipeline(transaction=True) as pipeline:
            pipeline.incr(key)
            pipeline.expire(key, 120)
            results = await pipeline.execute()
        used = int(results[0])
    except RedisError as exc:
        if settings.rate_limit_fail_open:
            return
        from api.errors import APIError

        raise APIError(
            503,
            "RATE_LIMIT_BACKEND_UNAVAILABLE",
            "API key rate limiting is temporarily unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    if used > settings.api_key_rate_limit_per_minute:
        from api.errors import APIError

        raise APIError(
            429,
            "RATE_LIMIT_EXCEEDED",
            "API key request rate exceeded",
            headers={"Retry-After": str(60 - int(time.time()) % 60)},
        )


def require_api_permission(
    permission: Permission,
) -> Callable[[Annotated[Principal, Depends(get_principal)]], Principal]:
    def dependency(principal: Annotated[Principal, Depends(get_principal)]) -> Principal:
        try:
            require_permission(principal, permission)
        except PermissionError as exc:
            from api.errors import APIError

            raise APIError(403, "PERMISSION_DENIED", str(exc)) from exc
        return principal

    return dependency
