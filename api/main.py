import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog.contextvars
import uvicorn
from fastapi import FastAPI, Request, Response
from redis.exceptions import RedisError

from api.errors import REQUEST_ID_HEADER, register_exception_handlers
from api.routes import system, tasks, workers
from api.services.control_plane import ControlPlane
from api.services.outbox import OutboxDispatcher
from api.services.reaper import Reaper
from core.config import Settings, get_settings
from core.database import Database
from core.logging import configure_logging, get_logger
from core.redis import RedisQueue


def create_app(
    *,
    settings: Settings | None = None,
    database: Database | None = None,
    queue: RedisQueue | None = None,
    start_control_plane: bool | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    configure_logging(resolved_settings.log_level)
    logger = get_logger("api")
    resolved_database = database or Database(resolved_settings.database_url)
    resolved_queue = queue or RedisQueue(
        resolved_settings.redis_url,
        log_stream_maxlen=resolved_settings.log_stream_maxlen,
        log_stream_ttl_seconds=resolved_settings.log_stream_ttl_seconds,
        ready_stream_maxlen=resolved_settings.ready_stream_maxlen,
        socket_timeout=resolved_settings.redis_socket_timeout,
    )
    should_start_control = (
        resolved_settings.control_plane_enabled
        if start_control_plane is None
        else start_control_plane
    )
    control = ControlPlane(
        OutboxDispatcher(
            resolved_database, resolved_queue, batch_size=resolved_settings.batch_size
        ),
        Reaper(resolved_database, resolved_settings),
        resolved_settings,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if should_start_control:
            try:
                await resolved_queue.ensure_ready_group()
            except RedisError as exc:
                # Redis is a delivery accelerator, not the source of truth. The
                # outbox and Worker PostgreSQL fallback recover after it returns.
                logger.warning(
                    "Redis unavailable at API startup; control plane is degraded",
                    error=str(exc),
                )
            await control.start()
        try:
            yield
        finally:
            if should_start_control:
                await control.stop()
            await resolved_queue.close()
            await resolved_database.dispose()

    app = FastAPI(
        title="Mini Docker Cloud",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.database = resolved_database
    app.state.queue = resolved_queue
    app.state.control_plane = control
    register_exception_handlers(app)

    @app.middleware("http")
    async def request_id_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        incoming = request.headers.get(REQUEST_ID_HEADER)
        request_id = incoming if incoming and len(incoming) <= 255 else str(uuid.uuid4())
        request.state.request_id = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.clear_contextvars()
        response.headers[REQUEST_ID_HEADER] = request_id
        return response

    app.include_router(system.router)
    app.include_router(tasks.router)
    app.include_router(workers.router)
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    main()
