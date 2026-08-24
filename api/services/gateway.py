from __future__ import annotations

import asyncio
import ipaddress
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from api.errors import APIError
from api.schemas.gateway import OpenAIModelList, OpenAIModelObject
from core.database import Database
from core.logging import get_logger
from core.metrics import (
    GATEWAY_DURATION,
    GATEWAY_REQUESTS,
    SERVICE_REQUEST_DURATION,
    SERVICE_REQUESTS,
)
from repositories.services import ServiceRepository

HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
SENSITIVE_REQUEST_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "host",
        "x-api-key",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-port",
        "x-forwarded-proto",
        "forwarded",
        "via",
        "content-length",
        "accept-encoding",
    }
)
UNSAFE_RESPONSE_HEADERS = frozenset(
    {
        "content-length",
        "content-encoding",
        "set-cookie",
    }
)
_BUFFER_READ_CHUNK_BYTES = 64 * 1024


class _UpstreamResponseTooLarge(Exception):
    pass


@dataclass(frozen=True, slots=True)
class ServiceLoad:
    active_requests: int
    observed_at: datetime


class ServiceMetricsSource(Protocol):
    async def snapshot(self, service_id: uuid.UUID) -> ServiceLoad | None: ...


class GatewayMetrics(ServiceMetricsSource):
    """Process-local live concurrency metrics behind an injectable interface.

    Production deployments can replace this source with a shared metrics backend
    without coupling gateway forwarding to autoscaler persistence.
    """

    def __init__(self) -> None:
        self._active: dict[uuid.UUID, int] = {}
        self._observed_at: dict[uuid.UUID, datetime] = {}
        self._lock = asyncio.Lock()

    async def request_started(self, service_id: uuid.UUID) -> None:
        async with self._lock:
            self._active[service_id] = self._active.get(service_id, 0) + 1
            self._observed_at[service_id] = datetime.now(UTC)

    async def request_finished(self, service_id: uuid.UUID) -> None:
        async with self._lock:
            self._active[service_id] = max(0, self._active.get(service_id, 0) - 1)
            self._observed_at[service_id] = datetime.now(UTC)

    async def snapshot(self, service_id: uuid.UUID) -> ServiceLoad | None:
        async with self._lock:
            observed_at = self._observed_at.get(service_id)
            if observed_at is None:
                return None
            return ServiceLoad(
                active_requests=self._active.get(service_id, 0),
                observed_at=observed_at,
            )


@dataclass(slots=True)
class GatewayForwardResult:
    status_code: int
    headers: dict[str, str]
    body: bytes | None = None
    stream: AsyncIterator[bytes] | None = None


class GatewayService:
    def __init__(
        self,
        database: Database,
        http_client: httpx.AsyncClient,
        metrics: GatewayMetrics,
        *,
        request_timeout: float,
        max_response_bytes: int = 16 * 1024 * 1024,
        endpoint_host_allowlist: str | Iterable[str] = (),
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        self.database = database
        self.http_client = http_client
        self.metrics = metrics
        self.timeout = httpx.Timeout(request_timeout)
        self.max_response_bytes = max_response_bytes
        self.endpoint_host_allowlist = _normalize_endpoint_allowlist(endpoint_host_allowlist)
        self.logger = get_logger("gateway")

    async def list_models(self, *, project_id: uuid.UUID) -> OpenAIModelList:
        async with self.database.session() as session:
            services = await ServiceRepository.list_services(
                session,
                project_id=project_id,
                status=None,
                limit=1000,
                offset=0,
            )
            counts = await ServiceRepository.counts_for_service_ids(
                session, [service.id for service in services]
            )
        return OpenAIModelList(
            data=[
                OpenAIModelObject(
                    id=service.name,
                    created=int(_as_utc(service.created_at).timestamp()),
                    owned_by=f"project:{project_id}",
                )
                for service in services
                if service.desired_replicas > 0 and counts[service.id].healthy_replicas > 0
            ]
        )

    async def forward(
        self,
        *,
        project_id: uuid.UUID,
        public_model: str,
        path: str,
        payload: Mapping[str, Any],
        request_headers: Mapping[str, str],
        stream_requested: bool,
        client_disconnected: Callable[[], Awaitable[bool]],
    ) -> GatewayForwardResult:
        if path not in {"/v1/chat/completions", "/v1/completions"}:
            raise ValueError("unsupported gateway upstream path")
        async with self.database.session() as session, session.begin():
            service = await ServiceRepository.get_by_name(
                session,
                project_id=project_id,
                name=public_model,
                for_update=True,
            )
            if service is None:
                raise APIError(404, "MODEL_NOT_FOUND", "The requested model service was not found")
            selection = await ServiceRepository.choose_healthy_endpoint(
                session,
                service_id=service.id,
                project_id=project_id,
            )
            if selection is None:
                raise APIError(
                    503,
                    "SERVICE_NOT_READY",
                    "The requested model service has no healthy replicas",
                    headers={"Retry-After": "1"},
                )
            service_id = service.id
            upstream_model = service.model

        try:
            _validate_gateway_endpoint(
                selection.endpoint_url,
                host_allowlist=self.endpoint_host_allowlist,
            )
        except ValueError as exc:
            raise APIError(
                503,
                "UNSAFE_REPLICA_ENDPOINT",
                "The selected model replica endpoint is not permitted",
            ) from exc

        upstream_payload = dict(payload)
        upstream_payload["model"] = upstream_model
        headers = _forward_request_headers(request_headers)
        headers["accept-encoding"] = "identity"
        started_at = time.monotonic()
        await self.metrics.request_started(service_id)
        try:
            request = self.http_client.build_request(
                "POST",
                f"{selection.endpoint_url.rstrip('/')}{path}",
                json=upstream_payload,
                headers=headers,
                timeout=self.timeout,
            )
            upstream = await self.http_client.send(
                request,
                stream=True,
                follow_redirects=False,
            )
        except httpx.TimeoutException as exc:
            await self.metrics.request_finished(service_id)
            _observe_gateway("timeout", started_at)
            raise APIError(
                503,
                "UPSTREAM_TIMEOUT",
                "The model service did not respond before the gateway timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.RequestError as exc:
            await self.metrics.request_finished(service_id)
            _observe_gateway("unavailable", started_at)
            raise APIError(
                503,
                "UPSTREAM_UNAVAILABLE",
                "The selected model replica is unavailable",
                headers={"Retry-After": "1"},
            ) from exc

        if upstream.is_redirect:
            await upstream.aclose()
            await self.metrics.request_finished(service_id)
            _observe_gateway("redirect_rejected", started_at)
            raise APIError(
                502,
                "UPSTREAM_REDIRECT_REJECTED",
                "The model replica returned a redirect, which the gateway does not permit",
            )

        response_headers = _forward_response_headers(upstream.headers)
        is_sse = upstream.headers.get("content-type", "").lower().startswith("text/event-stream")
        if stream_requested and upstream.is_success and is_sse:
            return GatewayForwardResult(
                status_code=upstream.status_code,
                headers=response_headers,
                stream=self._stream_response(
                    upstream,
                    service_id=service_id,
                    started_at=started_at,
                    client_disconnected=client_disconnected,
                ),
            )

        outcome = "success" if upstream.is_success else "upstream_error"
        try:
            body = await self._read_buffered_response(upstream)
        except _UpstreamResponseTooLarge as exc:
            outcome = "response_too_large"
            self.logger.warning(
                "gateway rejected oversized buffered upstream response",
                service_id=str(service_id),
                max_response_bytes=self.max_response_bytes,
            )
            raise APIError(
                502,
                "UPSTREAM_RESPONSE_TOO_LARGE",
                "The model service response exceeds the gateway buffer limit",
            ) from exc
        except httpx.TimeoutException as exc:
            outcome = "timeout"
            raise APIError(
                503,
                "UPSTREAM_TIMEOUT",
                "The model service did not respond before the gateway timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.RequestError as exc:
            outcome = "unavailable"
            raise APIError(
                503,
                "UPSTREAM_UNAVAILABLE",
                "The selected model replica disconnected before completing the response",
                headers={"Retry-After": "1"},
            ) from exc
        except asyncio.CancelledError:
            outcome = "client_disconnect"
            raise
        finally:
            try:
                await upstream.aclose()
            finally:
                await self.metrics.request_finished(service_id)
                _observe_gateway(outcome, started_at)
        return GatewayForwardResult(
            status_code=upstream.status_code,
            headers=response_headers,
            body=body,
        )

    async def _read_buffered_response(self, upstream: httpx.Response) -> bytes:
        declared_length = upstream.headers.get("content-length")
        if declared_length is not None:
            try:
                declared_bytes = int(declared_length)
            except ValueError:
                declared_bytes = -1
            if declared_bytes > self.max_response_bytes:
                raise _UpstreamResponseTooLarge

        body = bytearray()
        chunk_size = min(_BUFFER_READ_CHUNK_BYTES, self.max_response_bytes + 1)
        async for chunk in upstream.aiter_bytes(chunk_size=chunk_size):
            if len(body) + len(chunk) > self.max_response_bytes:
                raise _UpstreamResponseTooLarge
            body.extend(chunk)
        return bytes(body)

    async def _stream_response(
        self,
        upstream: httpx.Response,
        *,
        service_id: uuid.UUID,
        started_at: float,
        client_disconnected: Callable[[], Awaitable[bool]],
    ) -> AsyncIterator[bytes]:
        outcome = "success"
        try:
            async for chunk in upstream.aiter_bytes():
                if await client_disconnected():
                    outcome = "client_disconnect"
                    return
                if chunk:
                    yield chunk
        except asyncio.CancelledError:
            outcome = "client_disconnect"
            raise
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            outcome = "stream_error"
            self.logger.warning(
                "gateway stream ended before upstream completion",
                service_id=str(service_id),
                error=type(exc).__name__,
            )
        finally:
            await upstream.aclose()
            await self.metrics.request_finished(service_id)
            _observe_gateway(outcome, started_at)


def _forward_request_headers(headers: Mapping[str, str]) -> dict[str, str]:
    connection_tokens = _connection_tokens(headers)
    blocked = HOP_BY_HOP_HEADERS | SENSITIVE_REQUEST_HEADERS | connection_tokens
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def _forward_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    connection_tokens = _connection_tokens(headers)
    blocked = HOP_BY_HOP_HEADERS | UNSAFE_RESPONSE_HEADERS | connection_tokens
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def _connection_tokens(headers: Mapping[str, str]) -> frozenset[str]:
    connection = next(
        (value for key, value in headers.items() if key.lower() == "connection"),
        "",
    )
    return frozenset(token.strip().lower() for token in connection.split(",") if token.strip())


def _normalize_endpoint_allowlist(value: str | Iterable[str]) -> frozenset[str]:
    values = value.split(",") if isinstance(value, str) else value
    normalized: set[str] = set()
    for item in values:
        pattern = item.strip().lower().rstrip(".")
        if not pattern:
            continue
        if "://" in pattern or "/" in pattern or pattern == "*":
            raise ValueError("endpoint host allowlist entries must be hostnames or *.suffix")
        if "*" in pattern and not pattern.startswith("*."):
            raise ValueError("endpoint host wildcards are only supported as *.suffix")
        normalized.add(pattern)
    return frozenset(normalized)


def _validate_gateway_endpoint(endpoint_url: str, *, host_allowlist: frozenset[str]) -> None:
    parsed = urlsplit(endpoint_url)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("gateway endpoints must be plain HTTP origins")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("gateway endpoint port is invalid") from exc
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("gateway endpoint port is invalid")

    host = parsed.hostname.lower().rstrip(".")
    if _host_is_allowlisted(host, host_allowlist):
        return
    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError("gateway endpoint hostname is not allowlisted") from exc
    if address.is_loopback:
        return
    if (
        address.is_private
        and not address.is_link_local
        and not address.is_multicast
        and not address.is_unspecified
        and not address.is_reserved
    ):
        return
    raise ValueError("gateway endpoint address is not private or allowlisted")


def _host_is_allowlisted(host: str, allowlist: frozenset[str]) -> bool:
    for pattern in allowlist:
        if pattern.startswith("*."):
            suffix = pattern[1:]
            if host.endswith(suffix) and host != pattern[2:]:
                return True
        elif host == pattern:
            return True
    return False


def _observe_gateway(outcome: str, started_at: float) -> None:
    duration = max(0.0, time.monotonic() - started_at)
    GATEWAY_REQUESTS.labels(outcome).inc()
    GATEWAY_DURATION.observe(duration)
    SERVICE_REQUESTS.labels(outcome).inc()
    SERVICE_REQUEST_DURATION.observe(duration)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
