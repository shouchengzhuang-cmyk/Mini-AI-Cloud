from __future__ import annotations

import asyncio
import ipaddress
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
from anyio import CancelScope
from sqlalchemy.ext.asyncio import AsyncSession

from api.errors import APIError
from api.schemas.gateway import OpenAIModelList, OpenAIModelObject
from core.database import Database
from core.enums import ErrorCode
from core.logging import get_logger
from core.metrics import (
    GATEWAY_DURATION,
    GATEWAY_ERRORS,
    GATEWAY_IN_FLIGHT,
    GATEWAY_REQUESTS,
    GATEWAY_TOKENS,
    GATEWAY_TTFT,
    REPLICA_ACTIVE_REQUESTS,
    SERVICE_REQUEST_DURATION,
    SERVICE_REQUESTS,
)
from repositories.audit import AuditRepository
from repositories.gateway_model_names import GatewayModelNameConflictError
from repositories.gateway_routing import GatewayRoute, GatewayRoutingRepository
from repositories.services import EndpointSelection, ServiceRepository
from repositories.usage import UsageRepository

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
        "x-mini-ai-replica-id",
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
_MAX_OBSERVED_SSE_EVENT_BYTES = 1024 * 1024


class _UpstreamResponseTooLarge(Exception):
    pass


class _PreDispatchFailure(Exception):
    """A failure that proves the POST body was not delivered to an upstream."""

    def __init__(
        self,
        error: APIError,
        *,
        accounting: _RequestAccounting,
        outcome: str,
    ) -> None:
        super().__init__(error.message)
        self.error = error
        self.accounting = accounting
        self.outcome = outcome


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
        self._replica_active: dict[tuple[uuid.UUID, uuid.UUID], int] = {}
        self._observed_at: dict[uuid.UUID, datetime] = {}
        self._lock = asyncio.Lock()

    async def request_started(
        self, service_id: uuid.UUID, replica_id: uuid.UUID | None = None
    ) -> None:
        async with self._lock:
            self._active[service_id] = self._active.get(service_id, 0) + 1
            self._observed_at[service_id] = datetime.now(UTC)
            GATEWAY_IN_FLIGHT.inc()
            if replica_id is not None:
                key = (service_id, replica_id)
                active = self._replica_active.get(key, 0) + 1
                self._replica_active[key] = active
                REPLICA_ACTIVE_REQUESTS.labels(str(service_id), str(replica_id)).set(active)

    async def request_finished(
        self, service_id: uuid.UUID, replica_id: uuid.UUID | None = None
    ) -> None:
        async with self._lock:
            service_active = self._active.get(service_id, 0)
            if service_active <= 0:
                return
            self._active[service_id] = service_active - 1
            self._observed_at[service_id] = datetime.now(UTC)
            GATEWAY_IN_FLIGHT.dec()
            if replica_id is not None:
                key = (service_id, replica_id)
                replica_active = self._replica_active.get(key, 0)
                if replica_active <= 1:
                    self._replica_active.pop(key, None)
                    REPLICA_ACTIVE_REQUESTS.remove(str(service_id), str(replica_id))
                else:
                    replica_active -= 1
                    self._replica_active[key] = replica_active
                    REPLICA_ACTIVE_REQUESTS.labels(str(service_id), str(replica_id)).set(
                        replica_active
                    )

    async def snapshot(self, service_id: uuid.UUID) -> ServiceLoad | None:
        async with self._lock:
            observed_at = self._observed_at.get(service_id)
            if observed_at is None:
                return None
            return ServiceLoad(
                active_requests=self._active.get(service_id, 0),
                observed_at=observed_at,
            )

    async def replica_snapshot(
        self, service_id: uuid.UUID, replica_id: uuid.UUID
    ) -> ServiceLoad | None:
        async with self._lock:
            observed_at = self._observed_at.get(service_id)
            if observed_at is None:
                return None
            return ServiceLoad(
                active_requests=self._replica_active.get((service_id, replica_id), 0),
                observed_at=observed_at,
            )


@dataclass(frozen=True, slots=True)
class ReportedTokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass(slots=True)
class _RequestAccounting:
    request_id: uuid.UUID
    project_id: uuid.UUID
    service_id: uuid.UUID
    selection: EndpointSelection | None
    path: str
    streamed: bool
    gpu_count: int
    logical_model_id: uuid.UUID | None
    model_variant_id: uuid.UUID | None
    selected_vendor: str | None
    started_at: datetime
    started_monotonic: float
    logical_started_monotonic: float
    tracked_in_memory: bool = False
    time_to_first_token_seconds: float | None = None
    token_usage: ReportedTokenUsage | None = None
    attempt_released: bool = False
    finalized: bool = False
    release_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    finalize_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


@dataclass(frozen=True, slots=True)
class _FallbackAttempt:
    request_id: uuid.UUID
    from_route: GatewayRoute
    reason: str
    failure: _PreDispatchFailure


@dataclass(slots=True)
class _RouteCompletion:
    request_id: uuid.UUID
    project_id: uuid.UUID
    route: GatewayRoute
    fallback: _FallbackAttempt | None
    recorded: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


@dataclass(slots=True)
class GatewayForwardResult:
    status_code: int
    headers: dict[str, str]
    body: bytes | None = None
    stream: AsyncIterator[bytes] | None = None
    cleanup: Callable[[], Awaitable[None]] | None = None


class GatewayService:
    def __init__(
        self,
        database: Database,
        http_client: httpx.AsyncClient,
        metrics: GatewayMetrics,
        *,
        request_timeout: float,
        connect_timeout: float | None = None,
        first_token_timeout: float | None = None,
        max_response_bytes: int = 16 * 1024 * 1024,
        endpoint_host_allowlist: str | Iterable[str] = (),
        fallback_attempts: int = 1,
        circuit_failure_threshold: int = 2,
        circuit_cooldown_seconds: int = 30,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        if connect_timeout is not None and connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")
        if first_token_timeout is not None and first_token_timeout <= 0:
            raise ValueError("first_token_timeout must be positive")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        if fallback_attempts not in {0, 1}:
            raise ValueError("fallback_attempts must be zero or one")
        if circuit_failure_threshold < 1:
            raise ValueError("circuit_failure_threshold must be positive")
        if circuit_cooldown_seconds < 1:
            raise ValueError("circuit_cooldown_seconds must be positive")
        self.database = database
        self.http_client = http_client
        self.metrics = metrics
        self.request_timeout = request_timeout
        self.connect_timeout = min(request_timeout, connect_timeout or request_timeout)
        self.first_token_timeout = min(request_timeout, first_token_timeout or request_timeout)
        self.timeout = httpx.Timeout(
            connect=self.connect_timeout,
            read=None,
            write=request_timeout,
            pool=self.connect_timeout,
        )
        self.max_response_bytes = max_response_bytes
        self.endpoint_host_allowlist = _normalize_endpoint_allowlist(endpoint_host_allowlist)
        self.fallback_attempts = fallback_attempts
        self.circuit_failure_threshold = circuit_failure_threshold
        self.circuit_cooldown_seconds = circuit_cooldown_seconds
        self.logger = get_logger("gateway")

    async def list_models(self, *, project_id: uuid.UUID) -> OpenAIModelList:
        async with self.database.session() as session:
            models = await GatewayRoutingRepository.list_available_models(
                session,
                project_id=project_id,
            )
        return OpenAIModelList(
            data=[
                OpenAIModelObject(
                    id=model.model_id,
                    created=int(_as_utc(model.created_at).timestamp()),
                    owned_by=f"project:{project_id}",
                )
                for model in models
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
        request_id = uuid.uuid4()
        request_started_at = datetime.now(UTC)
        request_started_monotonic = time.monotonic()
        excluded_vendors: set[str] = set()
        fallback: _FallbackAttempt | None = None
        for attempt in range(self.fallback_attempts + 1):
            try:
                async with self.database.session() as session, session.begin():
                    anchor, route = await GatewayRoutingRepository.choose_route(
                        session,
                        project_id=project_id,
                        public_model=public_model,
                        excluded_vendors=frozenset(excluded_vendors),
                    )
            except GatewayModelNameConflictError as exc:
                if fallback is not None:
                    await self._finalize_predispatch_failure(fallback.failure)
                    await self._record_fallback_without_route(
                        fallback=fallback,
                        error_code="GATEWAY_MODEL_NAME_CONFLICT",
                    )
                _observe_gateway_error("GATEWAY_MODEL_NAME_CONFLICT")
                raise APIError(
                    409,
                    "GATEWAY_MODEL_NAME_CONFLICT",
                    "The requested model name is owned by conflicting gateway resources",
                ) from exc
            if anchor is None:
                if fallback is not None:
                    await self._finalize_predispatch_failure(fallback.failure)
                    await self._record_fallback_without_route(
                        fallback=fallback,
                        error_code="MODEL_NOT_FOUND",
                    )
                _observe_gateway_error("MODEL_NOT_FOUND")
                raise APIError(404, "MODEL_NOT_FOUND", "The requested model service was not found")
            if route is None:
                if fallback is not None:
                    await self._finalize_predispatch_failure(fallback.failure)
                    await self._record_fallback_without_route(
                        fallback=fallback,
                        error_code=ErrorCode.NO_HEALTHY_REPLICA.value,
                    )
                    raise fallback.failure.error from fallback.failure.__cause__
                unavailable_accounting = _RequestAccounting(
                    request_id=request_id,
                    project_id=project_id,
                    service_id=anchor.id,
                    selection=None,
                    path=path,
                    streamed=stream_requested,
                    gpu_count=anchor.gpu_count,
                    logical_model_id=anchor.logical_model_id,
                    model_variant_id=anchor.model_variant_id,
                    selected_vendor=anchor.selected_vendor,
                    started_at=request_started_at,
                    started_monotonic=request_started_monotonic,
                    logical_started_monotonic=request_started_monotonic,
                )
                await self._finalize_request(
                    unavailable_accounting,
                    outcome="no_healthy_replica",
                    error_code=ErrorCode.NO_HEALTHY_REPLICA.value,
                    completed=False,
                )
                raise APIError(
                    503,
                    ErrorCode.NO_HEALTHY_REPLICA.value,
                    "The requested model service has no healthy vendor backend",
                    headers={"Retry-After": "1"},
                )

            completion = _RouteCompletion(
                request_id=request_id,
                project_id=project_id,
                route=route,
                fallback=fallback,
            )
            try:
                result = await self._forward_route(
                    route=route,
                    request_id=request_id,
                    project_id=project_id,
                    path=path,
                    payload=payload,
                    request_headers=request_headers,
                    stream_requested=stream_requested,
                    client_disconnected=client_disconnected,
                    request_started_monotonic=request_started_monotonic,
                    route_completion=completion,
                )
            except _PreDispatchFailure as failure:
                await self._complete_route(
                    completion,
                    success=False,
                    error_code=failure.error.code,
                )
                if (
                    fallback is not None
                    or attempt >= self.fallback_attempts
                    or route.selected_vendor is None
                ):
                    await self._finalize_predispatch_failure(failure)
                    raise failure.error from failure.__cause__
                if not await self._release_request_attempt(failure.accounting):
                    await self._finalize_predispatch_failure(failure)
                    raise failure.error from failure.__cause__
                excluded_vendors.add(route.selected_vendor)
                fallback = _FallbackAttempt(
                    request_id=request_id,
                    from_route=route,
                    reason=failure.error.code,
                    failure=failure,
                )
                continue
            except APIError as exc:
                await self._complete_route(
                    completion,
                    success=False,
                    error_code=exc.code,
                )
                raise
            except asyncio.CancelledError:
                await self._complete_route(
                    completion,
                    success=None,
                    error_code="CLIENT_DISCONNECTED",
                )
                raise
            except Exception:
                await self._complete_route(
                    completion,
                    success=False,
                    error_code="INTERNAL_SERVER_ERROR",
                )
                raise

            if result.stream is None:
                await self._complete_route(
                    completion,
                    success=result.status_code < 500,
                    error_code=(None if result.status_code < 500 else "UPSTREAM_HTTP_5XX"),
                )
            return result
        raise RuntimeError("gateway fallback loop exhausted without a result")

    async def _complete_route(
        self,
        completion: _RouteCompletion,
        *,
        success: bool | None,
        error_code: str | None,
    ) -> None:
        async with completion.lock:
            if completion.recorded:
                return
            async with self.database.session() as session, session.begin():
                if success is not None:
                    await GatewayRoutingRepository.record_outcome(
                        session,
                        route=completion.route,
                        project_id=completion.project_id,
                        success=success,
                        error_code=error_code,
                        failure_threshold=self.circuit_failure_threshold,
                        cooldown_seconds=self.circuit_cooldown_seconds,
                    )
                if completion.fallback is not None:
                    if success is True:
                        await GatewayRoutingRepository.record_fallback(
                            session,
                            project_id=completion.project_id,
                            request_id=completion.request_id,
                            from_route=completion.fallback.from_route,
                            to_route=completion.route,
                            reason=completion.fallback.reason,
                        )
                    else:
                        await self._add_fallback_failure(
                            session,
                            fallback=completion.fallback,
                            to_route=completion.route,
                            error_code=error_code,
                        )
            completion.recorded = True

    async def _record_fallback_without_route(
        self,
        *,
        fallback: _FallbackAttempt,
        error_code: str,
    ) -> None:
        async with self.database.session() as session, session.begin():
            await self._add_fallback_failure(
                session,
                fallback=fallback,
                to_route=None,
                error_code=error_code,
            )

    @staticmethod
    async def _add_fallback_failure(
        session: AsyncSession,
        *,
        fallback: _FallbackAttempt,
        to_route: GatewayRoute | None,
        error_code: str | None,
    ) -> None:
        from_route = fallback.from_route
        await AuditRepository.record(
            session,
            project_id=fallback.failure.accounting.project_id,
            actor_type="gateway",
            actor_user_id=None,
            api_key_id=None,
            action="gateway.vendor_fallback",
            resource_type="logical_model",
            resource_id=(
                str(from_route.logical_model_id)
                if from_route.logical_model_id is not None
                else None
            ),
            outcome="failure",
            request_id=str(fallback.request_id),
            source_ip=None,
            details={
                "from_service_id": str(from_route.service_id),
                "from_variant_id": str(from_route.model_variant_id),
                "from_vendor": from_route.selected_vendor,
                "to_service_id": str(to_route.service_id) if to_route is not None else None,
                "to_variant_id": (str(to_route.model_variant_id) if to_route is not None else None),
                "to_vendor": to_route.selected_vendor if to_route is not None else None,
                "reason": fallback.reason,
                "failure_reason": error_code,
            },
        )

    async def _finalize_predispatch_failure(self, failure: _PreDispatchFailure) -> None:
        await self._finalize_request(
            failure.accounting,
            outcome=failure.outcome,
            error_code=failure.error.code,
            completed=False,
        )

    async def _forward_route(
        self,
        *,
        route: GatewayRoute,
        request_id: uuid.UUID,
        project_id: uuid.UUID,
        path: str,
        payload: Mapping[str, Any],
        request_headers: Mapping[str, str],
        stream_requested: bool,
        client_disconnected: Callable[[], Awaitable[bool]],
        request_started_monotonic: float,
        route_completion: _RouteCompletion,
    ) -> GatewayForwardResult:
        if path not in {"/v1/chat/completions", "/v1/completions"}:
            raise ValueError("unsupported gateway upstream path")
        started_monotonic = time.monotonic()
        started_at = datetime.now(UTC)
        overall_deadline = request_started_monotonic + self.request_timeout
        first_token_deadline = min(
            overall_deadline,
            started_monotonic + self.first_token_timeout,
        )
        selection = route.selection
        service_id = route.service_id
        upstream_model = route.upstream_model
        gpu_count = route.gpu_count

        accounting = _RequestAccounting(
            request_id=request_id,
            project_id=project_id,
            service_id=service_id,
            selection=selection,
            path=path,
            streamed=stream_requested,
            gpu_count=gpu_count,
            logical_model_id=route.logical_model_id,
            model_variant_id=route.model_variant_id,
            selected_vendor=route.selected_vendor,
            started_at=started_at,
            started_monotonic=started_monotonic,
            logical_started_monotonic=request_started_monotonic,
        )
        try:
            await self.metrics.request_started(service_id, selection.replica_id)
            accounting.tracked_in_memory = True
        except asyncio.CancelledError:
            await self._finalize_request(
                accounting,
                outcome="client_disconnect",
                error_code="CLIENT_DISCONNECTED",
                completed=False,
            )
            raise
        except Exception:
            await self._finalize_request(
                accounting,
                outcome="internal_error",
                error_code="INTERNAL_SERVER_ERROR",
                completed=False,
            )
            raise

        try:
            _validate_gateway_endpoint(
                selection.endpoint_url,
                host_allowlist=self.endpoint_host_allowlist,
            )
        except ValueError as exc:
            raise _PreDispatchFailure(
                APIError(
                    503,
                    "UNSAFE_REPLICA_ENDPOINT",
                    "The selected model replica endpoint is not permitted",
                ),
                accounting=accounting,
                outcome="unsafe_endpoint",
            ) from exc

        upstream_payload = dict(payload)
        upstream_payload["model"] = upstream_model
        headers = _forward_request_headers(request_headers)
        headers["accept-encoding"] = "identity"
        try:
            request = self.http_client.build_request(
                "POST",
                f"{selection.endpoint_url.rstrip('/')}{path}",
                json=upstream_payload,
                headers=headers,
                timeout=self.timeout,
            )
            async with asyncio.timeout(_remaining_seconds(first_token_deadline)):
                upstream = await self.http_client.send(
                    request,
                    stream=True,
                    follow_redirects=False,
                )
        except httpx.ConnectTimeout as exc:
            raise _PreDispatchFailure(
                APIError(
                    503,
                    ErrorCode.UPSTREAM_CONNECT_TIMEOUT.value,
                    "The gateway could not connect to the model replica before the timeout",
                    headers={"Retry-After": "1"},
                ),
                accounting=accounting,
                outcome="connect_timeout",
            ) from exc
        except httpx.ConnectError as exc:
            raise _PreDispatchFailure(
                APIError(
                    503,
                    ErrorCode.UPSTREAM_DISCONNECTED.value,
                    "The selected model replica is unavailable",
                    headers={"Retry-After": "1"},
                ),
                accounting=accounting,
                outcome="upstream_disconnected",
            ) from exc
        except TimeoutError as exc:
            await self._finalize_request(
                accounting,
                outcome="first_token_timeout",
                error_code=ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                completed=False,
            )
            raise APIError(
                503,
                ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                "The model service did not produce response headers before the first-token timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.TimeoutException as exc:
            await self._finalize_request(
                accounting,
                outcome="inference_timeout",
                error_code=ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                completed=False,
            )
            raise APIError(
                503,
                ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                "The model service did not respond before the gateway timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.RequestError as exc:
            await self._finalize_request(
                accounting,
                outcome="upstream_disconnected",
                error_code=ErrorCode.UPSTREAM_DISCONNECTED.value,
                completed=False,
            )
            raise APIError(
                503,
                ErrorCode.UPSTREAM_DISCONNECTED.value,
                "The selected model replica is unavailable",
                headers={"Retry-After": "1"},
            ) from exc
        except asyncio.CancelledError:
            await self._finalize_request(
                accounting,
                outcome="client_disconnect",
                error_code="CLIENT_DISCONNECTED",
                completed=False,
            )
            raise
        except Exception:
            await self._finalize_request(
                accounting,
                outcome="internal_error",
                error_code="INTERNAL_SERVER_ERROR",
                completed=False,
            )
            raise

        if upstream.is_redirect:
            await self._close_upstream_and_finalize(
                upstream,
                accounting,
                outcome="redirect_rejected",
                error_code="UPSTREAM_REDIRECT_REJECTED",
                completed=False,
            )
            raise APIError(
                502,
                "UPSTREAM_REDIRECT_REJECTED",
                "The model replica returned a redirect, which the gateway does not permit",
            )

        response_headers = _forward_response_headers(upstream.headers)
        response_headers["x-mini-ai-replica-id"] = str(selection.replica_id)
        if route.model_variant_id is not None:
            response_headers["x-mini-ai-model-variant-id"] = str(route.model_variant_id)
        if route.selected_vendor is not None:
            response_headers["x-mini-ai-accelerator-vendor"] = route.selected_vendor
        is_sse = upstream.headers.get("content-type", "").lower().startswith("text/event-stream")
        if stream_requested and upstream.is_success and is_sse:
            iterator = upstream.aiter_bytes().__aiter__()
            try:
                first_chunk = await _read_first_nonempty_chunk(iterator, first_token_deadline)
            except TimeoutError as exc:
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="first_token_timeout",
                    error_code=ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                    completed=False,
                )
                raise APIError(
                    503,
                    ErrorCode.INFERENCE_REQUEST_TIMEOUT.value,
                    "The model service did not produce its first response chunk before the timeout",
                    headers={"Retry-After": "1"},
                ) from exc
            except (httpx.TimeoutException, httpx.RequestError) as exc:
                code = (
                    ErrorCode.INFERENCE_REQUEST_TIMEOUT.value
                    if isinstance(exc, httpx.TimeoutException)
                    else ErrorCode.UPSTREAM_DISCONNECTED.value
                )
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="stream_start_failed",
                    error_code=code,
                    completed=False,
                )
                raise APIError(
                    503,
                    code,
                    "The model service disconnected before producing its first response chunk",
                    headers={"Retry-After": "1"},
                ) from exc
            except asyncio.CancelledError:
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="client_disconnect",
                    error_code="CLIENT_DISCONNECTED",
                    completed=False,
                )
                raise
            except Exception:
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="internal_error",
                    error_code="INTERNAL_SERVER_ERROR",
                    completed=False,
                )
                raise
            if first_chunk is None:
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="upstream_disconnected",
                    error_code=ErrorCode.UPSTREAM_DISCONNECTED.value,
                    completed=False,
                )
                raise APIError(
                    503,
                    ErrorCode.UPSTREAM_DISCONNECTED.value,
                    "The model service ended the stream before producing a response chunk",
                    headers={"Retry-After": "1"},
                )
            accounting.time_to_first_token_seconds = max(0.0, time.monotonic() - started_monotonic)
            usage_observer = _SSEUsageObserver()
            usage_observer.feed(first_chunk)

            async def cleanup_stream() -> None:
                await self._close_upstream_and_finalize(
                    upstream,
                    accounting,
                    outcome="client_disconnect",
                    error_code="CLIENT_DISCONNECTED",
                    completed=False,
                    route_completion=route_completion,
                    route_success=None,
                )

            return GatewayForwardResult(
                status_code=upstream.status_code,
                headers=response_headers,
                stream=self._stream_response(
                    upstream,
                    iterator=iterator,
                    first_chunk=first_chunk,
                    accounting=accounting,
                    overall_deadline=overall_deadline,
                    usage_observer=usage_observer,
                    client_disconnected=client_disconnected,
                    route_completion=route_completion,
                ),
                cleanup=cleanup_stream,
            )

        outcome = "success" if upstream.is_success else "upstream_error"
        error_code: str | None = None if upstream.is_success else "UPSTREAM_HTTP_ERROR"
        completed = False
        try:
            body = await self._read_buffered_response(
                upstream,
                accounting=accounting,
                first_token_deadline=first_token_deadline,
                overall_deadline=overall_deadline,
            )
            completed = True
            accounting.token_usage = _reported_usage_from_json(
                body,
                content_type=upstream.headers.get("content-type", ""),
            )
        except _UpstreamResponseTooLarge as exc:
            outcome = "response_too_large"
            error_code = "UPSTREAM_RESPONSE_TOO_LARGE"
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
        except TimeoutError as exc:
            outcome = (
                "first_token_timeout"
                if accounting.time_to_first_token_seconds is None
                else "inference_timeout"
            )
            error_code = ErrorCode.INFERENCE_REQUEST_TIMEOUT.value
            raise APIError(
                503,
                error_code,
                "The model service did not complete before the inference request timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.TimeoutException as exc:
            outcome = "inference_timeout"
            error_code = ErrorCode.INFERENCE_REQUEST_TIMEOUT.value
            raise APIError(
                503,
                error_code,
                "The model service did not respond before the gateway timeout",
                headers={"Retry-After": "1"},
            ) from exc
        except httpx.RequestError as exc:
            outcome = "upstream_disconnected"
            error_code = ErrorCode.UPSTREAM_DISCONNECTED.value
            raise APIError(
                503,
                error_code,
                "The selected model replica disconnected before completing the response",
                headers={"Retry-After": "1"},
            ) from exc
        except asyncio.CancelledError:
            outcome = "client_disconnect"
            error_code = "CLIENT_DISCONNECTED"
            raise
        except Exception:
            outcome = "internal_error"
            error_code = "INTERNAL_SERVER_ERROR"
            raise
        finally:
            await self._close_upstream_and_finalize(
                upstream,
                accounting,
                outcome=outcome,
                error_code=error_code,
                completed=completed,
            )
        return GatewayForwardResult(
            status_code=upstream.status_code,
            headers=response_headers,
            body=body,
        )

    async def _read_buffered_response(
        self,
        upstream: httpx.Response,
        *,
        accounting: _RequestAccounting,
        first_token_deadline: float,
        overall_deadline: float,
    ) -> bytes:
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
        iterator = upstream.aiter_bytes(chunk_size=chunk_size).__aiter__()
        while True:
            deadline = (
                first_token_deadline
                if accounting.time_to_first_token_seconds is None
                else overall_deadline
            )
            try:
                chunk = await _next_chunk(iterator, deadline)
            except StopAsyncIteration:
                break
            if chunk and accounting.time_to_first_token_seconds is None:
                accounting.time_to_first_token_seconds = max(
                    0.0, time.monotonic() - accounting.started_monotonic
                )
            if len(body) + len(chunk) > self.max_response_bytes:
                raise _UpstreamResponseTooLarge
            body.extend(chunk)
        return bytes(body)

    async def _stream_response(
        self,
        upstream: httpx.Response,
        *,
        iterator: AsyncIterator[bytes],
        first_chunk: bytes,
        accounting: _RequestAccounting,
        overall_deadline: float,
        usage_observer: _SSEUsageObserver,
        client_disconnected: Callable[[], Awaitable[bool]],
        route_completion: _RouteCompletion,
    ) -> AsyncIterator[bytes]:
        outcome = "success"
        error_code: str | None = None
        completed = False
        try:
            if await client_disconnected():
                outcome = "client_disconnect"
                error_code = "CLIENT_DISCONNECTED"
                return
            yield first_chunk
            while True:
                try:
                    chunk = await _next_chunk(iterator, overall_deadline)
                except StopAsyncIteration:
                    completed = True
                    usage_observer.finish()
                    accounting.token_usage = usage_observer.usage
                    return
                usage_observer.feed(chunk)
                if await client_disconnected():
                    outcome = "client_disconnect"
                    error_code = "CLIENT_DISCONNECTED"
                    return
                if chunk:
                    yield chunk
        except TimeoutError as exc:
            outcome = "inference_timeout"
            error_code = ErrorCode.INFERENCE_REQUEST_TIMEOUT.value
            self.logger.warning(
                "gateway stream exceeded inference request deadline",
                service_id=str(accounting.service_id),
                replica_id=str(accounting.selection.replica_id) if accounting.selection else None,
                error=type(exc).__name__,
            )
        except (GeneratorExit, asyncio.CancelledError):
            outcome = "client_disconnect"
            error_code = "CLIENT_DISCONNECTED"
            raise
        except httpx.TimeoutException as exc:
            outcome = "inference_timeout"
            error_code = ErrorCode.INFERENCE_REQUEST_TIMEOUT.value
            self.logger.warning(
                "gateway stream timed out before upstream completion",
                service_id=str(accounting.service_id),
                error=type(exc).__name__,
            )
        except httpx.RequestError as exc:
            outcome = "upstream_disconnected"
            error_code = ErrorCode.UPSTREAM_DISCONNECTED.value
            self.logger.warning(
                "gateway stream ended before upstream completion",
                service_id=str(accounting.service_id),
                error=type(exc).__name__,
            )
        except Exception:
            outcome = "internal_error"
            error_code = "INTERNAL_SERVER_ERROR"
            raise
        finally:
            await self._close_upstream_and_finalize(
                upstream,
                accounting,
                outcome=outcome,
                error_code=error_code,
                completed=completed,
                route_completion=route_completion,
                route_success=(
                    True if completed else None if error_code == "CLIENT_DISCONNECTED" else False
                ),
            )

    async def _close_upstream_and_finalize(
        self,
        upstream: httpx.Response,
        accounting: _RequestAccounting,
        *,
        outcome: str,
        error_code: str | None,
        completed: bool,
        route_completion: _RouteCompletion | None = None,
        route_success: bool | None = None,
    ) -> None:
        async def cleanup() -> None:
            try:
                await upstream.aclose()
            finally:
                try:
                    await self._finalize_request_inner(
                        accounting,
                        outcome=outcome,
                        error_code=error_code,
                        completed=completed,
                    )
                finally:
                    if route_completion is not None:
                        await self._complete_route(
                            route_completion,
                            success=route_success,
                            error_code=error_code,
                        )

        await _await_cancel_safe(cleanup())

    async def _finalize_request(
        self,
        accounting: _RequestAccounting,
        *,
        outcome: str,
        error_code: str | None,
        completed: bool,
    ) -> None:
        await _await_cancel_safe(
            self._finalize_request_inner(
                accounting,
                outcome=outcome,
                error_code=error_code,
                completed=completed,
            )
        )

    async def _finalize_request_inner(
        self,
        accounting: _RequestAccounting,
        *,
        outcome: str,
        error_code: str | None,
        completed: bool,
    ) -> None:
        async with accounting.finalize_lock:
            if accounting.finalized:
                return
            release_completed = await self._persist_request_finalization(
                accounting,
                outcome=outcome,
                error_code=error_code,
                completed=completed,
            )
            if release_completed:
                accounting.finalized = True

    async def _release_request_attempt(self, accounting: _RequestAccounting) -> bool:
        return await _await_cancel_safe(self._release_request_attempt_inner(accounting))

    async def _release_request_attempt_inner(self, accounting: _RequestAccounting) -> bool:
        async with accounting.release_lock:
            if accounting.attempt_released:
                return True

            if accounting.selection is not None:
                try:
                    async with self.database.session() as session, session.begin():
                        released = await ServiceRepository.release_endpoint_request(
                            session,
                            replica_id=accounting.selection.replica_id,
                            generation=accounting.selection.generation,
                            execution_id=accounting.selection.execution_id,
                        )
                    if not released:
                        self.logger.warning(
                            "gateway endpoint request release was rejected by its fence",
                            service_id=str(accounting.service_id),
                            replica_id=str(accounting.selection.replica_id),
                            generation=accounting.selection.generation,
                        )
                except Exception:
                    self.logger.exception(
                        "failed to persist gateway endpoint request release",
                        service_id=str(accounting.service_id),
                        replica_id=str(accounting.selection.replica_id),
                    )
                    return False

            if accounting.tracked_in_memory and accounting.selection is not None:
                try:
                    await self.metrics.request_finished(
                        accounting.service_id,
                        accounting.selection.replica_id,
                    )
                except Exception:
                    self.logger.exception(
                        "failed to release process-local gateway request metrics",
                        service_id=str(accounting.service_id),
                        replica_id=str(accounting.selection.replica_id),
                    )

            accounting.attempt_released = True
            return True

    async def _persist_request_finalization(
        self,
        accounting: _RequestAccounting,
        *,
        outcome: str,
        error_code: str | None,
        completed: bool,
    ) -> bool:
        duration = max(0.0, time.monotonic() - accounting.started_monotonic)
        if not await self._release_request_attempt_inner(accounting):
            return False

        _observe_gateway(outcome, accounting.logical_started_monotonic)
        if error_code is not None:
            _observe_gateway_error(error_code)
        if accounting.time_to_first_token_seconds is not None:
            GATEWAY_TTFT.observe(accounting.time_to_first_token_seconds)

        reported_usage = accounting.token_usage if completed else None
        if reported_usage is not None:
            GATEWAY_TOKENS.labels("prompt").inc(reported_usage.prompt_tokens)
            GATEWAY_TOKENS.labels("completion").inc(reported_usage.completion_tokens)
            GATEWAY_TOKENS.labels("total").inc(reported_usage.total_tokens)

        finished_at = datetime.now(UTC)
        duration_decimal = Decimal(str(duration))
        allocated_gpu_seconds = duration_decimal * accounting.gpu_count if completed else None
        try:
            async with self.database.session() as session, session.begin():
                await UsageRepository.record_serving_request(
                    session,
                    request_id=accounting.request_id,
                    project_id=accounting.project_id,
                    service_id=accounting.service_id,
                    replica_id=(
                        accounting.selection.replica_id
                        if accounting.selection is not None
                        else None
                    ),
                    logical_model_id=accounting.logical_model_id,
                    model_variant_id=accounting.model_variant_id,
                    selected_vendor=accounting.selected_vendor,
                    path=accounting.path,
                    outcome=outcome,
                    error_code=error_code,
                    streamed=accounting.streamed,
                    started_at=accounting.started_at,
                    finished_at=finished_at,
                    request_duration_seconds=duration_decimal,
                    time_to_first_token_seconds=(
                        None
                        if accounting.time_to_first_token_seconds is None
                        else Decimal(str(accounting.time_to_first_token_seconds))
                    ),
                    allocated_gpu_seconds=allocated_gpu_seconds,
                    prompt_tokens=(
                        reported_usage.prompt_tokens if reported_usage is not None else None
                    ),
                    completion_tokens=(
                        reported_usage.completion_tokens if reported_usage is not None else None
                    ),
                    total_tokens=(
                        reported_usage.total_tokens if reported_usage is not None else None
                    ),
                )
        except Exception:
            self.logger.exception(
                "failed to persist serving request usage",
                request_id=str(accounting.request_id),
                service_id=str(accounting.service_id),
            )
        return True


class _SSEUsageObserver:
    """Observe a bounded SSE event stream without buffering the proxied body."""

    def __init__(self) -> None:
        self._line_buffer = bytearray()
        self._data_lines: list[bytes] = []
        self._event_bytes = 0
        self._disabled = False
        self.usage: ReportedTokenUsage | None = None

    def feed(self, chunk: bytes) -> None:
        if self._disabled or not chunk:
            return
        if len(self._line_buffer) + len(chunk) > _MAX_OBSERVED_SSE_EVENT_BYTES:
            self._disable()
            return
        self._line_buffer.extend(chunk)
        while True:
            newline = self._line_buffer.find(b"\n")
            if newline < 0:
                return
            line = bytes(self._line_buffer[:newline])
            del self._line_buffer[: newline + 1]
            if line.endswith(b"\r"):
                line = line[:-1]
            self._consume_line(line)
            if self._disabled:
                return

    def finish(self) -> None:
        if self._disabled:
            return
        if self._line_buffer:
            line = bytes(self._line_buffer)
            self._line_buffer.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            self._consume_line(line)
        self._dispatch_event()

    def _consume_line(self, line: bytes) -> None:
        if not line:
            self._dispatch_event()
            return
        if not line.startswith(b"data:"):
            return
        value = line[5:]
        if value.startswith(b" "):
            value = value[1:]
        self._event_bytes += len(value)
        if self._event_bytes > _MAX_OBSERVED_SSE_EVENT_BYTES:
            self._disable()
            return
        self._data_lines.append(value)

    def _dispatch_event(self) -> None:
        if not self._data_lines:
            self._event_bytes = 0
            return
        data = b"\n".join(self._data_lines)
        self._data_lines.clear()
        self._event_bytes = 0
        if data.strip() == b"[DONE]":
            return
        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        observed = _reported_usage_from_payload(payload)
        if observed is not None:
            self.usage = observed

    def _disable(self) -> None:
        self._disabled = True
        self._line_buffer.clear()
        self._data_lines.clear()
        self._event_bytes = 0


async def _await_cancel_safe[CleanupResult](
    coroutine: Coroutine[Any, Any, CleanupResult],
) -> CleanupResult:
    """Finish critical cleanup under AnyIO and direct asyncio cancellation."""

    interrupted: asyncio.CancelledError | None = None
    with CancelScope(shield=True):
        cleanup_task = asyncio.create_task(coroutine)
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as exc:
                interrupted = exc
        result = cleanup_task.result()
    if interrupted is not None:
        raise interrupted
    return result


async def _read_first_nonempty_chunk(
    iterator: AsyncIterator[bytes], deadline: float
) -> bytes | None:
    while True:
        try:
            chunk = await _next_chunk(iterator, deadline)
        except StopAsyncIteration:
            return None
        if chunk:
            return chunk


async def _next_chunk(iterator: AsyncIterator[bytes], deadline: float) -> bytes:
    async with asyncio.timeout(_remaining_seconds(deadline)):
        return await anext(iterator)


def _remaining_seconds(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return remaining


def _reported_usage_from_json(body: bytes, *, content_type: str) -> ReportedTokenUsage | None:
    media_type = content_type.partition(";")[0].strip().lower()
    if media_type != "application/json" and not media_type.endswith("+json"):
        return None
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return _reported_usage_from_payload(payload)


def _reported_usage_from_payload(payload: object) -> ReportedTokenUsage | None:
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    total_tokens = usage.get("total_tokens")
    values = (prompt_tokens, completion_tokens, total_tokens)
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
        return None
    assert isinstance(prompt_tokens, int)
    assert isinstance(completion_tokens, int)
    assert isinstance(total_tokens, int)
    if total_tokens != prompt_tokens + completion_tokens:
        return None
    return ReportedTokenUsage(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )


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


def _observe_gateway_error(code: str) -> None:
    GATEWAY_ERRORS.labels(code).inc()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
