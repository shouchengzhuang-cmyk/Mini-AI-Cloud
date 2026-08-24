import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import httpx
import structlog.contextvars
import uvicorn
from fastapi import FastAPI, Request, Response
from redis.exceptions import RedisError

from api.errors import REQUEST_ID_HEADER, register_exception_handlers
from api.middleware import RequestBodyLimitMiddleware
from api.openapi import install_openapi_contract
from api.routes import (
    admin,
    artifacts,
    audit,
    datasets,
    events,
    gateway,
    identity,
    job_groups,
    registry,
    services,
    system,
    task_artifacts,
    tasks,
    usage,
    workers,
)
from api.services.audit import record_authenticated_write
from api.services.autoscaler import ServiceAutoscaler
from api.services.cleanup import CleanupController
from api.services.control_plane import ControllerSpec, ControlPlane
from api.services.fake_replica_runtime import FakeReplicaRuntimeController
from api.services.gateway import GatewayMetrics, GatewayService
from api.services.kubernetes_replica_runtime import KubernetesReplicaRuntimeController
from api.services.outbox import OutboxDispatcher
from api.services.reaper import Reaper
from api.services.service_health import ServiceHealthController
from api.services.service_reconciler import ServiceReconciler
from api.services.vllm_replica_runtime import VLLMReplicaRuntimeController
from core.config import Settings, get_settings
from core.database import Database
from core.logging import configure_logging, get_logger
from core.metrics import API_REQUEST_DURATION, API_REQUESTS
from core.redis import RedisQueue
from scheduler.global_scheduler import GlobalScheduler
from worker.kubernetes_serving_runtime import KubernetesServingRuntimeAdapter
from worker.vllm_runtime import DockerVLLMRuntimeAdapter


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
    service_reconciler = ServiceReconciler(
        resolved_database,
        batch_size=resolved_settings.batch_size,
        drain_timeout_seconds=resolved_settings.service_drain_timeout,
        kubernetes_drain_timeout_seconds=(resolved_settings.kubernetes_serving_drain_timeout),
    )
    upstream_client = httpx.AsyncClient(
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    gateway_metrics = GatewayMetrics()
    gateway_service = GatewayService(
        resolved_database,
        upstream_client,
        gateway_metrics,
        request_timeout=resolved_settings.service_proxy_timeout,
        connect_timeout=resolved_settings.service_proxy_connect_timeout,
        first_token_timeout=resolved_settings.service_proxy_first_token_timeout,
        max_response_bytes=resolved_settings.service_proxy_max_response_bytes,
        endpoint_host_allowlist=resolved_settings.service_endpoint_host_allowlist,
    )
    service_health = ServiceHealthController(
        resolved_database,
        upstream_client,
        timeout_seconds=resolved_settings.health_check_timeout,
        interval_seconds=resolved_settings.service_health_interval,
        batch_size=resolved_settings.batch_size,
    )
    service_autoscaler = ServiceAutoscaler(
        resolved_database,
        gateway_metrics,
        batch_size=resolved_settings.batch_size,
        scale_to_zero_enabled=resolved_settings.service_scale_to_zero_enabled,
    )
    cleanup = CleanupController(resolved_database, resolved_queue, resolved_settings)
    controllers = [
        ControllerSpec(
            "services",
            service_reconciler.run_once,
            resolved_settings.service_reconcile_interval,
        ),
        ControllerSpec(
            "service-health",
            service_health.run_once,
            resolved_settings.service_health_interval,
        ),
        ControllerSpec(
            "service-autoscaler",
            service_autoscaler.run_once,
            resolved_settings.service_autoscale_interval,
        ),
        ControllerSpec(
            "cleanup",
            cleanup.run_once,
            resolved_settings.cleanup_interval_seconds,
        ),
    ]
    fake_replica_runtime: FakeReplicaRuntimeController | None = None
    if resolved_settings.app_env in {"development", "test"}:
        fake_replica_runtime = FakeReplicaRuntimeController(
            resolved_database,
            app_env=resolved_settings.app_env,
            http_client=upstream_client,
            batch_size=resolved_settings.batch_size,
        )
        controllers.append(
            ControllerSpec(
                "fake-service-runtime",
                fake_replica_runtime.run_once,
                resolved_settings.service_reconcile_interval,
            )
        )
    vllm_replica_runtime: VLLMReplicaRuntimeController | None = None
    if resolved_settings.service_vllm_docker_enabled:
        vllm_replica_runtime = VLLMReplicaRuntimeController(
            resolved_database,
            DockerVLLMRuntimeAdapter(
                cluster_id=resolved_settings.cluster_id,
                endpoint_host=resolved_settings.service_vllm_endpoint_host,
                publish_address=resolved_settings.service_vllm_publish_address,
                cache_volume=resolved_settings.service_vllm_cache_volume,
                pids_limit=resolved_settings.docker_pids_limit,
                tmpfs_size_mb=resolved_settings.docker_tmpfs_size_mb,
                stop_timeout=resolved_settings.docker_stop_timeout,
                always_pull=resolved_settings.docker_always_pull,
            ),
            http_client=upstream_client,
            worker_id=resolved_settings.service_vllm_worker_id,
            batch_size=resolved_settings.batch_size,
            ready_timeout_seconds=resolved_settings.service_vllm_ready_timeout,
            probe_timeout_seconds=resolved_settings.service_vllm_probe_timeout,
            lease_seconds=resolved_settings.service_vllm_lease_seconds,
        )
        controllers.append(
            ControllerSpec(
                "vllm-service-runtime",
                vllm_replica_runtime.run_once,
                resolved_settings.service_reconcile_interval,
            )
        )
    kubernetes_replica_runtime: KubernetesReplicaRuntimeController | None = None
    if (
        resolved_settings.kubernetes_serving_enabled
        and resolved_settings.kubernetes_serving_fake_enabled
    ):
        kubernetes_replica_runtime = KubernetesReplicaRuntimeController(
            resolved_database,
            KubernetesServingRuntimeAdapter(
                namespace=resolved_settings.kubernetes_serving_namespace,
                cluster_id=resolved_settings.kubernetes_serving_cluster_id,
                kubeconfig=resolved_settings.kubernetes_kubeconfig,
                in_cluster=resolved_settings.kubernetes_in_cluster,
                termination_grace_seconds=(
                    resolved_settings.kubernetes_serving_termination_grace_seconds
                ),
                readiness_probe_timeout_seconds=(
                    resolved_settings.kubernetes_serving_probe_timeout
                ),
                readiness_probe_period_seconds=(resolved_settings.kubernetes_serving_poll_interval),
            ),
            app_env=resolved_settings.app_env,
            cluster_id=resolved_settings.kubernetes_serving_cluster_id,
            image=resolved_settings.kubernetes_serving_image,
            fake_enabled=resolved_settings.kubernetes_serving_fake_enabled,
            batch_size=resolved_settings.batch_size,
            startup_timeout_seconds=resolved_settings.kubernetes_serving_startup_timeout,
            drain_timeout_seconds=resolved_settings.kubernetes_serving_drain_timeout,
            poll_interval_seconds=resolved_settings.kubernetes_serving_poll_interval,
            lease_seconds=resolved_settings.kubernetes_serving_lease_seconds,
            failure_backoff_seconds=resolved_settings.kubernetes_serving_failure_backoff,
            termination_grace_seconds=(
                resolved_settings.kubernetes_serving_termination_grace_seconds
            ),
            fake_startup_delay_seconds=(resolved_settings.kubernetes_serving_fake_startup_delay),
            fake_chunk_delay_seconds=resolved_settings.kubernetes_serving_fake_chunk_delay,
        )
        controllers.append(
            ControllerSpec(
                "kubernetes-serving-runtime",
                kubernetes_replica_runtime.run_once,
                resolved_settings.kubernetes_serving_poll_interval,
                startup=kubernetes_replica_runtime.startup,
            )
        )
    if resolved_settings.scheduler_mode == "global":
        global_scheduler = GlobalScheduler(
            resolved_database.session_factory,
            scheduler_id=f"{resolved_settings.cluster_id}:{uuid.uuid4()}",
            lease_seconds=resolved_settings.task_lease_seconds,
            policy=resolved_settings.scheduling_policy,
            aging_interval_seconds=resolved_settings.scheduler_aging_interval_seconds,
            cpu_price_per_hour=resolved_settings.cpu_price_per_hour,
            memory_price_per_gb_hour=resolved_settings.memory_price_per_gb_hour,
            gpu_price_per_hour=resolved_settings.gpu_price_per_hour,
            preemption_enabled=resolved_settings.scheduler_preemption_enabled,
            preemption_min_delta=resolved_settings.scheduler_preemption_min_delta,
        )
        controllers.append(
            ControllerSpec(
                "scheduler",
                global_scheduler.run_once,
                resolved_settings.scheduler_poll_interval,
            )
        )
    control = ControlPlane(
        OutboxDispatcher(
            resolved_database, resolved_queue, batch_size=resolved_settings.batch_size
        ),
        Reaper(resolved_database, resolved_settings),
        resolved_settings,
        controllers=controllers,
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
            if fake_replica_runtime is not None:
                await fake_replica_runtime.close()
            if vllm_replica_runtime is not None:
                await vllm_replica_runtime.close()
            if kubernetes_replica_runtime is not None:
                await kubernetes_replica_runtime.close()
            await upstream_client.aclose()
            await resolved_queue.close()
            await resolved_database.dispose()

    app = FastAPI(
        title="Mini AI Cloud",
        version="0.2.0",
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.database = resolved_database
    app.state.queue = resolved_queue
    app.state.control_plane = control
    app.state.gateway_service = gateway_service
    app.state.fake_replica_runtime = fake_replica_runtime
    app.state.vllm_replica_runtime = vllm_replica_runtime
    app.state.kubernetes_replica_runtime = kubernetes_replica_runtime
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=resolved_settings.api_request_max_bytes,
    )
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
        started = time.monotonic()
        status_code = 500
        try:
            response = await call_next(request)
            await record_authenticated_write(request, response)
            status_code = response.status_code
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            route = request.scope.get("route")
            route_template = getattr(route, "path", "unmatched")
            API_REQUESTS.labels(request.method, route_template, str(status_code)).inc()
            API_REQUEST_DURATION.labels(request.method, route_template).observe(
                time.monotonic() - started
            )
            structlog.contextvars.clear_contextvars()

    app.include_router(system.router)
    app.include_router(admin.router)
    app.include_router(identity.router)
    app.include_router(tasks.router)
    app.include_router(task_artifacts.router)
    app.include_router(workers.router)
    app.include_router(services.router)
    app.include_router(registry.router)
    app.include_router(usage.router)
    app.include_router(artifacts.router)
    app.include_router(gateway.router)
    app.include_router(job_groups.router)
    app.include_router(audit.router)
    app.include_router(datasets.router)
    app.include_router(events.router)
    install_openapi_contract(app)
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    main()
