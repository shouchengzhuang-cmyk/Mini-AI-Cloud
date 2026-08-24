import time
import uuid

from api.services.gateway import GatewayMetrics, _observe_gateway
from core.metrics import SCHEDULER_ATTEMPTS, render_metrics


def test_phase_two_metric_contract_uses_bounded_labels() -> None:
    SCHEDULER_ATTEMPTS.labels("rejected", "insufficient_memory").inc(0)
    _observe_gateway("success", time.monotonic())

    metrics = render_metrics().decode("utf-8")
    expected_names = {
        "scheduler_attempts_total",
        "scheduler_failures_total",
        "scheduler_latency_seconds",
        "task_preemptions_total",
        "worker_capacity_cpu",
        "worker_capacity_memory",
        "worker_capacity_gpu",
        "worker_allocated_cpu",
        "worker_allocated_memory",
        "worker_allocated_gpu",
        "services_ready",
        "service_requests_total",
        "service_request_duration_seconds",
        "gateway_requests_total",
        "gateway_request_duration_seconds",
        "gateway_requests_in_flight",
        "gateway_errors_total",
        "gateway_time_to_first_token_seconds",
        "gateway_tokens_total",
        "replica_active_requests",
        "replica_health",
        "project_cpu_seconds_total",
        "project_gpu_seconds_total",
    }
    for name in expected_names:
        assert name in metrics
    assert "task_id=" not in metrics
    assert "user_id=" not in metrics


async def test_gateway_metrics_export_replica_concurrency_without_request_labels() -> None:
    service_id = uuid.uuid4()
    replica_id = uuid.uuid4()
    source = GatewayMetrics()

    await source.request_started(service_id, replica_id)
    load = await source.replica_snapshot(service_id, replica_id)
    metrics = render_metrics().decode("utf-8")

    assert load is not None and load.active_requests == 1
    replica_series = next(
        line
        for line in metrics.splitlines()
        if line.startswith("replica_active_requests{") and f'replica_id="{replica_id}"' in line
    )
    assert f'service_id="{service_id}"' in replica_series
    assert replica_series.endswith(" 1.0")

    await source.request_finished(service_id, replica_id)
    released = await source.replica_snapshot(service_id, replica_id)
    assert released is not None and released.active_requests == 0
