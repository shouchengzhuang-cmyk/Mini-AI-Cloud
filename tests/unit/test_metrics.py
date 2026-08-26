import time

from api.services.gateway import _observe_gateway
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
        "project_cpu_seconds_total",
        "project_gpu_seconds_total",
    }
    for name in expected_names:
        assert name in metrics
    assert "task_id=" not in metrics
    assert "user_id=" not in metrics
