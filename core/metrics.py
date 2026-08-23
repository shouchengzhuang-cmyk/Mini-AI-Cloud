from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

TASKS_CREATED = Counter(
    "tasks_created_total", "Number of tasks accepted by the API", registry=REGISTRY
)
TASKS_SUCCEEDED = Counter(
    "tasks_succeeded_total", "Number of tasks that succeeded", registry=REGISTRY
)
TASKS_FAILED = Counter(
    "tasks_failed_total", "Number of tasks that exhausted retries", registry=REGISTRY
)
TASKS_CANCELLED = Counter("tasks_cancelled_total", "Number of tasks cancelled", registry=REGISTRY)
TASKS_RUNNING = Gauge("tasks_running", "Currently running tasks", registry=REGISTRY)
TASKS_QUEUED = Gauge("tasks_queued", "Currently queued tasks", registry=REGISTRY)
WORKERS_ONLINE = Gauge("workers_online", "Currently online workers", registry=REGISTRY)
TASK_DURATION = Histogram(
    "task_duration_seconds",
    "Task wall-clock execution duration",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600),
    registry=REGISTRY,
)
TASK_QUEUE_WAIT = Histogram(
    "task_queue_wait_seconds",
    "Time from enqueue to assignment",
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300),
    registry=REGISTRY,
)


def render_metrics() -> bytes:
    return generate_latest(REGISTRY)
