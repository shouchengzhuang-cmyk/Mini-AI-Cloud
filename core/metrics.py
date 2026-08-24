from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

API_REQUESTS = Counter(
    "api_requests_total",
    "HTTP requests by route template and status",
    ("method", "route", "status"),
    registry=REGISTRY,
)
API_REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "HTTP request latency by route template",
    ("method", "route"),
    registry=REGISTRY,
)

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
SCHEDULING_ATTEMPTS = Counter(
    "scheduling_attempts_total",
    "Scheduler placement outcomes by bounded reason",
    ("outcome", "reason"),
    registry=REGISTRY,
)
SCHEDULER_ATTEMPTS = Counter(
    "scheduler_attempts_total",
    "Global scheduler placement outcomes by bounded reason",
    ("outcome", "reason"),
    registry=REGISTRY,
)
SCHEDULER_FAILURES = Counter(
    "scheduler_failures_total",
    "Unexpected global scheduler control-loop failures",
    registry=REGISTRY,
)
SCHEDULER_LATENCY = Histogram(
    "scheduler_latency_seconds",
    "Global scheduler control-loop latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
    registry=REGISTRY,
)
TASK_PREEMPTIONS = Counter(
    "task_preemptions_total",
    "Durable task preemption requests dispatched",
    registry=REGISTRY,
)
OUTBOX_PENDING = Gauge(
    "outbox_pending", "Unprocessed transactional outbox events", registry=REGISTRY
)
OUTBOX_OLDEST_AGE = Gauge(
    "outbox_oldest_age_seconds", "Age of the oldest available outbox event", registry=REGISTRY
)
WORKER_ALLOCATED = Gauge(
    "worker_allocated_resources",
    "Cluster-wide allocated resources",
    ("resource",),
    registry=REGISTRY,
)
WORKER_CAPACITY_CPU = Gauge(
    "worker_capacity_cpu",
    "Schedulable worker CPU capacity in millicores",
    registry=REGISTRY,
)
WORKER_CAPACITY_MEMORY = Gauge(
    "worker_capacity_memory",
    "Schedulable worker memory capacity in MiB",
    registry=REGISTRY,
)
WORKER_CAPACITY_GPU = Gauge(
    "worker_capacity_gpu",
    "Schedulable worker GPU device capacity",
    registry=REGISTRY,
)
WORKER_ALLOCATED_CPU = Gauge(
    "worker_allocated_cpu",
    "Cluster CPU reservations in millicores",
    registry=REGISTRY,
)
WORKER_ALLOCATED_MEMORY = Gauge(
    "worker_allocated_memory",
    "Cluster memory reservations in MiB",
    registry=REGISTRY,
)
WORKER_ALLOCATED_GPU = Gauge(
    "worker_allocated_gpu",
    "Cluster GPU device reservations",
    registry=REGISTRY,
)
SERVICE_REPLICAS = Gauge(
    "service_replicas",
    "Aggregate model service replicas",
    ("state",),
    registry=REGISTRY,
)
SERVICES_READY = Gauge(
    "services_ready",
    "Model services with a running aggregate state",
    registry=REGISTRY,
)
GATEWAY_REQUESTS = Counter(
    "gateway_requests_total",
    "OpenAI-compatible gateway outcomes",
    ("status",),
    registry=REGISTRY,
)
GATEWAY_DURATION = Histogram(
    "gateway_request_duration_seconds",
    "OpenAI-compatible gateway upstream latency",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
    registry=REGISTRY,
)
GATEWAY_IN_FLIGHT = Gauge(
    "gateway_requests_in_flight",
    "OpenAI-compatible requests currently being proxied",
    registry=REGISTRY,
)
GATEWAY_ERRORS = Counter(
    "gateway_errors_total",
    "OpenAI-compatible gateway failures by bounded serving error code",
    ("code",),
    registry=REGISTRY,
)
GATEWAY_TTFT = Histogram(
    "gateway_time_to_first_token_seconds",
    "Time from gateway request handling to the first non-empty upstream response chunk",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
    registry=REGISTRY,
)
GATEWAY_TOKENS = Counter(
    "gateway_tokens_total",
    "Tokens reported by inference upstreams",
    ("type",),
    registry=REGISTRY,
)
REPLICA_ACTIVE_REQUESTS = Gauge(
    "replica_active_requests",
    "Requests currently routed to a model service replica",
    ("service_id", "replica_id"),
    registry=REGISTRY,
)
REPLICA_HEALTH = Gauge(
    "replica_health",
    "Current model replica health as a one-hot state series",
    ("service_id", "replica_id", "health"),
    registry=REGISTRY,
)
SERVICE_REQUESTS = Counter(
    "service_requests_total",
    "OpenAI-compatible service proxy outcomes",
    ("status",),
    registry=REGISTRY,
)
SERVICE_REQUEST_DURATION = Histogram(
    "service_request_duration_seconds",
    "OpenAI-compatible service proxy upstream latency",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
    registry=REGISTRY,
)
K8S_SERVING_PODS = Gauge(
    "k8s_serving_pods",
    "Kubernetes serving Pods by bounded lifecycle state",
    ("state",),
    registry=REGISTRY,
)
K8S_SERVING_LAUNCHES = Counter(
    "k8s_serving_launch_total",
    "Kubernetes serving Pod launch outcomes",
    ("outcome",),
    registry=REGISTRY,
)
K8S_SERVING_LAUNCH_FAILURES = Counter(
    "k8s_serving_launch_failures_total",
    "Kubernetes serving Pod launch failures by bounded reason",
    ("reason",),
    registry=REGISTRY,
)
K8S_SERVING_REPLACEMENTS = Counter(
    "k8s_serving_replacements_total",
    "Kubernetes serving replica replacements by bounded reason",
    ("reason",),
    registry=REGISTRY,
)
K8S_SERVING_RECONCILE_DURATION = Histogram(
    "k8s_serving_reconcile_duration_seconds",
    "Kubernetes serving controller reconciliation latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
    registry=REGISTRY,
)
PROJECT_CPU_SECONDS = Counter(
    "project_cpu_seconds_total",
    "Settled CPU seconds across all projects",
    registry=REGISTRY,
)
PROJECT_GPU_SECONDS = Counter(
    "project_gpu_seconds_total",
    "Settled GPU seconds across all projects",
    registry=REGISTRY,
)


def render_metrics() -> bytes:
    return generate_latest(REGISTRY)
