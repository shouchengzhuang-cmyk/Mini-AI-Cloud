from enum import StrEnum


class AcceleratorVendor(StrEnum):
    NVIDIA = "nvidia"
    HUAWEI_ASCEND = "huawei-ascend"


class AcceleratorKind(StrEnum):
    GPU = "gpu"
    NPU = "npu"


class AcceleratorSelectionPolicy(StrEnum):
    ANY = "any"
    NVIDIA_ONLY = "nvidia-only"
    ASCEND_ONLY = "ascend-only"
    PREFER_NVIDIA = "prefer-nvidia"
    PREFER_ASCEND = "prefer-ascend"


class AllocationAuthority(StrEnum):
    CONTROL_PLANE_EXACT_DEVICE = "control_plane_exact_device"
    KUBERNETES_DEVICE_PLUGIN = "kubernetes_device_plugin"


class ModelAvailabilityStatus(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"
    DISABLED = "disabled"


class TaskStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    SCHEDULING = "scheduling"
    ASSIGNED = "assigned"
    PREPARING = "preparing"
    PULLING = "pulling"
    STARTING = "starting"
    RUNNING = "running"
    PREEMPTING = "preempting"
    PREEMPTED = "preempted"
    STOPPING = "stopping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    RETRYING = "retrying"


class WorkerStatus(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"
    DRAINING = "draining"


class LogStream(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"
    SYSTEM = "system"


FINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMED_OUT,
        TaskStatus.PREEMPTED,
    }
)

ACTIVE_TASK_STATUSES = frozenset(
    {
        TaskStatus.SCHEDULING,
        TaskStatus.ASSIGNED,
        TaskStatus.PREPARING,
        TaskStatus.PULLING,
        TaskStatus.STARTING,
        TaskStatus.RUNNING,
        TaskStatus.PREEMPTING,
        TaskStatus.STOPPING,
    }
)


class RuntimeType(StrEnum):
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    FAKE = "fake"


class WorkloadType(StrEnum):
    BATCH_JOB = "batch_job"
    MODEL_SERVICE = "model_service"


class RetryBackoff(StrEnum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


class ErrorCategory(StrEnum):
    USER_ERROR = "USER_ERROR"
    INFRA_ERROR = "INFRA_ERROR"
    RESOURCE_ERROR = "RESOURCE_ERROR"
    TIMEOUT = "TIMEOUT"
    PREEMPTED = "PREEMPTED"
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ErrorCode(StrEnum):
    IMAGE_PULL_FAILED = "IMAGE_PULL_FAILED"
    CONTAINER_START_FAILED = "CONTAINER_START_FAILED"
    OOM_KILLED = "OOM_KILLED"
    GPU_UNAVAILABLE = "GPU_UNAVAILABLE"
    WORKER_LOST = "WORKER_LOST"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    MODEL_LOAD_FAILED = "MODEL_LOAD_FAILED"
    MODEL_LOAD_TIMEOUT = "MODEL_LOAD_TIMEOUT"
    REPLICA_UNHEALTHY = "REPLICA_UNHEALTHY"
    NO_HEALTHY_REPLICA = "NO_HEALTHY_REPLICA"
    UPSTREAM_CONNECT_TIMEOUT = "UPSTREAM_CONNECT_TIMEOUT"
    INFERENCE_REQUEST_TIMEOUT = "INFERENCE_REQUEST_TIMEOUT"
    UPSTREAM_DISCONNECTED = "UPSTREAM_DISCONNECTED"
    GPU_ALLOCATION_FAILED = "GPU_ALLOCATION_FAILED"


class ProjectRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"
