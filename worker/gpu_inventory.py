import asyncio
import csv
import io
import re
import subprocess
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from inspect import isawaitable
from typing import Any, Protocol

from kubernetes_asyncio import client, config

from core.accelerators import AcceleratorDevice, kind_for_vendor
from core.config import Settings
from core.enums import AcceleratorKind, AcceleratorVendor
from core.runtime_profiles import (
    KubernetesNodeSelectorRequirement,
    RuntimeProfile,
    RuntimeProfileCatalog,
    runtime_profile_binding_id,
)

MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024
MAX_INVENTORY_ROWS = 256


class InventoryStatus(StrEnum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class InventoryProviderResult:
    provider: str
    status: InventoryStatus
    devices: tuple[AcceleratorDevice, ...] = ()
    message: str | None = None
    rejected_rows: int = 0


@dataclass(frozen=True, slots=True)
class InventorySnapshot:
    devices: tuple[AcceleratorDevice, ...]
    provider_results: tuple[InventoryProviderResult, ...]

    @property
    def status(self) -> InventoryStatus:
        if not self.provider_results:
            return InventoryStatus.UNAVAILABLE
        statuses = {result.status for result in self.provider_results}
        if statuses == {InventoryStatus.UNAVAILABLE}:
            return InventoryStatus.UNAVAILABLE
        if InventoryStatus.DEGRADED in statuses or InventoryStatus.UNAVAILABLE in statuses:
            return InventoryStatus.DEGRADED
        return InventoryStatus.AVAILABLE


class DeviceInventory(Protocol):
    def list_devices(self) -> tuple[AcceleratorDevice, ...]: ...


class AcceleratorInventoryProvider(DeviceInventory, Protocol):
    name: str

    def discover(self) -> InventoryProviderResult: ...

    def list_devices(self) -> tuple[AcceleratorDevice, ...]: ...


class KubernetesNodeReader(Protocol):
    def __call__(
        self,
        *,
        node_name: str,
        kubeconfig: str | None,
        in_cluster: bool,
        request_timeout: float,
    ) -> Awaitable[Mapping[str, object]]: ...


class KubernetesPodReader(Protocol):
    def __call__(
        self,
        *,
        node_name: str,
        kubeconfig: str | None,
        in_cluster: bool,
        request_timeout: float,
    ) -> Awaitable[tuple[Mapping[str, object], ...]]: ...


Runner = Callable[..., subprocess.CompletedProcess[Any]]


@dataclass(frozen=True, slots=True)
class _CommandResult:
    output: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class _ParseResult:
    devices: tuple[AcceleratorDevice, ...]
    rejected_rows: int = 0


class _InventoryProviderBase:
    name: str

    def discover(self) -> InventoryProviderResult:
        raise NotImplementedError

    def list_devices(self) -> tuple[AcceleratorDevice, ...]:
        return self.discover().devices


def _run_text_command(
    runner: Runner,
    command: list[str],
    *,
    timeout: float,
) -> _CommandResult:
    try:
        completed = runner(
            command,
            check=False,
            capture_output=True,
            text=False,
            timeout=timeout,
        )
    except FileNotFoundError:
        return _CommandResult(None, "command_not_found")
    except subprocess.TimeoutExpired:
        return _CommandResult(None, "command_timeout")
    except (OSError, subprocess.SubprocessError):
        return _CommandResult(None, "command_failed")

    if completed.returncode != 0:
        return _CommandResult(None, "command_failed")
    raw_output = completed.stdout
    try:
        output = (
            raw_output.decode("utf-8", errors="strict")
            if isinstance(raw_output, bytes)
            else raw_output
        )
    except UnicodeDecodeError:
        return _CommandResult(None, "invalid_utf8")
    if not isinstance(output, str):
        return _CommandResult(None, "invalid_output_type")
    if len(output.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES:
        return _CommandResult(None, "output_too_large")
    return _CommandResult(output, None)


def parse_nvidia_smi_csv(output: str) -> _ParseResult:
    devices: list[AcceleratorDevice] = []
    rejected_rows = 0
    seen_ids: set[str] = set()
    for row_number, row in enumerate(
        csv.reader(io.StringIO(output), skipinitialspace=True), start=1
    ):
        if row_number > MAX_INVENTORY_ROWS:
            rejected_rows += 1
            break
        if not row:
            continue
        if len(row) != len(NvidiaSMIInventoryProvider.QUERY_FIELDS):
            rejected_rows += 1
            continue
        raw_uuid, raw_index, raw_model, raw_total, raw_free, raw_arch = (
            item.strip() for item in row
        )
        try:
            index = int(raw_index)
            memory_total_mb = int(raw_total)
            memory_free_mb = int(raw_free)
        except ValueError:
            rejected_rows += 1
            continue
        if (
            not raw_uuid
            or raw_uuid in seen_ids
            or not raw_model
            or index < 0
            or memory_total_mb <= 0
        ):
            rejected_rows += 1
            continue
        seen_ids.add(raw_uuid)
        devices.append(
            AcceleratorDevice(
                device_id=raw_uuid,
                device_index=index,
                vendor=AcceleratorVendor.NVIDIA,
                kind=AcceleratorKind.GPU,
                model=raw_model,
                memory_total_mb=memory_total_mb,
                memory_free_mb=min(memory_total_mb, max(0, memory_free_mb)),
                compute_arch=raw_arch if raw_arch and raw_arch != "N/A" else None,
            )
        )
    return _ParseResult(
        tuple(sorted(devices, key=lambda device: (device.device_index, device.device_id))),
        rejected_rows,
    )


class NvidiaSMIInventoryProvider(_InventoryProviderBase):
    """Discover NVIDIA devices without importing CUDA or NVML into the control plane."""

    name = "nvidia-smi"
    QUERY_FIELDS = (
        "uuid",
        "index",
        "name",
        "memory.total",
        "memory.free",
        "compute_cap",
    )

    def __init__(self, *, runner: Runner | None = None, timeout: float = 5.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._runner = runner or subprocess.run
        self.timeout = timeout

    def discover(self) -> InventoryProviderResult:
        command = [
            "nvidia-smi",
            f"--query-gpu={','.join(self.QUERY_FIELDS)}",
            "--format=csv,noheader,nounits",
        ]
        command_result = _run_text_command(self._runner, command, timeout=self.timeout)
        if command_result.error is not None:
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.UNAVAILABLE,
                message=command_result.error,
            )
        parsed = parse_nvidia_smi_csv(command_result.output or "")
        status = InventoryStatus.DEGRADED if parsed.rejected_rows else InventoryStatus.AVAILABLE
        return InventoryProviderResult(
            provider=self.name,
            status=status,
            devices=parsed.devices,
            message=(
                f"rejected {parsed.rejected_rows} malformed inventory row(s)"
                if parsed.rejected_rows
                else None
            ),
            rejected_rows=parsed.rejected_rows,
        )


@dataclass(frozen=True, slots=True)
class AscendMappingEntry:
    npu_id: int
    chip_id: int
    logic_id: int
    model: str


@dataclass(frozen=True, slots=True)
class AscendMappingResult:
    entries: tuple[AscendMappingEntry, ...]
    rejected_rows: int = 0


def _normalized_column(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _table_cells(line: str) -> list[str]:
    stripped = line.strip().strip("|").strip()
    if not stripped or set(stripped) <= {"+", "-", "="}:
        return []
    if "|" in line:
        return [cell.strip() for cell in stripped.split("|")]
    return [cell.strip() for cell in re.split(r"\s{2,}", stripped) if cell.strip()]


def parse_ascend_mapping(output: str) -> AscendMappingResult:
    required_columns = {"npuid", "chipid", "chiplogicid", "chipname"}
    column_indexes: dict[str, int] | None = None
    entries: list[AscendMappingEntry] = []
    rejected_rows = 0
    seen_ids: set[tuple[int, int]] = set()
    for row_number, line in enumerate(output.splitlines(), start=1):
        if row_number > MAX_INVENTORY_ROWS:
            rejected_rows += 1
            break
        cells = _table_cells(line)
        if not cells:
            continue
        normalized = [_normalized_column(cell) for cell in cells]
        if required_columns <= set(normalized):
            column_indexes = {name: normalized.index(name) for name in required_columns}
            continue
        if column_indexes is None:
            continue
        if max(column_indexes.values()) >= len(cells):
            rejected_rows += 1
            continue
        model = cells[column_indexes["chipname"]].strip()
        if model.casefold() == "mcu":
            continue
        try:
            npu_id = int(cells[column_indexes["npuid"]])
            chip_id = int(cells[column_indexes["chipid"]])
            logic_id = int(cells[column_indexes["chiplogicid"]])
        except ValueError:
            rejected_rows += 1
            continue
        identity = (npu_id, chip_id)
        if npu_id < 0 or chip_id < 0 or logic_id < 0 or not model:
            rejected_rows += 1
            continue
        if identity in seen_ids:
            rejected_rows += 1
            continue
        seen_ids.add(identity)
        entries.append(
            AscendMappingEntry(
                npu_id=npu_id,
                chip_id=chip_id,
                logic_id=logic_id,
                model=model,
            )
        )
    return AscendMappingResult(
        tuple(sorted(entries, key=lambda entry: (entry.logic_id, entry.npu_id, entry.chip_id))),
        rejected_rows,
    )


def parse_ascend_memory(output: str) -> dict[tuple[int, int], tuple[int, int]]:
    current_npu_id: int | None = None
    current_chip_id: int | None = None
    values: dict[tuple[int, int], dict[str, int]] = {}
    for line in output.splitlines()[:MAX_INVENTORY_ROWS]:
        if ":" not in line:
            continue
        raw_key, raw_value = line.split(":", 1)
        key = _normalized_column(raw_key)
        value_match = re.search(r"-?\d+", raw_value)
        if value_match is None:
            continue
        value = int(value_match.group())
        if key == "npuid":
            current_npu_id = value
            current_chip_id = None
            continue
        if key == "chipid":
            current_chip_id = value
            if current_npu_id is not None:
                values.setdefault((current_npu_id, current_chip_id), {})
            continue
        if current_npu_id is None or current_chip_id is None:
            continue
        record = values.setdefault((current_npu_id, current_chip_id), {})
        if key in {"totalcapacitymb", "totalmemorymb", "hbmtotalmb"}:
            record["total"] = value
        elif key in {"capacitymb", "freememorymb", "hbmfreemb"}:
            record["free"] = value

    memory: dict[tuple[int, int], tuple[int, int]] = {}
    for identity, record in values.items():
        total = record.get("total")
        free = record.get("free")
        if total is not None and free is not None and total > 0 and 0 <= free <= total:
            memory[identity] = (total, free)
    return memory


class AscendNpuSMIInventoryProvider(_InventoryProviderBase):
    """Discover Ascend devices through bounded, version-tolerant npu-smi queries."""

    name = "ascend-npu-smi"

    def __init__(self, *, runner: Runner | None = None, timeout: float = 5.0) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self._runner = runner or subprocess.run
        self.timeout = timeout

    def discover(self) -> InventoryProviderResult:
        mapping_result = _run_text_command(
            self._runner, ["npu-smi", "info", "-m"], timeout=self.timeout
        )
        if mapping_result.error is not None:
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.UNAVAILABLE,
                message=mapping_result.error,
            )
        parsed_mapping = parse_ascend_mapping(mapping_result.output or "")
        if not parsed_mapping.entries:
            return InventoryProviderResult(
                provider=self.name,
                status=(
                    InventoryStatus.DEGRADED
                    if parsed_mapping.rejected_rows
                    else InventoryStatus.AVAILABLE
                ),
                message=(
                    "npu-smi mapping output contained no usable accelerator rows"
                    if parsed_mapping.rejected_rows
                    else None
                ),
                rejected_rows=parsed_mapping.rejected_rows,
            )

        memory_by_device: dict[tuple[int, int], tuple[int, int]] = {}
        failed_queries = 0
        for npu_id in sorted({entry.npu_id for entry in parsed_mapping.entries}):
            memory_result = _run_text_command(
                self._runner,
                ["npu-smi", "info", "-t", "memory", "-i", str(npu_id)],
                timeout=self.timeout,
            )
            if memory_result.error is not None:
                failed_queries += 1
                continue
            memory_by_device.update(parse_ascend_memory(memory_result.output or ""))

        devices: list[AcceleratorDevice] = []
        rejected_rows = parsed_mapping.rejected_rows
        seen_indexes: set[int] = set()
        for entry in parsed_mapping.entries:
            memory = memory_by_device.get((entry.npu_id, entry.chip_id))
            if memory is None or entry.logic_id in seen_indexes:
                rejected_rows += 1
                continue
            seen_indexes.add(entry.logic_id)
            total_memory_mb, free_memory_mb = memory
            devices.append(
                AcceleratorDevice(
                    device_id=f"ASCEND-{entry.npu_id}-{entry.chip_id}",
                    device_index=entry.logic_id,
                    vendor=AcceleratorVendor.HUAWEI_ASCEND,
                    kind=AcceleratorKind.NPU,
                    model=entry.model,
                    memory_total_mb=total_memory_mb,
                    memory_free_mb=free_memory_mb,
                    health="unknown",
                    compute_arch=entry.model,
                )
            )
        status = (
            InventoryStatus.DEGRADED
            if rejected_rows or failed_queries
            else InventoryStatus.AVAILABLE
        )
        details: list[str] = []
        if rejected_rows:
            details.append(f"rejected {rejected_rows} incomplete or malformed device row(s)")
        if failed_queries:
            details.append(f"{failed_queries} memory query or queries failed")
        return InventoryProviderResult(
            provider=self.name,
            status=status,
            devices=tuple(sorted(devices, key=lambda device: device.device_index)),
            message="; ".join(details) or None,
            rejected_rows=rejected_rows,
        )


def _parse_memory_label(value: object) -> int | None:
    match = re.fullmatch(r"\s*(\d+)\s*(Mi|M|Gi|G)?\s*", str(value), re.IGNORECASE)
    if match is None:
        return None
    amount = int(match.group(1))
    unit = (match.group(2) or "Mi").casefold()
    if unit in {"gi", "g"}:
        amount *= 1024
    return amount if amount > 0 else None


def _kubernetes_resource_contract(
    resource_name: str,
) -> tuple[AcceleratorVendor, AcceleratorKind] | None:
    if resource_name == "nvidia.com/gpu":
        return AcceleratorVendor.NVIDIA, AcceleratorKind.GPU
    if not resource_name.startswith("huawei.com/"):
        return None
    suffix = resource_name.removeprefix("huawei.com/")
    lowered = suffix.casefold()
    excluded_markers = (
        "memory",
        "core",
        "unhealthy",
        "recover",
        "fault",
        "network",
    )
    if any(marker in lowered for marker in excluded_markers):
        return None
    if lowered == "npu" or lowered.startswith("ascend"):
        return AcceleratorVendor.HUAWEI_ASCEND, AcceleratorKind.NPU
    return None


def _parse_kubernetes_node_taints(
    value: object,
) -> tuple[tuple[str, str | None, str], ...] | None:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > 64:
        return None
    taints: list[tuple[str, str | None, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            return None
        key = item.get("key")
        effect = item.get("effect")
        raw_value = item.get("value")
        if (
            not isinstance(key, str)
            or not key
            or key != key.strip()
            or len(key) > 253
            or any(character.isspace() or ord(character) < 32 for character in key)
            or not isinstance(effect, str)
            or effect not in {"NoSchedule", "PreferNoSchedule", "NoExecute"}
            or (raw_value is not None and not isinstance(raw_value, str))
        ):
            return None
        taint_value = raw_value if isinstance(raw_value, str) else None
        if taint_value is not None and (
            taint_value != taint_value.strip()
            or len(taint_value) > 63
            or any(character.isspace() or ord(character) < 32 for character in taint_value)
        ):
            return None
        taints.append((key, taint_value, str(effect)))
    return tuple(sorted(taints, key=lambda item: (item[0], item[2], item[1] or "")))


def parse_kubernetes_node(node: Mapping[str, object]) -> _ParseResult:
    metadata = node.get("metadata")
    spec = node.get("spec")
    status = node.get("status")
    if (
        not isinstance(metadata, Mapping)
        or not isinstance(spec, Mapping)
        or not isinstance(status, Mapping)
    ):
        return _ParseResult((), 1)
    node_taints = _parse_kubernetes_node_taints(spec.get("taints"))
    if node_taints is None:
        return _ParseResult((), 1)
    unschedulable = spec.get("unschedulable")
    if unschedulable is not None and unschedulable is not False:
        return _ParseResult((), 1)
    conditions = status.get("conditions")
    if not isinstance(conditions, list) or not any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    ):
        return _ParseResult((), 1)
    node_uid = str(metadata.get("uid") or "").strip()
    labels = metadata.get("labels")
    labels = labels if isinstance(labels, Mapping) else {}
    node_labels = tuple(
        sorted(
            (key, value)
            for key, value in labels.items()
            if isinstance(key, str) and isinstance(value, str)
        )
    )
    allocatable = status.get("allocatable")
    if not node_uid or not isinstance(allocatable, Mapping):
        return _ParseResult((), 1)

    devices: list[AcceleratorDevice] = []
    rejected_rows = 0
    next_index = 0
    for resource_name in sorted(str(name) for name in allocatable):
        contract = _kubernetes_resource_contract(resource_name)
        if contract is None:
            continue
        raw_count = allocatable.get(resource_name)
        try:
            count = int(str(raw_count))
        except ValueError:
            rejected_rows += 1
            continue
        if count <= 0:
            continue
        remaining_slots = MAX_INVENTORY_ROWS - len(devices)
        if count > remaining_slots:
            rejected_rows += count - remaining_slots
            count = remaining_slots
        vendor, kind = contract
        if vendor == AcceleratorVendor.NVIDIA:
            model = str(labels.get("nvidia.com/gpu.product") or "NVIDIA GPU")
            memory_mb = _parse_memory_label(labels.get("nvidia.com/gpu.memory"))
            compute_arch = str(labels.get("nvidia.com/gpu.compute.major") or "").strip() or None
        else:
            model = str(
                labels.get("node.kubernetes.io/npu.chip.name")
                or labels.get("accelerator")
                or resource_name.removeprefix("huawei.com/")
            )
            memory_mb = _parse_memory_label(labels.get("mind-cluster/npu-chip-memory"))
            compute_arch = str(labels.get("node.kubernetes.io/npu.chip.name") or "").strip() or None
        if memory_mb is None:
            rejected_rows += count
            continue
        for slot in range(count):
            devices.append(
                AcceleratorDevice(
                    device_id=f"k8s-capacity:{node_uid}:{resource_name}:{slot}",
                    device_index=next_index,
                    vendor=vendor,
                    kind=kind,
                    model=model,
                    memory_total_mb=memory_mb,
                    memory_free_mb=memory_mb,
                    health="inventory-only",
                    compute_arch=compute_arch,
                    capabilities=("kubernetes-capacity-slot",),
                    kubernetes_resource_name=resource_name,
                    kubernetes_node_labels=node_labels,
                    kubernetes_node_taints=node_taints,
                )
            )
            next_index += 1
    return _ParseResult(tuple(devices), rejected_rows)


_MANAGED_LABEL = "mini-ai-cloud/managed"
_CLUSTER_ID_LABEL = "mini-ai-cloud/cluster-id"
_WORKER_ID_LABEL = "mini-ai-cloud/worker-id"


def _database_accounted_managed_pod(
    labels: Mapping[object, object],
    *,
    cluster_id: str | None,
    worker_id: str | None,
) -> bool:
    if labels.get(_MANAGED_LABEL) != "true":
        return False
    return bool(
        (cluster_id is not None and labels.get(_CLUSTER_ID_LABEL) == cluster_id)
        or (worker_id is not None and labels.get(_WORKER_ID_LABEL) == worker_id)
    )


def _resource_quantity(resources: Mapping[object, object], resource_name: str) -> int:
    raw_value = resources.get(resource_name)
    try:
        value = int(str(raw_value))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid Kubernetes accelerator resource quantity") from exc
    if value < 0:
        raise ValueError("Kubernetes accelerator resource quantity must not be negative")
    return value


def _container_accelerator_requests(container: object) -> dict[str, int]:
    if not isinstance(container, Mapping):
        raise ValueError("invalid Kubernetes Pod container")
    resources = container.get("resources")
    if resources is None:
        return {}
    if not isinstance(resources, Mapping):
        raise ValueError("invalid Kubernetes Pod resources")
    raw_requests = resources.get("requests")
    raw_limits = resources.get("limits")
    requests = raw_requests if isinstance(raw_requests, Mapping) else {}
    limits = raw_limits if isinstance(raw_limits, Mapping) else {}
    resource_names = {
        name
        for name in (*requests.keys(), *limits.keys())
        if isinstance(name, str) and _kubernetes_resource_contract(name) is not None
    }
    result: dict[str, int] = {}
    for resource_name in resource_names:
        source = requests if resource_name in requests else limits
        result[resource_name] = _resource_quantity(source, resource_name)
    return result


def _external_pod_accelerator_requests(
    pods: Iterable[Mapping[str, object]],
    *,
    node_name: str,
    cluster_id: str | None,
    worker_id: str | None,
) -> dict[str, int]:
    requested: dict[str, int] = {}
    for pod in pods:
        metadata = pod.get("metadata")
        spec = pod.get("spec")
        status = pod.get("status")
        if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
            raise ValueError("invalid Kubernetes Pod object")
        if spec.get("nodeName") != node_name:
            continue
        if isinstance(status, Mapping) and status.get("phase") in {"Succeeded", "Failed"}:
            continue
        labels = metadata.get("labels")
        labels = labels if isinstance(labels, Mapping) else {}
        if _database_accounted_managed_pod(
            labels,
            cluster_id=cluster_id,
            worker_id=worker_id,
        ):
            continue

        application_requests: dict[str, int] = {}
        containers = spec.get("containers", [])
        if not isinstance(containers, list):
            raise ValueError("invalid Kubernetes Pod container list")
        for container in containers:
            for resource_name, quantity in _container_accelerator_requests(container).items():
                application_requests[resource_name] = (
                    application_requests.get(resource_name, 0) + quantity
                )

        restartable_init_requests: dict[str, int] = {}
        init_stage_requests: dict[str, int] = {}
        init_containers = spec.get("initContainers", [])
        if not isinstance(init_containers, list):
            raise ValueError("invalid Kubernetes Pod init container list")
        for container in init_containers:
            container_requests = _container_accelerator_requests(container)
            restart_policy = (
                container.get("restartPolicy") if isinstance(container, Mapping) else None
            )
            if restart_policy == "Always":
                for resource_name, quantity in container_requests.items():
                    restartable_init_requests[resource_name] = (
                        restartable_init_requests.get(resource_name, 0) + quantity
                    )
                stage_resources = restartable_init_requests
            else:
                stage_resources = {
                    resource_name: restartable_init_requests.get(resource_name, 0)
                    + container_requests.get(resource_name, 0)
                    for resource_name in (
                        restartable_init_requests.keys() | container_requests.keys()
                    )
                }
            for resource_name, quantity in stage_resources.items():
                init_stage_requests[resource_name] = max(
                    init_stage_requests.get(resource_name, 0),
                    quantity,
                )

        steady_state_requests = {
            resource_name: application_requests.get(resource_name, 0)
            + restartable_init_requests.get(resource_name, 0)
            for resource_name in (application_requests.keys() | restartable_init_requests.keys())
        }
        for resource_name in steady_state_requests.keys() | init_stage_requests.keys():
            quantity = max(
                steady_state_requests.get(resource_name, 0),
                init_stage_requests.get(resource_name, 0),
            )
            requested[resource_name] = requested.get(resource_name, 0) + quantity

        overhead = spec.get("overhead")
        if overhead is not None:
            if not isinstance(overhead, Mapping):
                raise ValueError("invalid Kubernetes Pod overhead")
            for resource_name in (
                name
                for name in overhead
                if isinstance(name, str) and _kubernetes_resource_contract(name) is not None
            ):
                requested[resource_name] = requested.get(resource_name, 0) + (
                    _resource_quantity(overhead, resource_name)
                )
    return requested


def _deduct_external_pod_requests(
    devices: tuple[AcceleratorDevice, ...],
    requested: Mapping[str, int],
) -> tuple[tuple[AcceleratorDevice, ...], int]:
    capacity: dict[str, int] = {}
    for device in devices:
        resource_name = device.kubernetes_resource_name
        if resource_name is not None:
            capacity[resource_name] = capacity.get(resource_name, 0) + 1
    if any(count > capacity.get(resource_name, 0) for resource_name, count in requested.items()):
        raise ValueError("Kubernetes Pod requests exceed Node allocatable capacity")

    available = {
        resource_name: count - requested.get(resource_name, 0)
        for resource_name, count in capacity.items()
    }
    accounted: list[AcceleratorDevice] = []
    available_by_resource: dict[str, int] = {}
    for device in devices:
        resource_name = device.kubernetes_resource_name
        if resource_name is None:
            accounted.append(device)
            continue
        available_count = available_by_resource.get(resource_name, 0)
        if available_count >= available[resource_name]:
            accounted.append(replace(device, health="externally-allocated"))
            continue
        accounted.append(device)
        available_by_resource[resource_name] = available_count + 1
    return tuple(accounted), sum(requested.values())


class KubernetesNodeAcceleratorProvider(_InventoryProviderBase):
    """Read Device Plugin capacity from one Kubernetes Node object.

    Generated IDs identify stable node capacity slots, not physical device IDs.
    Runtime observations remain authoritative for the concrete allocation.
    """

    name = "kubernetes-node"

    def __init__(
        self,
        *,
        node_name: str | None,
        kubeconfig: str | None = None,
        in_cluster: bool = False,
        node_reader: KubernetesNodeReader | None = None,
        pod_reader: KubernetesPodReader | None = None,
        cluster_id: str | None = None,
        worker_id: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        self.node_name = node_name.strip() if node_name is not None else None
        self.kubeconfig = kubeconfig
        self.in_cluster = in_cluster
        self._node_reader = node_reader or _read_kubernetes_node
        self._pod_reader = pod_reader or _read_kubernetes_pods
        self.cluster_id = cluster_id.strip() if cluster_id is not None else None
        self.worker_id = worker_id.strip() if worker_id is not None else None
        self.timeout = timeout

    def discover(self) -> InventoryProviderResult:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.discover_async())
        raise RuntimeError("Kubernetes inventory discovery must be awaited inside an event loop")

    async def discover_async(self) -> InventoryProviderResult:
        if not self.node_name:
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.UNAVAILABLE,
                message="worker_node_name_required",
            )
        try:
            async with asyncio.timeout(self.timeout):
                node, pods = await asyncio.gather(
                    self._node_reader(
                        node_name=self.node_name,
                        kubeconfig=self.kubeconfig,
                        in_cluster=self.in_cluster,
                        request_timeout=self.timeout,
                    ),
                    self._pod_reader(
                        node_name=self.node_name,
                        kubeconfig=self.kubeconfig,
                        in_cluster=self.in_cluster,
                        request_timeout=self.timeout,
                    ),
                )
        except Exception:
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.UNAVAILABLE,
                message="kubernetes_api_request_failed",
            )
        if not isinstance(node, Mapping):
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.DEGRADED,
                message="invalid_node_object",
                rejected_rows=1,
            )
        parsed = parse_kubernetes_node(node)
        try:
            external_requests = _external_pod_accelerator_requests(
                pods,
                node_name=self.node_name,
                cluster_id=self.cluster_id,
                worker_id=self.worker_id,
            )
            devices, excluded_slots = _deduct_external_pod_requests(
                parsed.devices,
                external_requests,
            )
        except (TypeError, ValueError):
            return InventoryProviderResult(
                provider=self.name,
                status=InventoryStatus.DEGRADED,
                message="invalid_pod_resource_requests",
                rejected_rows=1,
            )
        details: list[str] = []
        if parsed.rejected_rows:
            details.append(f"rejected {parsed.rejected_rows} incomplete capacity slot(s)")
        if excluded_slots:
            details.append(f"excluded {excluded_slots} externally requested capacity slot(s)")
        return InventoryProviderResult(
            provider=self.name,
            status=(
                InventoryStatus.DEGRADED if parsed.rejected_rows else InventoryStatus.AVAILABLE
            ),
            devices=devices,
            message="; ".join(details) or None,
            rejected_rows=parsed.rejected_rows,
        )


async def _read_kubernetes_node(
    *,
    node_name: str,
    kubeconfig: str | None,
    in_cluster: bool,
    request_timeout: float,
) -> Mapping[str, object]:
    configuration = client.Configuration()
    api_client: client.ApiClient | None = None
    try:
        if in_cluster:
            loaded = config.load_incluster_config(client_configuration=configuration)
            if isawaitable(loaded):
                await loaded
        else:
            await config.load_kube_config(
                config_file=kubeconfig,
                client_configuration=configuration,
                persist_config=False,
            )
        api_client = client.ApiClient(configuration=configuration)
        api = client.CoreV1Api(api_client=api_client)
        node = await api.read_node(name=node_name, _request_timeout=request_timeout)
        serialized = api_client.sanitize_for_serialization(node)
        if not isinstance(serialized, Mapping):
            raise TypeError("Kubernetes Node response must be an object")
        return serialized
    finally:
        if api_client is not None:
            await api_client.close()


async def _read_kubernetes_pods(
    *,
    node_name: str,
    kubeconfig: str | None,
    in_cluster: bool,
    request_timeout: float,
) -> tuple[Mapping[str, object], ...]:
    configuration = client.Configuration()
    api_client: client.ApiClient | None = None
    try:
        if in_cluster:
            loaded = config.load_incluster_config(client_configuration=configuration)
            if isawaitable(loaded):
                await loaded
        else:
            await config.load_kube_config(
                config_file=kubeconfig,
                client_configuration=configuration,
                persist_config=False,
            )
        api_client = client.ApiClient(configuration=configuration)
        api = client.CoreV1Api(api_client=api_client)
        pod_list = await api.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name}",
            _request_timeout=request_timeout,
        )
        serialized = api_client.sanitize_for_serialization(pod_list)
        if not isinstance(serialized, Mapping) or not isinstance(serialized.get("items"), list):
            raise TypeError("Kubernetes PodList response must contain an items list")
        pods = serialized["items"]
        if not all(isinstance(pod, Mapping) for pod in pods):
            raise TypeError("Kubernetes PodList items must be objects")
        return tuple(pods)
    finally:
        if api_client is not None:
            await api_client.close()


class FakeAcceleratorInventoryProvider(_InventoryProviderBase):
    """Deterministic development/test inventory that never probes the host."""

    name = "fake"

    def __init__(
        self,
        *,
        count: int,
        model: str,
        memory_mb: int,
        worker_id: str,
        vendor: AcceleratorVendor = AcceleratorVendor.NVIDIA,
        compute_capability: str = "0.0",
    ) -> None:
        if count < 0:
            raise ValueError("count must not be negative")
        if memory_mb <= 0:
            raise ValueError("memory_mb must be greater than zero")
        if not model.strip():
            raise ValueError("model must not be blank")
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if not isinstance(vendor, AcceleratorVendor):
            raise TypeError("vendor must be an AcceleratorVendor")
        self.count = count
        self.model = model.strip()
        self.memory_mb = memory_mb
        self.worker_id = worker_id.strip()
        self.vendor = vendor
        self.kind = kind_for_vendor(vendor)
        self.compute_arch = compute_capability

    def discover(self) -> InventoryProviderResult:
        devices = []
        for index in range(self.count):
            device_uuid = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"mini-ai-cloud://fake-accelerator/{self.vendor.value}/{self.worker_id}/{index}",
            )
            devices.append(
                AcceleratorDevice(
                    device_id=f"FAKE-{device_uuid}",
                    device_index=index,
                    vendor=self.vendor,
                    kind=self.kind,
                    model=self.model,
                    memory_total_mb=self.memory_mb,
                    memory_free_mb=self.memory_mb,
                    compute_arch=self.compute_arch,
                    fake=True,
                )
            )
        return InventoryProviderResult(
            provider=self.name,
            status=InventoryStatus.AVAILABLE,
            devices=tuple(devices),
        )


class NoAcceleratorProvider(_InventoryProviderBase):
    name = "none"

    def discover(self) -> InventoryProviderResult:
        return InventoryProviderResult(
            provider=self.name,
            status=InventoryStatus.AVAILABLE,
            message="accelerator inventory explicitly disabled",
        )


class InventoryProviderRegistry(_InventoryProviderBase):
    name = "registry"

    def __init__(self, providers: Iterable[AcceleratorInventoryProvider]) -> None:
        self.providers = tuple(providers)
        if not self.providers:
            raise ValueError("inventory registry requires at least one provider")
        names = [provider.name for provider in self.providers]
        if len(names) != len(set(names)):
            raise ValueError("inventory provider names must be unique")
        if "kubernetes-node" in names and len(names) > 1:
            raise ValueError(
                "kubernetes-node must be the only accelerator inventory provider because "
                "cross-authority physical-device aliasing is not available"
            )

    def snapshot(self) -> InventorySnapshot:
        results = tuple(provider.discover() for provider in self.providers)
        return self._snapshot_from_results(results)

    async def snapshot_async(self) -> InventorySnapshot:
        results: list[InventoryProviderResult] = []
        for provider in self.providers:
            if isinstance(provider, KubernetesNodeAcceleratorProvider):
                results.append(await provider.discover_async())
            else:
                results.append(await asyncio.to_thread(provider.discover))
        return self._snapshot_from_results(tuple(results))

    @staticmethod
    def _snapshot_from_results(
        results: tuple[InventoryProviderResult, ...],
    ) -> InventorySnapshot:
        devices = tuple(
            sorted(
                (device for result in results for device in result.devices),
                key=lambda device: (device.vendor.value, device.device_index, device.device_id),
            )
        )
        identities = [device.device_id for device in devices]
        if len(identities) != len(set(identities)):
            raise ValueError("inventory providers returned duplicate device IDs")
        slots = [(device.vendor, device.device_index) for device in devices]
        if len(slots) != len(set(slots)):
            raise ValueError("inventory providers returned duplicate vendor device indexes")
        return InventorySnapshot(devices=devices, provider_results=results)

    def discover(self) -> InventoryProviderResult:
        snapshot = self.snapshot()
        messages = tuple(
            f"{result.provider}:{result.message}"
            for result in snapshot.provider_results
            if result.message
        )
        return InventoryProviderResult(
            provider=self.name,
            status=snapshot.status,
            devices=snapshot.devices,
            message="; ".join(messages) or None,
            rejected_rows=sum(result.rejected_rows for result in snapshot.provider_results),
        )


def bind_kubernetes_runtime_profiles(
    devices: Iterable[AcceleratorDevice],
    catalog: RuntimeProfileCatalog,
) -> tuple[AcceleratorDevice, ...]:
    """Bind Node capacity slots to exact, catalog-owned Kubernetes contracts.

    A3 intentionally discovers Device Plugin resources and Node labels without
    importing runtime policy. The worker performs this later join only when it
    owns a validated Runtime Profile catalog and the Node satisfies the exact
    resource, nodeSelector, required node-affinity, and blocking-taint
    toleration contract. Host CLI inventory without a Device Plugin resource
    remains unbound and cannot become Kubernetes capacity by inference.
    """

    contracts: dict[
        tuple[AcceleratorVendor, AcceleratorKind, str],
        list[tuple[RuntimeProfile, str]],
    ] = {}
    for entry in catalog.manifest.profiles:
        profile = catalog.load_exact(
            profile_id=entry.profile_id,
            profile_version=entry.profile_version,
            semantic_digest=entry.semantic_digest,
        )
        binding_id = runtime_profile_binding_id(
            profile_id=entry.profile_id,
            profile_version=entry.profile_version,
            semantic_digest=entry.semantic_digest,
        )
        contract_key = (profile.vendor, profile.kind, profile.kubernetes.resource_name)
        contracts.setdefault(contract_key, []).append((profile, binding_id))

    bound: list[AcceleratorDevice] = []
    for device in devices:
        resource_name = device.kubernetes_resource_name
        if resource_name is None:
            bound.append(device)
            continue
        matches = [
            binding_id
            for profile, binding_id in contracts.get(
                (device.vendor, device.kind, resource_name), []
            )
            if _kubernetes_node_matches_profile(
                device.kubernetes_node_labels,
                device.kubernetes_node_taints,
                profile,
            )
        ]
        if not matches:
            bound.append(device)
            continue
        profile_ids = tuple(sorted(set(matches)))
        bound.append(
            replace(
                device,
                runtime_profile_ids=profile_ids,
            )
        )
    return tuple(bound)


def _kubernetes_node_matches_profile(
    node_label_items: tuple[tuple[str, str], ...],
    node_taints: tuple[tuple[str, str | None, str], ...],
    profile: RuntimeProfile,
) -> bool:
    node_labels = dict(node_label_items)
    if any(
        node_labels.get(key) != value for key, value in profile.kubernetes.node_selector.items()
    ):
        return False
    if not all(
        _kubernetes_node_matches_requirement(node_labels, requirement)
        for requirement in profile.kubernetes.node_affinity
    ):
        return False
    return all(
        effect == "PreferNoSchedule"
        or any(
            toleration.key == key
            and toleration.effect == effect
            and (
                toleration.operator == "Exists"
                or (toleration.operator == "Equal" and toleration.value == value)
            )
            for toleration in profile.kubernetes.tolerations
        )
        for key, value, effect in node_taints
    )


def _kubernetes_node_matches_requirement(
    node_labels: Mapping[str, str],
    requirement: KubernetesNodeSelectorRequirement,
) -> bool:
    key_present = requirement.key in node_labels
    value = node_labels.get(requirement.key)
    if requirement.operator == "Exists":
        return key_present
    if requirement.operator == "DoesNotExist":
        return not key_present
    if requirement.operator == "In":
        return key_present and value in requirement.values
    if requirement.operator == "NotIn":
        return not key_present or value not in requirement.values
    if not key_present or value is None:
        return False
    try:
        numeric_value = int(value)
        threshold = int(requirement.values[0])
    except (ValueError, IndexError):
        return False
    if requirement.operator == "Gt":
        return numeric_value > threshold
    if requirement.operator == "Lt":
        return numeric_value < threshold
    return False


def _provider_names(settings: Settings) -> tuple[str, ...]:
    if settings.fake_gpu_count:
        return ("fake",)
    return tuple(
        name.strip() for name in settings.accelerator_inventory_providers.split(",") if name.strip()
    )


def build_accelerator_inventory_registry(
    settings: Settings,
    *,
    worker_id: str,
) -> InventoryProviderRegistry:
    """Build only known providers; vendor/kind always comes from provider code."""

    providers: list[AcceleratorInventoryProvider] = []
    for name in _provider_names(settings):
        if name == "nvidia-smi":
            providers.append(NvidiaSMIInventoryProvider())
        elif name == "ascend-npu-smi":
            providers.append(AscendNpuSMIInventoryProvider())
        elif name == "kubernetes-node":
            providers.append(
                KubernetesNodeAcceleratorProvider(
                    node_name=settings.worker_node_name,
                    kubeconfig=settings.kubernetes_kubeconfig,
                    in_cluster=settings.kubernetes_in_cluster,
                    cluster_id=settings.kubernetes_serving_cluster_id,
                    worker_id=worker_id,
                )
            )
        elif name == "fake":
            if settings.app_env == "production":
                raise ValueError("fake accelerator inventory is forbidden in production")
            providers.append(
                FakeAcceleratorInventoryProvider(
                    count=settings.fake_gpu_count,
                    model=settings.fake_gpu_model,
                    memory_mb=settings.fake_gpu_memory_mb,
                    worker_id=worker_id,
                )
            )
        elif name == "none":
            providers.append(NoAcceleratorProvider())
        else:
            raise ValueError(f"unknown accelerator inventory provider: {name}")
    return InventoryProviderRegistry(providers)


def build_gpu_inventory_provider(
    settings: Settings,
    *,
    worker_id: str,
) -> AcceleratorInventoryProvider:
    """Compatibility factory for v0.4 internal callers."""

    registry = build_accelerator_inventory_registry(settings, worker_id=worker_id)
    return registry.providers[0] if len(registry.providers) == 1 else registry


# v0.4 internal import aliases. Providers themselves return AcceleratorDevice.
GPUDevice = AcceleratorDevice
GPUInventoryProvider = DeviceInventory
FakeGPUInventoryProvider = FakeAcceleratorInventoryProvider
NoGPUInventoryProvider = NoAcceleratorProvider
