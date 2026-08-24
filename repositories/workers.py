from __future__ import annotations

import uuid
from datetime import timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.pagination import TextCursorKey
from core.enums import WorkerStatus
from models.scheduling import GPUDevice
from models.worker import Worker
from repositories.clock import database_utcnow


class WorkerRepository:
    @staticmethod
    async def register(
        session: AsyncSession,
        *,
        worker_id: str,
        hostname: str,
        concurrency: int,
        cpu_count: int,
        memory_total_mb: int,
        docker_version: str | None,
        labels: dict[str, str],
        gpu_count: int,
        gpu_model: str | None,
        gpu_memory_mb: int,
        worker_session_id: uuid.UUID | None = None,
        node_name: str | None = None,
        runtime_types: list[str] | None = None,
        taints: list[dict[str, str]] | None = None,
    ) -> Worker:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        now = await database_utcnow(session)
        if worker is None:
            session_id = worker_session_id or uuid.uuid4()
            worker = Worker(
                id=worker_id,
                worker_session_id=session_id,
                hostname=hostname,
                node_name=node_name or hostname,
                runtime_types=runtime_types or ["docker"],
                status=WorkerStatus.ONLINE,
                started_at=now,
                last_heartbeat_at=now,
                running_tasks=0,
                concurrency=concurrency,
                reserved_cpu=0.0,
                reserved_memory_mb=0,
                reserved_gpus=0,
                cpu_count=cpu_count,
                cpu_total_millicores=cpu_count * 1000,
                cpu_allocatable_millicores=cpu_count * 1000,
                memory_total_mb=memory_total_mb,
                memory_allocatable_mb=memory_total_mb,
                docker_version=docker_version,
                labels=labels,
                taints=taints or [],
                gpu_count=gpu_count,
                gpu_model=gpu_model,
                gpu_memory_mb=gpu_memory_mb,
                inventory_updated_at=now,
            )
            session.add(worker)
            return worker

        worker.hostname = hostname
        worker.worker_session_id = worker_session_id or uuid.uuid4()
        worker.node_name = node_name or hostname
        worker.runtime_types = runtime_types or ["docker"]
        worker.taints = taints or []
        worker.started_at = now
        worker.last_heartbeat_at = now
        worker.concurrency = concurrency
        # Existing execution leases still own their persisted reservations.
        # A process restart that reuses a configured worker ID must not make
        # that capacity available until those executions finish or are
        # recovered by the lease reaper.
        worker.cpu_count = cpu_count
        worker.cpu_total_millicores = cpu_count * 1000
        worker.cpu_allocatable_millicores = cpu_count * 1000
        worker.memory_total_mb = memory_total_mb
        worker.memory_allocatable_mb = memory_total_mb
        worker.docker_version = docker_version
        worker.labels = labels
        worker.gpu_count = gpu_count
        worker.gpu_model = gpu_model
        worker.gpu_memory_mb = gpu_memory_mb
        worker.inventory_generation += 1
        worker.inventory_updated_at = now
        worker.overcommitted = (
            worker.reserved_cpu * 1000 > worker.cpu_allocatable_millicores
            or worker.reserved_memory_mb > worker.memory_allocatable_mb
            or worker.reserved_gpus > worker.gpu_count
        )
        worker.status = WorkerStatus.DRAINING if worker.overcommitted else WorkerStatus.ONLINE
        worker.drain_reason = (
            "inventory capacity is below active reservations" if worker.overcommitted else None
        )
        worker.version += 1
        return worker

    @staticmethod
    async def heartbeat(
        session: AsyncSession,
        worker_id: str,
        running_tasks: int,
        *,
        worker_session_id: uuid.UUID | None = None,
    ) -> Worker | None:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None:
            return None
        if worker_session_id is not None and worker.worker_session_id != worker_session_id:
            return None
        if worker.status != WorkerStatus.DRAINING:
            worker.status = WorkerStatus.ONLINE
        worker.last_heartbeat_at = await database_utcnow(session)
        # Claim and terminal transitions own the persisted capacity counters.
        # A heartbeat snapshot can race either transition and must not overwrite
        # their row-locked accounting.
        del running_tasks
        worker.version += 1
        return worker

    @staticmethod
    async def replace_gpu_inventory(
        session: AsyncSession,
        *,
        worker_id: str,
        worker_session_id: uuid.UUID,
        devices: list[dict[str, object]],
    ) -> list[GPUDevice]:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None or worker.worker_session_id != worker_session_id:
            raise LookupError("worker session is stale")
        existing = {
            device.device_uuid: device
            for device in await session.scalars(
                select(GPUDevice).where(GPUDevice.worker_id == worker_id).with_for_update()
            )
        }
        now = await database_utcnow(session)
        seen: set[str] = set()
        result: list[GPUDevice] = []
        for item in devices:
            device_uuid = str(item["uuid"])
            device_index = _inventory_integer(item.get("index"), "index", minimum=0)
            memory_total_mb = _inventory_integer(
                item.get("memory_total_mb"), "memory_total_mb", minimum=1
            )
            memory_free_mb = _inventory_integer(
                item.get("memory_free_mb"), "memory_free_mb", minimum=0
            )
            if memory_free_mb > memory_total_mb:
                raise ValueError("GPU memory_free_mb must not exceed memory_total_mb")
            seen.add(device_uuid)
            device = existing.get(device_uuid)
            if device is None:
                device = GPUDevice(worker_id=worker_id, device_uuid=device_uuid)
                session.add(device)
            device.device_index = device_index
            device.vendor = str(item.get("vendor", "nvidia"))
            device.model = str(item["model"])
            device.memory_total_mb = memory_total_mb
            device.memory_free_mb = memory_free_mb
            capability = item.get("compute_capability")
            device.compute_capability = str(capability) if capability is not None else None
            device.health = str(item.get("health", "healthy"))
            device.fake = bool(item.get("fake", False))
            device.inventory_generation = worker.inventory_generation
            device.last_seen_at = now
            result.append(device)
        # A disappeared device with no active reservation can be removed. The
        # FK protects devices that are still owned by reservation history.
        for key, device in existing.items():
            if key not in seen:
                device.health = "missing"
                device.last_seen_at = now
        worker.gpu_count = len(result)
        worker.gpu_model = ",".join(sorted({device.model for device in result})) or None
        worker.gpu_memory_mb = sum(device.memory_total_mb for device in result)
        worker.inventory_updated_at = now
        worker.version += 1
        await session.flush()
        return result

    @staticmethod
    async def drain(session: AsyncSession, worker_id: str, *, reason: str) -> Worker | None:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None:
            return None
        worker.status = WorkerStatus.DRAINING
        worker.drain_reason = reason
        worker.version += 1
        return worker

    @staticmethod
    async def set_status(
        session: AsyncSession,
        worker_id: str,
        status: WorkerStatus,
        *,
        worker_session_id: uuid.UUID | None = None,
    ) -> Worker | None:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None:
            return None
        if worker_session_id is not None and worker.worker_session_id != worker_session_id:
            return None
        worker.status = status
        worker.last_heartbeat_at = await database_utcnow(session)
        worker.version += 1
        return worker

    @staticmethod
    async def get(session: AsyncSession, worker_id: str) -> Worker | None:
        return await session.get(Worker, worker_id)

    @staticmethod
    async def list_workers(
        session: AsyncSession,
        *,
        limit: int,
        offset: int,
        after: TextCursorKey | None = None,
    ) -> list[Worker]:
        query = select(Worker)
        if after is not None:
            query = query.where(
                or_(
                    Worker.started_at < after.created_at,
                    and_(
                        Worker.started_at == after.created_at,
                        Worker.id < after.item_id,
                    ),
                )
            )
        result = await session.scalars(
            query.order_by(Worker.started_at.desc(), Worker.id.desc()).limit(limit).offset(offset)
        )
        return list(result)

    @staticmethod
    async def mark_stale_offline(
        session: AsyncSession,
        *,
        offline_timeout_seconds: float,
        limit: int | None = None,
    ) -> list[str]:
        if limit is not None and limit < 1:
            raise ValueError("limit must be at least one")
        cutoff = await database_utcnow(session) - timedelta(seconds=offline_timeout_seconds)
        query = (
            select(Worker)
            .where(
                Worker.status == WorkerStatus.ONLINE,
                Worker.last_heartbeat_at < cutoff,
            )
            .order_by(Worker.last_heartbeat_at, Worker.id)
        )
        if limit is not None:
            query = query.limit(limit)
        workers = list(await session.scalars(query.with_for_update(skip_locked=True)))
        for worker in workers:
            worker.status = WorkerStatus.OFFLINE
            # Heartbeat expiry prevents new claims, but it does not revoke
            # task leases. The lease recovery or terminal transition owns the
            # corresponding capacity release under the same worker row lock.
            worker.version += 1
        return [worker.id for worker in workers]


def _inventory_integer(value: object, field: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"GPU {field} must be an integer")
    if value < minimum:
        raise ValueError(f"GPU {field} must be at least {minimum}")
    return value
