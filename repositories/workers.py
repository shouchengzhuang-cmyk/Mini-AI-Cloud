from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import WorkerStatus
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
    ) -> Worker:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        now = await database_utcnow(session)
        if worker is None:
            worker = Worker(
                id=worker_id,
                hostname=hostname,
                status=WorkerStatus.ONLINE,
                started_at=now,
                last_heartbeat_at=now,
                running_tasks=0,
                concurrency=concurrency,
                reserved_cpu=0.0,
                reserved_memory_mb=0,
                reserved_gpus=0,
                cpu_count=cpu_count,
                memory_total_mb=memory_total_mb,
                docker_version=docker_version,
                labels=labels,
                gpu_count=gpu_count,
                gpu_model=gpu_model,
                gpu_memory_mb=gpu_memory_mb,
            )
            session.add(worker)
            return worker

        worker.hostname = hostname
        worker.status = WorkerStatus.ONLINE
        worker.started_at = now
        worker.last_heartbeat_at = now
        worker.concurrency = concurrency
        # Existing execution leases still own their persisted reservations.
        # A process restart that reuses a configured worker ID must not make
        # that capacity available until those executions finish or are
        # recovered by the lease reaper.
        worker.cpu_count = cpu_count
        worker.memory_total_mb = memory_total_mb
        worker.docker_version = docker_version
        worker.labels = labels
        worker.gpu_count = gpu_count
        worker.gpu_model = gpu_model
        worker.gpu_memory_mb = gpu_memory_mb
        worker.version += 1
        return worker

    @staticmethod
    async def heartbeat(session: AsyncSession, worker_id: str, running_tasks: int) -> Worker | None:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None:
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
    async def set_status(
        session: AsyncSession, worker_id: str, status: WorkerStatus
    ) -> Worker | None:
        worker = await session.get(Worker, worker_id, with_for_update=True)
        if worker is None:
            return None
        worker.status = status
        worker.last_heartbeat_at = await database_utcnow(session)
        worker.version += 1
        return worker

    @staticmethod
    async def get(session: AsyncSession, worker_id: str) -> Worker | None:
        return await session.get(Worker, worker_id)

    @staticmethod
    async def list_workers(session: AsyncSession, *, limit: int, offset: int) -> list[Worker]:
        result = await session.scalars(
            select(Worker).order_by(Worker.started_at.desc()).limit(limit).offset(offset)
        )
        return list(result)

    @staticmethod
    async def mark_stale_offline(
        session: AsyncSession, *, offline_timeout_seconds: float
    ) -> list[str]:
        cutoff = await database_utcnow(session) - timedelta(seconds=offline_timeout_seconds)
        workers = list(
            await session.scalars(
                select(Worker)
                .where(
                    Worker.status == WorkerStatus.ONLINE,
                    Worker.last_heartbeat_at < cutoff,
                )
                .with_for_update(skip_locked=True)
            )
        )
        for worker in workers:
            worker.status = WorkerStatus.OFFLINE
            # Heartbeat expiry prevents new claims, but it does not revoke
            # task leases. The lease recovery or terminal transition owns the
            # corresponding capacity release under the same worker row lock.
            worker.version += 1
        return [worker.id for worker in workers]
