from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta

import httpx
from sqlalchemy import or_, select

from core.database import Database
from core.logging import get_logger
from models.service import ModelService, ReplicaHealth, ReplicaStatus, ServiceReplica
from repositories.clock import database_utcnow
from repositories.services import ServiceRepository


@dataclass(frozen=True, slots=True)
class HealthProbe:
    replica_id: uuid.UUID
    generation: int
    execution_id: uuid.UUID
    endpoint_url: str
    token: uuid.UUID


@dataclass(frozen=True, slots=True)
class ProbeOutcome:
    probe: HealthProbe
    healthy: bool
    error_message: str | None


@dataclass(frozen=True, slots=True)
class HealthRunResult:
    claimed: int = 0
    healthy: int = 0
    failed: int = 0
    stale: int = 0


class ServiceHealthController:
    def __init__(
        self,
        database: Database,
        http_client: httpx.AsyncClient,
        *,
        timeout_seconds: float,
        interval_seconds: float = 5.0,
        failure_threshold: int = 3,
        batch_size: int = 100,
        concurrency: int = 20,
    ) -> None:
        if timeout_seconds <= 0 or interval_seconds <= 0:
            raise ValueError("timeout_seconds and interval_seconds must be positive")
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least one")
        if batch_size < 1 or concurrency < 1:
            raise ValueError("batch_size and concurrency must be at least one")
        self.database = database
        self.http_client = http_client
        self.timeout = httpx.Timeout(timeout_seconds)
        self.interval_seconds = interval_seconds
        self.claim_seconds = max(5.0, timeout_seconds * 2)
        self.failure_threshold = failure_threshold
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = get_logger("service_health")

    async def run_once(self) -> HealthRunResult:
        probes = await self._claim_probes()
        if not probes:
            return HealthRunResult()
        outcomes = await asyncio.gather(*(self._probe(item) for item in probes))
        healthy = 0
        failed = 0
        stale = 0
        for outcome in outcomes:
            async with self.database.session() as session, session.begin():
                accepted = await ServiceRepository.record_replica_health(
                    session,
                    replica_id=outcome.probe.replica_id,
                    generation=outcome.probe.generation,
                    execution_id=outcome.probe.execution_id,
                    health=(ReplicaHealth.HEALTHY if outcome.healthy else ReplicaHealth.UNHEALTHY),
                    error_message=outcome.error_message,
                    failure_threshold=self.failure_threshold,
                    probe_token=outcome.probe.token,
                )
            if not accepted:
                stale += 1
            elif outcome.healthy:
                healthy += 1
            else:
                failed += 1
        result = HealthRunResult(
            claimed=len(probes),
            healthy=healthy,
            failed=failed,
            stale=stale,
        )
        self.logger.info(
            "model replica health probes completed",
            claimed=result.claimed,
            healthy=result.healthy,
            failed=result.failed,
            stale=result.stale,
        )
        return result

    async def _claim_probes(self) -> list[HealthProbe]:
        async with self.database.session() as session, session.begin():
            now = await database_utcnow(session)
            due_before = now - timedelta(seconds=self.interval_seconds)
            replicas = list(
                await session.scalars(
                    select(ServiceReplica)
                    .join(ModelService, ModelService.id == ServiceReplica.service_id)
                    .where(
                        ServiceReplica.generation == ModelService.generation,
                        ServiceReplica.status == ReplicaStatus.RUNNING,
                        ServiceReplica.execution_id.is_not(None),
                        ServiceReplica.endpoint_url.is_not(None),
                        ServiceReplica.endpoint_url != "",
                        ModelService.desired_replicas > 0,
                        or_(
                            ServiceReplica.last_health_at.is_(None),
                            ServiceReplica.last_health_at <= due_before,
                        ),
                        or_(
                            ServiceReplica.health_probe_expires_at.is_(None),
                            ServiceReplica.health_probe_expires_at < now,
                        ),
                    )
                    .order_by(
                        ServiceReplica.last_health_at.asc().nullsfirst(),
                        ServiceReplica.id,
                    )
                    .limit(self.batch_size)
                    .with_for_update(of=ServiceReplica, skip_locked=True)
                )
            )
            claimed_until = now + timedelta(seconds=self.claim_seconds)
            probes: list[HealthProbe] = []
            for replica in replicas:
                assert replica.execution_id is not None
                assert replica.endpoint_url is not None
                token = uuid.uuid4()
                replica.health_probe_token = token
                replica.health_probe_expires_at = claimed_until
                probes.append(
                    HealthProbe(
                        replica_id=replica.id,
                        generation=replica.generation,
                        execution_id=replica.execution_id,
                        endpoint_url=replica.endpoint_url,
                        token=token,
                    )
                )
            return probes

    async def _probe(self, probe: HealthProbe) -> ProbeOutcome:
        async with self.semaphore:
            try:
                response = await self.http_client.get(
                    f"{probe.endpoint_url.rstrip('/')}/health",
                    timeout=self.timeout,
                )
                if 200 <= response.status_code < 300:
                    return ProbeOutcome(probe=probe, healthy=True, error_message=None)
                return ProbeOutcome(
                    probe=probe,
                    healthy=False,
                    error_message=f"health endpoint returned HTTP {response.status_code}",
                )
            except httpx.TimeoutException:
                return ProbeOutcome(
                    probe=probe,
                    healthy=False,
                    error_message="health probe timed out",
                )
            except httpx.RequestError as exc:
                return ProbeOutcome(
                    probe=probe,
                    healthy=False,
                    error_message=f"health probe transport error: {type(exc).__name__}"[:512],
                )
