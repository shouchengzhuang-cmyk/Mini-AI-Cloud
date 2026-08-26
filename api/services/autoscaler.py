from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select

from api.services.gateway import ServiceLoad, ServiceMetricsSource
from core.database import Database
from core.logging import get_logger
from models.service import ModelService
from repositories.clock import database_utcnow
from repositories.quotas import QuotaExceededError, QuotaRepository
from repositories.services import ServiceRepository


@dataclass(frozen=True, slots=True)
class AutoscaleRunResult:
    examined: int = 0
    scaled: int = 0
    held: int = 0
    missing_metrics: int = 0
    cooling_down: int = 0


class ServiceAutoscaler:
    """Adjust service desired replicas from live concurrency observations only."""

    def __init__(
        self,
        database: Database,
        metrics: ServiceMetricsSource,
        *,
        batch_size: int = 100,
        metric_max_age_seconds: float = 30.0,
        scale_to_zero_enabled: bool = False,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if metric_max_age_seconds <= 0:
            raise ValueError("metric_max_age_seconds must be positive")
        self.database = database
        self.metrics = metrics
        self.batch_size = batch_size
        self.metric_max_age_seconds = metric_max_age_seconds
        self.scale_to_zero_enabled = scale_to_zero_enabled
        self.logger = get_logger("service_autoscaler")

    async def run_once(self) -> AutoscaleRunResult:
        examined = 0
        scaled = 0
        held = 0
        missing = 0
        cooling_down = 0
        async with self.database.session() as session, session.begin():
            services = list(
                await session.scalars(
                    select(ModelService)
                    .where(ModelService.autoscaling_enabled.is_(True))
                    .order_by(
                        ModelService.last_autoscale_checked_at.asc().nullsfirst(),
                        ModelService.id,
                    )
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            # Candidate rows are already locked. Lock all project quota rows in
            # one stable order before applying per-service deltas so concurrent
            # autoscaler passes cannot deadlock across projects.
            for project_id in sorted({service.project_id for service in services}, key=str):
                await QuotaRepository.get_locked(session, project_id=project_id)
            now = await database_utcnow(session)
            for service in services:
                examined += 1
                service.last_autoscale_checked_at = now
                load = await self.metrics.snapshot(service.id)
                if not _usable_load(load, now, self.metric_max_age_seconds):
                    missing += 1
                    continue
                assert load is not None
                if _in_cooldown(service, now):
                    cooling_down += 1
                    continue
                target = _target_replicas(
                    service,
                    active_requests=load.active_requests,
                    scale_to_zero_enabled=self.scale_to_zero_enabled,
                )
                if target == service.desired_replicas:
                    held += 1
                    continue
                try:
                    updated = await ServiceRepository.set_desired_replicas(
                        session,
                        service_id=service.id,
                        project_id=service.project_id,
                        desired_replicas=target,
                    )
                except QuotaExceededError:
                    held += 1
                    continue
                if updated is None:
                    continue
                updated.last_scaled_at = now
                scaled += 1
        result = AutoscaleRunResult(
            examined=examined,
            scaled=scaled,
            held=held,
            missing_metrics=missing,
            cooling_down=cooling_down,
        )
        if result.examined:
            self.logger.info(
                "model service autoscaling pass completed",
                examined=result.examined,
                scaled=result.scaled,
                held=result.held,
                missing_metrics=result.missing_metrics,
                cooling_down=result.cooling_down,
            )
        return result


def _target_replicas(
    service: ModelService,
    *,
    active_requests: int,
    scale_to_zero_enabled: bool,
) -> int:
    minimum = service.autoscaling_min_replicas
    if not scale_to_zero_enabled:
        minimum = max(1, minimum)
    requested = math.ceil(active_requests / service.autoscaling_target_concurrency)
    return min(service.autoscaling_max_replicas, max(minimum, requested))


def _in_cooldown(service: ModelService, now: datetime) -> bool:
    if service.last_scaled_at is None:
        return False
    elapsed = (_as_utc(now) - _as_utc(service.last_scaled_at)).total_seconds()
    return elapsed < service.autoscaling_cooldown_seconds


def _usable_load(
    load: ServiceLoad | None,
    now: datetime,
    max_age_seconds: float,
) -> bool:
    if load is None or load.active_requests < 0:
        return False
    age = (_as_utc(now) - _as_utc(load.observed_at)).total_seconds()
    # The database clock owns cooldowns while an in-process collector timestamps
    # observations locally. Tolerate a small forward skew without accepting old data.
    return -5.0 <= age <= max_age_seconds


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
