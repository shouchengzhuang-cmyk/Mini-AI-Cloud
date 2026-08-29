from __future__ import annotations

import math
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select

from api.services.gateway import ServiceLoad, ServiceMetricsSource
from core.database import Database
from core.enums import RuntimeType
from core.logging import get_logger
from core.runtime_profiles import RuntimeProfileCatalog
from models.model_variant import LogicalModel
from models.service import ModelService
from repositories.admission import AdmissionRepository
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


class KubernetesRuntimeAdmission(Protocol):
    @property
    def admission_ready(self) -> bool: ...


AutoscaleCandidate = tuple[UUID, UUID | None, RuntimeType, datetime | None]


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
        kubernetes_runtime: KubernetesRuntimeAdmission | None = None,
        runtime_profile_catalog: RuntimeProfileCatalog | None = None,
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
        self.kubernetes_runtime = kubernetes_runtime
        self.runtime_profile_catalog = runtime_profile_catalog
        self.logger = get_logger("service_autoscaler")

    async def run_once(self) -> AutoscaleRunResult:
        examined = 0
        scaled = 0
        held = 0
        missing = 0
        cooling_down = 0
        async for service_id, logical_model_id, runtime_type, checked_at in self._candidates():
            if examined >= self.batch_size:
                break
            async with self.database.session() as session, session.begin():
                if runtime_type == RuntimeType.KUBERNETES and logical_model_id is not None:
                    locked_logical_model_id = await session.scalar(
                        select(LogicalModel.id)
                        .where(LogicalModel.id == logical_model_id)
                        .with_for_update(skip_locked=True)
                    )
                    if locked_logical_model_id is None:
                        continue
                service = await session.scalar(
                    select(ModelService)
                    .where(
                        ModelService.id == service_id,
                        ModelService.autoscaling_enabled.is_(True),
                    )
                    .with_for_update(skip_locked=True)
                )
                if service is None:
                    continue
                if service.last_autoscale_checked_at != checked_at:
                    # Another pass processed this candidate after the cursor
                    # snapshot. Leave it for a future fair pass.
                    continue
                expected_logical_model_id = (
                    logical_model_id if runtime_type == RuntimeType.KUBERNETES else None
                )
                actual_logical_model_id = (
                    service.logical_model_id
                    if service.runtime_type == RuntimeType.KUBERNETES
                    else None
                )
                if actual_logical_model_id != expected_logical_model_id:
                    # The admission identity is immutable through public APIs. If a
                    # direct database mutation races the unlocked scan, retry it on a
                    # later pass after acquiring the correct logical-model lock.
                    continue
                # Preserve the shared lock order used by gateway routing and scale:
                # logical model -> service -> project quota.
                await QuotaRepository.get_locked(session, project_id=service.project_id)
                now = await database_utcnow(session)
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
                eligible_node_names: tuple[str, ...] | None = None
                if (
                    service.runtime_type == RuntimeType.KUBERNETES
                    and target > service.desired_replicas
                    and not self._kubernetes_admission_ready()
                ):
                    held += 1
                    continue
                if (
                    service.runtime_type == RuntimeType.KUBERNETES
                    and service.logical_model_id is not None
                    and target > service.desired_replicas
                ):
                    if self.runtime_profile_catalog is None:
                        held += 1
                        continue
                    admission = await AdmissionRepository.revalidate_logical_model_service_scale(
                        session,
                        catalog=self.runtime_profile_catalog,
                        service=service,
                        desired_replicas=target,
                    )
                    if not admission.allowed or admission.snapshot is None:
                        held += 1
                        continue
                    eligible_node_names = admission.snapshot.eligible_node_names
                try:
                    updated = await ServiceRepository.set_desired_replicas(
                        session,
                        service_id=service.id,
                        project_id=service.project_id,
                        desired_replicas=target,
                        eligible_node_names=eligible_node_names,
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

    async def _candidates(self) -> AsyncGenerator[AutoscaleCandidate]:
        query = (
            select(
                ModelService.id,
                ModelService.logical_model_id,
                ModelService.runtime_type,
                ModelService.last_autoscale_checked_at,
            )
            .where(ModelService.autoscaling_enabled.is_(True))
            .order_by(
                ModelService.last_autoscale_checked_at.asc().nullsfirst(),
                ModelService.id,
            )
        )
        if self.database.engine.dialect.name == "sqlite":
            # SQLite readers block a writer commit while a cursor is open. Its
            # supported local/test deployments therefore close a materialized
            # bounded snapshot before the per-candidate write transactions begin.
            async with self.database.session() as session:
                candidates = list((await session.execute(query.limit(self.batch_size))).all())
            for service_id, logical_model_id, runtime_type, checked_at in candidates:
                yield service_id, logical_model_id, runtime_type, checked_at
            return

        # A single cursor provides a stable candidate snapshot for the pass: a row
        # skipped by a concurrent lock cannot move forward and re-enter. Production
        # PostgreSQL fetches at most batch_size rows into the client at a time.
        async with self.database.session() as session, session.begin():
            result = await session.stream(query.execution_options(yield_per=self.batch_size))
            async for service_id, logical_model_id, runtime_type, checked_at in result:
                yield service_id, logical_model_id, runtime_type, checked_at

    def _kubernetes_admission_ready(self) -> bool:
        runtime = self.kubernetes_runtime
        return runtime is not None and runtime.admission_ready is True


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
