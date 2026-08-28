import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.service import ModelService, ServiceReplica
from models.usage import ServingRequestUsage, TaskExecution, UsageLedger
from repositories.clock import database_utcnow

ZERO = Decimal("0")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
_SERVING_OUTCOME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_SERVING_PATHS = frozenset({"/v1/chat/completions", "/v1/completions"})


class UsageInvariantViolation(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CurrencyCost:
    currency: str
    cost: Decimal


@dataclass(frozen=True, slots=True)
class GPUUsage:
    gpu_model: str | None
    gpu_seconds: Decimal


@dataclass(frozen=True, slots=True)
class UsageAggregate:
    project_id: uuid.UUID
    from_time: datetime
    to_time: datetime
    execution_count: int
    cpu_seconds: Decimal
    memory_gb_seconds: Decimal
    gpu_seconds: Decimal
    costs: tuple[CurrencyCost, ...]
    gpu_breakdown: tuple[GPUUsage, ...]
    serving_request_count: int
    serving_requests_with_token_usage: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    serving_allocated_gpu_seconds: Decimal
    serving_replica_gpu_seconds: Decimal


class UsageRepository:
    """Write exactly one immutable ledger row per execution and aggregate settlements."""

    @staticmethod
    async def record_execution(
        session: AsyncSession,
        *,
        execution_id: uuid.UUID,
        project_id: uuid.UUID,
        task_id: uuid.UUID,
        started_at: datetime,
        finished_at: datetime,
        cpu_seconds: Decimal,
        memory_gb_seconds: Decimal,
        gpu_seconds: Decimal,
        cost: Decimal,
        gpu_model: str | None = None,
        currency: str = "USD",
        pricing_source: str = "rate_snapshot",
    ) -> tuple[UsageLedger, bool]:
        started_at, finished_at = _normalize_period(started_at, finished_at)
        if finished_at < started_at:
            raise ValueError("usage period end must not be before its start")
        _validate_nonnegative_usage(cpu_seconds, memory_gb_seconds, gpu_seconds, cost)
        normalized_currency = currency.strip().upper()
        if not _CURRENCY.fullmatch(normalized_currency):
            raise ValueError("currency must be a three-letter ISO-style code")
        normalized_source = pricing_source.strip()
        if not normalized_source or len(normalized_source) > 32:
            raise ValueError("pricing_source must contain 1-32 characters")
        execution = await session.get(TaskExecution, execution_id, with_for_update=True)
        if execution is None:
            raise UsageInvariantViolation("usage execution does not exist")
        if execution.project_id != project_id or execution.task_id != task_id:
            raise UsageInvariantViolation("usage identity does not match its execution")
        existing = await session.scalar(
            select(UsageLedger).where(UsageLedger.execution_id == execution_id)
        )
        if existing is not None:
            if not _same_usage(
                existing,
                started_at=started_at,
                finished_at=finished_at,
                cpu_seconds=cpu_seconds,
                memory_gb_seconds=memory_gb_seconds,
                gpu_seconds=gpu_seconds,
                gpu_model=gpu_model,
                cost=cost,
                currency=normalized_currency,
                pricing_source=normalized_source,
            ):
                raise UsageInvariantViolation(
                    "conflicting usage was submitted for an already settled execution"
                )
            return existing, False
        ledger = UsageLedger(
            project_id=project_id,
            task_id=task_id,
            execution_id=execution_id,
            started_at=started_at,
            finished_at=finished_at,
            cpu_seconds=cpu_seconds,
            memory_gb_seconds=memory_gb_seconds,
            gpu_seconds=gpu_seconds,
            gpu_model=gpu_model,
            cost=cost,
            currency=normalized_currency,
            pricing_source=normalized_source,
            created_at=await database_utcnow(session),
        )
        session.add(ledger)
        await session.flush()
        return ledger, True

    @staticmethod
    async def record_serving_request(
        session: AsyncSession,
        *,
        request_id: uuid.UUID,
        project_id: uuid.UUID,
        service_id: uuid.UUID,
        replica_id: uuid.UUID | None,
        logical_model_id: uuid.UUID | None = None,
        model_variant_id: uuid.UUID | None = None,
        selected_vendor: str | None = None,
        path: str,
        outcome: str,
        error_code: str | None,
        streamed: bool,
        started_at: datetime,
        finished_at: datetime,
        request_duration_seconds: Decimal,
        time_to_first_token_seconds: Decimal | None,
        allocated_gpu_seconds: Decimal | None,
        prompt_tokens: int | None,
        completion_tokens: int | None,
        total_tokens: int | None,
    ) -> tuple[ServingRequestUsage, bool]:
        started_at, finished_at = _normalize_period(started_at, finished_at)
        if finished_at < started_at:
            raise ValueError("serving usage period end must not be before its start")
        duration = _decimal(request_duration_seconds)
        ttft = (
            None if time_to_first_token_seconds is None else _decimal(time_to_first_token_seconds)
        )
        allocated_gpu = None if allocated_gpu_seconds is None else _decimal(allocated_gpu_seconds)
        if (
            duration < ZERO
            or (ttft is not None and ttft < ZERO)
            or (allocated_gpu is not None and allocated_gpu < ZERO)
        ):
            raise ValueError(
                "serving request duration, TTFT and GPU allocation must be non-negative"
            )
        if path not in _SERVING_PATHS:
            raise ValueError("unsupported serving gateway path")
        normalized_outcome = outcome.strip()
        if not _SERVING_OUTCOME.fullmatch(normalized_outcome):
            raise ValueError("serving outcome must be a bounded machine-readable value")
        normalized_error = error_code.strip() if error_code is not None else None
        if normalized_error is not None and not _SERVING_OUTCOME.fullmatch(normalized_error):
            raise ValueError("serving error code must be a bounded machine-readable value")
        _validate_token_usage(prompt_tokens, completion_tokens, total_tokens)

        existing = await session.scalar(
            select(ServingRequestUsage).where(ServingRequestUsage.request_id == request_id)
        )
        if existing is not None:
            if not _same_serving_usage(
                existing,
                project_id=project_id,
                service_id=service_id,
                replica_id=replica_id,
                logical_model_id=logical_model_id,
                model_variant_id=model_variant_id,
                selected_vendor=selected_vendor,
                path=path,
                outcome=normalized_outcome,
                error_code=normalized_error,
                streamed=streamed,
                started_at=started_at,
                finished_at=finished_at,
                request_duration_seconds=duration,
                time_to_first_token_seconds=ttft,
                allocated_gpu_seconds=allocated_gpu,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
            ):
                raise UsageInvariantViolation(
                    "conflicting usage was submitted for an already settled serving request"
                )
            return existing, False

        owner_project_id = await session.scalar(
            select(ModelService.project_id).where(ModelService.id == service_id)
        )
        if owner_project_id is None or owner_project_id != project_id:
            raise UsageInvariantViolation("serving usage service does not belong to the project")
        if replica_id is not None:
            replica_service_id = await session.scalar(
                select(ServiceReplica.service_id).where(ServiceReplica.id == replica_id)
            )
            if replica_service_id != service_id:
                raise UsageInvariantViolation(
                    "serving usage replica does not belong to the service"
                )

        usage = ServingRequestUsage(
            request_id=request_id,
            project_id=project_id,
            service_id=service_id,
            replica_id=replica_id,
            logical_model_id=logical_model_id,
            model_variant_id=model_variant_id,
            selected_vendor=selected_vendor,
            path=path,
            outcome=normalized_outcome,
            error_code=normalized_error,
            streamed=streamed,
            started_at=started_at,
            finished_at=finished_at,
            request_duration_seconds=duration,
            time_to_first_token_seconds=ttft,
            allocated_gpu_seconds=allocated_gpu,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            created_at=await database_utcnow(session),
        )
        session.add(usage)
        await session.flush()
        return usage, True

    @staticmethod
    async def aggregate_settled(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        from_time: datetime,
        to_time: datetime,
    ) -> UsageAggregate:
        """Aggregate rows whose `finished_at` is in the half-open `[from, to)` window."""

        from_time, to_time = _normalize_period(from_time, to_time)
        if to_time <= from_time:
            raise ValueError("usage period end must be after its start")
        base_filters = (
            UsageLedger.project_id == project_id,
            UsageLedger.finished_at >= from_time,
            UsageLedger.finished_at < to_time,
        )
        total = (
            await session.execute(
                select(
                    func.count(UsageLedger.id),
                    func.coalesce(func.sum(UsageLedger.cpu_seconds), 0),
                    func.coalesce(func.sum(UsageLedger.memory_gb_seconds), 0),
                    func.coalesce(func.sum(UsageLedger.gpu_seconds), 0),
                ).where(*base_filters)
            )
        ).one()
        currency_rows = (
            await session.execute(
                select(
                    UsageLedger.currency,
                    func.coalesce(func.sum(UsageLedger.cost), 0),
                )
                .where(*base_filters)
                .group_by(UsageLedger.currency)
                .order_by(UsageLedger.currency)
            )
        ).all()
        gpu_rows = (
            await session.execute(
                select(
                    UsageLedger.gpu_model,
                    func.coalesce(func.sum(UsageLedger.gpu_seconds), 0),
                )
                .where(*base_filters, UsageLedger.gpu_seconds > 0)
                .group_by(UsageLedger.gpu_model)
                .order_by(UsageLedger.gpu_model)
            )
        ).all()
        serving_total = (
            await session.execute(
                select(
                    func.count(ServingRequestUsage.id),
                    func.count(ServingRequestUsage.prompt_tokens),
                    func.coalesce(func.sum(ServingRequestUsage.prompt_tokens), 0),
                    func.coalesce(func.sum(ServingRequestUsage.completion_tokens), 0),
                    func.coalesce(func.sum(ServingRequestUsage.total_tokens), 0),
                    func.coalesce(func.sum(ServingRequestUsage.allocated_gpu_seconds), 0),
                ).where(
                    ServingRequestUsage.project_id == project_id,
                    ServingRequestUsage.finished_at >= from_time,
                    ServingRequestUsage.finished_at < to_time,
                )
            )
        ).one()
        replica_runtime_rows = (
            await session.execute(
                select(
                    ServiceReplica.container_started_at,
                    ServiceReplica.stopped_at,
                    ModelService.gpu_count,
                )
                .join(ModelService, ModelService.id == ServiceReplica.service_id)
                .where(
                    ModelService.project_id == project_id,
                    ModelService.gpu_count > 0,
                    ServiceReplica.container_started_at.is_not(None),
                    ServiceReplica.container_started_at < to_time,
                    or_(
                        ServiceReplica.stopped_at.is_(None),
                        ServiceReplica.stopped_at > from_time,
                    ),
                )
            )
        ).all()
        observed_until = min(to_time, await database_utcnow(session))
        replica_gpu_seconds = ZERO
        for replica_started_at, replica_stopped_at, gpu_count in replica_runtime_rows:
            if replica_started_at is None:
                continue
            interval_start = max(from_time, _as_utc(replica_started_at))
            interval_end = observed_until
            if replica_stopped_at is not None:
                interval_end = min(interval_end, _as_utc(replica_stopped_at))
            if interval_end > interval_start:
                replica_gpu_seconds += Decimal(
                    str((interval_end - interval_start).total_seconds())
                ) * int(gpu_count)
        return UsageAggregate(
            project_id=project_id,
            from_time=from_time,
            to_time=to_time,
            execution_count=int(total[0]),
            cpu_seconds=_decimal(total[1]),
            memory_gb_seconds=_decimal(total[2]),
            gpu_seconds=_decimal(total[3]),
            costs=tuple(
                CurrencyCost(currency=str(currency), cost=_decimal(cost))
                for currency, cost in currency_rows
            ),
            gpu_breakdown=tuple(
                GPUUsage(gpu_model=model, gpu_seconds=_decimal(seconds))
                for model, seconds in gpu_rows
            ),
            serving_request_count=int(serving_total[0]),
            serving_requests_with_token_usage=int(serving_total[1]),
            input_tokens=int(serving_total[2]),
            output_tokens=int(serving_total[3]),
            total_tokens=int(serving_total[4]),
            serving_allocated_gpu_seconds=_decimal(serving_total[5]),
            serving_replica_gpu_seconds=replica_gpu_seconds.quantize(Decimal("0.000001")),
        )


def _normalize_period(started_at: datetime, finished_at: datetime) -> tuple[datetime, datetime]:
    if (
        started_at.tzinfo is None
        or started_at.utcoffset() is None
        or finished_at.tzinfo is None
        or finished_at.utcoffset() is None
    ):
        raise ValueError("usage timestamps must include a timezone")
    normalized_start = started_at.astimezone(UTC)
    normalized_finish = finished_at.astimezone(UTC)
    return normalized_start, normalized_finish


def _validate_nonnegative_usage(
    cpu_seconds: Decimal,
    memory_gb_seconds: Decimal,
    gpu_seconds: Decimal,
    cost: Decimal,
) -> None:
    if min(cpu_seconds, memory_gb_seconds, gpu_seconds, cost) < ZERO:
        raise ValueError("usage and cost values must be non-negative")


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _same_usage(
    ledger: UsageLedger,
    *,
    started_at: datetime,
    finished_at: datetime,
    cpu_seconds: Decimal,
    memory_gb_seconds: Decimal,
    gpu_seconds: Decimal,
    gpu_model: str | None,
    cost: Decimal,
    currency: str,
    pricing_source: str,
) -> bool:
    return (
        _as_utc(ledger.started_at) == started_at
        and _as_utc(ledger.finished_at) == finished_at
        and ledger.cpu_seconds == cpu_seconds
        and ledger.memory_gb_seconds == memory_gb_seconds
        and ledger.gpu_seconds == gpu_seconds
        and ledger.gpu_model == gpu_model
        and ledger.cost == cost
        and ledger.currency == currency
        and ledger.pricing_source == pricing_source
    )


def _validate_token_usage(
    prompt_tokens: int | None,
    completion_tokens: int | None,
    total_tokens: int | None,
) -> None:
    values = (prompt_tokens, completion_tokens, total_tokens)
    if all(value is None for value in values):
        return
    if any(value is None for value in values):
        raise ValueError("serving token usage must be entirely available or unavailable")
    assert prompt_tokens is not None
    assert completion_tokens is not None
    assert total_tokens is not None
    if (
        any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (prompt_tokens, completion_tokens, total_tokens)
        )
        or total_tokens != prompt_tokens + completion_tokens
    ):
        raise ValueError("serving token usage must be non-negative and internally consistent")


def _same_serving_usage(
    usage: ServingRequestUsage,
    *,
    project_id: uuid.UUID,
    service_id: uuid.UUID,
    replica_id: uuid.UUID | None,
    logical_model_id: uuid.UUID | None = None,
    model_variant_id: uuid.UUID | None = None,
    selected_vendor: str | None = None,
    path: str,
    outcome: str,
    error_code: str | None,
    streamed: bool,
    started_at: datetime,
    finished_at: datetime,
    request_duration_seconds: Decimal,
    time_to_first_token_seconds: Decimal | None,
    allocated_gpu_seconds: Decimal | None,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    total_tokens: int | None,
) -> bool:
    return (
        usage.project_id == project_id
        and usage.service_id == service_id
        and usage.replica_id == replica_id
        and usage.logical_model_id == logical_model_id
        and usage.model_variant_id == model_variant_id
        and usage.selected_vendor == selected_vendor
        and usage.path == path
        and usage.outcome == outcome
        and usage.error_code == error_code
        and usage.streamed is streamed
        and _as_utc(usage.started_at) == started_at
        and _as_utc(usage.finished_at) == finished_at
        and usage.request_duration_seconds == request_duration_seconds
        and usage.time_to_first_token_seconds == time_to_first_token_seconds
        and usage.allocated_gpu_seconds == allocated_gpu_seconds
        and usage.prompt_tokens == prompt_tokens
        and usage.completion_tokens == completion_tokens
        and usage.total_tokens == total_tokens
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
