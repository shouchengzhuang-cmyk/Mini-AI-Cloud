import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.usage import TaskExecution, UsageLedger
from repositories.clock import database_utcnow

ZERO = Decimal("0")
_CURRENCY = re.compile(r"^[A-Z]{3}$")


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
