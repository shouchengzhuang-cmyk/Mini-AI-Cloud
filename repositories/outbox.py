import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.base import utcnow
from models.outbox import OutboxEvent
from repositories.clock import database_utcnow


class OutboxRepository:
    @staticmethod
    def add(
        session: AsyncSession,
        *,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        available_at: datetime | None = None,
    ) -> OutboxEvent:
        event = OutboxEvent(
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            available_at=available_at or utcnow(),
        )
        session.add(event)
        return event

    @staticmethod
    def pending_query(limit: int, now: datetime) -> Select[tuple[OutboxEvent]]:
        return (
            select(OutboxEvent)
            .where(
                OutboxEvent.processed_at.is_(None),
                OutboxEvent.available_at <= now,
                or_(
                    OutboxEvent.locked_until.is_(None),
                    OutboxEvent.locked_until < now,
                ),
            )
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    @staticmethod
    async def claim_batch(
        session: AsyncSession,
        *,
        owner: uuid.UUID,
        limit: int,
        lock_seconds: float = 30.0,
    ) -> list[OutboxEvent]:
        now = await database_utcnow(session)
        events = list(await session.scalars(OutboxRepository.pending_query(limit, now)))
        locked_until = now + timedelta(seconds=lock_seconds)
        for event in events:
            event.locked_by = owner
            event.locked_until = locked_until
        return events

    @staticmethod
    async def mark_processed(
        session: AsyncSession, event_id: uuid.UUID, owner: uuid.UUID
    ) -> OutboxEvent | None:
        event = await session.get(OutboxEvent, event_id, with_for_update=True)
        if event is None or event.processed_at is not None or event.locked_by != owner:
            return None
        event.processed_at = await database_utcnow(session)
        event.locked_by = None
        event.locked_until = None
        event.last_error = None
        return event

    @staticmethod
    async def mark_failed(
        session: AsyncSession,
        *,
        event_id: uuid.UUID,
        owner: uuid.UUID,
        error: str,
        max_backoff_seconds: float = 60.0,
    ) -> None:
        event = await session.get(OutboxEvent, event_id, with_for_update=True)
        if event is None or event.processed_at is not None or event.locked_by != owner:
            return
        event.attempts += 1
        delay = min(float(2 ** min(event.attempts - 1, 10)), max_backoff_seconds)
        event.available_at = await database_utcnow(session) + timedelta(seconds=delay)
        event.last_error = error[:4000]
        event.locked_by = None
        event.locked_until = None
