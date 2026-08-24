from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.pagination import CursorKey
from models.usage import AuditEvent
from repositories.clock import database_utcnow


class AuditRepository:
    """Project-scoped writes and reads for the append-only audit ledger."""

    @staticmethod
    async def record(
        session: AsyncSession,
        *,
        project_id: uuid.UUID | None,
        actor_type: str,
        actor_user_id: uuid.UUID | None,
        api_key_id: uuid.UUID | None,
        action: str,
        resource_type: str,
        resource_id: str | None,
        outcome: str,
        request_id: str | None,
        source_ip: str | None,
        details: dict[str, object] | None = None,
        occurred_at: datetime | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            project_id=project_id,
            actor_type=actor_type,
            actor_user_id=actor_user_id,
            api_key_id=api_key_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome=outcome,
            request_id=request_id,
            source_ip=source_ip,
            details=details or {},
            occurred_at=occurred_at or await database_utcnow(session),
        )
        session.add(event)
        await session.flush()
        return event

    @staticmethod
    async def get_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        event_id: uuid.UUID,
    ) -> AuditEvent | None:
        return await session.scalar(
            select(AuditEvent).where(
                AuditEvent.project_id == project_id,
                AuditEvent.id == event_id,
            )
        )

    @staticmethod
    async def list_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        offset: int = 0,
        after: CursorKey | None = None,
    ) -> list[AuditEvent]:
        query = select(AuditEvent).where(AuditEvent.project_id == project_id)
        if after is not None:
            query = query.where(
                or_(
                    AuditEvent.occurred_at < after.created_at,
                    (
                        (AuditEvent.occurred_at == after.created_at)
                        & (AuditEvent.id < after.item_id)
                    ),
                )
            )
        return list(
            await session.scalars(
                query.order_by(AuditEvent.occurred_at.desc(), AuditEvent.id.desc())
                .limit(limit)
                .offset(0 if after is not None else offset)
            )
        )

    @staticmethod
    async def count_for_project(session: AsyncSession, *, project_id: uuid.UUID) -> int:
        return int(
            await session.scalar(
                select(func.count(AuditEvent.id)).where(AuditEvent.project_id == project_id)
            )
            or 0
        )
