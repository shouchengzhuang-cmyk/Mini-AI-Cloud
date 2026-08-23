from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def database_utcnow(session: AsyncSession) -> datetime:
    """Read the database clock used for distributed leases and lock deadlines."""

    value = await session.scalar(select(func.current_timestamp()))
    if value is None:
        raise RuntimeError("database did not return CURRENT_TIMESTAMP")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
