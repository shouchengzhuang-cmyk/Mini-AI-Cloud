from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.database import Database
from core.redis import RedisQueue


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


def get_app_settings(request: Request) -> Settings:
    return request.app.state.settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    database: Database = request.app.state.database
    async with database.session() as session:
        yield session
