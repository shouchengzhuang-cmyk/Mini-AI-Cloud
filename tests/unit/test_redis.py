import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from redis.asyncio import Redis

from core.redis import READINESS_WRITE_KEY, RedisQueue


@pytest_asyncio.fixture
async def redis_queue() -> AsyncIterator[RedisQueue]:
    queue = RedisQueue(
        "redis://unused.invalid/0",
        log_stream_maxlen=100,
        log_stream_ttl_seconds=30,
    )
    original_client = queue.client
    queue.client = cast(Redis, FakeRedis(decode_responses=True))
    await original_client.aclose()
    try:
        yield queue
    finally:
        await queue.close()


async def test_publish_log_adds_stream_entry_and_ttl(redis_queue: RedisQueue) -> None:
    task_id = uuid.uuid4()
    message_id = await redis_queue.publish_log(
        task_id=task_id,
        sequence=1,
    )

    key = redis_queue.log_stream_key(task_id)
    entries = await redis_queue.client.xrange(key)
    ttl = await redis_queue.client.ttl(key)

    assert message_id == entries[0][0]
    assert entries[0][1] == {"sequence": "1"}
    assert 0 < ttl <= redis_queue.log_stream_ttl_seconds


async def test_publish_log_refreshes_ttl(redis_queue: RedisQueue) -> None:
    task_id = uuid.uuid4()
    key = redis_queue.log_stream_key(task_id)
    await redis_queue.publish_log(
        task_id=task_id,
        sequence=1,
    )
    await redis_queue.client.expire(key, 1)
    shortened_ttl = await redis_queue.client.ttl(key)

    await redis_queue.publish_log(
        task_id=task_id,
        sequence=2,
    )

    refreshed_ttl = await redis_queue.client.ttl(key)
    assert shortened_ttl <= 1
    assert refreshed_ttl > shortened_ttl
    assert refreshed_ttl <= redis_queue.log_stream_ttl_seconds
    assert await redis_queue.client.xlen(key) == 2


async def test_log_stream_expires_after_ttl() -> None:
    queue = RedisQueue(
        "redis://unused.invalid/0",
        log_stream_ttl_seconds=1,
    )
    original_client = queue.client
    queue.client = cast(Redis, FakeRedis(decode_responses=True))
    await original_client.aclose()
    try:
        task_id = uuid.uuid4()
        key = queue.log_stream_key(task_id)
        await queue.publish_log(
            task_id=task_id,
            sequence=1,
        )

        await asyncio.sleep(1.1)
        assert await queue.client.exists(key) == 0
    finally:
        await queue.close()


async def test_delete_log_stream_removes_only_requested_task(redis_queue: RedisQueue) -> None:
    deleted_task_id = uuid.uuid4()
    retained_task_id = uuid.uuid4()
    await redis_queue.publish_log(task_id=deleted_task_id, sequence=1)
    await redis_queue.publish_log(task_id=retained_task_id, sequence=1)

    await redis_queue.delete_log_stream(deleted_task_id)

    assert await redis_queue.client.exists(redis_queue.log_stream_key(deleted_task_id)) == 0
    assert await redis_queue.client.exists(redis_queue.log_stream_key(retained_task_id)) == 1


async def test_rate_limit_readiness_exercises_a_bounded_redis_write(
    redis_queue: RedisQueue,
) -> None:
    assert await redis_queue.rate_limit_backend_ready() is True
    assert await redis_queue.client.get(READINESS_WRITE_KEY) == "1"
    ttl = await redis_queue.client.ttl(READINESS_WRITE_KEY)
    assert 0 < ttl <= 30


@pytest.mark.parametrize("ttl", [0, -1])
def test_log_stream_ttl_must_be_positive(ttl: int) -> None:
    with pytest.raises(ValueError, match="log_stream_ttl_seconds must be greater than zero"):
        RedisQueue("redis://unused.invalid/0", log_stream_ttl_seconds=ttl)
