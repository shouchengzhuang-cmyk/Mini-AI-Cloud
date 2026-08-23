import json
import uuid
from collections.abc import Mapping
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

READY_STREAM = "tasks:ready"
READY_GROUP = "task-workers"
EVENT_STREAM = "tasks:events"


class RedisQueue:
    def __init__(
        self,
        url: str,
        *,
        log_stream_maxlen: int = 10000,
        log_stream_ttl_seconds: int = 86400,
        ready_stream_maxlen: int = 100000,
        socket_timeout: float = 5.0,
    ) -> None:
        if log_stream_ttl_seconds <= 0:
            raise ValueError("log_stream_ttl_seconds must be greater than zero")
        self.client: Redis = Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=socket_timeout,
            socket_timeout=socket_timeout,
        )
        self.log_stream_maxlen = log_stream_maxlen
        self.log_stream_ttl_seconds = log_stream_ttl_seconds
        self.ready_stream_maxlen = ready_stream_maxlen

    async def ping(self) -> bool:
        return bool(await self.client.ping())

    async def close(self) -> None:
        await self.client.aclose()

    async def ensure_ready_group(self) -> None:
        try:
            await self.client.xgroup_create(READY_STREAM, READY_GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish_outbox(
        self,
        *,
        event_id: uuid.UUID,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> str:
        fields: dict[Any, Any] = {
            "event_id": str(event_id),
            "event_type": event_type,
            "payload": json.dumps(payload, separators=(",", ":"), sort_keys=True),
        }
        if event_type == "task.ready":
            fields["task_id"] = str(payload["task_id"])
            return str(
                await self.client.xadd(
                    READY_STREAM,
                    fields,
                    maxlen=self.ready_stream_maxlen,
                    approximate=True,
                )
            )
        return str(await self.client.xadd(EVENT_STREAM, fields, maxlen=10000, approximate=True))

    async def read_ready(
        self, *, consumer: str, count: int, block_ms: int
    ) -> list[tuple[str, uuid.UUID]]:
        try:
            response = await self.client.xreadgroup(
                READY_GROUP,
                consumer,
                {READY_STREAM: ">"},
                count=count,
                block=block_ms,
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            await self.ensure_ready_group()
            response = await self.client.xreadgroup(
                READY_GROUP,
                consumer,
                {READY_STREAM: ">"},
                count=count,
                block=block_ms,
            )
        messages: list[tuple[str, uuid.UUID]] = []
        for _stream, entries in response:
            for message_id, fields in entries:
                task_id = fields.get("task_id")
                if task_id is not None:
                    messages.append((str(message_id), uuid.UUID(task_id)))
        return messages

    async def reclaim_ready(
        self, *, consumer: str, min_idle_ms: int, count: int
    ) -> list[tuple[str, uuid.UUID]]:
        try:
            response = await self.client.xautoclaim(
                READY_STREAM,
                READY_GROUP,
                consumer,
                min_idle_ms,
                "0-0",
                count=count,
            )
        except ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            await self.ensure_ready_group()
            return []
        entries = response[1] if len(response) > 1 else []
        messages: list[tuple[str, uuid.UUID]] = []
        for message_id, fields in entries:
            task_id = fields.get("task_id")
            if task_id is not None:
                messages.append((str(message_id), uuid.UUID(task_id)))
        return messages

    async def acknowledge_ready(self, message_id: str) -> None:
        async with self.client.pipeline(transaction=True) as pipeline:
            pipeline.xack(READY_STREAM, READY_GROUP, message_id)
            pipeline.xdel(READY_STREAM, message_id)
            await pipeline.execute()

    async def publish_log(
        self,
        *,
        task_id: uuid.UUID,
        sequence: int,
    ) -> str:
        key = self.log_stream_key(task_id)
        async with self.client.pipeline(transaction=True) as pipeline:
            pipeline.xadd(
                key,
                {"sequence": str(sequence)},
                maxlen=self.log_stream_maxlen,
                approximate=True,
            )
            pipeline.expire(key, self.log_stream_ttl_seconds)
            results = await pipeline.execute()
        return str(results[0])

    async def wait_for_logs(
        self, *, task_id: uuid.UUID, last_id: str, block_ms: int
    ) -> list[tuple[str, dict[str, str]]]:
        response = await self.client.xread(
            {self.log_stream_key(task_id): last_id}, count=100, block=block_ms
        )
        if not response:
            return []
        return [(str(message_id), fields) for message_id, fields in response[0][1]]

    async def delete_log_stream(self, task_id: uuid.UUID) -> None:
        await self.client.delete(self.log_stream_key(task_id))

    @staticmethod
    def log_stream_key(task_id: uuid.UUID) -> str:
        return f"tasks:logs:{task_id}"
