from __future__ import annotations

import asyncio
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

from api.dependencies import get_websocket_principal
from api.pagination import CursorKey, decode_cursor, encode_cursor
from api.schemas.events import ProjectEventEnvelope
from core.config import Settings
from core.database import Database
from core.rbac import Permission, Principal, has_permission
from repositories.events import ProjectEventRepository

router = APIRouter(tags=["events"])


@router.websocket("/api/v1/projects/{project_id}/events/ws")
async def project_event_stream(
    websocket: WebSocket,
    project_id: uuid.UUID,
    principal: Annotated[Principal, Depends(get_websocket_principal)],
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> None:
    if principal.project_id != project_id or not has_permission(principal, Permission.TASK_READ):
        await websocket.close(code=4403, reason="PROJECT_ACCESS_DENIED")
        return
    try:
        after = decode_cursor(cursor) if cursor is not None else None
    except ValueError:
        await websocket.close(code=4400, reason="INVALID_CURSOR")
        return

    database: Database = websocket.app.state.database
    settings: Settings = websocket.app.state.settings
    await websocket.accept()
    poll_seconds = min(max(settings.outbox_poll_interval, 0.05), 1.0)
    heartbeat_seconds = max(settings.sse_heartbeat_seconds, poll_seconds)
    last_heartbeat = asyncio.get_running_loop().time()
    try:
        while True:
            after = await _send_available(websocket, database, project_id, after, settings)
            now = asyncio.get_running_loop().time()
            if now - last_heartbeat >= heartbeat_seconds:
                await websocket.send_json(
                    {
                        "type": "heartbeat",
                        "cursor": (
                            encode_cursor(after.created_at, after.item_id) if after else None
                        ),
                    }
                )
                last_heartbeat = now
            try:
                message = await asyncio.wait_for(websocket.receive(), timeout=poll_seconds)
            except TimeoutError:
                continue
            if message["type"] == "websocket.disconnect":
                return
    except (WebSocketDisconnect, RuntimeError):
        return
    finally:
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.close()


async def _send_available(
    websocket: WebSocket,
    database: Database,
    project_id: uuid.UUID,
    after: CursorKey | None,
    settings: Settings,
) -> CursorKey | None:
    batch_size = min(settings.websocket_queue_size, settings.batch_size, 1000)
    async with database.session() as session:
        events = await ProjectEventRepository.list_for_project(
            session,
            project_id=project_id,
            after=after,
            limit=batch_size,
        )
    for event in events:
        cursor = encode_cursor(event.created_at, event.id)
        envelope = ProjectEventEnvelope(
            id=event.id,
            cursor=cursor,
            aggregate_id=event.aggregate_id,
            aggregate_type=event.aggregate_type,
            event_type=event.event_type,
            event_version=event.event_version,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            payload=event.payload,
            occurred_at=event.created_at,
        )
        await websocket.send_json(envelope.model_dump(mode="json"))
        after = CursorKey(created_at=event.created_at, item_id=event.id)
    return after
