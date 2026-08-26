from typing import Annotated

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_database, require_api_permission
from api.errors import APIError
from api.pagination import decode_cursor, encode_cursor
from api.schemas.audit import AuditEventListResponse, AuditEventResponse
from api.schemas.common import PaginationMeta
from core.database import Database
from core.rbac import Permission, Principal
from repositories.audit import AuditRepository

router = APIRouter(prefix="/api/v1/audit-events", tags=["audit"])


@router.get("", response_model=AuditEventListResponse)
async def list_audit_events(
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.AUDIT_READ))],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> AuditEventListResponse:
    if principal.project_id is None:
        raise APIError(403, "PERMISSION_DENIED", "A project principal is required")
    if cursor is not None and offset:
        raise APIError(422, "INVALID_PAGINATION", "cursor and non-zero offset cannot be combined")
    try:
        after = decode_cursor(cursor) if cursor is not None else None
    except ValueError as exc:
        raise APIError(422, "INVALID_CURSOR", "The pagination cursor is invalid") from exc
    async with database.session() as session:
        rows = await AuditRepository.list_for_project(
            session,
            project_id=principal.project_id,
            limit=limit + 1,
            offset=offset,
            after=after,
        )
        total = await AuditRepository.count_for_project(session, project_id=principal.project_id)
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = encode_cursor(items[-1].occurred_at, items[-1].id) if has_more and items else None
    return AuditEventListResponse(
        items=[AuditEventResponse.model_validate(item) for item in items],
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset if cursor is None else 0,
            next_cursor=next_cursor,
        ),
    )
