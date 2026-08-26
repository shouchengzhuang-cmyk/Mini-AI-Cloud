import uuid
from datetime import datetime

from api.schemas.common import PaginationMeta, ResponseModel


class AuditEventResponse(ResponseModel):
    id: uuid.UUID
    project_id: uuid.UUID | None
    actor_type: str
    actor_user_id: uuid.UUID | None
    api_key_id: uuid.UUID | None
    action: str
    resource_type: str
    resource_id: str | None
    outcome: str
    request_id: str | None
    source_ip: str | None
    details: dict[str, object]
    occurred_at: datetime


class AuditEventListResponse(ResponseModel):
    items: list[AuditEventResponse]
    pagination: PaginationMeta
