import uuid
from datetime import datetime

from api.schemas.common import ResponseModel


class ProjectEventEnvelope(ResponseModel):
    id: uuid.UUID
    cursor: str
    aggregate_id: uuid.UUID
    aggregate_type: str
    event_type: str
    event_version: int
    correlation_id: str | None
    trace_id: str | None
    payload: dict[str, object]
    occurred_at: datetime
