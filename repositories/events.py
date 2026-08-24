from __future__ import annotations

import uuid

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from api.pagination import CursorKey
from models.artifact import Artifact, Dataset, JobGroup
from models.outbox import OutboxEvent
from models.service import ModelService, ServiceReplica
from models.task import Task


class ProjectEventRepository:
    """Read project events from the durable outbox without trusting client filters."""

    @staticmethod
    async def list_for_project(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        limit: int,
        after: CursorKey | None = None,
    ) -> list[OutboxEvent]:
        query = select(OutboxEvent).where(_belongs_to_project(project_id))
        if after is not None:
            query = query.where(
                or_(
                    OutboxEvent.created_at > after.created_at,
                    and_(
                        OutboxEvent.created_at == after.created_at,
                        OutboxEvent.id > after.item_id,
                    ),
                )
            )
        return list(
            await session.scalars(
                query.order_by(OutboxEvent.created_at, OutboxEvent.id).limit(limit)
            )
        )


def _belongs_to_project(project_id: uuid.UUID) -> ColumnElement[bool]:
    project_text = str(project_id)
    task_ids = select(Task.id).where(Task.project_id == project_id)
    service_ids = select(ModelService.id).where(ModelService.project_id == project_id)
    replica_ids = (
        select(ServiceReplica.id)
        .join(ModelService, ModelService.id == ServiceReplica.service_id)
        .where(ModelService.project_id == project_id)
    )
    artifact_ids = select(Artifact.id).where(Artifact.project_id == project_id)
    dataset_ids = select(Dataset.id).where(Dataset.project_id == project_id)
    group_ids = select(JobGroup.id).where(JobGroup.project_id == project_id)
    known_aggregate_types = (
        "task",
        "service",
        "model_service",
        "service_replica",
        "artifact",
        "dataset",
        "job_group",
    )
    return or_(
        and_(
            OutboxEvent.aggregate_type.not_in(known_aggregate_types),
            OutboxEvent.payload["project_id"].as_string() == project_text,
        ),
        and_(OutboxEvent.aggregate_type == "task", OutboxEvent.aggregate_id.in_(task_ids)),
        and_(
            OutboxEvent.aggregate_type.in_(("service", "model_service")),
            OutboxEvent.aggregate_id.in_(service_ids),
        ),
        and_(
            OutboxEvent.aggregate_type == "service_replica",
            OutboxEvent.aggregate_id.in_(replica_ids),
        ),
        and_(
            OutboxEvent.aggregate_type == "artifact",
            OutboxEvent.aggregate_id.in_(artifact_ids),
        ),
        and_(
            OutboxEvent.aggregate_type == "dataset",
            OutboxEvent.aggregate_id.in_(dataset_ids),
        ),
        and_(
            OutboxEvent.aggregate_type == "job_group",
            OutboxEvent.aggregate_id.in_(group_ids),
        ),
    )
