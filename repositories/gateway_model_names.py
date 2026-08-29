from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.identity import Project
from models.model_variant import LogicalModel
from models.service import ModelService


class GatewayModelNameConflictError(ValueError):
    def __init__(self, name: str) -> None:
        super().__init__(f"gateway model name {name!r} is already owned by another model")
        self.name = name


async def lock_gateway_model_namespace(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
) -> Project | None:
    return await session.scalar(select(Project).where(Project.id == project_id).with_for_update())


async def check_logical_public_name_available(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    public_name: str,
) -> None:
    conflicting_service = await session.scalar(
        select(ModelService.id).where(
            ModelService.project_id == project_id,
            ModelService.name == public_name,
        )
    )
    if conflicting_service is not None:
        raise GatewayModelNameConflictError(public_name)


async def check_service_name_available(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    service_name: str,
    logical_model_id: uuid.UUID | None,
) -> None:
    public_owner = await session.scalar(
        select(LogicalModel.id).where(
            LogicalModel.project_id == project_id,
            LogicalModel.public_name == service_name,
        )
    )
    if public_owner is not None and public_owner != logical_model_id:
        raise GatewayModelNameConflictError(service_name)
