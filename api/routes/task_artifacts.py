import uuid
from typing import Annotated

from fastapi import APIRouter, Depends

from api.dependencies import get_database, require_api_permission
from api.errors import NotFoundError
from api.schemas.task_artifacts import TaskArtifactResponse
from core.database import Database
from core.rbac import Permission, Principal
from repositories.task_artifacts import TaskArtifactNotFoundError, TaskArtifactRepository

router = APIRouter(prefix="/api/v1/tasks/{task_id}/artifacts", tags=["task-artifacts"])


@router.get("", response_model=list[TaskArtifactResponse])
async def list_task_artifacts(
    task_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> list[TaskArtifactResponse]:
    if principal.project_id is None:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found")
    try:
        async with database.session() as session:
            bindings = await TaskArtifactRepository.list_for_task(
                session,
                task_id=task_id,
                project_id=principal.project_id,
            )
    except TaskArtifactNotFoundError as exc:
        raise NotFoundError("TASK_NOT_FOUND", "Task not found") from exc
    return [TaskArtifactResponse.model_validate(item) for item in bindings]
