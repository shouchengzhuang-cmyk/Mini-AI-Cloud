import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import StreamingResponse

from api.dependencies import get_app_settings, get_database, require_api_permission
from api.errors import APIError, ConflictError, NotFoundError, ServiceUnavailableError
from api.pagination import encode_cursor
from api.routes._pagination import parse_list_cursor
from api.schemas.artifacts import (
    ArtifactCreate,
    ArtifactFinalize,
    ArtifactListResponse,
    ArtifactResponse,
    ArtifactTransferResponse,
)
from api.schemas.common import PaginationMeta
from api.services.artifacts import ArtifactBackendMismatchError, ArtifactService
from core.artifacts import (
    ArtifactIntegrityError,
    ArtifactObjectChangedError,
    ArtifactObjectNotFoundError,
    ArtifactState,
    ArtifactStore,
    ArtifactStoreError,
    ArtifactTooLargeError,
    SignedArtifactURL,
    artifact_content_disposition,
    build_artifact_store,
)
from core.config import Settings
from core.database import Database
from core.rbac import Permission, Principal
from repositories.artifacts import (
    ArtifactProjectUnavailableError,
    ArtifactQuotaExceededError,
    ArtifactRecordNotFoundError,
    ArtifactReferencedError,
    ArtifactStateConflictError,
)

router = APIRouter(prefix="/api/v1/artifacts", tags=["artifacts"])


def get_artifact_store(
    request: Request,
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> ArtifactStore:
    existing = getattr(request.app.state, "artifact_store", None)
    if existing is not None:
        return cast(ArtifactStore, existing)
    store = build_artifact_store(settings)
    request.app.state.artifact_store = store
    return store


@router.post("", response_model=ArtifactResponse, status_code=status.HTTP_201_CREATED)
async def create_artifact(
    payload: ArtifactCreate,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> ArtifactResponse:
    with _artifact_errors():
        artifact = await ArtifactService(database, settings, store).create(
            payload, principal=principal
        )
    return ArtifactResponse.model_validate(artifact)


@router.get("", response_model=ArtifactListResponse)
async def list_artifacts(
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
    artifact_state: Annotated[ArtifactState | None, Query(alias="state")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> ArtifactListResponse:
    after = parse_list_cursor(cursor=cursor, offset=offset)
    with _artifact_errors():
        rows, total = await ArtifactService(database, settings, store).list(
            principal=principal,
            state=artifact_state,
            limit=limit + 1,
            offset=offset,
            after=after,
        )
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = encode_cursor(items[-1].created_at, items[-1].id) if has_more and items else None
    return ArtifactListResponse(
        items=[ArtifactResponse.model_validate(item) for item in items],
        pagination=PaginationMeta(
            total=total,
            limit=limit,
            offset=offset if cursor is None else 0,
            next_cursor=next_cursor,
        ),
    )


@router.get("/{artifact_id}", response_model=ArtifactResponse)
async def get_artifact(
    artifact_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> ArtifactResponse:
    with _artifact_errors():
        artifact = await ArtifactService(database, settings, store).get(
            artifact_id, principal=principal
        )
        if artifact is None:
            raise ArtifactRecordNotFoundError("artifact does not exist in project")
    return ArtifactResponse.model_validate(artifact)


@router.post("/{artifact_id}/upload-url", response_model=ArtifactTransferResponse)
async def create_artifact_upload_url(
    artifact_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> ArtifactTransferResponse:
    with _artifact_errors():
        artifact, signed = await ArtifactService(database, settings, store).upload_access(
            artifact_id, principal=principal
        )
    if signed is not None:
        return _signed_response(signed)
    assert artifact.size_bytes is not None
    assert artifact.sha256 is not None
    return ArtifactTransferResponse(
        method="PUT",
        url=_content_url(request, artifact.id),
        headers={
            "Content-Type": artifact.content_type or "application/octet-stream",
            "Content-Length": str(artifact.size_bytes),
            "X-Content-SHA256": artifact.sha256,
        },
        expires_at=None,
        authorization="api",
    )


@router.put("/{artifact_id}/content", response_model=ArtifactResponse)
async def upload_artifact_content(
    artifact_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
    content_length: Annotated[int | None, Header(alias="Content-Length", ge=0)] = None,
    content_sha256: Annotated[
        str | None,
        Header(
            alias="X-Content-SHA256",
            min_length=64,
            max_length=64,
            pattern=r"^[0-9A-Fa-f]{64}$",
        ),
    ] = None,
) -> ArtifactResponse:
    with _artifact_errors():
        artifact = await ArtifactService(database, settings, store).upload(
            artifact_id,
            request.stream(),
            principal=principal,
            content_length=content_length,
            content_sha256=content_sha256,
        )
    return ArtifactResponse.model_validate(artifact)


@router.post("/{artifact_id}/finalize", response_model=ArtifactResponse)
async def finalize_artifact(
    artifact_id: uuid.UUID,
    payload: ArtifactFinalize,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> ArtifactResponse:
    with _artifact_errors():
        artifact = await ArtifactService(database, settings, store).finalize(
            artifact_id, payload, principal=principal
        )
    return ArtifactResponse.model_validate(artifact)


@router.get("/{artifact_id}/download-url", response_model=ArtifactTransferResponse)
async def create_artifact_download_url(
    artifact_id: uuid.UUID,
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> ArtifactTransferResponse:
    with _artifact_errors():
        artifact, signed = await ArtifactService(database, settings, store).download_access(
            artifact_id, principal=principal
        )
    if signed is not None:
        return _signed_response(signed)
    return ArtifactTransferResponse(
        method="GET",
        url=_content_url(request, artifact.id),
        headers={},
        expires_at=None,
        authorization="api",
    )


@router.get("/{artifact_id}/content")
async def download_artifact_content(
    artifact_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_READ))],
) -> StreamingResponse:
    with _artifact_errors():
        artifact, content = await ArtifactService(database, settings, store).download(
            artifact_id, principal=principal
        )
    headers = {
        "Content-Disposition": artifact_content_disposition(artifact.name),
        "X-Content-Type-Options": "nosniff",
    }
    if artifact.size_bytes is not None:
        headers["Content-Length"] = str(artifact.size_bytes)
    if artifact.sha256 is not None:
        headers["ETag"] = f'"{artifact.sha256}"'
    return StreamingResponse(
        content,
        media_type=artifact.content_type or "application/octet-stream",
        headers=headers,
    )


@router.delete("/{artifact_id}", response_model=ArtifactResponse)
async def delete_artifact(
    artifact_id: uuid.UUID,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    store: Annotated[ArtifactStore, Depends(get_artifact_store)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.TASK_CREATE))],
) -> ArtifactResponse:
    with _artifact_errors():
        artifact = await ArtifactService(database, settings, store).delete(
            artifact_id, principal=principal
        )
    return ArtifactResponse.model_validate(artifact)


def _signed_response(signed: SignedArtifactURL) -> ArtifactTransferResponse:
    if signed.method not in {"GET", "PUT"}:
        raise ValueError("artifact store returned an unsupported signed URL method")
    return ArtifactTransferResponse(
        method=cast(Literal["GET", "PUT"], signed.method),
        url=signed.url,
        headers=signed.headers,
        expires_at=signed.expires_at,
        authorization="presigned",
    )


def _content_url(request: Request, artifact_id: uuid.UUID) -> str:
    # Keep API-authorized transfer grants same-origin without reflecting the
    # untrusted Host header into a URL that clients will follow with an API key.
    root_path = str(request.scope.get("root_path", "")).rstrip("/")
    return f"{root_path}/api/v1/artifacts/{artifact_id}/content"


@contextmanager
def _artifact_errors() -> Iterator[None]:
    try:
        yield
    except ArtifactRecordNotFoundError as exc:
        raise NotFoundError("ARTIFACT_NOT_FOUND", "Artifact not found") from exc
    except ArtifactProjectUnavailableError as exc:
        raise NotFoundError("PROJECT_NOT_FOUND", "Project not found") from exc
    except ArtifactQuotaExceededError as exc:
        raise ConflictError(
            "ARTIFACT_QUOTA_EXCEEDED",
            "Project artifact storage quota would be exceeded",
            details={
                "limit_bytes": exc.limit_bytes,
                "used_bytes": exc.used_bytes,
                "requested_bytes": exc.requested_bytes,
            },
        ) from exc
    except ArtifactTooLargeError as exc:
        raise APIError(
            413,
            "ARTIFACT_TOO_LARGE",
            f"Artifact exceeds the maximum size of {exc.maximum_bytes} bytes",
        ) from exc
    except ArtifactIntegrityError as exc:
        raise APIError(422, "ARTIFACT_INTEGRITY_MISMATCH", str(exc)) from exc
    except ArtifactObjectNotFoundError as exc:
        raise ConflictError(
            "ARTIFACT_OBJECT_NOT_UPLOADED",
            "Artifact content has not been uploaded",
        ) from exc
    except ArtifactReferencedError as exc:
        raise ConflictError(
            "ARTIFACT_REFERENCED",
            "Artifact is still referenced by a task or dataset",
        ) from exc
    except (ArtifactStateConflictError, ArtifactObjectChangedError) as exc:
        raise ConflictError("ARTIFACT_STATE_CONFLICT", str(exc)) from exc
    except ArtifactBackendMismatchError as exc:
        raise ServiceUnavailableError(
            "ARTIFACT_BACKEND_UNAVAILABLE",
            "The artifact storage backend is unavailable",
        ) from exc
    except ArtifactStoreError as exc:
        raise ServiceUnavailableError(
            "ARTIFACT_STORE_UNAVAILABLE",
            "Artifact storage is temporarily unavailable",
        ) from exc
