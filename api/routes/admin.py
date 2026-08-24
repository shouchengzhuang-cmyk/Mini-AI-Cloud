from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from api.dependencies import get_app_settings, get_database, require_api_permission
from api.errors import NotFoundError
from api.schemas.admin import (
    ActiveReservationResponse,
    AdminDiagnosticsResponse,
    AdminRepairResponse,
    ConsistencyResponse,
    OfflineWorkerResponse,
    OutboxLagResponse,
    RepairCapabilityResponse,
    SchedulerDiagnosticResponse,
    SchedulerDiagnosticStatus,
    StuckTaskResponse,
    WorkerDrainRequest,
)
from api.schemas.workers import WorkerResponse
from core.config import Settings
from core.database import Database
from core.rbac import Permission, Principal
from repositories.diagnostics import DiagnosticSnapshot, DiagnosticsRepository
from repositories.workers import WorkerRepository

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.get("/diagnostics", response_model=AdminDiagnosticsResponse)
async def get_diagnostics(
    request: Request,
    database: Annotated[Database, Depends(get_database)],
    settings: Annotated[Settings, Depends(get_app_settings)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.AUDIT_READ))],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminDiagnosticsResponse:
    async with database.session() as session:
        snapshot = await DiagnosticsRepository.snapshot(
            session,
            project_id=principal.project_id,
            worker_offline_timeout_seconds=settings.worker_offline_timeout,
            stuck_after_seconds=max(60.0, settings.task_lease_seconds * 2),
            limit=limit,
        )
    return _response(
        snapshot,
        scheduler=_scheduler_diagnostic(request, settings=settings, snapshot=snapshot),
    )


@router.post("/diagnostics/repair", response_model=AdminRepairResponse)
async def repair_diagnostics(
    database: Annotated[Database, Depends(get_database)],
    principal: Annotated[Principal, Depends(require_api_permission(Permission.AUDIT_READ))],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AdminRepairResponse:
    async with database.session() as session, session.begin():
        result = await DiagnosticsRepository.repair_conservative(
            session,
            project_id=principal.project_id,
            limit=limit,
        )
    message = (
        "Conservative repairs completed."
        if result.repaired_total
        else "No safe repair candidates were changed."
    )
    return AdminRepairResponse(
        project_id=result.project_id,
        observed_at=result.observed_at,
        candidates_total=result.candidates_total,
        repaired_total=result.repaired_total,
        skipped_total=result.skipped_total,
        actions=list(result.actions),
        message=message,
    )


@router.post(
    "/workers/{worker_id}/drain",
    response_model=WorkerResponse,
    summary="Stop assigning new work while existing executions finish",
)
async def drain_worker(
    worker_id: str,
    payload: WorkerDrainRequest,
    database: Annotated[Database, Depends(get_database)],
    _principal: Annotated[
        Principal,
        Depends(require_api_permission(Permission.WORKER_MANAGE)),
    ],
) -> WorkerResponse:
    async with database.session() as session, session.begin():
        worker = await WorkerRepository.drain(session, worker_id, reason=payload.reason)
        if worker is None:
            raise NotFoundError("WORKER_NOT_FOUND", "Worker not found")
    return WorkerResponse.model_validate(worker)


def _response(
    snapshot: DiagnosticSnapshot,
    *,
    scheduler: SchedulerDiagnosticResponse,
) -> AdminDiagnosticsResponse:
    return AdminDiagnosticsResponse(
        project_id=snapshot.project_id,
        observed_at=snapshot.observed_at,
        scheduler=scheduler,
        outbox=OutboxLagResponse.model_validate(snapshot.outbox),
        offline_workers_total=snapshot.offline_workers_total,
        offline_workers=[
            OfflineWorkerResponse.model_validate(worker) for worker in snapshot.offline_workers
        ],
        stuck_tasks_total=snapshot.stuck_tasks_total,
        stuck_tasks=[StuckTaskResponse.model_validate(task) for task in snapshot.stuck_tasks],
        active_reservations_total=snapshot.active_reservations_total,
        active_reservations=[
            ActiveReservationResponse.model_validate(reservation)
            for reservation in snapshot.active_reservations
        ],
        consistency=ConsistencyResponse.model_validate(snapshot.consistency),
        repair=RepairCapabilityResponse(
            reason=(
                "Repair is limited to releasing terminal-task reservations and clearing "
                "terminal-task leases in one database transaction; runtimes are never contacted."
            ),
        ),
    )


def _scheduler_diagnostic(
    request: Request,
    *,
    settings: Settings,
    snapshot: DiagnosticSnapshot,
) -> SchedulerDiagnosticResponse:
    if not settings.control_plane_enabled:
        return _scheduler_response(
            settings,
            snapshot,
            status=SchedulerDiagnosticStatus.DISABLED,
            source="configuration",
        )
    if settings.scheduler_mode == "pull":
        return _scheduler_response(
            settings,
            snapshot,
            status=SchedulerDiagnosticStatus.NOT_OBSERVABLE,
            source="worker_pull_mode_without_persisted_scheduler_heartbeat",
        )

    state = _controller_state(request, "scheduler")
    if state is None:
        return _scheduler_response(
            settings,
            snapshot,
            status=SchedulerDiagnosticStatus.STARTING,
            source="controller_snapshot",
        )
    runs = _nonnegative_int(state.get("runs"))
    failures = _nonnegative_int(state.get("failures"))
    last_started_at = _datetime(state.get("last_started_at"))
    last_succeeded_at = _datetime(state.get("last_succeeded_at"))
    last_error_present = bool(state.get("last_error"))
    stale_after = timedelta(
        seconds=max(
            settings.scheduler_poll_interval * 3,
            settings.control_operation_timeout + settings.scheduler_poll_interval,
        )
    )
    stale = last_succeeded_at is not None and snapshot.observed_at - last_succeeded_at > stale_after
    if last_error_present or stale:
        status = SchedulerDiagnosticStatus.DEGRADED
    elif last_succeeded_at is not None:
        status = SchedulerDiagnosticStatus.HEALTHY
    else:
        status = SchedulerDiagnosticStatus.STARTING
    return _scheduler_response(
        settings,
        snapshot,
        status=status,
        source="in_process_controller_snapshot",
        runs=runs,
        failures=failures,
        last_started_at=last_started_at,
        last_succeeded_at=last_succeeded_at,
        last_error_present=last_error_present,
    )


def _scheduler_response(
    settings: Settings,
    snapshot: DiagnosticSnapshot,
    *,
    status: SchedulerDiagnosticStatus,
    source: str,
    runs: int | None = None,
    failures: int | None = None,
    last_started_at: datetime | None = None,
    last_succeeded_at: datetime | None = None,
    last_error_present: bool = False,
) -> SchedulerDiagnosticResponse:
    return SchedulerDiagnosticResponse(
        mode=settings.scheduler_mode,
        status=status,
        source=source,
        control_plane_enabled=settings.control_plane_enabled,
        heartbeat_observable=settings.scheduler_mode == "global",
        queued_tasks=snapshot.queued_tasks,
        online_workers=snapshot.online_workers,
        runs=runs,
        failures=failures,
        last_started_at=last_started_at,
        last_succeeded_at=last_succeeded_at,
        last_error_present=last_error_present,
    )


def _controller_state(request: Request, name: str) -> Mapping[str, object] | None:
    control_plane = getattr(request.app.state, "control_plane", None)
    snapshot_method = getattr(control_plane, "snapshot", None)
    if not callable(snapshot_method):
        return None
    raw = snapshot_method()
    if not isinstance(raw, Mapping):
        return None
    state = raw.get(name)
    if not isinstance(state, Mapping):
        return None
    return state


def _nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return max(0, value)


def _datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
