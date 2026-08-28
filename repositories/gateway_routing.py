from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import case, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.routing import VendorCircuitState
from models.service import ModelService, ServiceStatus
from models.usage import AuditEvent
from repositories.clock import database_utcnow
from repositories.services import EndpointSelection, ServiceRepository


@dataclass(frozen=True, slots=True)
class GatewayRoute:
    service_id: uuid.UUID
    logical_model_id: uuid.UUID | None
    model_variant_id: uuid.UUID | None
    selected_vendor: str | None
    upstream_model: str
    gpu_count: int
    selection: EndpointSelection


class GatewayRoutingRepository:
    @staticmethod
    async def choose_route(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        public_model: str,
        excluded_vendors: frozenset[str] = frozenset(),
    ) -> tuple[ModelService | None, GatewayRoute | None]:
        anchor = await ServiceRepository.get_by_name(
            session,
            project_id=project_id,
            name=public_model,
            for_update=True,
        )
        if anchor is None:
            return None, None

        candidates = [anchor]
        if anchor.logical_model_id is not None:
            preference = _vendor_preference(anchor.selection_policy)
            candidates = list(
                await session.scalars(
                    select(ModelService)
                    .where(
                        ModelService.project_id == project_id,
                        ModelService.logical_model_id == anchor.logical_model_id,
                        ModelService.desired_replicas > 0,
                        ModelService.status.in_({ServiceStatus.RUNNING, ServiceStatus.DEGRADED}),
                    )
                    .order_by(
                        case(preference, value=ModelService.selected_vendor, else_=len(preference)),
                        ModelService.created_at,
                        ModelService.id,
                    )
                    .with_for_update()
                )
            )

        now = await database_utcnow(session)
        for candidate in candidates:
            vendor = candidate.selected_vendor
            if vendor is not None and vendor in excluded_vendors:
                continue
            if candidate.logical_model_id is not None and vendor is not None:
                circuit = await session.scalar(
                    select(VendorCircuitState).where(
                        VendorCircuitState.project_id == project_id,
                        VendorCircuitState.logical_model_id == candidate.logical_model_id,
                        VendorCircuitState.vendor == vendor,
                    )
                )
                if circuit is not None and circuit.state == "open":
                    if circuit.opened_until is not None and circuit.opened_until > now:
                        continue
                    circuit.state = "closed"
                    circuit.failure_count = 0
                    circuit.opened_until = None
                    circuit.last_error_code = None
                    circuit.version += 1
                    circuit.updated_at = now
            selection = await ServiceRepository.choose_healthy_endpoint(
                session,
                service_id=candidate.id,
                project_id=project_id,
            )
            if selection is not None:
                return anchor, GatewayRoute(
                    service_id=candidate.id,
                    logical_model_id=candidate.logical_model_id,
                    model_variant_id=candidate.model_variant_id,
                    selected_vendor=vendor,
                    upstream_model=candidate.model,
                    gpu_count=candidate.gpu_count,
                    selection=selection,
                )
        return anchor, None

    @staticmethod
    async def record_outcome(
        session: AsyncSession,
        *,
        route: GatewayRoute,
        project_id: uuid.UUID,
        success: bool,
        error_code: str | None,
        failure_threshold: int,
        cooldown_seconds: int,
    ) -> None:
        if route.logical_model_id is None or route.selected_vendor is None:
            return
        state = await session.scalar(
            select(VendorCircuitState)
            .where(
                VendorCircuitState.project_id == project_id,
                VendorCircuitState.logical_model_id == route.logical_model_id,
                VendorCircuitState.vendor == route.selected_vendor,
            )
            .with_for_update()
        )
        now = await database_utcnow(session)
        if state is None:
            state = VendorCircuitState(
                project_id=project_id,
                logical_model_id=route.logical_model_id,
                vendor=route.selected_vendor,
                state="closed",
                failure_count=0,
                version=0,
                updated_at=now,
            )
            session.add(state)
        if success:
            state.state = "closed"
            state.failure_count = 0
            state.opened_until = None
            state.last_error_code = None
        else:
            state.failure_count += 1
            state.last_error_code = error_code
            if state.failure_count >= failure_threshold:
                state.state = "open"
                state.opened_until = now + timedelta(seconds=cooldown_seconds)
        state.version += 1
        state.updated_at = now

    @staticmethod
    async def record_fallback(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        request_id: uuid.UUID,
        from_route: GatewayRoute,
        to_route: GatewayRoute,
        reason: str,
    ) -> None:
        session.add(
            AuditEvent(
                project_id=project_id,
                actor_type="gateway",
                action="gateway.vendor_fallback",
                resource_type="logical_model",
                resource_id=(
                    str(from_route.logical_model_id)
                    if from_route.logical_model_id is not None
                    else None
                ),
                outcome="success",
                request_id=str(request_id),
                details={
                    "from_service_id": str(from_route.service_id),
                    "from_variant_id": str(from_route.model_variant_id),
                    "from_vendor": from_route.selected_vendor,
                    "to_service_id": str(to_route.service_id),
                    "to_variant_id": str(to_route.model_variant_id),
                    "to_vendor": to_route.selected_vendor,
                    "reason": reason,
                },
                occurred_at=await database_utcnow(session),
            )
        )


def _vendor_preference(policy: str | None) -> dict[str, int]:
    if policy == "prefer-ascend":
        return {"huawei-ascend": 0, "nvidia": 1}
    return {"nvidia": 0, "huawei-ascend": 1}
