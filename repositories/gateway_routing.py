from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import case, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from core.enums import GatewayRoutingPolicy, ModelAvailabilityStatus
from models.model_variant import LogicalModel, ModelVariant
from models.routing import VendorCircuitState
from models.service import (
    ModelService,
    ReplicaHealth,
    ReplicaStatus,
    ServiceReplica,
)
from models.usage import AuditEvent
from repositories.clock import database_utcnow
from repositories.gateway_model_names import GatewayModelNameConflictError
from repositories.services import EndpointSelection, ServiceRepository

_MAX_ROUTING_CURSOR = 2**63 - 1
_SQLITE_ROUTING_TRANSACTION = "gateway_routing_sqlite_transaction"


@dataclass(frozen=True, slots=True)
class GatewayPreflightSkip:
    service_id: uuid.UUID
    model_variant_id: uuid.UUID | None
    selected_vendor: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class GatewayModelCatalogEntry:
    model_id: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class _CandidatePolicy:
    vendor_order: tuple[str, ...]
    allowed_vendors: frozenset[str]
    balanced: bool = False

    @property
    def vendor_rank(self) -> dict[str, int]:
        return {vendor: rank for rank, vendor in enumerate(self.vendor_order)}


@dataclass(frozen=True, slots=True)
class GatewayRoute:
    service_id: uuid.UUID
    logical_model_id: uuid.UUID | None
    model_variant_id: uuid.UUID | None
    selected_vendor: str | None
    upstream_model: str
    gpu_count: int
    selection: EndpointSelection
    preflight_skips: tuple[GatewayPreflightSkip, ...] = ()


class GatewayRoutingRepository:
    @staticmethod
    async def list_available_models(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
    ) -> list[GatewayModelCatalogEntry]:
        services = list(
            await session.scalars(
                select(ModelService)
                .where(ModelService.project_id == project_id)
                .order_by(ModelService.created_at.desc(), ModelService.id.desc())
            )
        )
        routable_service_ids = set(
            await session.scalars(
                select(ServiceReplica.service_id)
                .join(ModelService, ModelService.id == ServiceReplica.service_id)
                .where(
                    ModelService.project_id == project_id,
                    ServiceReplica.generation == ModelService.generation,
                    ServiceReplica.status == ReplicaStatus.RUNNING,
                    ServiceReplica.health == ReplicaHealth.HEALTHY,
                    ServiceReplica.endpoint_url.is_not(None),
                    ServiceReplica.endpoint_url != "",
                    ServiceReplica.execution_id.is_not(None),
                )
                .distinct()
            )
        )
        logical_models = list(
            await session.scalars(select(LogicalModel).where(LogicalModel.project_id == project_id))
        )
        logical_by_id = {model.id: model for model in logical_models}
        logical_ids = set(logical_by_id)
        variants = (
            list(
                await session.scalars(
                    select(ModelVariant).where(ModelVariant.logical_model_id.in_(logical_ids))
                )
            )
            if logical_ids
            else []
        )
        variants_by_id = {variant.id: variant for variant in variants}
        circuits = (
            list(
                await session.scalars(
                    select(VendorCircuitState).where(
                        VendorCircuitState.project_id == project_id,
                        VendorCircuitState.logical_model_id.in_(logical_ids),
                    )
                )
            )
            if logical_ids
            else []
        )
        circuits_by_model_vendor = {
            (circuit.logical_model_id, circuit.vendor): circuit for circuit in circuits
        }
        service_by_name = {service.name: service for service in services}
        ambiguous_names = {
            model.public_name
            for model in logical_models
            if (owner := service_by_name.get(model.public_name)) is not None
            and owner.logical_model_id != model.id
        }

        entries = [
            GatewayModelCatalogEntry(
                model_id=service.name,
                created_at=service.created_at,
            )
            for service in services
            if service.logical_model_id is None
            and service.name not in ambiguous_names
            and service.desired_replicas > 0
            and service.id in routable_service_ids
        ]
        now = await database_utcnow(session)
        available_logical_ids: set[uuid.UUID] = set()
        for service in services:
            logical_model = (
                logical_by_id.get(service.logical_model_id)
                if service.logical_model_id is not None
                else None
            )
            if (
                logical_model is None
                or logical_model.id in available_logical_ids
                or logical_model.public_name in ambiguous_names
                or logical_model.status != ModelAvailabilityStatus.READY
                or service.desired_replicas <= 0
                or service.id not in routable_service_ids
            ):
                continue
            vendor = service.selected_vendor
            policy = _candidate_policy(logical_model.routing_policy)
            if vendor not in policy.allowed_vendors:
                continue
            variant = (
                variants_by_id.get(service.model_variant_id)
                if service.model_variant_id is not None
                else None
            )
            if (
                variant is None
                or variant.logical_model_id != logical_model.id
                or variant.status != ModelAvailabilityStatus.READY
                or vendor != variant.vendor.value
                or service.selected_kind != variant.kind.value
            ):
                continue
            circuit = circuits_by_model_vendor.get((logical_model.id, vendor))
            if (
                circuit is not None
                and circuit.state == "open"
                and circuit.opened_until is not None
                and _as_utc(circuit.opened_until) > now
            ):
                continue
            available_logical_ids.add(logical_model.id)

        entries.extend(
            GatewayModelCatalogEntry(
                model_id=model.public_name,
                created_at=model.created_at,
            )
            for model in logical_models
            if model.id in available_logical_ids
        )
        return sorted(
            entries,
            key=lambda entry: (_as_utc(entry.created_at), entry.model_id),
            reverse=True,
        )

    @staticmethod
    async def choose_route(
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        public_model: str,
        excluded_vendors: frozenset[str] = frozenset(),
    ) -> tuple[ModelService | None, GatewayRoute | None]:
        await _acquire_sqlite_routing_write_lock(session)
        anchor = await ServiceRepository.get_by_name(
            session,
            project_id=project_id,
            name=public_model,
            for_update=False,
        )
        public_logical_model = await session.scalar(
            select(LogicalModel)
            .where(
                LogicalModel.project_id == project_id,
                LogicalModel.public_name == public_model,
            )
            .order_by(LogicalModel.created_at, LogicalModel.id)
            .limit(1)
            .with_for_update()
        )
        if (
            anchor is not None
            and public_logical_model is not None
            and anchor.logical_model_id != public_logical_model.id
        ):
            raise GatewayModelNameConflictError(public_model)

        logical_model: LogicalModel | None = public_logical_model
        if logical_model is not None:
            if logical_model.status != ModelAvailabilityStatus.READY:
                return anchor, None
            if anchor is None:
                anchor = await session.scalar(
                    select(ModelService)
                    .where(
                        ModelService.project_id == project_id,
                        ModelService.logical_model_id == logical_model.id,
                    )
                    .order_by(
                        case((ModelService.name == logical_model.name, 0), else_=1),
                        ModelService.created_at,
                        ModelService.id,
                    )
                    .limit(1)
                )
                if anchor is None:
                    return None, None
        elif anchor is None:
            return None, None
        elif anchor.logical_model_id is None:
            anchor = await ServiceRepository.get_by_name(
                session,
                project_id=project_id,
                name=public_model,
                for_update=True,
            )
            if anchor is None:
                return None, None
        else:
            logical_model = await session.scalar(
                select(LogicalModel)
                .where(
                    LogicalModel.id == anchor.logical_model_id,
                    LogicalModel.project_id == project_id,
                )
                .with_for_update()
            )
            if logical_model is None or logical_model.status != ModelAvailabilityStatus.READY:
                return anchor, None

        candidates = [anchor]
        variants_by_id: dict[uuid.UUID, ModelVariant] = {}
        if anchor.logical_model_id is not None:
            if logical_model is None or logical_model.id != anchor.logical_model_id:
                return anchor, None
            candidate_policy = _candidate_policy(logical_model.routing_policy)
            variants_by_id = {
                variant.id: variant
                for variant in await session.scalars(
                    select(ModelVariant).where(
                        ModelVariant.logical_model_id == anchor.logical_model_id
                    )
                )
            }
            candidates = list(
                await session.scalars(
                    select(ModelService)
                    .where(
                        ModelService.project_id == project_id,
                        ModelService.logical_model_id == anchor.logical_model_id,
                        ModelService.desired_replicas > 0,
                    )
                    .order_by(
                        case(
                            candidate_policy.vendor_rank,
                            value=ModelService.selected_vendor,
                            else_=len(candidate_policy.vendor_order),
                        ),
                        ModelService.created_at,
                        ModelService.id,
                    )
                    .with_for_update(of=ModelService)
                )
            )
        else:
            candidate_policy = _candidate_policy(None)

        now = await database_utcnow(session)
        preflight_skips: list[GatewayPreflightSkip] = []
        eligible_candidates: list[ModelService] = []
        for candidate in candidates:
            vendor = candidate.selected_vendor
            if (
                candidate.logical_model_id is not None
                and vendor not in candidate_policy.allowed_vendors
            ):
                preflight_skips.append(_preflight_skip(candidate, "policy_excluded"))
                continue
            if candidate.logical_model_id is not None:
                variant = (
                    variants_by_id.get(candidate.model_variant_id)
                    if candidate.model_variant_id is not None
                    else None
                )
                if variant is None:
                    preflight_skips.append(_preflight_skip(candidate, "variant_missing"))
                    continue
                if variant.status != ModelAvailabilityStatus.READY:
                    preflight_skips.append(_preflight_skip(candidate, "variant_not_ready"))
                    continue
                if vendor != variant.vendor.value or candidate.selected_kind != variant.kind.value:
                    preflight_skips.append(_preflight_skip(candidate, "variant_snapshot_mismatch"))
                    continue
            eligible_candidates.append(candidate)

        if candidate_policy.balanced and eligible_candidates:
            assert logical_model is not None
            start = logical_model.routing_cursor % len(eligible_candidates)
            eligible_candidates = eligible_candidates[start:] + eligible_candidates[:start]

        for offset, candidate in enumerate(eligible_candidates):
            vendor = candidate.selected_vendor
            if vendor is not None and vendor in excluded_vendors:
                preflight_skips.append(_preflight_skip(candidate, "vendor_excluded"))
                continue
            if candidate.logical_model_id is not None and vendor is not None:
                circuit = await session.scalar(
                    select(VendorCircuitState)
                    .where(
                        VendorCircuitState.project_id == project_id,
                        VendorCircuitState.logical_model_id == candidate.logical_model_id,
                        VendorCircuitState.vendor == vendor,
                    )
                    .with_for_update()
                )
                if circuit is not None and circuit.state == "open":
                    if circuit.opened_until is not None and _as_utc(circuit.opened_until) > now:
                        preflight_skips.append(_preflight_skip(candidate, "circuit_open"))
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
                if candidate_policy.balanced:
                    assert logical_model is not None
                    await _advance_routing_cursor(
                        session,
                        logical_model=logical_model,
                        steps=offset + 1,
                    )
                return anchor, GatewayRoute(
                    service_id=candidate.id,
                    logical_model_id=candidate.logical_model_id,
                    model_variant_id=candidate.model_variant_id,
                    selected_vendor=vendor,
                    upstream_model=candidate.model,
                    gpu_count=candidate.gpu_count,
                    selection=selection,
                    preflight_skips=tuple(preflight_skips),
                )
            preflight_skips.append(_preflight_skip(candidate, "no_healthy_replica"))
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
        state = await _get_or_create_circuit_state(
            session,
            project_id=project_id,
            logical_model_id=route.logical_model_id,
            vendor=route.selected_vendor,
        )
        now = await database_utcnow(session)
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


def _candidate_policy(policy: GatewayRoutingPolicy | None) -> _CandidatePolicy:
    if policy is None:
        return _CandidatePolicy(
            vendor_order=("nvidia", "huawei-ascend"),
            allowed_vendors=frozenset({"nvidia", "huawei-ascend"}),
        )
    if policy is GatewayRoutingPolicy.PREFER_NVIDIA:
        return _CandidatePolicy(
            vendor_order=("nvidia", "huawei-ascend"),
            allowed_vendors=frozenset({"nvidia", "huawei-ascend"}),
        )
    if policy is GatewayRoutingPolicy.PREFER_ASCEND:
        return _CandidatePolicy(
            vendor_order=("huawei-ascend", "nvidia"),
            allowed_vendors=frozenset({"nvidia", "huawei-ascend"}),
        )
    if policy is GatewayRoutingPolicy.STRICT_NVIDIA:
        return _CandidatePolicy(
            vendor_order=("nvidia", "huawei-ascend"),
            allowed_vendors=frozenset({"nvidia"}),
        )
    if policy is GatewayRoutingPolicy.STRICT_ASCEND:
        return _CandidatePolicy(
            vendor_order=("huawei-ascend", "nvidia"),
            allowed_vendors=frozenset({"huawei-ascend"}),
        )
    if policy is GatewayRoutingPolicy.BALANCED:
        return _CandidatePolicy(
            vendor_order=("nvidia", "huawei-ascend"),
            allowed_vendors=frozenset({"nvidia", "huawei-ascend"}),
            balanced=True,
        )
    raise ValueError(f"unsupported gateway routing policy: {policy}")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _preflight_skip(candidate: ModelService, reason: str) -> GatewayPreflightSkip:
    return GatewayPreflightSkip(
        service_id=candidate.id,
        model_variant_id=candidate.model_variant_id,
        selected_vendor=candidate.selected_vendor,
        reason=reason,
    )


async def _advance_routing_cursor(
    session: AsyncSession,
    *,
    logical_model: LogicalModel,
    steps: int,
) -> None:
    if not 1 <= steps <= _MAX_ROUTING_CURSOR:
        raise ValueError("routing cursor steps must be positive and fit in BigInteger")
    wrap_threshold = _MAX_ROUTING_CURSOR - steps
    next_cursor = case(
        (
            LogicalModel.routing_cursor > wrap_threshold,
            LogicalModel.routing_cursor - wrap_threshold - 1,
        ),
        else_=LogicalModel.routing_cursor + steps,
    )
    persisted_cursor = await session.scalar(
        update(LogicalModel)
        .where(LogicalModel.id == logical_model.id)
        .values(
            routing_cursor=next_cursor,
            updated_at=LogicalModel.updated_at,
        )
        .returning(LogicalModel.routing_cursor)
    )
    if persisted_cursor is None:
        raise RuntimeError("locked logical model disappeared while advancing routing cursor")
    set_committed_value(logical_model, "routing_cursor", persisted_cursor)


async def _acquire_sqlite_routing_write_lock(session: AsyncSession) -> None:
    if session.get_bind().dialect.name != "sqlite":
        return
    transaction = session.sync_session.get_transaction()
    if session.info.get(_SQLITE_ROUTING_TRANSACTION) is transaction and transaction is not None:
        return
    try:
        await session.execute(text("BEGIN IMMEDIATE"))
    except OperationalError as error:
        if "cannot start a transaction within a transaction" not in str(error.orig).casefold():
            raise
    transaction = session.sync_session.get_transaction()
    if transaction is None:
        raise RuntimeError("SQLite did not start a routing write transaction")
    session.info[_SQLITE_ROUTING_TRANSACTION] = transaction


async def _get_or_create_circuit_state(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    logical_model_id: uuid.UUID,
    vendor: str,
) -> VendorCircuitState:
    values = {
        "id": uuid.uuid4(),
        "project_id": project_id,
        "logical_model_id": logical_model_id,
        "vendor": vendor,
        "state": "closed",
        "failure_count": 0,
        "opened_until": None,
        "last_error_code": None,
        "version": 0,
        "updated_at": func.current_timestamp(),
    }
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        await session.execute(
            postgresql_insert(VendorCircuitState)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[
                    VendorCircuitState.project_id,
                    VendorCircuitState.logical_model_id,
                    VendorCircuitState.vendor,
                ]
            )
        )
    elif dialect_name == "sqlite":
        await session.execute(
            sqlite_insert(VendorCircuitState)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[
                    VendorCircuitState.project_id,
                    VendorCircuitState.logical_model_id,
                    VendorCircuitState.vendor,
                ]
            )
        )
    state = await session.scalar(
        select(VendorCircuitState)
        .where(
            VendorCircuitState.project_id == project_id,
            VendorCircuitState.logical_model_id == logical_model_id,
            VendorCircuitState.vendor == vendor,
        )
        .with_for_update()
    )
    if state is None:
        now = await database_utcnow(session)
        state = VendorCircuitState(
            project_id=project_id,
            logical_model_id=logical_model_id,
            vendor=vendor,
            state="closed",
            failure_count=0,
            opened_until=None,
            last_error_code=None,
            version=0,
            updated_at=now,
        )
        session.add(state)
        await session.flush()
    return state
