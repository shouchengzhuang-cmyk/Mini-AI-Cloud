import uuid

from core.database import Database
from core.logging import get_logger
from repositories.services import EndpointSelection, ReconcileResult, ServiceRepository


class ServiceReconciler:
    """Converge persisted model-service intent into fenced replica records.

    The PostgreSQL service row is the serialization point. Multiple controller
    processes may call ``run_once`` concurrently because candidate services are
    claimed with ``FOR UPDATE SKIP LOCKED`` and replica ordinals also have a
    database uniqueness constraint. This controller deliberately does not start
    runtimes; Workers will consume the persisted replica intent in a later slice.
    """

    def __init__(
        self,
        database: Database,
        *,
        batch_size: int = 100,
        drain_timeout_seconds: float = 30.0,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if drain_timeout_seconds < 0:
            raise ValueError("drain_timeout_seconds must not be negative")
        self.database = database
        self.batch_size = batch_size
        self.drain_timeout_seconds = drain_timeout_seconds
        self.logger = get_logger("service_reconciler")

    async def run_once(self) -> ReconcileResult:
        async with self.database.session() as session, session.begin():
            recovery = await ServiceRepository.recover_expired_leases(
                session, limit=self.batch_size
            )
            result = await ServiceRepository.reconcile_batch(
                session,
                limit=self.batch_size,
                drain_timeout_seconds=self.drain_timeout_seconds,
            )
        if recovery.replicas_lost or recovery.replicas_stopped:
            self.logger.warning(
                "expired service replica leases recovered",
                services_seen=recovery.services_seen,
                replicas_lost=recovery.replicas_lost,
                replicas_stopped=recovery.replicas_stopped,
            )
        if result.replicas_created or result.replicas_stopping or result.replicas_stopped:
            self.logger.info(
                "model services reconciled",
                services_seen=result.services_seen,
                replicas_created=result.replicas_created,
                replicas_stopping=result.replicas_stopping,
                replicas_stopped=result.replicas_stopped,
                services_updated=result.services_updated,
            )
        return result

    async def reconcile_service(
        self,
        service_id: uuid.UUID,
        *,
        project_id: uuid.UUID | None = None,
    ) -> ReconcileResult | None:
        async with self.database.session() as session, session.begin():
            service = await ServiceRepository.get(
                session,
                service_id,
                project_id=project_id,
                for_update=True,
            )
            if service is None:
                return None
            return await ServiceRepository.reconcile_locked(
                session,
                service,
                drain_timeout_seconds=self.drain_timeout_seconds,
            )

    async def choose_endpoint(
        self,
        service_id: uuid.UUID,
        *,
        project_id: uuid.UUID,
    ) -> EndpointSelection | None:
        async with self.database.session() as session, session.begin():
            return await ServiceRepository.choose_healthy_endpoint(
                session,
                service_id=service_id,
                project_id=project_id,
            )
