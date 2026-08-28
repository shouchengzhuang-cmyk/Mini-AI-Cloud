import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace

from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import (
    AcceleratorKind,
    AcceleratorSelectionPolicy,
    AcceleratorVendor,
    RuntimeType,
)
from core.logging import get_logger
from core.metrics import SCHEDULER_ATTEMPTS, SCHEDULING_ATTEMPTS
from core.runtime_profiles import RuntimeProfileCatalog
from repositories.admission import AdmissionRepository, BatchAdmissionSnapshot
from repositories.quotas import QuotaExceededError
from repositories.scheduling import PlacementConflict, SchedulingRepository
from scheduler.admission import AdmissionRequest
from scheduler.policies import Placement, choose_placement, evaluate_snapshot

SessionFactory = Callable[[], AsyncSession]


@dataclass(frozen=True, slots=True)
class GlobalSchedulerResult:
    task_id: uuid.UUID | None
    worker_id: str | None
    placed: bool
    reason: str | None = None
    attempted_count: int = 0
    placed_count: int = 0


class GlobalScheduler:
    """PostgreSQL-fenced placement authority for all Workers."""

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        scheduler_id: str,
        lease_seconds: float,
        policy: str,
        aging_interval_seconds: int,
        cpu_price_per_hour: float,
        memory_price_per_gb_hour: float,
        gpu_price_per_hour: float,
        preemption_enabled: bool = False,
        preemption_min_delta: int = 10,
        batch_size: int = 16,
        candidate_scan_limit: int = 128,
        runtime_profile_catalog: RuntimeProfileCatalog | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least one")
        if candidate_scan_limit < 1:
            raise ValueError("candidate_scan_limit must be at least one")
        self._session_factory = session_factory
        self._scheduler_id = scheduler_id
        self._lease_seconds = lease_seconds
        self._policy = policy
        self._aging_interval_seconds = aging_interval_seconds
        self._cpu_price_per_hour = cpu_price_per_hour
        self._memory_price_per_gb_hour = memory_price_per_gb_hour
        self._gpu_price_per_hour = gpu_price_per_hour
        self._preemption_enabled = preemption_enabled
        self._preemption_min_delta = preemption_min_delta
        self._batch_size = batch_size
        self._candidate_scan_limit = candidate_scan_limit
        self._runtime_profile_catalog = runtime_profile_catalog
        self._logger = get_logger("global_scheduler")

    async def run_once(self) -> GlobalSchedulerResult:
        async with self._session_factory() as session, session.begin():
            candidates = await SchedulingRepository.choose_candidates(
                session,
                aging_interval_seconds=self._aging_interval_seconds,
                scan_limit=self._candidate_scan_limit,
            )
            if not candidates:
                return GlobalSchedulerResult(None, None, False, "queue_empty")

            workers = await SchedulingRepository.worker_snapshots(session)
            first_result: GlobalSchedulerResult | None = None
            attempted_count = 0
            placed_count = 0
            excluded_task_ids: set[uuid.UUID] = set()
            pending_candidates = candidates
            while pending_candidates and attempted_count < self._batch_size:
                candidate = pending_candidates.pop(0)
                attempted_count += 1
                task_id = candidate.task.id
                excluded_task_ids.add(task_id)
                effective_priority_value = candidate.effective_priority
                admission: BatchAdmissionSnapshot | None = None
                rejected: Mapping[str, object]
                if (
                    candidate.task.runtime_type == RuntimeType.KUBERNETES
                    and candidate.task.gpu_count > 0
                ):
                    if self._runtime_profile_catalog is None:
                        placement = None
                        rejected = {"runtime-profile-catalog": "runtime_profile_unavailable"}
                    else:
                        cpu_only = dataclass_replace(
                            candidate.snapshot,
                            gpu_count=0,
                            gpu_memory_mb=0,
                            gpu_model=None,
                        )
                        allowed_worker_ids = frozenset(
                            worker.id
                            for worker in workers
                            if evaluate_snapshot(worker, cpu_only)[0] is None
                        )
                        result = await AdmissionRepository.admit_batch_task(
                            session,
                            catalog=self._runtime_profile_catalog,
                            task=candidate.task,
                            request=_task_admission_request(candidate.task),
                            allowed_worker_ids=allowed_worker_ids,
                        )
                        admission = result.snapshot
                        placement = (
                            Placement(
                                worker_id=admission.worker_id,
                                gpu_device_ids=(),
                                score=(),
                            )
                            if admission is not None
                            else None
                        )
                        rejected = {
                            str(item.get("candidate_id", "vendor-admission")): item.get(
                                "reason", result.reason.value if result.reason else "rejected"
                            )
                            for item in result.summary
                        }
                else:
                    placement, rejected = choose_placement(
                        candidate.snapshot, workers, policy=self._policy
                    )
                if placement is None:
                    reason = _admission_reason(rejected)
                    worker_id: str | None = None
                    outcome = "rejected"
                    detail: str | None = None
                    if self._preemption_enabled:
                        decision = await SchedulingRepository.request_preemption(
                            session,
                            candidate=candidate,
                            workers=workers,
                            min_priority_delta=self._preemption_min_delta,
                        )
                        if decision is not None:
                            reason = "preemption_in_progress"
                            worker_id = decision.worker_id
                            outcome = "preemption_requested"
                            detail = ",".join(str(item) for item in decision.victim_task_ids)
                    if outcome == "rejected":
                        await SchedulingRepository.mark_unschedulable(
                            session, task_id=task_id, reason=reason
                        )
                    SchedulingRepository.record_attempt(
                        session,
                        task_id=task_id,
                        scheduler_id=self._scheduler_id,
                        worker_id=worker_id,
                        policy=self._policy,
                        outcome=outcome,
                        reason=reason,
                        effective_priority_value=effective_priority_value,
                        detail=detail,
                    )
                    _record_scheduler_metric(outcome, reason)
                    if first_result is None:
                        first_result = GlobalSchedulerResult(task_id, worker_id, False, reason)
                    continue

                try:
                    # Quota validation happens late in ``place`` after several
                    # fenced writes.  A savepoint makes a quota race a local
                    # admission miss instead of poisoning the whole batch.
                    async with session.begin_nested():
                        task, _execution_id = await SchedulingRepository.place(
                            session,
                            task_id=task_id,
                            worker_id=placement.worker_id,
                            gpu_device_ids=placement.gpu_device_ids,
                            lease_seconds=self._lease_seconds,
                            cpu_price_per_hour=self._cpu_price_per_hour,
                            memory_price_per_gb_hour=self._memory_price_per_gb_hour,
                            gpu_price_per_hour=self._gpu_price_per_hour,
                            admission=admission,
                        )
                except QuotaExceededError as exc:
                    reason = "project_quota_exceeded"
                    await SchedulingRepository.mark_unschedulable(
                        session, task_id=task_id, reason=reason
                    )
                    SchedulingRepository.record_attempt(
                        session,
                        task_id=task_id,
                        scheduler_id=self._scheduler_id,
                        worker_id=placement.worker_id,
                        policy=self._policy,
                        outcome="rejected",
                        reason=reason,
                        effective_priority_value=effective_priority_value,
                        detail=str(exc),
                    )
                    _record_scheduler_metric("rejected", reason)
                    if first_result is None:
                        first_result = GlobalSchedulerResult(task_id, None, False, reason)
                    continue
                except PlacementConflict as exc:
                    reason = "placement_conflict"
                    self._logger.info(
                        "placement lost a concurrent race",
                        task_id=str(task_id),
                        reason=str(exc),
                    )
                    SchedulingRepository.record_attempt(
                        session,
                        task_id=task_id,
                        scheduler_id=self._scheduler_id,
                        worker_id=placement.worker_id,
                        policy=self._policy,
                        outcome="conflict",
                        reason=reason,
                        effective_priority_value=effective_priority_value,
                        detail=str(exc),
                    )
                    _record_scheduler_metric("conflict", reason)
                    if first_result is None:
                        first_result = GlobalSchedulerResult(task_id, None, False, reason)
                    continue

                placed_count += 1
                SchedulingRepository.record_attempt(
                    session,
                    task_id=task.id,
                    scheduler_id=self._scheduler_id,
                    worker_id=placement.worker_id,
                    policy=self._policy,
                    outcome="placed",
                    reason=None,
                    effective_priority_value=effective_priority_value,
                )
                _record_scheduler_metric("placed", None)
                if first_result is None:
                    first_result = GlobalSchedulerResult(task.id, placement.worker_id, True)
                # Subsequent candidates must see capacity and concrete GPU
                # reservations consumed earlier in this same tick. Rebuilding
                # the bounded pool also refreshes project dominant shares, so a
                # batch cannot consume all slots from one project using a stale
                # pre-placement DRF snapshot.
                workers = await SchedulingRepository.worker_snapshots(session)
                pending_candidates = await SchedulingRepository.choose_candidates(
                    session,
                    aging_interval_seconds=self._aging_interval_seconds,
                    scan_limit=self._candidate_scan_limit,
                    excluded_task_ids=frozenset(excluded_task_ids),
                )

            assert first_result is not None
            return GlobalSchedulerResult(
                task_id=first_result.task_id,
                worker_id=first_result.worker_id,
                placed=first_result.placed,
                reason=first_result.reason,
                attempted_count=attempted_count,
                placed_count=placed_count,
            )


def _admission_reason(rejected: Mapping[str, object]) -> str:
    if not rejected:
        return "no_online_workers"
    counts: dict[str, int] = {}
    for value in rejected.values():
        reason = str(value)
        counts[reason] = counts.get(reason, 0) + 1
    return max(counts, key=lambda item: (counts[item], item))


def _task_admission_request(task: object) -> AdmissionRequest:
    raw = getattr(task, "accelerator_request_json", None)
    if not isinstance(raw, dict):
        raise ValueError("Kubernetes accelerator task has no normalized request")
    return AdmissionRequest(
        count=int(raw["count"]),
        allowed_vendors=frozenset(
            AcceleratorVendor(value) for value in raw.get("allowed_vendors", [])
        ),
        allowed_kinds=frozenset(AcceleratorKind(value) for value in raw.get("allowed_kinds", [])),
        allowed_models=frozenset(str(value) for value in raw.get("allowed_models", [])),
        required_capabilities=frozenset(
            str(value) for value in raw.get("required_capabilities", [])
        ),
        runtime_profile_id=raw.get("runtime_profile"),
        selection_policy=AcceleratorSelectionPolicy(raw.get("selection_policy", "any")),
    )


def _record_scheduler_metric(outcome: str, reason: str | None) -> None:
    bounded_reason = reason or "none"
    SCHEDULER_ATTEMPTS.labels(outcome, bounded_reason).inc()
    # Keep the original Phase I metric during the Phase II name transition.
    SCHEDULING_ATTEMPTS.labels(outcome, bounded_reason).inc()
