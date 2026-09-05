# Code review remediation plan

This document turns the 2026-09-05 code review into a bounded correctness plan. The goal is to close concurrency and resource-accounting gaps without reopening v0.6 feature scope.

## Goal

Keep candidate discovery cheap and non-owning, keep placement as the single PostgreSQL-fenced mutation authority, and make every future accelerator/preemption path respect its declared allocation authority.

## Tasks

### P0 — Remove long-lived candidate row locks

Candidate ranking is a snapshot operation. It must not reserve a large candidate lane for the lifetime of a scheduler batch transaction.

- Remove `FOR UPDATE SKIP LOCKED` from effective-priority, raw-priority, and project-fair candidate queries.
- Preserve authoritative row locking and revalidation in `SchedulingRepository.place`.
- Treat a placement lost to another scheduler as `PlacementConflict`, not as a reason to reserve candidate rows early.
- Add compile-time assertions that candidate queries are non-locking.
- Add a real PostgreSQL regression with two concurrent scheduler transactions proving that both can observe the same highest-ranked snapshot before either performs a placement mutation.

Acceptance criteria:

- Candidate query SQL contains no `FOR UPDATE` clause.
- `place` still locks Task, Worker, and concrete accelerator state before mutation.
- Two concurrent candidate scans return the same top-ranked tasks rather than hiding them behind `SKIP LOCKED`.
- Unit, integration, Ruff, and mypy CI remain green.

### P1 — Align preemption with allocation authority

Preemption currently models released accelerator capacity from exact device IDs. Kubernetes device-plugin admission intentionally does not bind database device UUIDs, so that model must not be reused implicitly for typed device-plugin allocations.

Required follow-up:

- Branch preemption simulation by `AllocationAuthority`.
- Keep exact UUID release simulation for `CONTROL_PLANE_EXACT_DEVICE`.
- Model `KUBERNETES_DEVICE_PLUGIN` release as typed quantity capacity keyed by node/worker, vendor, kind, model, Kubernetes resource name, and runtime-profile binding.
- Re-run admission after simulated victim release before committing a preemption plan.
- Cover NVIDIA GPU and Huawei Ascend NPU paths.

Acceptance criteria:

- A device-plugin victim with empty `gpu_device_ids` can still contribute released typed capacity in simulation.
- A preemption plan is never accepted merely because legacy NVIDIA exact-device snapshots look feasible.
- Exact-device and device-plugin tests prove that allocation authorities cannot be mixed accidentally.

### P2 — Replace per-execution cancellation polling

The worker currently polls cancellation state for each execution. Move the fast path to event delivery and retain database polling only as a slower reconciliation fallback.

Acceptance criteria:

- Cancellation latency remains bounded.
- Database query rate does not scale at roughly four queries per second per running execution.
- Lost event delivery is recovered by periodic reconciliation.

### P2 — Store CPU reservations as integer millicores

Replace floating-point `reserved_cpu` accounting with an integer millicore field through a migration and compatibility transition.

Acceptance criteria:

- Reserve/release cycles are exact in integer arithmetic.
- Capacity checks do not require float multiplication and rounding.
- Migration and rollback behavior are documented and tested.

## Scope boundary

P0 is the only code change in the current scheduler-lock PR. P1 and P2 are deliberately separate follow-ups because they change accelerator/preemption semantics and persistent accounting respectively. Do not combine them into the release-fix PR merely to reduce PR count.
