# Generated evidence contract matrix

> Generated from `claims.yaml`, `invariants.yaml`, and `environments.yaml`.
> Run `uv run python scripts/validate_evidence.py --write-generated` to update.

| Claim | Required environments | Current contract status | Invariants |
|---|---|---|---|
| `artifact.hash-size-readiness` | unit-sqlite, docker-compose | unit-sqlite=PENDING, docker-compose=PENDING | artifact.integrity-before-ready, project.no-cross-tenant-disclosure |
| `project.resource-isolation` | unit-sqlite | unit-sqlite=PENDING | project.no-cross-tenant-disclosure |
| `quota.non-negative-reservations` | unit-sqlite | unit-sqlite=PENDING | quota.non-negative |
| `scheduler.gpu-reservation-uniqueness` | unit-sqlite | unit-sqlite=PENDING | scheduler.unique-gpu-reservation |
| `service.active-sse-drain` | kind-serving | kind-serving=PENDING | service.drain-preserves-active-requests |
| `service.controller-restart-adoption` | kind-serving | kind-serving=PENDING | service.adopt-exact-identity |
| `service.desired-actual-convergence` | postgres-integration | postgres-integration=PENDING | service.desired-actual-convergence |
| `serving.fail-closed-positive-capacity` | unit-sqlite | unit-sqlite=PENDING | serving.positive-capacity-fails-closed |
| `task.atomic-claim` | postgres-integration | postgres-integration=PENDING | task.single-active-execution |
| `task.stale-execution-rejection` | postgres-integration | postgres-integration=PENDING | task.single-active-execution |

`PENDING` means the command is registered but has not been executed for this contract commit.
`NOT_RUN` is an explicit environment boundary, not a failure or a PASS.
