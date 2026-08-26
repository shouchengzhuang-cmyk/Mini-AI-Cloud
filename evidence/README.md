# Evidence contract

This directory defines claims, invariants, environments, and evidence commands as a reviewable contract. It is not a runtime source of truth and does not promote unexecuted commands to `PASS`.

- `claims.yaml`: capability claims, failure models, required environments, evidence records, and limitations.
- `invariants.yaml`: system properties a claim depends on.
- `environments.yaml`: execution levels and their boundaries.
- `schema.json`: generated JSON Schema for the aggregate contract.
- `matrix.md`: generated human-readable projection.

Validate committed inputs and generated files:

```bash
uv run python scripts/validate_evidence.py
```

After an intentional contract change, regenerate and review the schema and matrix:

```bash
uv run python scripts/validate_evidence.py --write-generated
git diff -- evidence/schema.json evidence/matrix.md
```

`PASS` requires an exact `verified_commit`. `PENDING` means a command is registered but has not been run for the contract commit. `NOT_RUN` records an unavailable environment explicitly. Evidence bundles produced by later release tooling may update execution status; this YAML never infers it from file presence.

Generate a commit-bound projection with `make evidence`; bundle fields and security boundaries are
documented in [`docs/evidence-bundles.md`](../docs/evidence-bundles.md).
