from __future__ import annotations

import argparse
import json
import re
import subprocess
from enum import StrEnum
from pathlib import Path
from typing import Literal

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field

ID_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]+$")
EXTERNAL_LEVELS = {"DOCKER", "KIND", "REAL_GPU"}


class EvidenceStatus(StrEnum):
    PASS = "PASS"
    PENDING = "PENDING"
    NOT_RUN = "NOT_RUN"
    NOT_APPLICABLE = "N/A"


class EvidenceLevel(StrEnum):
    UNIT = "UNIT"
    INTEGRATION = "INTEGRATION"
    DOCKER = "DOCKER"
    KIND = "KIND"
    REAL_GPU = "REAL_GPU"


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["test", "command", "document"]
    path: str = Field(min_length=1)


class Environment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=ID_PATTERN.pattern)
    description: str = Field(min_length=1)
    level: EvidenceLevel
    execution: str = Field(min_length=1)
    limitations: list[str] = Field(min_length=1)


class Invariant(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=ID_PATTERN.pattern)
    description: str = Field(min_length=1)
    scope: str = Field(min_length=1)
    failure_condition: str = Field(min_length=1)
    references: list[Reference] = Field(min_length=1)


class EvidenceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment_id: str = Field(pattern=ID_PATTERN.pattern)
    level: EvidenceLevel
    status: EvidenceStatus
    command: str = Field(min_length=1)
    references: list[Reference] = Field(min_length=1)
    verified_commit: str | None = Field(default=None, pattern=r"^[0-9a-f]{7,40}$")


class Claim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=ID_PATTERN.pattern)
    description: str = Field(min_length=1)
    invariant_ids: list[str] = Field(min_length=1)
    failure_model: str = Field(min_length=1)
    required_environment_ids: list[str] = Field(min_length=1)
    evidence: list[EvidenceRecord] = Field(min_length=1)
    known_limitations: list[str] = Field(min_length=1)


class CoverageProof(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["code", "test", "migration", "profile", "evidence", "document"]
    path: str = Field(min_length=1)
    note: str = Field(min_length=1)


class CoverageArea(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^(?:A(?:[1-9]|1[01])|post-a11-hardening|benchmark-review-fixes)$")
    title: str = Field(min_length=1)
    status: Literal["fully-represented", "partially-represented", "missing"]
    proofs: list[CoverageProof] = Field(min_length=1)
    limitations: list[str] = Field(min_length=1)


class StackedPullRequestCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    number: int = Field(ge=20, le=27)
    area_ids: list[str] = Field(min_length=1)
    source_head: str = Field(pattern=r"^[0-9a-f]{40}$")
    integration: Literal["literal-ancestor", "semantic-integration"]
    classification: Literal["fully-represented", "partially-represented", "missing"]
    rationale: str = Field(min_length=1)


class M6ReleaseCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(pattern=r"^1\.\d+\.\d+$")
    canonical_m6_head: str = Field(pattern=r"^[0-9a-f]{40}$")
    main_baseline: str = Field(pattern=r"^[0-9a-f]{40}$")
    real_hardware_status: Literal["REAL_HW_NOT_RUN"]
    areas: list[CoverageArea] = Field(min_length=1)
    stacked_pull_requests: list[StackedPullRequestCoverage] = Field(min_length=1)


class EvidenceContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(pattern=r"^1\.\d+\.\d+$")
    environments: list[Environment] = Field(min_length=1)
    invariants: list[Invariant] = Field(min_length=1)
    claims: list[Claim] = Field(min_length=1)


class ContractValidationError(ValueError):
    pass


REQUIRED_M6_AREAS = {
    *(f"A{index}" for index in range(1, 12)),
    "post-a11-hardening",
    "benchmark-review-fixes",
}
REQUIRED_MIGRATION_AREAS = {"A2", "A5", "A9", "A10", "post-a11-hardening"}
REQUIRED_PROFILE_AREAS = {"A4", "A7", "A8"}
REQUIRED_EVIDENCE_AREAS = {"A7", "A8", "A11", "benchmark-review-fixes"}
REQUIRED_STACKED_PULL_REQUESTS = set(range(20, 28))


def _load_yaml(path: Path, key: str) -> list[dict[str, object]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get(key), list):
        raise ContractValidationError(f"{path}: expected top-level list '{key}'")
    return payload[key]


def load_contract(repository_root: Path) -> EvidenceContract:
    evidence_root = repository_root / "evidence"
    return EvidenceContract.model_validate(
        {
            "schema_version": "1.0.0",
            "environments": _load_yaml(evidence_root / "environments.yaml", "environments"),
            "invariants": _load_yaml(evidence_root / "invariants.yaml", "invariants"),
            "claims": _load_yaml(evidence_root / "claims.yaml", "claims"),
        }
    )


def load_m6_release_coverage(repository_root: Path) -> M6ReleaseCoverage:
    path = repository_root / "evidence" / "m6-release-coverage.json"
    return M6ReleaseCoverage.model_validate_json(path.read_text(encoding="utf-8"))


def _duplicates(values: list[str]) -> list[str]:
    return sorted({value for value in values if values.count(value) > 1})


def _validate_references(references: list[Reference], repository_root: Path) -> list[str]:
    errors: list[str] = []
    for reference in references:
        path = repository_root / reference.path
        if not path.exists():
            errors.append(f"missing {reference.kind} reference: {reference.path}")
    return errors


def validate_contract(contract: EvidenceContract, repository_root: Path) -> None:
    errors: list[str] = []
    environment_ids = [item.id for item in contract.environments]
    invariant_ids = [item.id for item in contract.invariants]
    claim_ids = [item.id for item in contract.claims]
    for kind, identifiers in (
        ("environment", environment_ids),
        ("invariant", invariant_ids),
        ("claim", claim_ids),
    ):
        if duplicates := _duplicates(identifiers):
            errors.append(f"duplicate {kind} ids: {', '.join(duplicates)}")

    environments = {item.id: item for item in contract.environments}
    known_invariants = set(invariant_ids)
    for invariant in contract.invariants:
        errors.extend(
            f"{invariant.id}: {error}"
            for error in _validate_references(invariant.references, repository_root)
        )

    for claim in contract.claims:
        unknown_invariants = sorted(set(claim.invariant_ids) - known_invariants)
        if unknown_invariants:
            errors.append(f"{claim.id}: unknown invariants: {', '.join(unknown_invariants)}")
        unknown_environments = sorted(set(claim.required_environment_ids) - set(environments))
        if unknown_environments:
            errors.append(f"{claim.id}: unknown environments: {', '.join(unknown_environments)}")
        evidence_environments = [record.environment_id for record in claim.evidence]
        missing_evidence = sorted(set(claim.required_environment_ids) - set(evidence_environments))
        if missing_evidence:
            missing_list = ", ".join(missing_evidence)
            errors.append(
                f"{claim.id}: required environments without evidence records: {missing_list}"
            )
        for record in claim.evidence:
            environment = environments.get(record.environment_id)
            if environment is None:
                errors.append(
                    f"{claim.id}: evidence uses unknown environment: {record.environment_id}"
                )
                continue
            if record.level is not environment.level:
                errors.append(
                    f"{claim.id}: level {record.level} does not match "
                    f"{record.environment_id} level {environment.level}"
                )
            if record.status is EvidenceStatus.PASS and record.verified_commit is None:
                errors.append(f"{claim.id}: PASS evidence requires verified_commit")
            if record.status is not EvidenceStatus.PASS and record.verified_commit is not None:
                errors.append(f"{claim.id}: only PASS evidence may set verified_commit")
            errors.extend(
                f"{claim.id}: {error}"
                for error in _validate_references(record.references, repository_root)
            )
        if (
            any(record.level.value in EXTERNAL_LEVELS for record in claim.evidence)
            and not claim.known_limitations
        ):
            errors.append(f"{claim.id}: external capability claim requires known limitations")

    if errors:
        raise ContractValidationError("\n".join(errors))


def validate_m6_release_coverage(
    coverage: M6ReleaseCoverage,
    repository_root: Path,
) -> None:
    errors: list[str] = []
    resolved_commits: set[str] = set()
    commit_labels = {
        coverage.main_baseline: "main baseline",
        coverage.canonical_m6_head: "canonical M6 head",
        **{
            pull_request.source_head: f"PR #{pull_request.number} source head"
            for pull_request in coverage.stacked_pull_requests
        },
    }
    for commit, label in commit_labels.items():
        if _git_succeeds(repository_root, "cat-file", "-e", f"{commit}^{{commit}}"):
            resolved_commits.add(commit)
        else:
            errors.append(f"{label} cannot be resolved as a commit: {commit}")

    if (
        coverage.main_baseline in resolved_commits
        and coverage.canonical_m6_head in resolved_commits
        and not _git_succeeds(
            repository_root,
            "merge-base",
            "--is-ancestor",
            coverage.main_baseline,
            coverage.canonical_m6_head,
        )
    ):
        errors.append("main baseline is not an ancestor of the canonical M6 head")
    if coverage.canonical_m6_head in resolved_commits and not _git_succeeds(
        repository_root,
        "merge-base",
        "--is-ancestor",
        coverage.canonical_m6_head,
        "HEAD",
    ):
        errors.append("canonical M6 head is not an ancestor of release HEAD")

    area_ids = [area.id for area in coverage.areas]
    if duplicates := _duplicates(area_ids):
        errors.append(f"duplicate M6 coverage area ids: {', '.join(duplicates)}")
    missing_areas = sorted(REQUIRED_M6_AREAS - set(area_ids))
    unexpected_areas = sorted(set(area_ids) - REQUIRED_M6_AREAS)
    if missing_areas:
        errors.append(f"missing M6 coverage areas: {', '.join(missing_areas)}")
    if unexpected_areas:
        errors.append(f"unexpected M6 coverage areas: {', '.join(unexpected_areas)}")

    for area in coverage.areas:
        if area.status != "fully-represented":
            errors.append(f"{area.id}: release candidate coverage is {area.status}")
        proof_kinds = {proof.kind for proof in area.proofs}
        for required_kind in ("code", "test"):
            if required_kind not in proof_kinds:
                errors.append(f"{area.id}: missing {required_kind} proof")
        if area.id in REQUIRED_MIGRATION_AREAS and "migration" not in proof_kinds:
            errors.append(f"{area.id}: missing migration proof")
        if area.id in REQUIRED_PROFILE_AREAS and "profile" not in proof_kinds:
            errors.append(f"{area.id}: missing profile proof")
        if area.id in REQUIRED_EVIDENCE_AREAS and "evidence" not in proof_kinds:
            errors.append(f"{area.id}: missing evidence-boundary proof")
        for proof in area.proofs:
            if not (repository_root / proof.path).exists():
                errors.append(f"{area.id}: missing {proof.kind} proof path: {proof.path}")

    pull_request_numbers = [item.number for item in coverage.stacked_pull_requests]
    if duplicates := _duplicates([str(number) for number in pull_request_numbers]):
        errors.append(f"duplicate stacked PR coverage: {', '.join(duplicates)}")
    missing_pull_requests = sorted(REQUIRED_STACKED_PULL_REQUESTS - set(pull_request_numbers))
    unexpected_pull_requests = sorted(set(pull_request_numbers) - REQUIRED_STACKED_PULL_REQUESTS)
    if missing_pull_requests:
        errors.append(
            "missing stacked PR coverage: "
            + ", ".join(f"#{number}" for number in missing_pull_requests)
        )
    if unexpected_pull_requests:
        errors.append(
            "unexpected stacked PR coverage: "
            + ", ".join(f"#{number}" for number in unexpected_pull_requests)
        )
    known_areas = set(area_ids)
    for pull_request in coverage.stacked_pull_requests:
        if pull_request.classification != "fully-represented":
            errors.append(
                f"PR #{pull_request.number}: release candidate coverage is "
                f"{pull_request.classification}"
            )
        unknown_areas = sorted(set(pull_request.area_ids) - known_areas)
        if unknown_areas:
            errors.append(f"PR #{pull_request.number}: unknown areas: {', '.join(unknown_areas)}")
        if (
            pull_request.integration == "literal-ancestor"
            and pull_request.source_head in resolved_commits
            and coverage.canonical_m6_head in resolved_commits
            and not _git_succeeds(
                repository_root,
                "merge-base",
                "--is-ancestor",
                pull_request.source_head,
                coverage.canonical_m6_head,
            )
        ):
            errors.append(
                f"PR #{pull_request.number} source head is not an ancestor of the canonical M6 head"
            )

    if errors:
        raise ContractValidationError("\n".join(errors))


def _git_succeeds(repository_root: Path, *arguments: str) -> bool:
    try:
        result = subprocess.run(
            ("git", *arguments),
            cwd=repository_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def generated_schema() -> dict[str, object]:
    return EvidenceContract.model_json_schema()


def render_matrix(contract: EvidenceContract) -> str:
    lines = [
        "# Generated evidence contract matrix",
        "",
        "> Generated from `claims.yaml`, `invariants.yaml`, and `environments.yaml`.",
        "> Run `uv run python scripts/validate_evidence.py --write-generated` to update.",
        "",
        "| Claim | Required environments | Current contract status | Invariants |",
        "|---|---|---|---|",
    ]
    for claim in sorted(contract.claims, key=lambda item: item.id):
        statuses = ", ".join(
            f"{record.environment_id}={record.status.value}" for record in claim.evidence
        )
        lines.append(
            f"| `{claim.id}` | {', '.join(claim.required_environment_ids)} | "
            f"{statuses} | {', '.join(claim.invariant_ids)} |"
        )
    lines.extend(
        [
            "",
            "`PENDING` means the command is registered but has not been executed "
            "for this contract commit.",
            "`NOT_RUN` is an explicit environment boundary, not a failure or a PASS.",
            "",
        ]
    )
    return "\n".join(lines)


def render_m6_release_coverage(coverage: M6ReleaseCoverage) -> str:
    lines = [
        "# M6 release coverage",
        "",
        "> Generated from `evidence/m6-release-coverage.json`.",
        "> Run `uv run python scripts/validate_evidence.py --write-generated` to update.",
        "",
        f"- Canonical M6 head: `{coverage.canonical_m6_head}`",
        f"- Main baseline: `{coverage.main_baseline}`",
        f"- Real hardware status: `{coverage.real_hardware_status}`",
        "",
        "## A1-A11 and closure coverage",
        "",
        "| Area | Status | Proofs | Limitations |",
        "|---|---|---|---|",
    ]
    for area in coverage.areas:
        proofs = "<br>".join(
            f"`{proof.kind}`: `{proof.path}` — {proof.note}" for proof in area.proofs
        )
        limitations = "<br>".join(area.limitations)
        lines.append(f"| `{area.id}` {area.title} | `{area.status}` | {proofs} | {limitations} |")
    lines.extend(
        [
            "",
            "## Open stacked PR disposition",
            "",
            "| PR | Areas | Integration | Classification | Rationale |",
            "|---|---|---|---|---|",
        ]
    )
    for pull_request in coverage.stacked_pull_requests:
        lines.append(
            f"| #{pull_request.number} | {', '.join(pull_request.area_ids)} | "
            f"`{pull_request.integration}` | `{pull_request.classification}` | "
            f"{pull_request.rationale} Source head: `{pull_request.source_head}`. |"
        )
    lines.extend(
        [
            "",
            "`fully-represented` is based on final code, tests, migrations, profiles, and "
            "evidence semantics. Literal ancestry alone is not accepted as proof.",
            "",
        ]
    )
    return "\n".join(lines)


def validate_generated_files(
    contract: EvidenceContract,
    repository_root: Path,
    coverage: M6ReleaseCoverage | None = None,
) -> None:
    coverage = coverage or load_m6_release_coverage(repository_root)
    expected_schema = json.dumps(generated_schema(), indent=2, sort_keys=True) + "\n"
    expected_matrix = render_matrix(contract)
    expected_coverage = render_m6_release_coverage(coverage)
    actual_schema = (repository_root / "evidence" / "schema.json").read_text(encoding="utf-8")
    actual_matrix = (repository_root / "evidence" / "matrix.md").read_text(encoding="utf-8")
    actual_coverage = (repository_root / "docs" / "m6-release-coverage.md").read_text(
        encoding="utf-8"
    )
    if actual_schema != expected_schema:
        raise ContractValidationError("evidence/schema.json is stale; run with --write-generated")
    if actual_matrix != expected_matrix:
        raise ContractValidationError("evidence/matrix.md is stale; run with --write-generated")
    if actual_coverage != expected_coverage:
        raise ContractValidationError(
            "docs/m6-release-coverage.md is stale; run with --write-generated"
        )


def write_generated_files(
    contract: EvidenceContract,
    repository_root: Path,
    coverage: M6ReleaseCoverage | None = None,
) -> None:
    coverage = coverage or load_m6_release_coverage(repository_root)
    evidence_root = repository_root / "evidence"
    (evidence_root / "schema.json").write_text(
        json.dumps(generated_schema(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (evidence_root / "matrix.md").write_text(render_matrix(contract), encoding="utf-8")
    (repository_root / "docs" / "m6-release-coverage.md").write_text(
        render_m6_release_coverage(coverage),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the Mini AI Cloud evidence contract")
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[1])
    parser.add_argument("--write-generated", action="store_true")
    args = parser.parse_args(argv)

    repository_root = args.root.resolve()
    contract = load_contract(repository_root)
    coverage = load_m6_release_coverage(repository_root)
    validate_contract(contract, repository_root)
    validate_m6_release_coverage(coverage, repository_root)
    if args.write_generated:
        write_generated_files(contract, repository_root, coverage)
    validate_generated_files(contract, repository_root, coverage)
    print(
        f"Validated {len(contract.claims)} claims, {len(contract.invariants)} invariants, "
        f"{len(contract.environments)} environments, {len(coverage.areas)} M6 areas, "
        f"and {len(coverage.stacked_pull_requests)} stacked PRs."
    )


if __name__ == "__main__":
    main()
