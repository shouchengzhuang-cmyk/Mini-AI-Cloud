from __future__ import annotations

import argparse
import json
import re
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


class EvidenceContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(pattern=r"^1\.\d+\.\d+$")
    environments: list[Environment] = Field(min_length=1)
    invariants: list[Invariant] = Field(min_length=1)
    claims: list[Claim] = Field(min_length=1)


class ContractValidationError(ValueError):
    pass


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


def validate_generated_files(contract: EvidenceContract, repository_root: Path) -> None:
    expected_schema = json.dumps(generated_schema(), indent=2, sort_keys=True) + "\n"
    expected_matrix = render_matrix(contract)
    actual_schema = (repository_root / "evidence" / "schema.json").read_text(encoding="utf-8")
    actual_matrix = (repository_root / "evidence" / "matrix.md").read_text(encoding="utf-8")
    if actual_schema != expected_schema:
        raise ContractValidationError("evidence/schema.json is stale; run with --write-generated")
    if actual_matrix != expected_matrix:
        raise ContractValidationError("evidence/matrix.md is stale; run with --write-generated")


def write_generated_files(contract: EvidenceContract, repository_root: Path) -> None:
    evidence_root = repository_root / "evidence"
    (evidence_root / "schema.json").write_text(
        json.dumps(generated_schema(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (evidence_root / "matrix.md").write_text(render_matrix(contract), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the Mini AI Cloud evidence contract")
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[1])
    parser.add_argument("--write-generated", action="store_true")
    args = parser.parse_args(argv)

    repository_root = args.root.resolve()
    contract = load_contract(repository_root)
    validate_contract(contract, repository_root)
    if args.write_generated:
        write_generated_files(contract, repository_root)
    validate_generated_files(contract, repository_root)
    print(
        f"Validated {len(contract.claims)} claims, {len(contract.invariants)} invariants, "
        f"and {len(contract.environments)} environments."
    )


if __name__ == "__main__":
    main()
