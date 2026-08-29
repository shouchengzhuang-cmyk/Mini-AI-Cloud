from __future__ import annotations

from pathlib import Path

import pytest

from scripts.validate_evidence import (
    ContractValidationError,
    load_contract,
    load_m6_release_coverage,
    render_m6_release_coverage,
    render_matrix,
    validate_contract,
    validate_generated_files,
    validate_m6_release_coverage,
)

REPOSITORY_ROOT = Path(__file__).parents[2]


def test_committed_evidence_contract_and_generated_files_are_valid() -> None:
    contract = load_contract(REPOSITORY_ROOT)
    coverage = load_m6_release_coverage(REPOSITORY_ROOT)

    validate_contract(contract, REPOSITORY_ROOT)
    validate_m6_release_coverage(coverage, REPOSITORY_ROOT)
    validate_generated_files(contract, REPOSITORY_ROOT, coverage)

    assert len(contract.claims) == 10
    assert "`service.active-sse-drain`" in render_matrix(contract)
    assert len(coverage.areas) == 13
    assert {item.number for item in coverage.stacked_pull_requests} == set(range(20, 28))
    assert "`A11`" in render_m6_release_coverage(coverage)


def test_duplicate_claim_id_is_rejected() -> None:
    contract = load_contract(REPOSITORY_ROOT)
    contract.claims.append(contract.claims[0].model_copy(deep=True))

    with pytest.raises(ContractValidationError, match="duplicate claim ids"):
        validate_contract(contract, REPOSITORY_ROOT)


def test_unknown_environment_and_missing_evidence_are_rejected() -> None:
    contract = load_contract(REPOSITORY_ROOT)
    contract.claims[0].required_environment_ids = ["unknown-environment"]

    with pytest.raises(ContractValidationError, match="unknown environments"):
        validate_contract(contract, REPOSITORY_ROOT)


def test_missing_reference_is_rejected() -> None:
    contract = load_contract(REPOSITORY_ROOT)
    contract.claims[0].evidence[0].references[0].path = "tests/does-not-exist.py"

    with pytest.raises(ContractValidationError, match="missing test reference"):
        validate_contract(contract, REPOSITORY_ROOT)


def test_missing_m6_proof_path_is_rejected() -> None:
    coverage = load_m6_release_coverage(REPOSITORY_ROOT)
    coverage.areas[0].proofs[0].path = "core/does-not-exist.py"

    with pytest.raises(ContractValidationError, match="missing code proof path"):
        validate_m6_release_coverage(coverage, REPOSITORY_ROOT)


def test_missing_stacked_pr_classification_is_rejected() -> None:
    coverage = load_m6_release_coverage(REPOSITORY_ROOT)
    coverage.stacked_pull_requests.pop()

    with pytest.raises(ContractValidationError, match="missing stacked PR coverage"):
        validate_m6_release_coverage(coverage, REPOSITORY_ROOT)


def test_m6_commit_relationships_are_resolved_and_verified() -> None:
    coverage = load_m6_release_coverage(REPOSITORY_ROOT)
    coverage.canonical_m6_head = "0" * 40

    with pytest.raises(ContractValidationError, match="canonical M6 head cannot be resolved"):
        validate_m6_release_coverage(coverage, REPOSITORY_ROOT)

    coverage = load_m6_release_coverage(REPOSITORY_ROOT)
    semantic_integration = next(
        item
        for item in coverage.stacked_pull_requests
        if item.integration == "semantic-integration"
    )
    semantic_integration.source_head = "0" * 40
    validate_m6_release_coverage(coverage, REPOSITORY_ROOT)

    semantic_integration.integration = "literal-ancestor"

    with pytest.raises(ContractValidationError, match="PR #22 source head cannot be resolved"):
        validate_m6_release_coverage(coverage, REPOSITORY_ROOT)
