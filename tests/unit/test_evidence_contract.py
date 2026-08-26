from __future__ import annotations

from pathlib import Path

import pytest

from scripts.validate_evidence import (
    ContractValidationError,
    load_contract,
    render_matrix,
    validate_contract,
    validate_generated_files,
)

REPOSITORY_ROOT = Path(__file__).parents[2]


def test_committed_evidence_contract_and_generated_files_are_valid() -> None:
    contract = load_contract(REPOSITORY_ROOT)

    validate_contract(contract, REPOSITORY_ROOT)
    validate_generated_files(contract, REPOSITORY_ROOT)

    assert len(contract.claims) == 10
    assert "`service.active-sse-drain`" in render_matrix(contract)


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
