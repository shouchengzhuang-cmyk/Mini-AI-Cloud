from __future__ import annotations

import argparse
from pathlib import Path

from core.ascend_runtime import (
    AscendRuntimeAcceptanceContract,
    load_ascend_acceptance_contract,
    validate_ascend_profile,
)
from scripts.validate_runtime_profiles import load_profile

ASCEND_PROFILE_PATH = Path("runtime_profiles/ascend-vllm-k8s.yaml")
ASCEND_ACCEPTANCE_PATH = Path("runtime_profiles/ascend-vllm-k8s.acceptance.json")


def validate_repository(repository_root: Path) -> AscendRuntimeAcceptanceContract:
    profile = load_profile(repository_root / ASCEND_PROFILE_PATH)
    contract = load_ascend_acceptance_contract(repository_root / ASCEND_ACCEPTANCE_PATH)
    validate_ascend_profile(profile, contract)
    return contract


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate the Ascend Kubernetes runtime contract")
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[1])
    args = parser.parse_args(argv)
    contract = validate_repository(args.root.resolve())
    print(
        "Validated Ascend runtime contract "
        f"{contract.profile_identity}; evidence_status={contract.evidence_status}."
    )


if __name__ == "__main__":
    main()
