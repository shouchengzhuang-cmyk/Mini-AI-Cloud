from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from core.kubernetes_serving_preflight import (
    KubernetesServingPreflightError,
    collect_kubernetes_serving_preflight,
    load_release_runtime_profile_contract,
)

DEFAULT_MANIFEST_PATH = Path(__file__).parents[1] / "runtime_profiles" / "manifest.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the fail-closed Kubernetes serving Runtime Profile preflight"
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--profile-version", required=True)
    parser.add_argument("--profile-digest", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    args = parser.parse_args(argv)

    try:
        contract = load_release_runtime_profile_contract(
            args.manifest,
            profile_id=args.profile_id,
            profile_version=args.profile_version,
            semantic_digest=args.profile_digest,
        )
        result = collect_kubernetes_serving_preflight(
            contract,
            namespace_name=args.namespace,
            kubectl=args.kubectl,
            kubeconfig=args.kubeconfig,
        )
    except (KubernetesServingPreflightError, ValueError) as error:
        print(f"Kubernetes serving preflight failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
