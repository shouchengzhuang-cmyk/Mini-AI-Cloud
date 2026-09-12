"""Run the offline Mini v2 -> GPU-Scheduler-Lab importer smoke with identity hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

CONTRACT_VERSION = "mini-ai-cloud.gpu-scheduler-lab/v2"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha(value: str, *, name: str) -> str:
    normalized = value.lower()
    if len(normalized) != 40 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{name} must be a 40-character Git SHA")
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--scheduler-repo", type=Path, required=True)
    parser.add_argument("--mini-sha", required=True)
    parser.add_argument("--scheduler-sha", required=True)
    parser.add_argument("--scheduler-python", default=sys.executable)
    args = parser.parse_args()

    payload = json.loads(args.input.read_text(encoding="utf-8"))
    if payload.get("contract_version") != CONTRACT_VERSION:
        raise SystemExit("input is not a Mini AI Cloud v2 export; refusing any fallback")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            args.scheduler_python,
            "-m",
            "gpu_scheduler_lab",
            "import-mini-ai-cloud",
            "--input",
            str(args.input.resolve()),
            "--output",
            str(args.output.resolve()),
        ],
        cwd=args.scheduler_repo,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            result.stderr.strip() or result.stdout.strip() or "Scheduler importer failed"
        )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(
            {
                "mini_sha": _sha(args.mini_sha, name="mini_sha"),
                "scheduler_sha": _sha(args.scheduler_sha, name="scheduler_sha"),
                "contract_version": CONTRACT_VERSION,
                "input_sha256": _sha256(args.input),
                "result_sha256": _sha256(args.output),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"X0 smoke report: {args.report}")


if __name__ == "__main__":
    main()
