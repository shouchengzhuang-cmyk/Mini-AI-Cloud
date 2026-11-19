"""Run the offline Mini v2 -> GPU-Scheduler-Lab importer smoke with identity hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from itertools import combinations
from pathlib import Path

CONTRACT_VERSION = "mini-ai-cloud.gpu-scheduler-lab/v2"
MINI_REPO = "shouchengzhuang-cmyk/Mini-AI-Cloud"
SCHEDULER_REPO = "shouchengzhuang-cmyk/GPU-Scheduler-Lab"

# Resolve and run the entry point in the same isolated interpreter.
IMPORT_CHECKOUT = """\
import importlib.util
import runpy
import sys
from pathlib import Path

checkout = Path(sys.argv.pop(1))
sys.path.insert(0, str(checkout))
package = "gpu_scheduler_lab"
for module, source in ((package, "__init__.py"), (package + ".__main__", "__main__.py")):
    spec = importlib.util.find_spec(module)
    expected = checkout / package / source
    if spec is None or spec.origin is None or Path(spec.origin).resolve() != expected:
        raise SystemExit(f"Scheduler importer {module} does not resolve to the verified checkout")

Path(sys.argv[sys.argv.index("--output") + 1]).parent.mkdir(parents=True, exist_ok=True)
runpy.run_module(package, run_name="__main__", alter_sys=True)
"""


def _sha(value: str, *, name: str) -> str:
    normalized = value.lower()
    if len(normalized) != 40 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{name} must be a 40-character Git SHA")
    return normalized


def _git(root: Path, *args: str) -> str:
    env = os.environ.copy()
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE"):
        env.pop(name, None)
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )
    if result.returncode:
        raise ValueError(f"could not verify Git checkout at {root}: git {args[0]} failed")
    return result.stdout.strip()


def _verify_checkout(root: Path, *, repository: str, expected_sha: str) -> str:
    if Path(_git(root, "rev-parse", "--show-toplevel")).resolve() != root:
        raise ValueError(f"{repository} requires its Git checkout root")
    origin = _git(root, "config", "--local", "--get-all", "remote.origin.url")
    prefix = r"(?:https://github\.com/|ssh://git@github\.com/|git@github\.com:)"
    if not re.fullmatch(prefix + re.escape(repository) + r"(?:\.git)?/?", origin, re.IGNORECASE):
        raise ValueError(f"origin must identify {repository}")
    actual_sha = _git(root, "rev-parse", "--verify", "HEAD")
    if actual_sha != expected_sha:
        raise ValueError(f"{repository} HEAD is {actual_sha}; expected {expected_sha}")
    if _git(root, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"):
        raise ValueError(f"{repository} working tree must be clean")
    return actual_sha


def _validate_artifact_paths(input_path: Path, output_path: Path, report_path: Path) -> None:
    artifacts = {"input": input_path, "output": output_path, "report": report_path}
    for (left_name, left), (right_name, right) in combinations(artifacts.items(), 2):
        same_file = left == right
        if not same_file:
            try:
                same_file = left.samefile(right)
            except FileNotFoundError:
                pass
        if same_file:
            raise ValueError(f"{left_name} and {right_name} artifact paths must be distinct")


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

    try:
        input_path, output_path, report_path = (
            path.resolve() for path in (args.input, args.output, args.report)
        )
        _validate_artifact_paths(input_path, output_path, report_path)
        mini_expected = _sha(args.mini_sha, name="mini_sha")
        scheduler_expected = _sha(args.scheduler_sha, name="scheduler_sha")
        mini_sha = _verify_checkout(
            Path(__file__).resolve().parents[1], repository=MINI_REPO, expected_sha=mini_expected
        )
        scheduler_root = args.scheduler_repo.resolve()
        scheduler_sha = _verify_checkout(
            scheduler_root, repository=SCHEDULER_REPO, expected_sha=scheduler_expected
        )
        input_bytes = input_path.read_bytes()
        payload = json.loads(input_bytes)
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    if not isinstance(payload, dict) or payload.get("contract_version") != CONTRACT_VERSION:
        raise SystemExit("input is not a Mini AI Cloud v2 export; refusing any fallback")
    if payload.get("producer") != f"mini-ai-cloud/{mini_sha}":
        raise SystemExit("input producer must match the verified Mini HEAD")

    result = subprocess.run(
        [
            args.scheduler_python,
            "-IB",
            "-c",
            IMPORT_CHECKOUT,
            str(scheduler_root),
            "import-mini-ai-cloud",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
        ],
        cwd=scheduler_root,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            result.stderr.strip() or result.stdout.strip() or "Scheduler importer failed"
        )
    if input_path.read_bytes() != input_bytes:
        raise SystemExit("input changed while running the Scheduler importer")
    result_sha = hashlib.sha256(output_path.read_bytes()).hexdigest()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "mini_sha": mini_sha,
                "scheduler_sha": scheduler_sha,
                "contract_version": CONTRACT_VERSION,
                "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
                "result_sha256": result_sha,
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"X0 smoke report: {report_path}")


if __name__ == "__main__":
    main()
