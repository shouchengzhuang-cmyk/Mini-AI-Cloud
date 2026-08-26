from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

from scheduler.simulator import (
    SchedulingPolicy,
    SimulationConfig,
    compare_policies,
    write_simulation_outputs,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare deterministic binpack and spread GPU scheduling simulations. "
            "Reported throughput measures only the in-process event loop, not the DB scheduler."
        )
    )
    parser.add_argument("--workers", type=int, default=100)
    parser.add_argument("--gpus-per-worker", type=int, default=4)
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20260823)
    parser.add_argument("--arrival-rate", type=float, default=1.2)
    parser.add_argument("--median-duration", type=float, default=240.0)
    parser.add_argument(
        "--enable-preemption",
        action="store_true",
        help="Allow higher-priority jobs to restart lower-priority preemptible jobs.",
    )
    parser.add_argument("--preemption-min-priority-delta", type=int, default=10)
    parser.add_argument("--preemptible-probability", type=float, default=0.75)
    parser.add_argument(
        "--policies",
        nargs="+",
        choices=("binpack", "spread"),
        default=("binpack", "spread"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("build/scheduler-simulation"),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = SimulationConfig(
        worker_count=args.workers,
        gpus_per_worker=args.gpus_per_worker,
        job_count=args.jobs,
        seed=args.seed,
        arrival_rate_per_second=args.arrival_rate,
        median_duration_seconds=args.median_duration,
        preemption_enabled=args.enable_preemption,
        preemption_min_priority_delta=args.preemption_min_priority_delta,
        preemptible_probability=args.preemptible_probability,
    )
    policies = tuple(cast(SchedulingPolicy, policy) for policy in args.policies)
    results = compare_policies(config, policies)
    json_path, csv_path = write_simulation_outputs(config, results, args.output_dir)
    print(
        json.dumps(
            {
                "json": str(json_path),
                "csv": str(csv_path),
                "results": [result.to_dict() for result in results],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
