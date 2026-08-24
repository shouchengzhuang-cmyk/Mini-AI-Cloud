from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scheduler.simulator import (
    SimulatedJob,
    SimulationConfig,
    compare_policies,
    generate_workload,
    simulate,
    write_simulation_outputs,
)


def test_simulation_is_deterministic_and_completes_every_job() -> None:
    config = SimulationConfig(worker_count=8, gpus_per_worker=4, job_count=250, seed=17)

    first = compare_policies(config)
    second = compare_policies(config)

    assert first == second
    assert {result.policy for result in first} == {"binpack", "spread"}
    for result in first:
        assert result.completed_jobs == config.job_count
        assert 0 < result.utilization_percent <= 100
        assert result.makespan_seconds > 0
        assert result.p50_queue_latency_seconds <= result.p95_queue_latency_seconds
        assert result.p95_queue_latency_seconds <= result.p99_queue_latency_seconds
        assert result.placements == config.job_count
        assert result.preemptions == 0
        assert result.simulator_elapsed_seconds > 0
        assert result.simulator_placements_per_second > 0
        assert result.simulator_measurement_scope == "in_process_event_loop_only_no_database"


def test_generated_priority_and_preemptibility_are_seeded() -> None:
    config = SimulationConfig(job_count=100, seed=29)

    first = generate_workload(config)
    second = generate_workload(config)

    assert first == second
    assert all(0 <= job.priority <= 100 for job in first)
    assert {job.priority for job in first} != {0}
    assert {job.preemptible for job in first} == {False, True}


def test_seeded_preemption_preserves_placement_invariant() -> None:
    config = SimulationConfig(
        worker_count=2,
        gpus_per_worker=2,
        job_count=40,
        seed=17,
        preemption_enabled=True,
    )

    first = simulate(config, "binpack")
    second = simulate(config, "binpack")

    assert first == second
    assert first.preemptions > 0
    assert first.completed_jobs == first.jobs
    assert first.placements == first.jobs + first.preemptions


def test_pending_jobs_use_priority_then_fifo_order() -> None:
    config = SimulationConfig(worker_count=1, gpus_per_worker=1, job_count=3)
    priority_workload = (
        SimulatedJob(0, 0.0, 10.0, 1, 2_000, 8_192),
        SimulatedJob(1, 1.0, 100.0, 1, 2_000, 8_192, priority=10),
        SimulatedJob(2, 1.0, 1.0, 1, 2_000, 8_192, priority=90),
    )
    fifo_workload = (
        SimulatedJob(0, 0.0, 10.0, 1, 2_000, 8_192),
        SimulatedJob(1, 1.0, 100.0, 1, 2_000, 8_192, priority=50),
        SimulatedJob(2, 1.0, 1.0, 1, 2_000, 8_192, priority=50),
    )

    priority_result = simulate(config, "binpack", workload=priority_workload)
    fifo_result = simulate(config, "binpack", workload=fifo_workload)

    assert priority_result.average_queue_latency_seconds == pytest.approx(19 / 3)
    assert fifo_result.average_queue_latency_seconds == pytest.approx(118 / 3)


def test_preemption_requeues_victim_and_ignores_stale_completion() -> None:
    config = SimulationConfig(
        worker_count=1,
        gpus_per_worker=1,
        job_count=2,
        preemption_enabled=True,
        preemption_min_priority_delta=10,
    )
    workload = (
        SimulatedJob(
            0,
            0.0,
            10.0,
            1,
            2_000,
            8_192,
            priority=10,
            preemptible=True,
        ),
        SimulatedJob(1, 1.0, 1.0, 1, 2_000, 8_192, priority=90),
    )

    result = simulate(config, "binpack", workload=workload)

    assert result.completed_jobs == 2
    assert result.preemptions == 1
    assert result.placements == result.jobs + result.preemptions == 3
    assert result.average_queue_latency_seconds == 0
    assert result.makespan_seconds == 12
    assert result.utilization_percent == 100


def test_binpack_uses_fewer_workers_for_identical_jobs() -> None:
    config = SimulationConfig(worker_count=4, gpus_per_worker=4, job_count=8)
    workload = tuple(
        SimulatedJob(
            id=index,
            arrival_time=0.0,
            duration_seconds=10.0,
            gpu_count=1,
            cpu_millicores=2_000,
            memory_mb=8_192,
        )
        for index in range(config.job_count)
    )

    binpack = simulate(config, "binpack", workload=workload)
    spread = simulate(config, "spread", workload=workload)

    assert binpack.peak_active_workers == 2
    assert spread.peak_active_workers == 4
    assert binpack.completed_jobs == spread.completed_jobs == 8


def test_fragmentation_tracks_aggregate_free_gpus_that_cannot_fit_a_job() -> None:
    config = SimulationConfig(worker_count=2, gpus_per_worker=4, job_count=3)
    workload = (
        SimulatedJob(0, 0.0, 10.0, 1, 2_000, 8_192),
        SimulatedJob(1, 0.0, 10.0, 1, 2_000, 8_192),
        SimulatedJob(2, 1.0, 1.0, 4, 8_000, 32_768),
    )

    binpack = simulate(config, "binpack", workload=workload)
    spread = simulate(config, "spread", workload=workload)

    assert binpack.fragmentation_events == 0
    assert binpack.fragmented_gpu_seconds == 0
    assert spread.fragmentation_events == 1
    assert spread.fragmented_gpu_seconds == pytest.approx(54.0)


def test_writes_machine_readable_json_and_csv(tmp_path: Path) -> None:
    config = SimulationConfig(worker_count=3, gpus_per_worker=2, job_count=30, seed=7)
    results = compare_policies(config)

    json_path, csv_path = write_simulation_outputs(config, results, tmp_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["config"]["seed"] == 7
    assert [item["policy"] for item in payload["results"]] == ["binpack", "spread"]
    assert all(item["preemptions"] == 0 for item in payload["results"])
    assert all(
        item["simulator_measurement_scope"] == "in_process_event_loop_only_no_database"
        for item in payload["results"]
    )
    with csv_path.open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    assert [row["policy"] for row in rows] == ["binpack", "spread"]
    assert all(int(row["completed_jobs"]) == config.job_count for row in rows)
    assert all(float(row["simulator_placements_per_second"]) > 0 for row in rows)


def test_rejects_workload_that_cannot_fit_on_one_worker() -> None:
    config = SimulationConfig(worker_count=1, gpus_per_worker=2, job_count=1)
    workload = (
        SimulatedJob(
            id=0,
            arrival_time=0.0,
            duration_seconds=1.0,
            gpu_count=3,
            cpu_millicores=2_000,
            memory_mb=8_192,
        ),
    )

    with pytest.raises(ValueError, match="GPU demand"):
        simulate(config, "binpack", workload=workload)
