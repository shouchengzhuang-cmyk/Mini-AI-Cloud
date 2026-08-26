from __future__ import annotations

import csv
import json
import math
import random
import time
from dataclasses import asdict, dataclass, field
from heapq import heappop, heappush
from pathlib import Path
from typing import Literal

type SchedulingPolicy = Literal["binpack", "spread"]


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    """Deterministic synthetic workload and cluster settings."""

    worker_count: int = 100
    gpus_per_worker: int = 4
    job_count: int = 10_000
    seed: int = 20260823
    arrival_rate_per_second: float = 1.2
    median_duration_seconds: float = 240.0
    duration_sigma: float = 0.65
    worker_cpu_millicores: int = 32_000
    worker_memory_mb: int = 131_072
    worker_concurrency: int = 64
    preemption_enabled: bool = False
    preemption_min_priority_delta: int = 10
    preemptible_probability: float = 0.75

    def __post_init__(self) -> None:
        positive_integers = {
            "worker_count": self.worker_count,
            "gpus_per_worker": self.gpus_per_worker,
            "job_count": self.job_count,
            "worker_cpu_millicores": self.worker_cpu_millicores,
            "worker_memory_mb": self.worker_memory_mb,
            "worker_concurrency": self.worker_concurrency,
        }
        for name, value in positive_integers.items():
            if value < 1:
                raise ValueError(f"{name} must be at least one")
        if self.arrival_rate_per_second <= 0:
            raise ValueError("arrival_rate_per_second must be positive")
        if self.median_duration_seconds <= 0:
            raise ValueError("median_duration_seconds must be positive")
        if self.duration_sigma < 0:
            raise ValueError("duration_sigma cannot be negative")
        if not 1 <= self.preemption_min_priority_delta <= 100:
            raise ValueError("preemption_min_priority_delta must be between 1 and 100")
        if not 0 <= self.preemptible_probability <= 1:
            raise ValueError("preemptible_probability must be between zero and one")


@dataclass(frozen=True, slots=True)
class SimulatedJob:
    id: int
    arrival_time: float
    duration_seconds: float
    gpu_count: int
    cpu_millicores: int
    memory_mb: int
    priority: int = 0
    preemptible: bool = False


type _PendingEntry = tuple[int, float, int, SimulatedJob]
type _PendingQueues = dict[int, list[_PendingEntry]]


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Summary metrics for one policy.

    ``fragmented_gpu_seconds`` integrates otherwise-free GPU devices on nodes
    that cannot fit the highest-priority FIFO-selected waiting job.
    ``fragmentation_events`` counts jobs that were blocked at least once even
    though the cluster had enough free GPUs in aggregate.

    Simulator throughput is measured around the in-process event loop. It is
    intentionally not a measurement of the database-backed production
    scheduler, network calls, or runtime startup latency. Timing fields do not
    participate in dataclass equality so deterministic scheduling results can
    still be compared across runs.
    """

    policy: SchedulingPolicy
    seed: int
    workers: int
    gpus_per_worker: int
    total_gpus: int
    jobs: int
    completed_jobs: int
    utilization_percent: float
    average_queue_latency_seconds: float
    p50_queue_latency_seconds: float
    p95_queue_latency_seconds: float
    p99_queue_latency_seconds: float
    makespan_seconds: float
    fragmented_gpu_seconds: float
    fragmentation_percent: float
    fragmentation_events: int
    peak_active_workers: int
    placements: int
    preemptions: int
    simulator_elapsed_seconds: float = field(compare=False)
    simulator_placements_per_second: float = field(compare=False)
    simulator_measurement_scope: str = "in_process_event_loop_only_no_database"

    def to_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


@dataclass(slots=True)
class _WorkerState:
    id: int
    gpu_capacity: int
    cpu_capacity: int
    memory_capacity: int
    concurrency: int
    allocated_gpus: int = 0
    allocated_cpu_millicores: int = 0
    allocated_memory_mb: int = 0
    running_jobs: int = 0

    @property
    def free_gpus(self) -> int:
        return self.gpu_capacity - self.allocated_gpus


@dataclass(frozen=True, slots=True)
class _RunningAttempt:
    serial: int
    worker_index: int
    job: SimulatedJob
    started_at: float


def generate_workload(config: SimulationConfig) -> tuple[SimulatedJob, ...]:
    """Generate one workload that can be replayed across scheduling policies."""

    rng = random.Random(config.seed)
    gpu_distribution = tuple(
        (count, weight)
        for count, weight in ((1, 0.60), (2, 0.28), (4, 0.12))
        if count <= config.gpus_per_worker
    )
    gpu_counts, gpu_weights = zip(*gpu_distribution, strict=True)
    arrival_time = 0.0
    jobs: list[SimulatedJob] = []
    for job_id in range(config.job_count):
        if job_id:
            arrival_time += rng.expovariate(config.arrival_rate_per_second)
        gpu_count = rng.choices(gpu_counts, weights=gpu_weights, k=1)[0]
        duration = rng.lognormvariate(
            math.log(config.median_duration_seconds), config.duration_sigma
        )
        jobs.append(
            SimulatedJob(
                id=job_id,
                arrival_time=arrival_time,
                duration_seconds=max(1.0, duration),
                gpu_count=gpu_count,
                cpu_millicores=gpu_count * 2_000,
                memory_mb=gpu_count * 8_192,
                priority=rng.randint(0, 100),
                preemptible=rng.random() < config.preemptible_probability,
            )
        )
    return tuple(jobs)


def simulate(
    config: SimulationConfig,
    policy: SchedulingPolicy,
    *,
    workload: tuple[SimulatedJob, ...] | None = None,
) -> SimulationResult:
    """Run an event-driven GPU scheduling simulation."""

    if policy not in {"binpack", "spread"}:
        raise ValueError("policy must be 'binpack' or 'spread'")
    jobs = workload if workload is not None else generate_workload(config)
    _validate_workload(config, jobs)
    workers = [
        _WorkerState(
            id=index,
            gpu_capacity=config.gpus_per_worker,
            cpu_capacity=config.worker_cpu_millicores,
            memory_capacity=config.worker_memory_mb,
            concurrency=config.worker_concurrency,
        )
        for index in range(config.worker_count)
    ]
    pending: _PendingQueues = {gpu_count: [] for gpu_count in range(1, config.gpus_per_worker + 1)}
    completions: list[tuple[float, int, int]] = []
    active_attempts: dict[int, _RunningAttempt] = {}
    initially_placed_job_ids: set[int] = set()
    queue_latencies: list[float] = []
    fragmented_job_ids: set[int] = set()
    fragmented_gpu_seconds = 0.0
    busy_gpu_seconds = 0.0
    arrival_index = 0
    completion_serial = 0
    completed_jobs = 0
    placements = 0
    preemptions = 0
    peak_active_workers = 0
    now = jobs[0].arrival_time
    last_completion = now
    simulation_started = time.perf_counter()

    while arrival_index < len(jobs) or completions or any(pending.values()):
        next_arrival = jobs[arrival_index].arrival_time if arrival_index < len(jobs) else math.inf
        next_completion = completions[0][0] if completions else math.inf
        next_event = min(next_arrival, next_completion)
        if math.isinf(next_event):
            raise RuntimeError("simulation stalled with unschedulable jobs")

        fragmented_gpu_seconds += _fragmented_gpu_count(workers, pending) * (next_event - now)
        busy_gpu_seconds += sum(worker.allocated_gpus for worker in workers) * (next_event - now)
        now = next_event

        while completions and completions[0][0] <= now:
            finished_at, attempt_serial, job_id = heappop(completions)
            attempt = active_attempts.get(job_id)
            if attempt is None or attempt.serial != attempt_serial:
                continue
            _release_attempt(workers[attempt.worker_index], attempt)
            del active_attempts[job_id]
            completed_jobs += 1
            last_completion = max(last_completion, finished_at)

        while arrival_index < len(jobs) and jobs[arrival_index].arrival_time <= now:
            job = jobs[arrival_index]
            heappush(pending[job.gpu_count], _pending_entry(job))
            arrival_index += 1

        completion_serial, new_placements, new_preemptions = _schedule_pending(
            workers=workers,
            pending=pending,
            policy=policy,
            now=now,
            completions=completions,
            active_attempts=active_attempts,
            initially_placed_job_ids=initially_placed_job_ids,
            queue_latencies=queue_latencies,
            fragmented_job_ids=fragmented_job_ids,
            completion_serial=completion_serial,
            preemption_enabled=config.preemption_enabled,
            preemption_min_priority_delta=config.preemption_min_priority_delta,
        )
        placements += new_placements
        preemptions += new_preemptions
        peak_active_workers = max(
            peak_active_workers, sum(worker.running_jobs > 0 for worker in workers)
        )

    simulator_elapsed = time.perf_counter() - simulation_started
    makespan = max(0.0, last_completion - jobs[0].arrival_time)
    capacity_seconds = config.worker_count * config.gpus_per_worker * makespan
    return SimulationResult(
        policy=policy,
        seed=config.seed,
        workers=config.worker_count,
        gpus_per_worker=config.gpus_per_worker,
        total_gpus=config.worker_count * config.gpus_per_worker,
        jobs=len(jobs),
        completed_jobs=completed_jobs,
        utilization_percent=_round_metric(100 * busy_gpu_seconds / max(1.0, capacity_seconds)),
        average_queue_latency_seconds=_round_metric(
            sum(queue_latencies) / max(1, len(queue_latencies))
        ),
        p50_queue_latency_seconds=_round_metric(_percentile(queue_latencies, 0.50)),
        p95_queue_latency_seconds=_round_metric(_percentile(queue_latencies, 0.95)),
        p99_queue_latency_seconds=_round_metric(_percentile(queue_latencies, 0.99)),
        makespan_seconds=_round_metric(makespan),
        fragmented_gpu_seconds=_round_metric(fragmented_gpu_seconds),
        fragmentation_percent=_round_metric(
            100 * fragmented_gpu_seconds / max(1.0, capacity_seconds)
        ),
        fragmentation_events=len(fragmented_job_ids),
        peak_active_workers=peak_active_workers,
        placements=placements,
        preemptions=preemptions,
        simulator_elapsed_seconds=round(simulator_elapsed, 9),
        simulator_placements_per_second=round(placements / max(simulator_elapsed, 1e-12), 3),
    )


def compare_policies(
    config: SimulationConfig,
    policies: tuple[SchedulingPolicy, ...] = ("binpack", "spread"),
) -> tuple[SimulationResult, ...]:
    if not policies:
        raise ValueError("at least one policy is required")
    workload = generate_workload(config)
    return tuple(simulate(config, policy, workload=workload) for policy in policies)


def write_simulation_outputs(
    config: SimulationConfig,
    results: tuple[SimulationResult, ...],
    output_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "scheduler-simulation.json"
    csv_path = output_dir / "scheduler-simulation.csv"
    payload = {
        "config": asdict(config),
        "results": [result.to_dict() for result in results],
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fieldnames = list(SimulationResult.__dataclass_fields__)
    with csv_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result.to_dict() for result in results)
    return json_path, csv_path


def _schedule_pending(
    *,
    workers: list[_WorkerState],
    pending: _PendingQueues,
    policy: SchedulingPolicy,
    now: float,
    completions: list[tuple[float, int, int]],
    active_attempts: dict[int, _RunningAttempt],
    initially_placed_job_ids: set[int],
    queue_latencies: list[float],
    fragmented_job_ids: set[int],
    completion_serial: int,
    preemption_enabled: bool,
    preemption_min_priority_delta: int,
) -> tuple[int, int, int]:
    placements = 0
    preemptions = 0
    while True:
        candidates = sorted(queue[0] for queue in pending.values() if queue)
        placed = False
        for candidate in candidates:
            job = candidate[3]
            worker_index = _select_worker(workers, job, policy)
            if worker_index is None and preemption_enabled:
                preemption_plan = _select_preemption_plan(
                    workers,
                    active_attempts,
                    job,
                    preemption_min_priority_delta,
                )
                if preemption_plan is not None:
                    worker_index, victims = preemption_plan
                    for victim in victims:
                        _release_attempt(workers[victim.worker_index], victim)
                        active_attempts.pop(victim.job.id, None)
                        heappush(
                            pending[victim.job.gpu_count],
                            _pending_entry(victim.job),
                        )
                    preemptions += len(victims)
            if worker_index is None:
                if _is_gpu_fragmented(workers, job.gpu_count):
                    fragmented_job_ids.add(job.id)
                continue
            heappop(pending[job.gpu_count])
            worker = workers[worker_index]
            worker.allocated_gpus += job.gpu_count
            worker.allocated_cpu_millicores += job.cpu_millicores
            worker.allocated_memory_mb += job.memory_mb
            worker.running_jobs += 1
            completion_serial += 1
            attempt = _RunningAttempt(
                serial=completion_serial,
                worker_index=worker_index,
                job=job,
                started_at=now,
            )
            active_attempts[job.id] = attempt
            heappush(
                completions,
                (now + job.duration_seconds, completion_serial, job.id),
            )
            if job.id not in initially_placed_job_ids:
                initially_placed_job_ids.add(job.id)
                queue_latencies.append(now - job.arrival_time)
            placements += 1
            placed = True
            break
        if not placed:
            return completion_serial, placements, preemptions


def _pending_sort_key(job: SimulatedJob) -> tuple[int, float, int]:
    """Order queued work by priority, then stable arrival FIFO and job id."""

    return (-job.priority, job.arrival_time, job.id)


def _pending_entry(job: SimulatedJob) -> _PendingEntry:
    return (*_pending_sort_key(job), job)


def _select_preemption_plan(
    workers: list[_WorkerState],
    active_attempts: dict[int, _RunningAttempt],
    incoming: SimulatedJob,
    min_priority_delta: int,
) -> tuple[int, tuple[_RunningAttempt, ...]] | None:
    plans: list[tuple[tuple[int, int, int, int], int, tuple[_RunningAttempt, ...]]] = []
    for worker_index, worker in enumerate(workers):
        eligible_victims = sorted(
            (
                attempt
                for attempt in active_attempts.values()
                if attempt.worker_index == worker_index
                and attempt.job.preemptible
                and incoming.priority - attempt.job.priority >= min_priority_delta
            ),
            key=lambda attempt: (
                attempt.job.priority,
                attempt.started_at,
                attempt.job.id,
            ),
        )
        victims: list[_RunningAttempt] = []
        freed_gpus = 0
        freed_cpu = 0
        freed_memory = 0
        for victim in eligible_victims:
            victims.append(victim)
            freed_gpus += victim.job.gpu_count
            freed_cpu += victim.job.cpu_millicores
            freed_memory += victim.job.memory_mb
            if _can_fit_after_preemption(
                worker,
                incoming,
                victim_count=len(victims),
                freed_gpus=freed_gpus,
                freed_cpu=freed_cpu,
                freed_memory=freed_memory,
            ):
                plan_score = (
                    len(victims),
                    sum(attempt.job.priority for attempt in victims),
                    sum(attempt.job.gpu_count for attempt in victims),
                    worker.id,
                )
                plans.append((plan_score, worker_index, tuple(victims)))
                break
    if not plans:
        return None
    plans.sort(key=lambda plan: plan[0])
    _, worker_index, selected_victims = plans[0]
    return worker_index, selected_victims


def _can_fit_after_preemption(
    worker: _WorkerState,
    incoming: SimulatedJob,
    *,
    victim_count: int,
    freed_gpus: int,
    freed_cpu: int,
    freed_memory: int,
) -> bool:
    return (
        worker.allocated_gpus - freed_gpus + incoming.gpu_count <= worker.gpu_capacity
        and worker.allocated_cpu_millicores - freed_cpu + incoming.cpu_millicores
        <= worker.cpu_capacity
        and worker.allocated_memory_mb - freed_memory + incoming.memory_mb <= worker.memory_capacity
        and worker.running_jobs - victim_count < worker.concurrency
    )


def _release_attempt(worker: _WorkerState, attempt: _RunningAttempt) -> None:
    job = attempt.job
    worker.allocated_gpus -= job.gpu_count
    worker.allocated_cpu_millicores -= job.cpu_millicores
    worker.allocated_memory_mb -= job.memory_mb
    worker.running_jobs -= 1
    if (
        worker.allocated_gpus < 0
        or worker.allocated_cpu_millicores < 0
        or worker.allocated_memory_mb < 0
        or worker.running_jobs < 0
    ):
        raise RuntimeError("worker resource accounting became negative")


def _select_worker(
    workers: list[_WorkerState], job: SimulatedJob, policy: SchedulingPolicy
) -> int | None:
    eligible: list[tuple[tuple[float, ...], int]] = []
    for index, worker in enumerate(workers):
        if worker.free_gpus < job.gpu_count:
            continue
        if worker.allocated_cpu_millicores + job.cpu_millicores > worker.cpu_capacity:
            continue
        if worker.allocated_memory_mb + job.memory_mb > worker.memory_capacity:
            continue
        if worker.running_jobs >= worker.concurrency:
            continue
        gpu_fraction = (worker.allocated_gpus + job.gpu_count) / worker.gpu_capacity
        cpu_fraction = (worker.allocated_cpu_millicores + job.cpu_millicores) / worker.cpu_capacity
        memory_fraction = (worker.allocated_memory_mb + job.memory_mb) / worker.memory_capacity
        dominant = max(gpu_fraction, cpu_fraction, memory_fraction)
        if policy == "binpack":
            score = (-dominant, -cpu_fraction, -memory_fraction)
        else:
            score = (dominant, 0.0, float(worker.running_jobs))
        eligible.append((score, index))
    if not eligible:
        return None
    eligible.sort(key=lambda item: (*item[0], workers[item[1]].id))
    return eligible[0][1]


def _fragmented_gpu_count(workers: list[_WorkerState], pending: _PendingQueues) -> int:
    selected = min((queue[0] for queue in pending.values() if queue), default=None)
    if selected is None:
        return 0
    selected_job = selected[3]
    return sum(
        worker.free_gpus for worker in workers if 0 < worker.free_gpus < selected_job.gpu_count
    )


def _is_gpu_fragmented(workers: list[_WorkerState], gpu_count: int) -> bool:
    free_gpus = [worker.free_gpus for worker in workers]
    return sum(free_gpus) >= gpu_count and max(free_gpus, default=0) < gpu_count


def _validate_workload(config: SimulationConfig, workload: tuple[SimulatedJob, ...]) -> None:
    if not workload:
        raise ValueError("workload cannot be empty")
    if len(workload) != config.job_count:
        raise ValueError("workload size must match config.job_count")
    previous_arrival = -math.inf
    job_ids: set[int] = set()
    for job in workload:
        if job.id in job_ids:
            raise ValueError("workload job ids must be unique")
        job_ids.add(job.id)
        if job.arrival_time < previous_arrival:
            raise ValueError("workload must be ordered by arrival_time")
        if not 1 <= job.gpu_count <= config.gpus_per_worker:
            raise ValueError("job GPU demand must fit on one worker")
        if job.cpu_millicores > config.worker_cpu_millicores:
            raise ValueError("job CPU demand must fit on one worker")
        if job.memory_mb > config.worker_memory_mb:
            raise ValueError("job memory demand must fit on one worker")
        if job.duration_seconds <= 0:
            raise ValueError("job duration must be positive")
        if not 0 <= job.priority <= 100:
            raise ValueError("job priority must be between 0 and 100")
        previous_arrival = job.arrival_time


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _round_metric(value: float) -> float:
    return round(value, 6)
