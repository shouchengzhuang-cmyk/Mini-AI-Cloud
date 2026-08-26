from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import func, select, text

from core.database import Database
from core.enums import RuntimeType, TaskStatus
from models import (
    Artifact,
    Project,
    ResourceReservation,
    Task,
    TaskArtifact,
    TaskEvent,
    TaskExecution,
    UsageLedger,
    Worker,
)

MARKER_BYTES = b"mini-ai-cloud-dr-marker-v1\n"


def marker_ids(run_id: str) -> dict[str, uuid.UUID]:
    return {
        name: uuid.uuid5(uuid.NAMESPACE_URL, f"mini-ai-cloud-dr:{run_id}:{name}")
        for name in ("project", "task", "execution", "reservation", "artifact", "binding", "event")
    }


def marker_path(artifact_root: Path, run_id: str) -> Path:
    if not run_id or not all(character.isalnum() or character == "-" for character in run_id):
        raise ValueError("run id must contain only letters, digits, and hyphens")
    root = artifact_root.resolve()
    path = (root / "dr-rehearsal" / run_id / "marker.bin").resolve()
    if root not in path.parents:
        raise ValueError("marker path escapes artifact root")
    return path


async def seed_marker(database: Database, artifact_root: Path, run_id: str) -> dict[str, object]:
    ids = marker_ids(run_id)
    now = datetime.now(UTC)
    path = marker_path(artifact_root, run_id)
    path.parent.mkdir(parents=True, exist_ok=False)
    path.write_bytes(MARKER_BYTES)
    digest = hashlib.sha256(MARKER_BYTES).hexdigest()
    worker_id = f"dr-worker-{run_id}"
    worker_session_id = uuid.uuid5(uuid.NAMESPACE_URL, f"mini-ai-cloud-dr:{run_id}:session")

    async with database.session() as session, session.begin():
        existing = await session.scalar(select(Project.id).where(Project.slug == run_id))
        if existing is not None:
            raise RuntimeError("DR marker project already exists")
        session.add(
            Project(id=ids["project"], name="DR rehearsal marker", slug=run_id),
        )
        session.add(
            Worker(
                id=worker_id,
                worker_session_id=worker_session_id,
                hostname="dr-rehearsal",
                runtime_types=[RuntimeType.FAKE.value],
                cpu_count=1,
                cpu_total_millicores=1000,
                cpu_allocatable_millicores=1000,
                memory_total_mb=1024,
                memory_allocatable_mb=1024,
            )
        )
        await session.flush()
        session.add(
            Task(
                id=ids["task"],
                project_id=ids["project"],
                image="dr-marker:local",
                command=["true"],
                environment={"DR_MARKER": run_id},
                status=TaskStatus.SUCCEEDED,
                queued_at=now,
                assigned_at=now,
                started_at=now,
                finished_at=now + timedelta(seconds=1),
                worker_id=worker_id,
                execution_id=ids["execution"],
                exit_code=0,
                cpu_limit=1,
                cpu_millicores=1000,
                memory_limit_mb=256,
                gpu_count=0,
                runtime_type=RuntimeType.FAKE,
                labels={"dr_run_id": run_id},
            )
        )
        await session.flush()
        session.add(
            TaskExecution(
                id=ids["execution"],
                task_id=ids["task"],
                project_id=ids["project"],
                worker_id=worker_id,
                worker_session_id=worker_session_id,
                attempt=1,
                status=TaskStatus.SUCCEEDED.value,
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                cpu_price_per_hour=Decimal("0.05"),
                memory_price_per_gb_hour=Decimal("0.005"),
                gpu_price_per_hour=Decimal("1.0"),
                assigned_at=now,
                started_at=now,
                finished_at=now + timedelta(seconds=1),
                runtime_type=RuntimeType.FAKE.value,
                runtime_object_id=f"dr:{run_id}",
            )
        )
        await session.flush()
        session.add(
            ResourceReservation(
                id=ids["reservation"],
                project_id=ids["project"],
                task_id=ids["task"],
                execution_id=ids["execution"],
                worker_id=worker_id,
                worker_session_id=worker_session_id,
                cpu_millicores=1000,
                memory_mb=256,
                gpu_count=0,
                state="released",
                released_at=now + timedelta(seconds=1),
                release_reason="dr-marker-complete",
            )
        )
        session.add(
            UsageLedger(
                project_id=ids["project"],
                task_id=ids["task"],
                execution_id=ids["execution"],
                started_at=now,
                finished_at=now + timedelta(seconds=1),
                cpu_seconds=Decimal("1.0"),
                memory_gb_seconds=Decimal("0.25"),
                gpu_seconds=Decimal("0"),
                cost=Decimal("0.0001"),
                currency="USD",
                pricing_source="dr-rehearsal",
            )
        )
        session.add(
            Artifact(
                id=ids["artifact"],
                project_id=ids["project"],
                name="dr-marker.bin",
                state="ready",
                backend="local",
                object_key=f"dr-rehearsal/{run_id}/marker.bin",
                content_type="application/octet-stream",
                size_bytes=len(MARKER_BYTES),
                sha256=digest,
                verified_at=now,
            )
        )
        await session.flush()
        session.add(
            TaskArtifact(
                id=ids["binding"],
                task_id=ids["task"],
                artifact_id=ids["artifact"],
                direction="output",
                name="marker",
                mount_path="/output/marker.bin",
                required=True,
            )
        )
        session.add(
            TaskEvent(
                id=ids["event"],
                project_id=ids["project"],
                task_id=ids["task"],
                event_type="dr_marker_succeeded",
                sequence=1,
                from_status=TaskStatus.RUNNING.value,
                status=TaskStatus.SUCCEEDED.value,
                execution_id=ids["execution"],
                worker_id=worker_id,
                details={"run_id": run_id},
            )
        )
    return await verify_marker(database, artifact_root, run_id)


async def verify_marker(database: Database, artifact_root: Path, run_id: str) -> dict[str, object]:
    ids = marker_ids(run_id)
    path = marker_path(artifact_root, run_id)
    if not path.is_file() or path.is_symlink():
        raise RuntimeError("marker artifact file is missing or unsafe")
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    async with database.session() as session:
        project = await session.get(Project, ids["project"])
        task = await session.get(Task, ids["task"])
        artifact = await session.get(Artifact, ids["artifact"])
        binding = await session.get(TaskArtifact, ids["binding"])
        execution = await session.get(TaskExecution, ids["execution"])
        reservation = await session.get(ResourceReservation, ids["reservation"])
        event_count = int(
            await session.scalar(
                select(func.count(TaskEvent.id)).where(TaskEvent.task_id == ids["task"])
            )
            or 0
        )
        usage_count = int(
            await session.scalar(
                select(func.count(UsageLedger.id)).where(
                    UsageLedger.execution_id == ids["execution"]
                )
            )
            or 0
        )
        active_reservations = int(
            await session.scalar(
                select(func.count(ResourceReservation.id)).where(
                    ResourceReservation.project_id == ids["project"],
                    ResourceReservation.released_at.is_(None),
                )
            )
            or 0
        )
        schema_version = await session.scalar(text("SELECT version_num FROM alembic_version"))

    failures = []
    if project is None or project.slug != run_id:
        failures.append("project")
    if task is None or task.status is not TaskStatus.SUCCEEDED:
        failures.append("task")
    if execution is None or execution.status != TaskStatus.SUCCEEDED.value:
        failures.append("execution")
    if event_count != 1:
        failures.append("timeline")
    if usage_count != 1:
        failures.append("usage")
    if artifact is None or artifact.sha256 != digest or artifact.size_bytes != len(content):
        failures.append("artifact-metadata")
    if binding is None or binding.artifact_id != ids["artifact"]:
        failures.append("artifact-binding")
    if content != MARKER_BYTES:
        failures.append("artifact-content")
    if reservation is None or reservation.released_at is None or active_reservations != 0:
        failures.append("reservation-invariants")
    if not isinstance(schema_version, str) or not schema_version:
        failures.append("schema-version")
    if failures:
        raise RuntimeError(f"DR marker verification failed: {', '.join(failures)}")
    assert task is not None
    return {
        "run_id": run_id,
        "project_id": str(ids["project"]),
        "task_id": str(ids["task"]),
        "execution_id": str(ids["execution"]),
        "task_status": task.status.value,
        "timeline_events": event_count,
        "usage_rows": usage_count,
        "artifact_sha256": digest,
        "artifact_size": len(content),
        "schema_version": schema_version,
        "active_reservations": active_reservations,
    }


async def _run(mode: str, run_id: str) -> None:
    database_url = os.getenv("DATABASE_URL")
    artifact_root = os.getenv("ARTIFACT_LOCAL_ROOT")
    if not database_url or not artifact_root:
        raise RuntimeError("DATABASE_URL and ARTIFACT_LOCAL_ROOT are required")
    database = Database(database_url)
    try:
        if mode == "seed":
            result = await seed_marker(database, Path(artifact_root), run_id)
        else:
            result = await verify_marker(database, Path(artifact_root), run_id)
    finally:
        await database.dispose()
    print(json.dumps(result, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed or verify an isolated DR marker")
    parser.add_argument("mode", choices=("seed", "verify"))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    asyncio.run(_run(args.mode, args.run_id))


if __name__ == "__main__":
    main()
