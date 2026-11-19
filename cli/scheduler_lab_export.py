"""Offline CLI support for the GPU-Scheduler-Lab v2 handoff."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer

from core.database import Database
from core.scheduler_lab_export import SchedulerLabExportError, export_v2_from_session


def export_scheduler_lab_v2(
    output: Path = typer.Option(..., "--output", file_okay=True, dir_okay=False, writable=True),
    producer: str = typer.Option(
        ..., "--producer", help="Exact Mini producer identity, usually SHA-bound."
    ),
    database_url: str = typer.Option(..., "--database-url", envvar="MINI_CLOUD_DATABASE_URL"),
) -> None:
    """Export persisted Mini data as a deterministic Scheduler-Lab v2 JSON file."""

    try:
        payload = asyncio.run(_export(database_url=database_url, producer=producer))
    except SchedulerLabExportError as exc:
        typer.echo(f"v2 export refused: {exc}", err=True)
        raise typer.Exit(1) from exc
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"Scheduler-Lab v2 export: {output}")


async def _export(*, database_url: str, producer: str) -> dict[str, object]:
    database = Database(database_url)
    try:
        async with database.session() as session:
            return await export_v2_from_session(session, producer=producer)
    finally:
        await database.dispose()
