import json
import os
import shlex
from typing import Annotated, Any

import httpx
import typer

app = typer.Typer(no_args_is_help=True, help="Submit and inspect distributed Docker tasks.")


def _base_url() -> str:
    return os.getenv("MINI_DOCKER_CLOUD_URL", "http://localhost:8000").rstrip("/")


def _mapping(values: list[str], option: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise typer.BadParameter(f"{option} values must use KEY=VALUE")
        result[key] = item
    return result


def _request(method: str, path: str, **kwargs: Any) -> object:
    try:
        response = httpx.request(method, f"{_base_url()}{path}", timeout=30, **kwargs)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        typer.echo(exc.response.text, err=True)
        raise typer.Exit(1) from exc
    except httpx.HTTPError as exc:
        typer.echo(f"request failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    return response.json()


def _print_json(value: object) -> None:
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2))


@app.command()
def submit(
    image: Annotated[str, typer.Option(help="Docker image reference")],
    command: Annotated[str, typer.Option(help="Container command, parsed with shlex")],
    environment: Annotated[
        list[str] | None, typer.Option("--env", help="Repeatable KEY=VALUE environment entry")
    ] = None,
    label: Annotated[
        list[str] | None, typer.Option("--label", help="Repeatable KEY=VALUE scheduling label")
    ] = None,
    timeout_seconds: Annotated[int, typer.Option(min=1)] = 60,
    max_retries: Annotated[int, typer.Option(min=0)] = 0,
    cpu_limit: Annotated[float, typer.Option(min=0.1)] = 1.0,
    memory_limit_mb: Annotated[int, typer.Option(min=16)] = 256,
    gpu_count: Annotated[int, typer.Option(min=0)] = 0,
    network_enabled: Annotated[bool, typer.Option()] = False,
    idempotency_key: Annotated[str | None, typer.Option()] = None,
) -> None:
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
    payload = {
        "image": image,
        "command": shlex.split(command),
        "environment": _mapping(environment or [], "--env"),
        "labels": _mapping(label or [], "--label"),
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "cpu_limit": cpu_limit,
        "memory_limit_mb": memory_limit_mb,
        "gpu_count": gpu_count,
        "network_enabled": network_enabled,
    }
    _print_json(_request("POST", "/api/v1/tasks", json=payload, headers=headers))


@app.command()
def status(task_id: str) -> None:
    _print_json(_request("GET", f"/api/v1/tasks/{task_id}"))


@app.command()
def logs(task_id: str, follow: Annotated[bool, typer.Option("--follow")] = False) -> None:
    if not follow:
        _print_json(_request("GET", f"/api/v1/tasks/{task_id}/logs"))
        return
    try:
        with httpx.stream(
            "GET", f"{_base_url()}/api/v1/tasks/{task_id}/logs/stream", timeout=None
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line.startswith("data: "):
                    typer.echo(line[6:])
    except httpx.HTTPError as exc:
        typer.echo(f"log stream failed: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command()
def cancel(task_id: str) -> None:
    _print_json(_request("POST", f"/api/v1/tasks/{task_id}/cancel"))


@app.command()
def workers() -> None:
    _print_json(_request("GET", "/api/v1/workers"))


if __name__ == "__main__":
    app()
