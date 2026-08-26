import json
import os
import re
import shlex
import stat
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, cast

import httpx
import typer

from cli.evidence import DeploymentStatus, EvidenceCollectionError, collect_evidence
from cli.hero_demo import HeroDemoError, ScenarioName, run_hero_scenarios

_DEFAULT_BASE_URL = "http://localhost:8000"
_API_KEY = re.compile(r"^mkc_[a-f0-9]{16}_[A-Za-z0-9_-]{43}$")
_CONFIG_ENV = "MINI_CLOUD_CONFIG"
_LEGACY_CONFIG_ENV = "MINI_DOCKER_CLOUD_CONFIG"
_URL_ENV = "MINI_CLOUD_URL"
_LEGACY_URL_ENV = "MINI_DOCKER_CLOUD_URL"
_API_KEY_ENV = "MINI_CLOUD_API_KEY"
_LEGACY_API_KEY_ENV = "MINI_DOCKER_CLOUD_API_KEY"

app = typer.Typer(
    no_args_is_help=True,
    help="Submit and inspect AI compute tasks and model services.",
)
auth_app = typer.Typer(no_args_is_help=True, help="Manage local CLI authentication.")
project_app = typer.Typer(no_args_is_help=True, help="Create and list projects.")
task_app = typer.Typer(no_args_is_help=True, help="Submit and inspect tasks.")
service_app = typer.Typer(no_args_is_help=True, help="Manage model services.")
admin_app = typer.Typer(no_args_is_help=True, help="Run admin diagnostics and safe repairs.")
worker_app = typer.Typer(no_args_is_help=True, help="Inspect and manage compute workers.")
demo_app = typer.Typer(no_args_is_help=True, help="Run evidence-producing hero scenarios.")
evidence_app = typer.Typer(no_args_is_help=True, help="Collect commit-bound evidence bundles.")
app.add_typer(auth_app, name="auth")
app.add_typer(project_app, name="project")
app.add_typer(task_app, name="task")
app.add_typer(service_app, name="service")
app.add_typer(admin_app, name="admin")
app.add_typer(worker_app, name="worker")
app.add_typer(demo_app, name="demo")
app.add_typer(evidence_app, name="evidence")


class CLIConfigError(RuntimeError):
    pass


def _environment_value(name: str, legacy_name: str) -> str | None:
    return os.getenv(name) or os.getenv(legacy_name)


def _config_root() -> Path:
    if os.name == "nt":
        return Path(os.getenv("APPDATA") or Path.home() / "AppData" / "Roaming")
    return Path(os.getenv("XDG_CONFIG_HOME") or Path.home() / ".config")


def _config_path() -> Path:
    override = _environment_value(_CONFIG_ENV, _LEGACY_CONFIG_ENV)
    if override:
        return Path(override).expanduser()
    return _config_root() / "mini-ai-cloud" / "config.json"


def _legacy_config_path() -> Path:
    return _config_root() / "mini-docker-cloud" / "config.json"


def _load_config() -> dict[str, str]:
    path = _config_path()
    if not _environment_value(_CONFIG_ENV, _LEGACY_CONFIG_ENV) and not path.exists():
        legacy_path = _legacy_config_path()
        if legacy_path.exists():
            path = legacy_path
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CLIConfigError(f"cannot read CLI config at {path}") from exc
    if not isinstance(value, dict):
        raise CLIConfigError(f"CLI config at {path} must contain a JSON object")
    result: dict[str, str] = {}
    for key in ("base_url", "api_key"):
        item = value.get(key)
        if item is not None and not isinstance(item, str):
            raise CLIConfigError(f"CLI config field {key!r} must be a string")
        if isinstance(item, str):
            result[key] = item
    return result


def _base_url() -> str:
    configured = _load_config()
    return (
        _environment_value(_URL_ENV, _LEGACY_URL_ENV)
        or configured.get("base_url", _DEFAULT_BASE_URL)
    ).rstrip("/")


def _configured_api_key() -> str | None:
    value = _environment_value(_API_KEY_ENV, _LEGACY_API_KEY_ENV) or _load_config().get("api_key")
    return value or None


def _mapping(values: list[str], option: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise typer.BadParameter(f"{option} values must use KEY=VALUE")
        result[key] = item
    return result


def _client(
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    timeout: float | None = 30.0,
) -> httpx.Client:
    headers = {}
    token = api_key if api_key is not None else _configured_api_key()
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        base_url=(base_url or _base_url()).rstrip("/"),
        headers=headers,
        timeout=timeout,
    )


def _request(
    method: str,
    path: str,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    **kwargs: Any,
) -> object:
    secret = api_key
    if secret is None:
        try:
            secret = _configured_api_key()
        except CLIConfigError:
            secret = None
    try:
        with _client(base_url=base_url, api_key=api_key) as client:
            response = client.request(method, path, **kwargs)
            response.raise_for_status()
            return response.json()
    except CLIConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except httpx.HTTPStatusError as exc:
        typer.echo(_redact(exc.response.text, secret), err=True)
        raise typer.Exit(1) from exc
    except httpx.HTTPError as exc:
        typer.echo(_redact(f"request failed: {exc}", secret), err=True)
        raise typer.Exit(1) from exc


def _print_json(value: object) -> None:
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2))


def _redact(value: str, secret: str | None) -> str:
    if not secret:
        return value
    return value.replace(secret, "[REDACTED]")


def _expect_mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        typer.echo(f"invalid {context} response", err=True)
        raise typer.Exit(1)
    return value


def _write_config(*, base_url: str, api_key: str) -> tuple[Path, bool]:
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        path.parent.chmod(0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                {"base_url": base_url.rstrip("/"), "api_key": api_key},
                handle,
                ensure_ascii=False,
                indent=2,
            )
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if os.name != "nt":
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        return path, True
    return path, _tighten_windows_acl(path)


def _tighten_windows_acl(path: Path) -> bool:
    username = os.getenv("USERNAME")
    if not username:
        return False
    domain = os.getenv("USERDOMAIN")
    account = f"{domain}\\{username}" if domain else username
    try:
        result = subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                f"{account}:(R,W)",
            ],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except OSError:
        return False
    return result.returncode == 0


@auth_app.command("login")
def auth_login(
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            help="API key; omit to use a hidden interactive prompt and avoid shell history.",
        ),
    ] = None,
    url: Annotated[str | None, typer.Option("--url", help="API base URL")] = None,
) -> None:
    token = api_key or typer.prompt("API key", hide_input=True)
    if not _API_KEY.fullmatch(token):
        raise typer.BadParameter("API key must be a valid mkc_ key", param_hint="--api-key")
    try:
        resolved_url = (url or _base_url()).rstrip("/")
    except CLIConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    _request(
        "GET",
        "/api/v1/auth/whoami",
        base_url=resolved_url,
        api_key=token,
    )
    path, tightened = _write_config(base_url=resolved_url, api_key=token)
    typer.echo(f"Authentication saved to {path}")
    if not tightened:
        typer.echo("warning: could not tighten the Windows ACL for the CLI config", err=True)


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


task_app.command("submit")(submit)


@app.command()
def status(task_id: str) -> None:
    _print_json(_request("GET", f"/api/v1/tasks/{task_id}"))


@app.command()
def logs(task_id: str, follow: Annotated[bool, typer.Option("--follow")] = False) -> None:
    if not follow:
        _print_json(_request("GET", f"/api/v1/tasks/{task_id}/logs"))
        return
    secret: str | None = None
    try:
        secret = _configured_api_key()
        with _client(api_key=secret, timeout=None) as client:
            with client.stream("GET", f"/api/v1/tasks/{task_id}/logs/stream") as response:
                response.raise_for_status()
                for line in response.iter_lines():
                    if line.startswith("data: "):
                        typer.echo(line[6:])
    except CLIConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except httpx.HTTPError as exc:
        typer.echo(_redact(f"log stream failed: {exc}", secret), err=True)
        raise typer.Exit(1) from exc


@app.command()
def cancel(task_id: str) -> None:
    _print_json(_request("POST", f"/api/v1/tasks/{task_id}/cancel"))


task_app.command("logs")(logs)
task_app.command("cancel")(cancel)


@app.command()
def workers() -> None:
    _print_json(_request("GET", "/api/v1/workers"))


worker_app.command("list")(workers)


@project_app.command("create")
def project_create(
    name: Annotated[str, typer.Option(help="Project display name")],
    slug: Annotated[str, typer.Option(help="Unique project slug")],
) -> None:
    _print_json(_request("POST", "/api/v1/projects", json={"name": name, "slug": slug}))


@project_app.command("list")
def project_list(
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 100,
    offset: Annotated[int, typer.Option(min=0)] = 0,
) -> None:
    _print_json(_request("GET", "/api/v1/projects", params={"limit": limit, "offset": offset}))


@task_app.command("list")
def task_list(
    task_status: Annotated[str | None, typer.Option("--status")] = None,
    worker_id: Annotated[str | None, typer.Option("--worker-id")] = None,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 100,
    offset: Annotated[int, typer.Option(min=0)] = 0,
) -> None:
    params: dict[str, object] = {"limit": limit, "offset": offset}
    if task_status is not None:
        params["status"] = task_status
    if worker_id is not None:
        params["worker_id"] = worker_id
    _print_json(_request("GET", "/api/v1/tasks", params=params))


@task_app.command("explain")
def task_explain(task_id: str) -> None:
    task = _expect_mapping(_request("GET", f"/api/v1/tasks/{task_id}"), "task")
    scheduling = _request("GET", f"/api/v1/tasks/{task_id}/scheduling")
    timeline = _request("GET", f"/api/v1/tasks/{task_id}/timeline")
    summary = {
        key: task.get(key)
        for key in (
            "id",
            "status",
            "worker_id",
            "retry_count",
            "max_retries",
            "recovery_count",
            "next_attempt_at",
            "lease_expires_at",
            "unschedulable_reason",
            "failure_category",
            "error_message",
        )
    }
    _print_json({"task": summary, "scheduler": scheduling, "timeline": timeline})


@service_app.command("deploy")
def service_deploy(
    name: Annotated[str, typer.Option(help="Service name")],
    model: Annotated[str | None, typer.Option(help="Model reference override")] = None,
    registered_model_id: Annotated[
        str | None, typer.Option(help="Project registered model UUID")
    ] = None,
    model_revision: Annotated[str | None, typer.Option()] = None,
    runtime: Annotated[str | None, typer.Option()] = None,
    runtime_type: Annotated[str | None, typer.Option()] = None,
    image: Annotated[str | None, typer.Option()] = None,
    cpu_millicores: Annotated[int, typer.Option(min=1)] = 1000,
    memory_mb: Annotated[int, typer.Option(min=16)] = 1024,
    gpu_count: Annotated[int | None, typer.Option(min=0)] = None,
    gpu_memory_mb: Annotated[int | None, typer.Option(min=0)] = None,
    gpu_model: Annotated[str | None, typer.Option()] = None,
    tensor_parallel_size: Annotated[int | None, typer.Option(min=1, max=64)] = None,
    dtype: Annotated[str | None, typer.Option()] = None,
    gpu_memory_utilization: Annotated[float | None, typer.Option(min=0.000001, max=1.0)] = None,
    max_model_len: Annotated[int | None, typer.Option(min=1)] = None,
    replicas: Annotated[int, typer.Option(min=0, max=1000)] = 1,
    autoscaling: Annotated[bool, typer.Option()] = False,
    min_replicas: Annotated[int, typer.Option(min=0, max=1000)] = 1,
    max_replicas: Annotated[int, typer.Option(min=1, max=1000)] = 4,
    target_concurrency: Annotated[int, typer.Option(min=1)] = 8,
    cooldown_seconds: Annotated[int, typer.Option(min=0)] = 60,
) -> None:
    if model is None and registered_model_id is None:
        raise typer.BadParameter("--model or --registered-model-id is required")
    payload: dict[str, object] = {
        "name": name,
        "image": image,
        "cpu_millicores": cpu_millicores,
        "memory_mb": memory_mb,
        "replicas": replicas,
    }
    optional_registry_overrides: dict[str, object | None] = {
        "model": model,
        "registered_model_id": registered_model_id,
        "model_revision": model_revision,
        "runtime": runtime,
        "runtime_type": runtime_type,
        "gpu_count": gpu_count,
        "gpu_memory_mb": gpu_memory_mb,
        "gpu_model": gpu_model,
        "dtype": dtype,
        "gpu_memory_utilization": gpu_memory_utilization,
    }
    payload.update(
        {key: value for key, value in optional_registry_overrides.items() if value is not None}
    )
    if tensor_parallel_size is not None:
        payload["tensor_parallel_size"] = tensor_parallel_size
    if max_model_len is not None:
        payload["max_model_len"] = max_model_len
    if autoscaling:
        payload["autoscaling"] = {
            "enabled": True,
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "target_concurrency": target_concurrency,
            "cooldown_seconds": cooldown_seconds,
        }
    _print_json(_request("POST", "/api/v1/services", json=payload))


@service_app.command("list")
def service_list(
    service_status: Annotated[str | None, typer.Option("--status")] = None,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 100,
    offset: Annotated[int, typer.Option(min=0)] = 0,
) -> None:
    params: dict[str, object] = {"limit": limit, "offset": offset}
    if service_status is not None:
        params["status"] = service_status
    _print_json(_request("GET", "/api/v1/services", params=params))


@service_app.command("scale")
def service_scale(
    service_id: str,
    replicas: Annotated[int, typer.Option(min=0, max=1000)],
) -> None:
    _print_json(
        _request(
            "POST",
            f"/api/v1/services/{service_id}/scale",
            json={"replicas": replicas},
        )
    )


@service_app.command("stop")
def service_stop(service_id: str) -> None:
    _print_json(_request("POST", f"/api/v1/services/{service_id}/stop"))


@app.command("usage")
def usage(
    project_id: Annotated[str | None, typer.Option("--project-id")] = None,
    from_time: Annotated[str | None, typer.Option("--from")] = None,
    to_time: Annotated[str | None, typer.Option("--to")] = None,
) -> None:
    end = _parse_timestamp(to_time, "--to") if to_time else datetime.now(UTC)
    start = _parse_timestamp(from_time, "--from") if from_time else end - timedelta(days=1)
    if end <= start:
        raise typer.BadParameter("--to must be after --from")
    resolved_project_id = project_id
    if resolved_project_id is None:
        project = _expect_mapping(_request("GET", "/api/v1/projects/current"), "project")
        value = project.get("id")
        if not isinstance(value, str):
            typer.echo("current project response has no project id", err=True)
            raise typer.Exit(1)
        resolved_project_id = value
    _print_json(
        _request(
            "GET",
            f"/api/v1/projects/{resolved_project_id}/usage",
            params={"from": start.isoformat(), "to": end.isoformat()},
        )
    )


def _parse_timestamp(value: str, option: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise typer.BadParameter(f"{option} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise typer.BadParameter(f"{option} must include a timezone")
    return parsed.astimezone(UTC)


@admin_app.command("doctor")
def admin_doctor(
    repair: Annotated[bool, typer.Option("--repair")] = False,
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
) -> None:
    repair_result: dict[str, object] | None = None
    if repair:
        repair_result = _expect_mapping(
            _request(
                "POST",
                "/api/v1/admin/diagnostics/repair",
                params={"limit": limit},
            ),
            "diagnostics repair",
        )
    diagnostic = _expect_mapping(
        _request("GET", "/api/v1/admin/diagnostics", params={"limit": limit}),
        "diagnostics",
    )
    if repair_result is not None:
        repaired_total = repair_result.get("repaired_total")
        diagnostic["repair_request"] = {
            "requested": True,
            "performed": (
                isinstance(repaired_total, int)
                and not isinstance(repaired_total, bool)
                and repaired_total > 0
            ),
            "result": repair_result,
        }
    _print_json(diagnostic)


def _run_hero_demo(names: tuple[ScenarioName, ...], output_dir: Path) -> None:
    try:
        run_hero_scenarios(names, output_dir)
    except HeroDemoError as exc:
        if exc.artifact_dir is not None:
            typer.echo(f"Diagnostics retained at {exc.artifact_dir}", err=True)
        raise typer.Exit(1) from exc


@demo_app.command("fencing")
def demo_fencing(
    output_dir: Annotated[Path, typer.Option(help="Evidence artifact root")] = Path(
        "build/hero-demo"
    ),
) -> None:
    _run_hero_demo(("fencing",), output_dir)


@demo_app.command("controller-adoption")
def demo_controller_adoption(
    output_dir: Annotated[Path, typer.Option(help="Evidence artifact root")] = Path(
        "build/hero-demo"
    ),
) -> None:
    _run_hero_demo(("controller-adoption",), output_dir)


@demo_app.command("sse-drain")
def demo_sse_drain(
    output_dir: Annotated[Path, typer.Option(help="Evidence artifact root")] = Path(
        "build/hero-demo"
    ),
) -> None:
    _run_hero_demo(("sse-drain",), output_dir)


@demo_app.command("all")
def demo_all(
    output_dir: Annotated[Path, typer.Option(help="Evidence artifact root")] = Path(
        "build/hero-demo"
    ),
) -> None:
    _run_hero_demo(("fencing", "controller-adoption", "sse-drain"), output_dir)


@evidence_app.command("collect")
def evidence_collect(
    output_dir: Annotated[Path, typer.Option(help="Evidence bundle root")] = Path("build/evidence"),
    allow_dirty: Annotated[
        bool,
        typer.Option(
            "--allow-dirty",
            help="Permit non-release evidence from a dirty tree and mark it explicitly.",
        ),
    ] = False,
    deployment_status: Annotated[
        str,
        typer.Option(help="Deployment state: NOT_DEPLOYED or UNKNOWN"),
    ] = "NOT_DEPLOYED",
) -> None:
    if deployment_status not in {"NOT_DEPLOYED", "UNKNOWN"}:
        raise typer.BadParameter(
            "deployment status must be NOT_DEPLOYED or UNKNOWN", param_hint="--deployment-status"
        )
    try:
        bundle = collect_evidence(
            output_dir,
            allow_dirty=allow_dirty,
            deployment_status=cast(DeploymentStatus, deployment_status),
        )
    except EvidenceCollectionError as exc:
        typer.echo(f"evidence collection failed: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Evidence bundle: {bundle.path}")


def main() -> None:
    app()


def legacy_main() -> None:
    typer.echo(
        "warning: 'mini-docker-cloud' is deprecated; use 'mini-cloud' instead.",
        err=True,
    )
    app()


if __name__ == "__main__":
    main()
