import json
import tomllib
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from cli import __main__ as cli

runner = CliRunner()
API_KEY = f"mkc_{'a' * 16}_{'B' * 43}"


def test_console_scripts_expose_canonical_and_compatibility_entrypoints() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    metadata = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    scripts = metadata["project"]["scripts"]
    assert scripts["mini-cloud"] == "cli.__main__:main"
    assert scripts["mini-docker-cloud"] == "cli.__main__:legacy_main"


def test_legacy_console_entrypoint_warns_and_invokes_cli(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "app", lambda: calls.append("called"))

    cli.legacy_main()

    captured = capsys.readouterr()
    assert calls == ["called"]
    assert "deprecated" in captured.err
    assert "mini-cloud" in captured.err


def test_http_client_automatically_loads_api_key(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"base_url": "https://control.example", "api_key": API_KEY}),
        encoding="utf-8",
    )
    monkeypatch.setenv("MINI_CLOUD_CONFIG", str(config))
    monkeypatch.delenv("MINI_CLOUD_URL", raising=False)
    monkeypatch.delenv("MINI_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("MINI_DOCKER_CLOUD_URL", raising=False)
    monkeypatch.delenv("MINI_DOCKER_CLOUD_API_KEY", raising=False)

    with cli._client() as client:
        assert str(client.base_url) == "https://control.example"
        assert client.headers["Authorization"] == f"Bearer {API_KEY}"


def test_auth_login_validates_then_writes_config_without_printing_token(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = tmp_path / "config.json"
    calls: list[tuple[str, str, dict[str, object]]] = []

    def request(method: str, path: str, **kwargs: object) -> object:
        calls.append((method, path, kwargs))
        return {"kind": "api_key"}

    monkeypatch.setenv("MINI_CLOUD_CONFIG", str(config))
    monkeypatch.setattr(cli, "_request", request)
    monkeypatch.setattr(cli, "_tighten_windows_acl", lambda _path: True)

    result = runner.invoke(
        cli.app,
        ["auth", "login", "--api-key", API_KEY, "--url", "https://control.example/"],
    )

    assert result.exit_code == 0
    assert API_KEY not in result.output
    assert calls == [
        (
            "GET",
            "/api/v1/auth/whoami",
            {"base_url": "https://control.example", "api_key": API_KEY},
        )
    ]
    saved = json.loads(config.read_text(encoding="utf-8"))
    assert saved == {"base_url": "https://control.example", "api_key": API_KEY}


def test_legacy_environment_variables_remain_compatible(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = tmp_path / "legacy-config.json"
    config.write_text(
        json.dumps({"base_url": "https://legacy.example", "api_key": API_KEY}),
        encoding="utf-8",
    )
    monkeypatch.delenv("MINI_CLOUD_CONFIG", raising=False)
    monkeypatch.delenv("MINI_CLOUD_URL", raising=False)
    monkeypatch.delenv("MINI_CLOUD_API_KEY", raising=False)
    monkeypatch.setenv("MINI_DOCKER_CLOUD_CONFIG", str(config))

    with cli._client() as client:
        assert str(client.base_url) == "https://legacy.example"
        assert client.headers["Authorization"] == f"Bearer {API_KEY}"


def test_legacy_default_config_path_remains_readable(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    legacy_config = tmp_path / "mini-docker-cloud" / "config.json"
    legacy_config.parent.mkdir()
    legacy_config.write_text(
        json.dumps({"base_url": "https://legacy-path.example", "api_key": API_KEY}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_config_root", lambda: tmp_path)
    for name in (
        "MINI_CLOUD_CONFIG",
        "MINI_CLOUD_URL",
        "MINI_CLOUD_API_KEY",
        "MINI_DOCKER_CLOUD_CONFIG",
        "MINI_DOCKER_CLOUD_URL",
        "MINI_DOCKER_CLOUD_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    with cli._client() as client:
        assert str(client.base_url) == "https://legacy-path.example"
        assert client.headers["Authorization"] == f"Bearer {API_KEY}"


def test_legacy_and_grouped_cli_commands_keep_expected_http_contracts(
    monkeypatch: Any,
) -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    def request(method: str, path: str, **kwargs: object) -> object:
        calls.append((method, path, kwargs))
        if path.endswith("/timeline"):
            return {"events": []}
        if path == "/api/v1/tasks/task-1":
            return {"id": "task-1", "status": "queued"}
        return {"ok": True}

    monkeypatch.setattr(cli, "_request", request)

    assert runner.invoke(cli.app, ["status", "task-1"]).exit_code == 0
    assert runner.invoke(cli.app, ["cancel", "task-1"]).exit_code == 0
    assert runner.invoke(cli.app, ["workers"]).exit_code == 0
    assert (
        runner.invoke(
            cli.app,
            ["task", "submit", "--image", "python:3.12", "--command", "python -V"],
        ).exit_code
        == 0
    )
    assert runner.invoke(cli.app, ["project", "list"]).exit_code == 0
    assert runner.invoke(cli.app, ["task", "list", "--status", "queued"]).exit_code == 0
    assert runner.invoke(cli.app, ["task", "logs", "task-1"]).exit_code == 0
    assert runner.invoke(cli.app, ["task", "cancel", "task-1"]).exit_code == 0
    assert runner.invoke(cli.app, ["worker", "list"]).exit_code == 0
    assert runner.invoke(cli.app, ["task", "explain", "task-1"]).exit_code == 0
    assert (
        runner.invoke(cli.app, ["service", "scale", "service-1", "--replicas", "2"]).exit_code == 0
    )
    assert runner.invoke(cli.app, ["service", "stop", "service-1"]).exit_code == 0

    assert [(method, path) for method, path, _kwargs in calls] == [
        ("GET", "/api/v1/tasks/task-1"),
        ("POST", "/api/v1/tasks/task-1/cancel"),
        ("GET", "/api/v1/workers"),
        ("POST", "/api/v1/tasks"),
        ("GET", "/api/v1/projects"),
        ("GET", "/api/v1/tasks"),
        ("GET", "/api/v1/tasks/task-1/logs"),
        ("POST", "/api/v1/tasks/task-1/cancel"),
        ("GET", "/api/v1/workers"),
        ("GET", "/api/v1/tasks/task-1"),
        ("GET", "/api/v1/tasks/task-1/scheduling"),
        ("GET", "/api/v1/tasks/task-1/timeline"),
        ("POST", "/api/v1/services/service-1/scale"),
        ("POST", "/api/v1/services/service-1/stop"),
    ]


def test_admin_doctor_is_read_only_by_default(
    monkeypatch: Any,
) -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    def request(method: str, path: str, **kwargs: object) -> object:
        calls.append((method, path, kwargs))
        return {"consistency": {"status": "incomplete"}}

    monkeypatch.setattr(cli, "_request", request)
    result = runner.invoke(cli.app, ["admin", "doctor"])

    assert result.exit_code == 0
    assert calls == [("GET", "/api/v1/admin/diagnostics", {"params": {"limit": 100}})]
    assert '"repair_request"' not in result.output


def test_service_deploy_can_use_registry_defaults_without_cli_shadowing(
    monkeypatch: Any,
) -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    def request(method: str, path: str, **kwargs: object) -> object:
        calls.append((method, path, kwargs))
        return {"ok": True}

    monkeypatch.setattr(cli, "_request", request)
    model_id = "10000000-0000-0000-0000-000000000099"

    result = runner.invoke(
        cli.app,
        [
            "service",
            "deploy",
            "--name",
            "registry-backed",
            "--registered-model-id",
            model_id,
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        (
            "POST",
            "/api/v1/services",
            {
                "json": {
                    "name": "registry-backed",
                    "registered_model_id": model_id,
                    "image": None,
                    "cpu_millicores": 1000,
                    "memory_mb": 1024,
                    "replicas": 1,
                }
            },
        )
    ]


def test_admin_doctor_repair_calls_only_the_conservative_repair_endpoint(
    monkeypatch: Any,
) -> None:
    calls: list[tuple[str, str, dict[str, object]]] = []

    def request(method: str, path: str, **kwargs: object) -> object:
        calls.append((method, path, kwargs))
        if method == "POST":
            return {
                "candidates_total": 0,
                "repaired_total": 0,
                "skipped_total": 0,
                "actions": [],
                "message": "No safe repair candidates were changed.",
            }
        return {"consistency": {"status": "incomplete"}}

    monkeypatch.setattr(cli, "_request", request)
    result = runner.invoke(cli.app, ["admin", "doctor", "--repair"])

    assert result.exit_code == 0
    assert calls == [
        ("POST", "/api/v1/admin/diagnostics/repair", {"params": {"limit": 100}}),
        ("GET", "/api/v1/admin/diagnostics", {"params": {"limit": 100}}),
    ]
    assert '"performed": false' in result.output
    assert "No safe repair candidates were changed." in result.output
