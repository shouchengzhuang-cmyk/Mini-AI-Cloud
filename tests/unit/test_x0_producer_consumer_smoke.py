from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import venv
from pathlib import Path

import pytest

from scripts.x0_producer_consumer_smoke import CONTRACT_VERSION, MINI_REPO, SCHEDULER_REPO

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "x0_producer_consumer_smoke.py"
IMPORTER = """\
import sys
from pathlib import Path

assert sys.argv[1] == "import-mini-ai-cloud"
source = Path(sys.argv[sys.argv.index("--input") + 1])
output = Path(sys.argv[sys.argv.index("--output") + 1])
output.write_bytes(source.read_bytes())
"""


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(root: Path) -> str:
    _git(root, "add", ".")
    _git(
        root,
        "-c",
        "user.name=X0 Test",
        "-c",
        "user.email=x0@example.invalid",
        "-c",
        "commit.gpgSign=false",
        "commit",
        "-m",
        "fixture",
    )
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def smoke_args(tmp_path: Path) -> dict[str, str]:
    mini = tmp_path / "mini"
    scheduler = tmp_path / "scheduler"
    for root, repository in ((mini, MINI_REPO), (scheduler, SCHEDULER_REPO)):
        root.mkdir()
        _git(root, "init")
        _git(root, "remote", "add", "origin", f"https://github.com/{repository}.git")

    script = mini / "scripts" / SCRIPT.name
    script.parent.mkdir()
    script.write_bytes(SCRIPT.read_bytes())
    package = scheduler / "gpu_scheduler_lab"
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "__main__.py").write_text(IMPORTER, encoding="utf-8")
    mini_sha = _commit(mini)
    scheduler_sha = _commit(scheduler)
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps({"contract_version": CONTRACT_VERSION, "producer": f"mini-ai-cloud/{mini_sha}"}),
        encoding="utf-8",
    )
    return {
        "script": str(script),
        "input": str(source),
        "output": str(tmp_path / "outputs" / "scenario.json"),
        "report": str(tmp_path / "outputs" / "report.json"),
        "mini-sha": mini_sha,
        "scheduler-sha": scheduler_sha,
        "scheduler-repo": str(scheduler),
    }


def _run(options: dict[str, str]) -> subprocess.CompletedProcess[str]:
    args = [sys.executable, options["script"]]
    for name, value in options.items():
        if name != "script":
            args.extend([f"--{name}", value])
    return subprocess.run(args, check=False, capture_output=True, text=True)


def _assert_refused(options: dict[str, str], match: str) -> None:
    result = _run(options)
    assert result.returncode != 0
    assert match in result.stderr
    assert not Path(options["output"]).parent.exists()


def test_reports_verified_heads_and_artifact_hashes(smoke_args: dict[str, str]) -> None:
    result = _run(smoke_args)
    assert result.returncode == 0, result.stderr
    source = Path(smoke_args["input"]).read_bytes()
    output = Path(smoke_args["output"]).read_bytes()
    report = json.loads(Path(smoke_args["report"]).read_text(encoding="utf-8"))
    assert report == {
        "mini_sha": smoke_args["mini-sha"],
        "scheduler_sha": smoke_args["scheduler-sha"],
        "contract_version": CONTRACT_VERSION,
        "input_sha256": hashlib.sha256(source).hexdigest(),
        "result_sha256": hashlib.sha256(output).hexdigest(),
    }


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("mini-sha", "a" * 40, f"{MINI_REPO} HEAD is"),
        ("scheduler-sha", "a" * 40, f"{SCHEDULER_REPO} HEAD is"),
        ("mini-sha", "abc", "mini_sha must be a 40-character Git SHA"),
    ],
)
def test_rejects_invalid_heads(
    smoke_args: dict[str, str], option: str, value: str, message: str
) -> None:
    smoke_args[option] = value
    _assert_refused(smoke_args, message)


def test_rejects_wrong_origin(smoke_args: dict[str, str]) -> None:
    root = Path(smoke_args["scheduler-repo"])
    _git(root, "remote", "set-url", "origin", "https://github.com/example/GPU-Scheduler-Lab.git")
    _assert_refused(smoke_args, f"origin must identify {SCHEDULER_REPO}")


def test_rejects_wrong_producer(smoke_args: dict[str, str]) -> None:
    Path(smoke_args["input"]).write_text(
        json.dumps({"contract_version": CONTRACT_VERSION, "producer": "mini-ai-cloud/unknown"}),
        encoding="utf-8",
    )
    _assert_refused(smoke_args, "input producer must match the verified Mini HEAD")


@pytest.mark.parametrize("repository", [MINI_REPO, SCHEDULER_REPO])
def test_rejects_dirty_checkouts(smoke_args: dict[str, str], repository: str) -> None:
    root = (
        Path(smoke_args["script"]).parents[1]
        if repository == MINI_REPO
        else Path(smoke_args["scheduler-repo"])
    )
    (root / "untracked.txt").touch()
    _assert_refused(smoke_args, f"{repository} working tree must be clean")


def test_rejects_importer_outside_checkout(smoke_args: dict[str, str], tmp_path: Path) -> None:
    root = Path(smoke_args["scheduler-repo"])
    _git(root, "mv", "gpu_scheduler_lab", "unrelated")
    smoke_args["scheduler-sha"] = _commit(root)

    env_dir = tmp_path / "external"
    venv.EnvBuilder(with_pip=False).create(env_dir)
    python = env_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site_packages = subprocess.run(
        [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    foreign_package = Path(site_packages) / "gpu_scheduler_lab"
    foreign_package.mkdir()
    (foreign_package / "__init__.py").touch()
    (foreign_package / "__main__.py").write_text(IMPORTER, encoding="utf-8")
    smoke_args["scheduler-python"] = str(python)
    _assert_refused(smoke_args, "does not resolve to the verified checkout")


@pytest.mark.parametrize(
    ("left", "right"), [("input", "output"), ("input", "report"), ("output", "report")]
)
def test_rejects_each_path_collision(smoke_args: dict[str, str], left: str, right: str) -> None:
    source = Path(smoke_args["input"])
    original = source.read_bytes()
    smoke_args[right] = smoke_args[left]
    result = _run(smoke_args)
    assert result.returncode != 0
    assert f"{left} and {right} artifact paths must be distinct" in result.stderr
    assert source.read_bytes() == original
    assert not (source.parent / "outputs").exists()


@pytest.mark.parametrize("alias_type", ["hardlink", "symlink"])
def test_rejects_file_aliases(smoke_args: dict[str, str], tmp_path: Path, alias_type: str) -> None:
    source = Path(smoke_args["input"])
    original = source.read_bytes()
    alias = tmp_path / "alias.json"
    if alias_type == "hardlink":
        os.link(source, alias)
    else:
        try:
            alias.symlink_to(source)
        except OSError as exc:
            pytest.skip(f"symlinks unavailable: {exc}")
    smoke_args["report"] = str(alias)
    _assert_refused(smoke_args, "input and report artifact paths must be distinct")
    assert source.read_bytes() == original
