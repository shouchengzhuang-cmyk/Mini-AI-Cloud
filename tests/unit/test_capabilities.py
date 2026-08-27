import subprocess
from unittest.mock import Mock

import pytest

from worker import capabilities


def test_detect_gpus_parses_multiple_devices_and_sums_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=["nvidia-smi"],
        returncode=0,
        stdout=(
            "GPU-first, 0, NVIDIA RTX Test, 8192, 4096, 8.9\n"
            "GPU-second, 1, NVIDIA RTX Test, 16384, 12000, 8.9\n"
        ),
    )
    run = Mock(return_value=completed)
    monkeypatch.setattr(capabilities.subprocess, "run", run)

    assert capabilities.detect_gpus() == (2, "NVIDIA RTX Test", 24_576)
    run.assert_called_once_with(
        [
            "nvidia-smi",
            "--query-gpu=uuid,index,name,memory.total,memory.free,compute_cap",
            "--format=csv,noheader,nounits",
        ],
        check=False,
        capture_output=True,
        text=False,
        timeout=5.0,
    )


@pytest.mark.parametrize(
    "failure",
    [FileNotFoundError(), subprocess.TimeoutExpired(cmd="nvidia-smi", timeout=5)],
)
def test_detect_gpus_degrades_cleanly_when_probe_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    monkeypatch.setattr(capabilities.subprocess, "run", Mock(side_effect=failure))

    assert capabilities.detect_gpus() == (0, None, 0)


def test_detect_gpus_ignores_unparseable_output(monkeypatch: pytest.MonkeyPatch) -> None:
    completed = subprocess.CompletedProcess(
        args=["nvidia-smi"], returncode=0, stdout="not a valid csv row\n"
    )
    monkeypatch.setattr(capabilities.subprocess, "run", Mock(return_value=completed))

    assert capabilities.detect_gpus() == (0, None, 0)
