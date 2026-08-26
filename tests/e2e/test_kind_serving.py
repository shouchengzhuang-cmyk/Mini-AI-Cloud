import os

import pytest

from scripts.kind_serving_e2e import run_e2e_from_environment


@pytest.mark.e2e
async def test_real_kind_kubernetes_model_serving_lifecycle() -> None:
    if os.getenv("KIND_SERVING_E2E") != "1":
        pytest.skip(
            "NOT RUN: use `make kind-serving-up && make test-kind-serving`; "
            "the Make target fails non-zero when Docker, kind, or kubectl is unavailable"
        )

    await run_e2e_from_environment()
