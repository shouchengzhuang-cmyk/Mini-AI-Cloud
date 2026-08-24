import pytest

from api.services.gateway import (
    _normalize_endpoint_allowlist,
    _validate_gateway_endpoint,
)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:8000",
        "http://10.20.30.40:8000",
        "http://[::1]:8000",
        "http://replica.compute.internal:8000",
    ],
)
def test_gateway_accepts_only_private_or_explicitly_allowlisted_endpoints(
    endpoint: str,
) -> None:
    allowlist = _normalize_endpoint_allowlist("*.compute.internal")
    _validate_gateway_endpoint(endpoint, host_allowlist=allowlist)


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://10.20.30.40:8000",
        "http://169.254.169.254/latest/meta-data",
        "http://8.8.8.8:8000",
        "http://public.example:8000",
        "http://user:password@127.0.0.1:8000",
        "http://127.0.0.1:8000/base",
        "http://127.0.0.1:8000?host=evil.example",
    ],
)
def test_gateway_rejects_unsafe_endpoint_shapes(endpoint: str) -> None:
    with pytest.raises(ValueError):
        _validate_gateway_endpoint(endpoint, host_allowlist=frozenset())
