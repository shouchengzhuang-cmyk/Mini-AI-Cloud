from __future__ import annotations

import re

_DNS_1123_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")


def validate_kubernetes_dns_subdomain(value: str, *, field_name: str) -> str:
    """Return a canonical Kubernetes DNS-1123 subdomain or raise ``ValueError``."""

    labels = value.split(".") if isinstance(value, str) else []
    if (
        not value
        or value != value.strip()
        or len(value) > 253
        or any(not _DNS_1123_LABEL.fullmatch(label) for label in labels)
    ):
        raise ValueError(f"{field_name} must be a valid Kubernetes DNS-1123 subdomain")
    return value
