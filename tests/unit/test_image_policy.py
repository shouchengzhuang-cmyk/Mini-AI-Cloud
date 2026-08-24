import uuid

import pytest

from core.image_policy import (
    ImagePolicyAction,
    ImagePolicyConfig,
    ImageReferenceError,
    ImageRule,
    canonicalize_image_reference,
    evaluate_image_policy,
)


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("python:3.12-slim", "docker.io/library/python:3.12-slim"),
        ("org/image:v1", "docker.io/org/image:v1"),
        ("ghcr.io/example/model:V1", "ghcr.io/example/model:V1"),
        (
            "python:ignored@sha256:" + "A" * 64,
            "docker.io/library/python@sha256:" + "a" * 64,
        ),
        ("localhost:5000/team/image:v2", "localhost:5000/team/image:v2"),
    ],
)
def test_image_references_are_canonicalized(raw: str, canonical: str) -> None:
    assert canonicalize_image_reference(raw).canonical == canonical


@pytest.mark.parametrize(
    "image",
    [
        "python",
        "python:latest",
        "python:LATEST",
        "https://docker.io/library/python:3.12",
        "user:password@registry.example/repo:v1",
        "Registry.example/repo:v1",
        "registry.example/repo@sha256:abc",
    ],
)
def test_ambiguous_latest_and_malformed_images_are_rejected(image: str) -> None:
    with pytest.raises(ImageReferenceError):
        canonicalize_image_reference(image)


def test_digest_requirement_fails_closed_before_allow_rules() -> None:
    rule = ImageRule(
        action=ImagePolicyAction.ALLOW,
        registry="docker.io",
        repository_glob="library/python",
    )

    decision = evaluate_image_policy(
        "python:3.12",
        ImagePolicyConfig(default_action=ImagePolicyAction.ALLOW, require_digest=True),
        [rule],
    )

    assert decision.allowed is False
    assert decision.reason == "digest_required"


def test_ordered_allow_deny_rules_are_deterministic_and_deny_wins_ties() -> None:
    digest = "sha256:" + "a" * 64
    allow_id = uuid.uuid4()
    deny_id = uuid.uuid4()
    rules = [
        ImageRule(
            rule_id=allow_id,
            action=ImagePolicyAction.ALLOW,
            repository_glob="example/*",
            priority=10,
        ),
        ImageRule(
            rule_id=deny_id,
            action=ImagePolicyAction.DENY,
            repository_glob="example/model",
            digest=digest,
            priority=10,
        ),
    ]

    denied = evaluate_image_policy(
        f"docker.io/example/model@{digest}",
        ImagePolicyConfig(),
        rules,
    )
    unmatched = evaluate_image_policy(
        f"docker.io/other/model@{digest}",
        ImagePolicyConfig(),
        rules,
    )

    assert denied.allowed is False
    assert denied.matched_rule_id == deny_id
    assert unmatched.allowed is False
    assert unmatched.reason == "default_deny"
