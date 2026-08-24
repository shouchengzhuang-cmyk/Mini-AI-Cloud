import fnmatch
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

_DIGEST = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
_TAG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")
_REPOSITORY_SEGMENT = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*$")
_RULE_GLOB = re.compile(r"^[a-z0-9*?._/!\[\]-]+$")
_MAX_IMAGE_LENGTH = 512


class ImageReferenceError(ValueError):
    pass


class ImagePolicyAction(StrEnum):
    ALLOW = "allow"
    DENY = "deny"


@dataclass(frozen=True, slots=True)
class ImageReference:
    original: str
    canonical: str
    registry: str
    repository: str
    tag: str | None
    digest: str | None


@dataclass(frozen=True, slots=True)
class ImageRule:
    action: ImagePolicyAction
    repository_glob: str
    priority: int = 100
    registry: str | None = None
    tag_glob: str | None = None
    digest: str | None = None
    rule_id: uuid.UUID | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.priority <= 1_000_000:
            raise ValueError("image rule priority must be between 0 and 1000000")
        normalized_registry = (
            _canonical_registry(self.registry) if self.registry is not None else None
        )
        normalized_repository_glob = self.repository_glob.strip().casefold()
        if (
            not normalized_repository_glob
            or not _RULE_GLOB.fullmatch(normalized_repository_glob)
            or normalized_repository_glob.startswith("/")
            or "//" in normalized_repository_glob
        ):
            raise ValueError("repository_glob is malformed")
        normalized_tag_glob = self.tag_glob.strip() if self.tag_glob is not None else None
        if normalized_tag_glob is not None and (
            not normalized_tag_glob
            or len(normalized_tag_glob) > 128
            or any(character.isspace() or ord(character) < 32 for character in normalized_tag_glob)
        ):
            raise ValueError("tag_glob is malformed")
        normalized_digest = self.digest.casefold() if self.digest is not None else None
        if normalized_digest is not None and not _DIGEST.fullmatch(normalized_digest):
            raise ValueError("image rule digest must be a sha256 digest")
        object.__setattr__(self, "registry", normalized_registry)
        object.__setattr__(self, "repository_glob", normalized_repository_glob)
        object.__setattr__(self, "tag_glob", normalized_tag_glob)
        object.__setattr__(self, "digest", normalized_digest)


@dataclass(frozen=True, slots=True)
class ImagePolicyConfig:
    default_action: ImagePolicyAction = ImagePolicyAction.DENY
    require_digest: bool = True


@dataclass(frozen=True, slots=True)
class ImagePolicyDecision:
    allowed: bool
    canonical_image: str
    reason: str
    matched_rule_id: uuid.UUID | None = None


def canonicalize_image_reference(value: str) -> ImageReference:
    original = value
    reference = value.strip()
    if (
        not reference
        or len(reference) > _MAX_IMAGE_LENGTH
        or any(character.isspace() or ord(character) < 32 for character in reference)
        or any(marker in reference for marker in ("://", "\\", "?", "#"))
    ):
        raise ImageReferenceError("image reference is malformed")
    if reference.count("@") > 1:
        raise ImageReferenceError("image reference contains multiple digests")

    name_and_tag, separator, raw_digest = reference.partition("@")
    digest = raw_digest.casefold() if separator else None
    if digest is not None and not _DIGEST.fullmatch(digest):
        raise ImageReferenceError("image digest must be sha256 followed by 64 hex characters")

    last_slash = name_and_tag.rfind("/")
    last_colon = name_and_tag.rfind(":")
    if last_colon > last_slash:
        raw_name = name_and_tag[:last_colon]
        tag = name_and_tag[last_colon + 1 :]
        if not _TAG.fullmatch(tag):
            raise ImageReferenceError("image tag is malformed")
    else:
        raw_name = name_and_tag
        tag = None
    if tag is not None and tag.casefold() == "latest":
        raise ImageReferenceError("the latest image tag is not allowed")
    if tag is None and digest is None:
        raise ImageReferenceError("an explicit non-latest tag or sha256 digest is required")

    registry, repository = _split_name(raw_name)
    if digest is not None:
        canonical = f"{registry}/{repository}@{digest}"
        tag = None
    else:
        canonical = f"{registry}/{repository}:{tag}"
    return ImageReference(
        original=original,
        canonical=canonical,
        registry=registry,
        repository=repository,
        tag=tag,
        digest=digest,
    )


def evaluate_image_policy(
    image: str,
    policy: ImagePolicyConfig,
    rules: Iterable[ImageRule],
) -> ImagePolicyDecision:
    reference = canonicalize_image_reference(image)
    if policy.require_digest and reference.digest is None:
        return ImagePolicyDecision(
            allowed=False,
            canonical_image=reference.canonical,
            reason="digest_required",
        )
    ordered_rules = sorted(
        rules,
        key=lambda rule: (
            rule.priority,
            0 if rule.action == ImagePolicyAction.DENY else 1,
            str(rule.rule_id or ""),
        ),
    )
    for rule in ordered_rules:
        if _rule_matches(rule, reference):
            return ImagePolicyDecision(
                allowed=rule.action == ImagePolicyAction.ALLOW,
                canonical_image=reference.canonical,
                reason=f"rule_{rule.action.value}",
                matched_rule_id=rule.rule_id,
            )
    return ImagePolicyDecision(
        allowed=policy.default_action == ImagePolicyAction.ALLOW,
        canonical_image=reference.canonical,
        reason=f"default_{policy.default_action.value}",
    )


def _rule_matches(rule: ImageRule, reference: ImageReference) -> bool:
    if rule.registry is not None and rule.registry != reference.registry:
        return False
    if not fnmatch.fnmatchcase(reference.repository, rule.repository_glob):
        return False
    if rule.tag_glob is not None and (
        reference.tag is None or not fnmatch.fnmatchcase(reference.tag, rule.tag_glob)
    ):
        return False
    return rule.digest is None or rule.digest == reference.digest


def _split_name(raw_name: str) -> tuple[str, str]:
    if not raw_name or raw_name != raw_name.casefold() or raw_name.startswith("/"):
        raise ImageReferenceError("image repository must be lowercase")
    components = raw_name.split("/")
    if any(not component for component in components):
        raise ImageReferenceError("image repository is malformed")
    first = components[0]
    if "." in first or ":" in first or first == "localhost":
        registry = _canonical_registry(first)
        repository_parts = components[1:]
    else:
        registry = "docker.io"
        repository_parts = components
    if not repository_parts:
        raise ImageReferenceError("image repository is missing")
    if registry == "docker.io" and len(repository_parts) == 1:
        repository_parts.insert(0, "library")
    if any(not _REPOSITORY_SEGMENT.fullmatch(part) for part in repository_parts):
        raise ImageReferenceError("image repository is malformed")
    return registry, "/".join(repository_parts)


def _canonical_registry(value: str) -> str:
    registry = value.strip().casefold()
    if not registry or "/" in registry or "@" in registry or registry.startswith("."):
        raise ValueError("image registry is malformed")
    host, separator, raw_port = registry.rpartition(":")
    if separator:
        if not host or not raw_port.isdigit() or not 1 <= int(raw_port) <= 65_535:
            raise ValueError("image registry port is malformed")
        registry_host = host
    else:
        registry_host = registry
    if registry_host != "localhost":
        labels = registry_host.split(".")
        if any(
            not label
            or len(label) > 63
            or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label)
            for label in labels
        ):
            raise ValueError("image registry is malformed")
    return registry
