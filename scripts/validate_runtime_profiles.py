from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml  # type: ignore[import-untyped]

from core.runtime_profiles import (
    RuntimeProfile,
    RuntimeProfileManifest,
    RuntimeProfileManifestEntry,
    generated_runtime_profile_schema,
)


class RuntimeProfileContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class LoadedRuntimeProfile:
    path: Path
    profile: RuntimeProfile


def _runtime_profile_root(repository_root: Path) -> Path:
    return repository_root / "runtime_profiles"


def load_profile(path: Path) -> RuntimeProfile:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeProfileContractError(f"{path}: expected a YAML mapping")
    return RuntimeProfile.model_validate(cast(dict[str, object], payload))


def load_profiles(repository_root: Path) -> tuple[LoadedRuntimeProfile, ...]:
    profile_root = _runtime_profile_root(repository_root)
    paths = sorted(profile_root.glob("*.yaml"))
    if not paths:
        raise RuntimeProfileContractError("no runtime profiles found")

    profiles = tuple(LoadedRuntimeProfile(path=path, profile=load_profile(path)) for path in paths)
    identities = [loaded.profile.identity for loaded in profiles]
    duplicates = sorted({identity for identity in identities if identities.count(identity) > 1})
    if duplicates:
        raise RuntimeProfileContractError(
            f"duplicate runtime profile identities: {', '.join(duplicates)}"
        )
    return profiles


def generated_manifest(
    profiles: tuple[LoadedRuntimeProfile, ...], repository_root: Path
) -> RuntimeProfileManifest:
    entries = tuple(
        RuntimeProfileManifestEntry(
            identity=loaded.profile.identity,
            profile_id=loaded.profile.id,
            profile_version=loaded.profile.version,
            path=loaded.path.relative_to(repository_root).as_posix(),
            semantic_digest=loaded.profile.semantic_digest(),
            vendor=loaded.profile.vendor,
            kind=loaded.profile.kind,
            engine=loaded.profile.engine,
            evidence_status=loaded.profile.evidence_status,
            hardware_families=loaded.profile.compatibility.hardware_families,
            model_architectures=loaded.profile.capabilities.model_architectures,
            dtypes=loaded.profile.capabilities.dtypes,
            features=loaded.profile.capabilities.features,
        )
        for loaded in sorted(profiles, key=lambda item: item.profile.identity)
    )
    return RuntimeProfileManifest(schema_version="1.0.0", profiles=entries)


def render_schema() -> str:
    return json.dumps(generated_runtime_profile_schema(), indent=2, sort_keys=True) + "\n"


def render_manifest(manifest: RuntimeProfileManifest) -> str:
    return json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


def validate_generated_files(
    profiles: tuple[LoadedRuntimeProfile, ...], repository_root: Path
) -> None:
    profile_root = _runtime_profile_root(repository_root)
    expected_schema = render_schema()
    expected_manifest = render_manifest(generated_manifest(profiles, repository_root))
    actual_schema = (profile_root / "schema.json").read_text(encoding="utf-8")
    actual_manifest = (profile_root / "manifest.json").read_text(encoding="utf-8")
    if actual_schema != expected_schema:
        raise RuntimeProfileContractError(
            "runtime_profiles/schema.json is stale; run with --write-generated"
        )
    if actual_manifest != expected_manifest:
        raise RuntimeProfileContractError(
            "runtime_profiles/manifest.json is stale; profile semantics changed without "
            "refreshing the digest manifest"
        )


def write_generated_files(
    profiles: tuple[LoadedRuntimeProfile, ...], repository_root: Path
) -> None:
    profile_root = _runtime_profile_root(repository_root)
    (profile_root / "schema.json").write_text(render_schema(), encoding="utf-8")
    manifest = generated_manifest(profiles, repository_root)
    (profile_root / "manifest.json").write_text(render_manifest(manifest), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate immutable runtime profile contracts")
    parser.add_argument("--root", type=Path, default=Path(__file__).parents[1])
    parser.add_argument("--write-generated", action="store_true")
    args = parser.parse_args(argv)

    repository_root = args.root.resolve()
    profiles = load_profiles(repository_root)
    if args.write_generated:
        write_generated_files(profiles, repository_root)
    validate_generated_files(profiles, repository_root)
    print(f"Validated {len(profiles)} immutable runtime profiles.")


if __name__ == "__main__":
    main()
