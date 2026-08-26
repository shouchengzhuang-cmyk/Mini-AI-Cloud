# Mini AI Cloud 0.4.0 release checklist

This checklist prepares a release. Completing it does not authorize a GitHub Release or a
deployment.

## Identity and compatibility

- [ ] `pyproject.toml`, installed metadata, API OpenAPI version, README, CLI and image labels say
  `mini-ai-cloud` / `mini-cloud` / `0.4.0` consistently.
- [ ] `python scripts/release_gate.py contracts` matches reviewed OpenAPI and CLI v1 snapshots.
- [ ] The deprecated `mini-docker-cloud` entry point still works and emits its warning.
- [ ] `CHANGELOG.md` and generated release notes describe only committed changes.

## Quality and safety

- [ ] `uv lock --check`, Ruff, mypy and pytest pass.
- [ ] PostgreSQL integration, Docker configuration, wheel install and container smoke pass.
- [ ] Real Kind serving acceptance passes and cleans its dedicated cluster and credentials.
- [ ] CI/default Kind runs keep `KIND_SERVING_PULL=true`; an explicitly offline local rerun may use
  `KIND_SERVING_PULL=false` only after the fixed base, PostgreSQL and Redis images are cached.
- [ ] All third-party GitHub Actions use full immutable commit SHAs.
- [ ] Dependency Review, Gitleaks and Trivy filesystem/container scans pass.
- [ ] CycloneDX and image SPDX SBOM artifacts are non-empty and retained.
- [ ] No credentials, private keys, kubeconfig, database contents, or environment dumps are in the
  release/evidence bundles.

## Evidence and reliability

- [ ] Evidence bundle manifest binds the exact clean Git SHA and says `NOT_DEPLOYED`.
- [ ] Hero Scenario claims match `evidence/claims.yaml` and `docs/verification-matrix.yaml`.
- [ ] A bounded soak was actually run for the release SHA, or is explicitly `NOT RUN`.
- [ ] Destructive DR rehearsal was actually run for the release SHA, restored the marker exactly,
  and left no project-labeled resources.
- [ ] Generated release-preparation manifest and `SHA256SUMS` verify.

## Limitations and authorization

- [ ] README and release notes retain **NOT RUN** for real NVIDIA GPU acceptance when applicable.
- [ ] No production HA, multi-physical-node, or managed-cloud claim was added.
- [ ] `docs/comparison.md` remains a responsibility comparison, not an unmeasured superiority claim.
- [ ] A human explicitly authorizes the tag/GitHub Release after reviewing CI and artifacts.
- [ ] A separate explicit authorization exists before any deployment.
