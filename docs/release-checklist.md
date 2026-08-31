# Mini AI Cloud 0.6.0 release checklist

This checklist prepares and reviews a release. Completing it does not authorize a tag,
GitHub Release, deployment, or physical accelerator claim.

## Identity and compatibility

- [ ] `pyproject.toml`, `core/project_identity.py`, installed metadata, OpenAPI, README, Helm
  Chart/appVersion/default image tag, workflow, and lock file say `0.6.0` consistently.
- [ ] `python scripts/release_gate.py contracts` matches the reviewed OpenAPI and CLI v1
  snapshots.
- [ ] The deprecated `mini-docker-cloud` entry point still works and emits its warning.
- [ ] `CHANGELOG.md`, [Kubernetes adaptation](kubernetes-adaptation-v0.6.md), and generated
  release notes describe only committed behavior.
- [ ] [v0.6 release readiness](v0.6-release-readiness.md) and
  `contracts/release/v0.6-readiness.json` retain
  `READY_FOR_OWNER_AUTHORIZATION`, `PENDING_FINAL_P4_EVIDENCE`, `REAL_HW_NOT_RUN`,
  `NOT_TAGGED`, `NOT_RELEASED`, and `NOT_DEPLOYED` until their states actually change.

## Quality and safety

- [ ] `uv lock --check`, Ruff, mypy, pytest, Helm render, Docker configuration, wheel smoke,
  PostgreSQL integration, and container smoke pass for the release SHA.
- [ ] All third-party GitHub Actions use full immutable commit SHAs.
- [ ] Dependency Review, Gitleaks, and Trivy filesystem/container scans pass.
- [ ] CycloneDX and image SPDX SBOM artifacts are non-empty and bound to the release version
  and SHA.
- [ ] No credentials, kubeconfig, Secret values, database contents, or unrestricted environment
  dumps appear in logs, release assets, or evidence.

## P4 Kubernetes evidence

- [ ] `make test-kind-kubernetes-adaptation` creates a unique cluster, release, namespaces,
  run ID, and private kubeconfig without changing the default context.
- [ ] The printed `EVIDENCE_BUNDLE=<absolute-path>` belongs to the exact clean release SHA and
  pins Kind `v0.27.0`, Kubernetes `v1.32.2`, the exact Kind node image, application image,
  PostgreSQL image, Redis image, Chart version/appVersion, and Chart directory digest.
- [ ] All eleven P4 claims are `PASS` and each references successful commands tagged with the
  same claim ID. `NOT_RUN` is not accepted as `KIND_K8S_PASS`.
- [ ] `kubernetes-summary.json` contains safe, non-empty resource evidence from the isolated
  namespaces.
- [ ] Cleanup proves zero release-owned resources, preserved external Secret and namespaces,
  deleted cluster and temporary state, and an unchanged default kubeconfig.
- [ ] `checksums.txt` covers every P4 evidence file exactly once and verifies.
- [ ] `python scripts/release_gate.py validate --p4-evidence "$EVIDENCE_BUNDLE"` passes.

## Reliability and release assets

- [ ] Generic commit-bound evidence exists at `build/evidence/<release-sha>` and verifies.
- [ ] Bounded soak and isolated DR rehearsal run for the exact release SHA and clean up.
- [ ] `python scripts/release_gate.py prepare --p4-evidence "$EVIDENCE_BUNDLE"` copies the
  validated P4 bundle and produces a recursive `SHA256SUMS`.
- [ ] The release preparation manifest binds the exact SHA, Chart digest, P4 run ID, status,
  and hardware/deployment limitations.
- [ ] The downloaded wheel, SBOMs, evidence archive, nested checksums, and release notes are
  verified before a draft release can be published.

## Limitations and authorization

- [ ] README and release notes retain `REAL_HW_NOT_RUN` for real NVIDIA and Huawei Ascend.
- [ ] No production HA, multi-physical-node, universal hardware, SLA, complete
  Kubernetes-native platform, or production artifact-pipeline claim was added.
- [ ] A repository Owner explicitly authorizes the exact version and default-branch SHA only
  after reviewing CI and the P4 bundle.
- [ ] A separate explicit authorization exists before any production deployment.
