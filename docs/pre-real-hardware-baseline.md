# Pre-real-hardware cross-repository baseline

Recorded: 2026-09-04.

This document freezes the software and evidence boundary that must be used before any real NVIDIA or Huawei Ascend experiment is allowed to make a hardware claim. It deliberately distinguishes released artifacts from later documentation-only governance commits.

## Released baselines

| Repository | Release | Exact release commit | Evidence class |
| --- | --- | --- | --- |
| `shouchengzhuang-cmyk/Mini-AI-Cloud` | `v0.6.0` | `ca0254230c988aef8327a3b078bc2fc86d95537e` | `KIND_K8S_PASS`; real hardware not run |
| `shouchengzhuang-cmyk/GPU-Scheduler-Lab` | `v0.4.0` | `99a5718da4751a01c090d13e73d1d982c1fc0e64` | deterministic `SIMULATED` study; real GPU/Kubernetes not run |

The Mini AI Cloud annotated `v0.6.0` tag resolves to the Mini commit above. The GPU Scheduler Lab annotated `v0.4.0` tag resolves to the Scheduler commit above. Post-release documentation commits on either repository do not mutate these baselines.

## Mini AI Cloud release evidence boundary

Mini AI Cloud v0.6.0 publication was authorized through Issue #46 and executed by GitHub Actions run `33880679629`. The exact release candidate reran final P4 and produced:

- `KIND_K8S_PASS`;
- P4 run id `m7-20260904135447-113f0c3a`;
- exact release SHA `ca0254230c988aef8327a3b078bc2fc86d95537e`;
- explicit `REAL_HW_NOT_RUN`;
- release gate, three-round bounded soak, isolated DR, package smoke, scans, SBOM generation and release-asset checksum verification before publication.

The following remain outside that evidence boundary: real NVIDIA/vLLM, real Ascend/vLLM-Ascend, real non-Kind Kubernetes (`E1_NOT_RUN`), production deployment and production HA/SLA claims.

## Cross-repository contract state

GPU-Scheduler-Lab v0.4.0 defines these Mini-AI-Cloud-facing consumer contract identifiers:

- v1: `mini-ai-cloud.gpu-scheduler-lab/v1`;
- v2: `mini-ai-cloud.gpu-scheduler-lab/v2`;
- result handoff: `gpu-scheduler-lab.result/v1`.

Scheduler v0.4.0 retains both `contracts/mini-ai-cloud-v1.schema.json` and `contracts/mini-ai-cloud-v2.schema.json`, plus `tests/fixtures/mini_ai_cloud/v1-golden.json` and `tests/fixtures/mini_ai_cloud/v2-golden.json`. Its contract tests exercise the v2 golden fixture as a typed vendor/kind-aware consumer input.

Mini AI Cloud v0.6.0 does **not** expose a matching `mini-ai-cloud.gpu-scheduler-lab/v2` export producer on the released baseline. Therefore the cross-repository producer-to-consumer v2 smoke is **NOT_COMPLETE**. A Scheduler-side golden-fixture test must not be represented as proof that Mini v0.6.0 produces that contract.

## G0 blocker before real GPU work

Status: **BLOCKED_ON_MINI_V2_PRODUCER**.

Owner: `@shouchengzhuang-cmyk`.

G0 is complete only when all of the following are true on exact recorded commits:

1. Mini AI Cloud exposes or generates a deterministic v2 export whose `contract_version` is `mini-ai-cloud.gpu-scheduler-lab/v2`.
2. The exported worker/device/task fields satisfy GPU-Scheduler-Lab's `contracts/mini-ai-cloud-v2.schema.json`, including typed accelerator vendor/kind information required by the v2 consumer.
3. A producer-to-consumer smoke uses a Mini-generated v2 fixture or export and passes GPU-Scheduler-Lab's importer without hand editing.
4. The smoke records Mini SHA, Scheduler SHA, contract identifier, input hash and result hash.
5. Any failure is fail-closed; no fallback to v1 or synthetic data may be reported as a v2 pass.

Until G0 passes, real GPU experiments may validate Mini AI Cloud independently, but no cross-repository v2 integration claim is permitted.

## Deferred Scheduler trace study

GPU-Scheduler-Lab Issue #23 remains a deliberate deferred research item, not a release blocker. It owns fresh verification of the full public Alibaba trace source/license/hash, bounded deterministic replay and the fairness-reporting correction inherited from closed PR #11. Its evidence remains simulation-only and cannot create a real GPU, Kubernetes, Alibaba-production or production-scheduler claim.

## Gate to the next plan

The software release closure is complete when this record and the human-facing v0.6 readiness/verification documents are merged, the pre-publication machine readiness contract remains valid under `scripts/release_gate.py validate`, and stale M7-0 governance is closed. The next execution plan may then be the real-hardware plan, with G0 above as its first cross-repository prerequisite.
