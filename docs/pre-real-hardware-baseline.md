# Pre-real-hardware cross-repository baseline

Recorded: 2026-09-12.

This document freezes the software and evidence boundary that must be used before any real NVIDIA or Huawei Ascend experiment is allowed to make a hardware claim. It deliberately distinguishes released artifacts from later post-release governance, correctness, and security hardening on `main`.

## Released baselines

| Repository | Release | Exact release commit | Evidence class |
| --- | --- | --- | --- |
| `shouchengzhuang-cmyk/Mini-AI-Cloud` | `v0.6.0` | `ca0254230c988aef8327a3b078bc2fc86d95537e` | `KIND_K8S_PASS`; real hardware not run |
| `shouchengzhuang-cmyk/GPU-Scheduler-Lab` | `v0.4.1` | `fdd3afa0fca9cbfba3089292374c3baf5d64aaef` | deterministic `SIMULATED` study; real GPU/Kubernetes not run |

The Mini AI Cloud annotated `v0.6.0` tag resolves to the Mini commit above. The GPU Scheduler Lab annotated `v0.4.1` tag resolves to the Scheduler commit above. Scheduler `v0.4.1` is the post-PR #26 canonical-evidence refresh: its release workflow re-ran the canonical 180-run simulator study, accepted the generated bundle with `study verify`, and verified the published wheel and study-archive checksums. Later post-release governance, correctness, and security hardening on either repository does not mutate these immutable release baselines, and must not be attributed to the tagged releases.

## Mini AI Cloud release evidence boundary

Mini AI Cloud v0.6.0 publication was authorized through Issue #46 and executed by GitHub Actions run `33880679629`. The exact release candidate reran final P4 and produced:

- `KIND_K8S_PASS`;
- P4 run id `m7-20260904135447-113f0c3a`;
- exact release SHA `ca0254230c988aef8327a3b078bc2fc86d95537e`;
- explicit `REAL_HW_NOT_RUN`;
- release gate, three-round bounded soak, isolated DR, package smoke, scans, SBOM generation and release-asset checksum verification before publication.

The following remain outside that evidence boundary: real NVIDIA/vLLM, real Ascend/vLLM-Ascend, real non-Kind Kubernetes (`E1_NOT_RUN`), production deployment and production HA/SLA claims.

## Cross-repository contract state

GPU-Scheduler-Lab v0.4.1 defines these Mini-AI-Cloud-facing consumer contract identifiers:

- v1: `mini-ai-cloud.gpu-scheduler-lab/v1`;
- v2: `mini-ai-cloud.gpu-scheduler-lab/v2`;
- result handoff: `gpu-scheduler-lab.result/v1`.

Scheduler v0.4.1 retains both `contracts/mini-ai-cloud-v1.schema.json` and `contracts/mini-ai-cloud-v2.schema.json`, plus `tests/fixtures/mini_ai_cloud/v1-golden.json` and `tests/fixtures/mini_ai_cloud/v2-golden.json`. Its contract tests exercise the v2 golden fixture as a typed vendor/kind-aware consumer input. Its refreshed canonical report defines `p95_waiting_time` as queue delay from arrival/submission to first start; old p95 values are historical and are not numerically comparable as though that definition had not changed.

Mini AI Cloud v0.6.0 does **not** expose a matching `mini-ai-cloud.gpu-scheduler-lab/v2` export producer on the released baseline. Therefore the cross-repository producer-to-consumer v2 smoke is **X0_NOT_COMPLETE** (`BLOCKED_ON_MINI_V2_PRODUCER`). A Scheduler-side golden-fixture test must not be represented as proof that Mini v0.6.0 produces that contract.

## Mission closure and next-stage X0 prerequisite

The pre-real-hardware software and evidence mission is **MISSION_COMPLETE** when the two released baselines above, their recorded evidence boundaries, and the post-release remediation closure are present. That statement is deliberately narrower than a physical-accelerator claim.

The missing Mini v2 producer is **not** a Class A blocker for that completed software/evidence mission. It is a Class B prerequisite named **X0** for the *next* real-hardware cross-repository plan. X0 is distinct from the provider-neutral real-hardware evidence-plan stages G0a/G0b/G0c.

Status: **X0_NOT_COMPLETE** (`BLOCKED_ON_MINI_V2_PRODUCER`) for the next-stage handoff only.

Owner: `@shouchengzhuang-cmyk`.

X0 is complete only when all of the following are true on exact recorded commits:

1. Mini AI Cloud exposes or generates a deterministic v2 export whose `contract_version` is `mini-ai-cloud.gpu-scheduler-lab/v2`.
2. The exported worker/device/task fields satisfy GPU-Scheduler-Lab's `contracts/mini-ai-cloud-v2.schema.json`, including typed accelerator vendor/kind information required by the v2 consumer.
3. A producer-to-consumer smoke uses a Mini-generated v2 fixture or export and passes GPU-Scheduler-Lab's importer without hand editing.
4. The smoke records Mini SHA, Scheduler SHA, contract identifier, input hash and result hash.
5. Any failure is fail-closed; no fallback to v1 or synthetic data may be reported as a v2 pass.

Until X0 passes, real GPU experiments may validate Mini AI Cloud independently, but no cross-repository v2 integration claim is permitted. No real GPU, NPU, CUDA, NCCL, production Kubernetes, or deployment evidence is created by this software closure.

## Runtime image handoff for paid hardware

The release-security workflow builds and scans a `mini-ai-cloud:release-gate` candidate image and records its SBOM. That build is not a frozen runtime image for a later paid hardware session: its Python base tag and Debian security-package resolution may change. Before any paid real-hardware execution, build the intended image, record its actual immutable image digest in the run manifest and evidence bundle, and deploy that digest. A Git SHA or release tag alone is insufficient evidence of the executed image.

## Deferred Scheduler trace study

GPU-Scheduler-Lab Issue #23 remains a deliberate deferred research item, not a release blocker. It owns fresh verification of the full public Alibaba trace source/license/hash, bounded deterministic replay and the fairness-reporting correction inherited from closed PR #11. Its evidence remains simulation-only and cannot create a real GPU, Kubernetes, Alibaba-production or production-scheduler claim.

## Gate to the next plan

This record, the human-facing v0.6 readiness/verification documents, and the Scheduler v0.4.1 refresh together close the pre-real-hardware software/evidence mission. The pre-publication machine readiness contract remains valid under `scripts/release_gate.py validate`; it is not retroactively rewritten as a post-publication status record. The required execution order is `MISSION_COMPLETE` → X0 → real-hardware G0a/G0b/G0c → G1 → real GPU sessions. The next execution plan may begin only with X0; it must record the actual immutable image digest before any paid real-hardware session.
