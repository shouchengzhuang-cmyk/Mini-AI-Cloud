# NVIDIA + Huawei Ascend dual-backend benchmark

This harness sends the same frozen prompt set to exactly one NVIDIA backend and one Huawei
Ascend backend. Warmup observations are kept separate from measured observations. Both buffered
and SSE requests validate OpenAI-compatible protocol shape, semantic sentinels, and internally
consistent usage fields.

The harness output is deliberately conservative. A completed HTTP run is labelled
`RUN_COMPLETED_UNVERIFIED`; it does not become real-hardware evidence automatically. Hardware
identity, runtime versions, device diagnostics, immutable commit, and raw artifacts still require
review before any external claim changes. The checked-in evidence remains `REAL_HW_NOT_RUN`.

## Run

Copy `benchmarks/config.example.json`, set endpoint/model values, and export only the named API-key
environment variables. Then run:

```bash
uv run python -m benchmarks.dual_backend \
  --config /path/to/dual-backend.json \
  --output /path/to/report.json
```

The measured phase is accepted only when every endpoint receives every prompt for every measured
iteration, protocol and semantic validation pass, streaming reaches `[DONE]`, and reported usage
is non-negative and internally consistent. Warmup failures remain visible but never enter measured
latency statistics.

For checkpoint safety, use fallback drill as a separate phase:

```bash
# Phase 1: baseline benchmark (no fallback drill)
uv run python -m benchmarks.dual_backend \
  --config /path/to/dual-backend.json \
  --output /path/to/baseline-report.json

# Operator injects fault between phases (external, manual process)

# Phase 2: fallback drill only
uv run python -m benchmarks.dual_backend \
  --fallback-only \
  --config /path/to/dual-backend.json \
  --output /path/to/fallback-report.json
```

## Stability and fallback drills

Measured repetitions form the bounded stability drill. The report includes success counts and
p50/p95 latency per backend and mode; it does not extrapolate beyond that sample.

Fallback drilling is opt-in and manual. The harness never disables a backend, changes cluster state,
deploys, or allocates paid resources.

`--fallback-only` sends only logical-model requests to verify fallback routing against a
pre-injected fault state. Restore the primary independently, and retain both fault-injection
record and harness output.

## Prompt matching semantics

Prompt matching supports:

- `match: exact` — normalized full-content equality is required.
- `match: contains` (default) — any configured expected token can appear anywhere.

Normalization is lower-case and whitespace-normalized for both modes.

## Evidence boundary

`evidence/m6-a11-dual-backend.json` is the authoritative current state and must remain
`REAL_HW_NOT_RUN` until an actual dual-hardware run is reviewed. MockTransport unit tests validate
the harness implementation only and are `SIMULATED`; they are not backend performance evidence.
