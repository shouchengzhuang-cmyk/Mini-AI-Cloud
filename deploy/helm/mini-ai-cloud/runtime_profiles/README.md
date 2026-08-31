# Runtime profiles

Runtime profiles are immutable, vendor-specific execution contracts consumed by the
vendor-neutral Mini AI Cloud control plane. They describe how a Kubernetes workload may
request and start one accelerator runtime without installing CUDA, CANN, PyTorch, or an
engine plugin into the control-plane package.

The two `*.example.yaml` files are schema examples, not deployable profiles. Their image
references use the reserved `example.invalid` domain and synthetic SHA-256 values. Their
`SCHEMA_READY` status means only that the files pass the local contract validator. It does
not mean an engine, Kubernetes control plane, device plugin, or physical accelerator ran.

## Contract boundaries

Each profile binds one vendor/kind pair to a digest-pinned image, Kubernetes extended
resource, RuntimeClass, vendor node selector, exact base command, environment-name
allowlist, probes, compatibility ownership, capabilities, allocation authority, evidence
status, and explicit limitations.

Positive evidence statuses (`MANIFEST_VALIDATED`, `SIMULATED`, and every `REAL_*_PASS`)
also require at least one immutable evidence reference. `SCHEMA_READY`,
`REAL_HW_NOT_RUN`, and `BLOCKED` deliberately do not manufacture such a reference.

The contract fails closed when a profile:

- enables `privileged`, `hostPID`, `hostNetwork`, `hostPath`, or privilege escalation;
- uses an image tag instead of an image digest;
- selects a node vendor that differs from the profile vendor/kind pair;
- runs a command outside its exact command allowlist or attempts to invoke a shell;
- permits credential-like or device-allocation environment variables;
- lets the control plane claim exact device IDs while Kubernetes Device Plugin owns
  allocation.

Profile files never contain environment values. In particular, cloud credentials, model
registry tokens, API keys, and device visibility values belong to separate secret and
device-plugin paths.

## Kubernetes serving rendering

The Kubernetes serving adapter accepts an already validated `RuntimeProfile` with an
accelerator count, tensor-parallel size, and explicit environment values. It renders the
profile image, base command, extended resource, RuntimeClass, node selector, tolerations,
and probes. Environment values are accepted only when their names appear in the profile
allowlist. The control plane still owns the generated model, host, port, and tensor-parallel
arguments.

The renderer fails before Pod creation unless the accelerator count equals the
tensor-parallel size and the profile security contract remains within the hard-coded
non-privileged baseline. The selected extended resource appears in both requests and
limits with the same count. During adoption, any additional or substituted extended
resource, asymmetric request/limit, profile identity drift, RuntimeClass drift, or
tensor-parallel mismatch quarantines the Pod instead of silently accepting it.

Profile vendor, kind, ID, version, and semantic digest participate in the owned Pod
contract hash. The full `sha256:` digest and configured resource name are annotations; an
equivalent Base32 digest is used for the Kubernetes label because a 64-character hex
digest exceeds the label value limit. Ready accelerator Pods may emit an observed
allocation callback. The callback reports the device-plugin-owned resource and count but
does not invent physical device IDs that the standard Pod API did not expose.

The example profiles remain non-deployable. The formal `2.0.0` profiles are independently
validated vendor contracts, but the current Fake Kubernetes controller does not select them
automatically; vendor-aware admission and selection remain an A9 responsibility.

## Ascend A2 profile

`ascend-vllm-k8s.yaml` is the first non-placeholder Ascend profile. It pins the official
vLLM Ascend A2 image by multi-platform digest and binds the vLLM Ascend, upstream vLLM,
CANN, PyTorch, torch-npu, MindCluster, product-generation, and Device Plugin contracts in
`ascend-vllm-k8s.acceptance.json`.

The profile is deliberately limited to Atlas A2 / Ascend 910B and Volcano full-card
scheduling. In that documented MindCluster mode, `ASCEND_VISIBLE_DEVICES` is populated
from the Device Plugin-owned `huawei.com/Ascend910` Pod annotation through the Kubernetes
Downward API. The control plane never supplies a device list or invents physical IDs.
Atlas A3, `huawei.com/npu`, vNPU, and non-Volcano scheduling need separate validated
profiles even though the vendor-neutral renderer keeps the resource/annotation pair
configurable.

Validate the static contract from Ubuntu-24.04 WSL:

```bash
make validate-ascend-runtime
uv run python scripts/ascend_runtime_acceptance.py diagnose
```

Cluster preflight and OpenAI-compatible engine acceptance are explicit real-environment
steps. They do not run in generic CI and do not turn `REAL_HW_NOT_RUN` into hardware
evidence:

```bash
uv run python scripts/ascend_runtime_acceptance.py preflight --kubeconfig /path/to/kubeconfig
uv run python scripts/ascend_runtime_acceptance.py accept \
  --base-url http://runtime.example.invalid:8000 --model /models/example
```

## NVIDIA profile

The production-candidate NVIDIA contract is `nvidia-vllm-k8s@2.0.0`. Its pinned image,
GFD constraints, fake Device Plugin scope, diagnostics, and real engine acceptance entry
are documented in [`docs/nvidia-runtime.md`](../docs/nvidia-runtime.md). Its status remains
`REAL_HW_NOT_RUN` until the real-hardware command completes and immutable evidence is
reviewed.

## Compatibility metadata

`python: profile-owned` keeps runtime Python independent of the control-plane interpreter.
The runtime image must pin vLLM and its vendor plugin. Driver and toolkit versions are
host/runtime observations and must be recorded with evidence instead of guessed in an
example. Each formal vendor profile binds its exact compatibility matrix and keeps real
NVIDIA and Ascend execution evidence separate from static validation.

The Kubernetes resource name, RuntimeClass, node selector, tolerations, and accelerator
families remain profile data. They are examples here, not hard-coded platform facts.

## Immutability and evidence

`manifest.json` records the canonical semantic SHA-256 digest of every profile identity
(`id@version`). Release evidence must record that identity and digest. Any semantic edit
changes the digest and therefore cannot continue to match earlier evidence silently.

After a profile has been referenced by release evidence, do not edit it in place. Copy it,
increment `version` (and `id` when the deployment contract uses IDs as immutable release
keys), then validate the new profile independently. Formatting-only YAML edits do not
change the semantic digest.

Run validation from Ubuntu-24.04 WSL:

```bash
uv run python scripts/validate_runtime_profiles.py
```

After an intentional new profile or version is added, regenerate derived files and review
the digest diff:

```bash
uv run python scripts/validate_runtime_profiles.py --write-generated
git diff -- runtime_profiles/schema.json runtime_profiles/manifest.json
```
