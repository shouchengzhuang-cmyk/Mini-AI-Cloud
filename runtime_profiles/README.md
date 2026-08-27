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

## Compatibility metadata

`python: profile-owned` keeps runtime Python independent of the control-plane interpreter.
The runtime image must pin vLLM and its vendor plugin. Driver and toolkit versions are
host/runtime observations and must be recorded with evidence instead of guessed in an
example. A7 and A8 are responsible for replacing the placeholder images and binding exact
version matrices to real NVIDIA and Ascend evidence.

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
