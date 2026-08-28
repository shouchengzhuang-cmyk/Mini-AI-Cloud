# Logical models and vendor variants

Mini AI Cloud keeps the existing `/models` registry API unchanged and adds a separate
logical-model layer for heterogeneous serving. A logical model is the stable public
identity. Each physical variant binds one vendor-specific artifact to exactly one immutable
Runtime Profile identity and semantic digest.

## State invariants

- New logical models start `disabled` and emit an append-only initial status event.
- A logical model may become `ready` only when it has at least one `ready` variant.
- A ready logical model cannot disable, degrade, or delete its last ready variant.
- Variant health changes update only variant state. They never rewrite logical-model audit
  events or silently change the public model state.
- Runtime Profile deletion must call
  `ModelVariantRepository.ensure_runtime_profile_unreferenced` first; disabled variants still
  count as references.

## Artifact and profile identity

Every variant stores these two independent immutable bindings:

```text
runtime profile = id + version + semantic sha256
model artifact  = source + revision + sha256
```

The API resolves profile metadata from the generated A4 `manifest.json` and rejects an
unknown identity, digest drift, vendor/kind mismatch, or undeclared dtype/architecture.
An empty profile architecture list means the profile has not narrowed architectures yet;
it does not infer that NVIDIA and Ascend can share an artifact.

NVIDIA and Ascend variants must be created separately with their own artifact revision and
digest. No repository or API path clones or automatically marks a vendor artifact as usable
by the other vendor.

The committed A4 profiles remain `SCHEMA_READY` examples with reserved image references.
A5 validates registry structure only. Real engine and hardware claims remain
`REAL_HW_NOT_RUN` until A7/A8 evidence exists.
