# NVIDIA vLLM Kubernetes runtime

`nvidia-vllm-k8s@2.0.0` is the production-candidate NVIDIA profile. It pins the official
multi-architecture `vllm/vllm-openai:v0.28.0` manifest by digest and records the image
metadata observed on 2026-08-28 in
`runtime_profiles/nvidia-vllm-k8s.acceptance.json`. The image metadata identifies vLLM
0.28.0, Python 3.12, CUDA 13.0.2, NCCL 2.30.7, and vLLM commit
`2cf0a6915ce544dc493a0990f2ea38d81601128a`.

These pins are a compatibility contract, not hardware evidence. The profile remains
`REAL_HW_NOT_RUN` until it passes the real diagnostics and OpenAI acceptance commands on a
declared NVIDIA node/model combination.

## Scheduling contract

The Pod requests `nvidia.com/gpu` with identical requests and limits. RuntimeClass,
resource name, tolerations, and scheduling labels remain profile data. Required node
affinity uses GPU Feature Discovery and NVIDIA Device Plugin labels to require:

- a product label;
- enough physical GPU capacity for the requested TP group;
- CUDA compute capability 8.0 or newer;
- `sharing-strategy=none`;
- `mig.strategy=none`.

This profile does not support MIG, time-slicing, MPS, vGPU, multi-node TP, or pipeline
parallel. It also does not claim that every compute-capability 8.0+ GPU has enough memory
or supports every model architecture. Admission must validate the model artifact and
runtime-observed capacity separately.

The label names and device-plugin behavior follow the official
[NVIDIA Device Plugin](https://github.com/NVIDIA/k8s-device-plugin/tree/v0.20.0) and
[GPU Feature Discovery](https://github.com/NVIDIA/k8s-device-plugin/blob/v0.20.0/docs/gpu-feature-discovery/README.md)
contracts. The image interface follows the official
[vLLM Docker deployment](https://docs.vllm.ai/en/stable/deployment/docker/) contract.

## Static and fake-device validation

Run the static contract validator from Ubuntu-24.04 WSL:

```bash
uv run python scripts/validate_nvidia_runtime.py
```

The PR Kind job also deploys Kubernetes' pinned sample Device Plugin and requests one
`example.com/resource`. This proves the Kubernetes extended-resource allocation path only.
It deliberately does not advertise `nvidia.com/gpu`, run vLLM, or count as NVIDIA evidence:

```bash
make kind-serving-up
make test-nvidia-fake-device-plugin
make kind-serving-down
```

The successful no-GPU CI status is `MANIFEST_VALIDATED`; the real-hardware boundary remains
`REAL_HW_NOT_RUN`.

The fake Device Plugin DaemonSet is privileged because it is test infrastructure that
registers with kubelet. The allocation workload remains non-privileged and has no hostPath
or service-account token. The infrastructure container uses a read-only root filesystem;
its narrowly scoped Trivy exceptions cover only the root/privileged/hostPath access required
to register the pinned sample plugin. `.trivyignore.yaml` is path-scoped to this single
manifest, and the NVIDIA contract validator rejects broader IDs or paths. This exception
never changes the Runtime Profile workload security baseline.

## Real hardware entry

Generate a credential-safe local diagnostic summary:

```bash
uv run python scripts/nvidia_runtime_acceptance.py diagnose
```

Without `nvidia-smi` and NVIDIA device nodes, it emits `REAL_HW_NOT_RUN`. A real evidence
run must fail instead of accepting that state:

```bash
uv run python scripts/nvidia_runtime_acceptance.py diagnose \
  --require-hardware \
  --output build/nvidia-runtime/diagnostic.json
```

The diagnostic records only model, driver version, total memory, compute capability, and a
device-interface summary. Native Linux uses `/dev/nvidia*`; Ubuntu under WSL can instead
report the presence of `/dev/dxg`. It omits UUIDs, tokens, complete environment variables,
kubeconfig, and private model paths.

Export node labels to a local JSON string mapping, then validate the selected node and TP
count:

```bash
uv run python scripts/nvidia_runtime_acceptance.py validate-node-labels \
  --input build/nvidia-runtime/node-labels.json \
  --accelerator-count 1
```

After the digest-pinned engine is Ready, validate health, exact vLLM version, an OpenAI
non-streaming completion, and SSE framing. The API key is read by environment-variable
name and is never written to output:

```bash
uv run python scripts/nvidia_runtime_acceptance.py accept \
  --base-url http://127.0.0.1:8000 \
  --model validated-model-id \
  --api-key-env NVIDIA_RUNTIME_API_KEY \
  --output build/nvidia-runtime/engine-acceptance.json
```

Only a reviewed run containing both hardware diagnostics and engine acceptance may advance
the evidence state beyond `REAL_HW_NOT_RUN`.
