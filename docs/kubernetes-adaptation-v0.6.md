# Mini AI Cloud v0.6 Kubernetes adaptation

Mini AI Cloud v0.6 packages the existing control plane and Worker for one Kubernetes cluster,
runs Kubernetes batch executions as fenced `batch/v1` Jobs, and renders NVIDIA or Huawei
Ascend serving Pods from release-pinned Runtime Profiles. The release keeps a single control
plane replica and an external PostgreSQL/Redis state layer. It does not add leader election,
multi-cluster control, a production artifact data plane, or real accelerator evidence.

The repository is prepared as `0.6.0`. The final P4 `KIND_K8S_PASS` bundle is not embedded in
this commit. `scripts/release_gate.py prepare` refuses to build a release preparation bundle
until the caller supplies that real, commit-bound P4 bundle. Current authorization and evidence
states are recorded in [v0.6 release readiness](v0.6-release-readiness.md).

## Architecture and ownership

```text
OpenAI-compatible API / REST / SSE
                 |
     single control-plane Deployment
                 |
      PostgreSQL source of truth -------- Redis notifications and limits
                 |
          Worker StatefulSet
           /             \
  batch/v1 Jobs       serving Pods
       |                  |
 execution fences    Runtime Profiles
                          |
              NVIDIA or Huawei Ascend
              administrator-owned stack
```

The Helm release owns one Deployment, one Worker StatefulSet, a migration Job, three Services,
two ServiceAccounts, namespaced Roles and RoleBindings, and ConfigMaps for settings and Runtime
Profiles. It never
owns namespaces, Secrets, PostgreSQL, Redis, object storage, Device Plugins, RuntimeClasses,
Volcano, Ingress, Gateway API objects, persistent volumes, or cluster-scoped RBAC.

PostgreSQL remains the state authority for desired state, claims, leases, execution identity,
service replicas, and reconciliation. Redis is not a source of truth. A Worker or controller
restart must adopt only resources whose labels, execution identity, controller session, UID,
resource version, Runtime Profile digest, and workload spec hash match the database record.

## Prerequisites

- Kubernetes 1.27 or newer and Helm 3.
- One pre-existing system namespace and one pre-existing workload namespace.
- External PostgreSQL and Redis endpoints reachable from the system namespace.
- An existing Secret in the system namespace.
- A non-`latest` application image; production should use a verified digest.
- For accelerator workloads, administrator-installed RuntimeClass, Device Plugin, node labels,
  extended resources, and any scheduler named by the Runtime Profile.

The v0.6 Kind evidence contract pins Kind `v0.27.0`, Kubernetes `v1.32.2`, and
`kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f`.
These pins define the reproducible test environment, not the full set of supported Kubernetes
distributions.

## External PostgreSQL, Redis, and Secret

The Chart consumes these Secret keys by default:

| Key | Value contract |
| --- | --- |
| `database-url` | SQLAlchemy async PostgreSQL URL |
| `redis-url` | Redis URL |
| `api-key-pepper` | production API-key pepper, at least 32 bytes |
| `worker-auth-token` | production Worker token, at least 32 bytes |
| `secret-master-key` | `key-id:base64-key`, where the active key decodes to 32 bytes |
| `bootstrap-token` | needed only when bootstrap is explicitly enabled |

Do not place these values in a Helm values file or command arguments. Create the Secret from
root-owned files or through the cluster's secret manager. The example below shows the file
boundary without supplying credentials:

```bash
kubectl create namespace mini-ai-cloud-system
kubectl create namespace mini-ai-cloud-workloads
kubectl --namespace mini-ai-cloud-system create secret generic mini-ai-cloud \
  --from-file=database-url=/secure/mini-ai-cloud/database-url \
  --from-file=redis-url=/secure/mini-ai-cloud/redis-url \
  --from-file=api-key-pepper=/secure/mini-ai-cloud/api-key-pepper \
  --from-file=worker-auth-token=/secure/mini-ai-cloud/worker-auth-token \
  --from-file=secret-master-key=/secure/mini-ai-cloud/secret-master-key
```

PostgreSQL migrations run as a Helm pre-install/pre-upgrade Job with `backoffLimit: 0`. A failed
or timed-out `alembic upgrade head` fails the install or upgrade. Helm uninstall leaves the
external Secret and both data services untouched.

## Helm installation

Keep non-secret settings in a reviewed values file:

```yaml
namespaces:
  workload: mini-ai-cloud-workloads

image:
  repository: registry.example/mini-ai-cloud
  digest: sha256:<verified-image-digest>
  pullPolicy: IfNotPresent

existingSecret:
  name: mini-ai-cloud

config:
  appEnvironment: production
  clusterId: mini-ai-cloud-production
  servingClusterId: mini-ai-cloud-production
  bootstrapEnabled: false
  servingEnabled: true
  servingFakeEnabled: false
  servingImage: ""

workload:
  serviceAccountName: mini-ai-workloads
  imagePullSecrets:
    - registry-pull
```

Install or reconcile the release:

```bash
helm upgrade --install mini-ai-cloud deploy/helm/mini-ai-cloud \
  --namespace mini-ai-cloud-system \
  --values values-production.yaml \
  --wait --timeout 10m
```

`namespaces.system` is empty by default and resolves to `--namespace`. If set, it must equal
the Helm release namespace. `namespaces.workload` is a required static allowlist entry. The
application receives that value as both `KUBERNETES_NAMESPACE` and
`KUBERNETES_SERVING_NAMESPACE`.

Important values are bounded as follows:

| Values path | Boundary |
| --- | --- |
| `controlPlane.replicas` | fixed to `1` |
| `service.type` | `ClusterIP` by default; test-only `NodePort` needs `global.testMode=true` |
| `config.appEnvironment` | `development`, `test`, or `production` |
| `config.servingFakeEnabled` | rejected in production and when serving is disabled |
| `config.artifactBackend` | existing `local` or `s3` backend only |
| `workload.serviceAccountName` | optional administrator-owned identity by name |
| `workload.imagePullSecrets` | administrator-owned Secret names, passed as a deduplicated list |
| `storage.*SizeLimit` | bounded `emptyDir`; no production `hostPath` template |

The complete schema is [values.schema.json](../deploy/helm/mini-ai-cloud/values.schema.json).

## Namespace and RBAC boundary

The control plane and Worker run in the system namespace. Their write-capable Roles can act only
in the one configured workload namespace. Accelerator admission uses the `kubernetes-node`
inventory provider, so the Worker additionally has a read-only ClusterRole with exactly Node
`list` and Pod `list`. Both reads are cluster-wide to deduct external accelerator requests from
Device Plugin allocatable capacity on every eligible node. No `watch`, write, Secret,
wildcard-resource, or wildcard-verb permission is granted at cluster scope.

The Worker Role manages the fenced batch Jobs, their Pods and logs/status, and deny-all
NetworkPolicies. The control-plane Role manages serving Pods and the pre-created headless
Service path. Application-created task and serving Pods disable service-account token
automount.

The API and controller still share one Deployment. That externally reachable Pod therefore
holds bounded write permissions in the workload namespace. Isolate its network, pin its image,
use a dedicated namespace, and treat the ServiceAccount as a control-plane credential.

## Single-replica control plane

`controlPlane.replicas` is fixed to one by JSON schema and Helm template validation. The
Deployment uses `Recreate`, so an upgrade cannot overlap two controller Pods. There is no
leader election, HPA, PDB, or active-active API/controller topology. PostgreSQL fences stale
work, but those fences do not turn the deployment into a highly available control plane.

## Batch Job lifecycle

Each Kubernetes execution creates one `batch/v1` Job with one completion, one parallel Pod,
`backoffLimit: 0`, `restartPolicy: Never`, and an active deadline derived from the task timeout.
CPU, memory, and accelerator requests equal their limits. The Pod runs as UID/GID 65532 with a
read-only root filesystem, `RuntimeDefault` seccomp, no added capabilities, and no mounted
service-account token.

Runtime Profile workloads use the profile's extended resource, RuntimeClass, node selector,
required node affinity, tolerations, and `schedulerName`. They do not set `nodeName`. CPU Jobs
also avoid `nodeName`. A legacy local artifact mount may pin a non-profile development/test Job
to the Worker node, but production rejects every Kubernetes task that declares artifact mounts
before the Kubernetes API is called.

The Worker observes the Job and its controlled Pod, classifies completion, cancellation,
timeout, image-pull failure, OOM, and loss, and deletes only an exact UID-fenced Job. On restart,
it may transfer the mutable controller-session annotation only through a resource-version CAS
after validating the immutable execution labels and spec hash. Malformed or drifted resources
are quarantined instead of adopted.

## Artifact boundary

The Chart's local artifact paths use bounded `emptyDir` storage and are ephemeral. v0.6 does
not provide a cross-node production artifact transport for Kubernetes Jobs. In production,
`worker.kubernetes_runtime` rejects artifact-bearing Jobs rather than falling back to
`hostPath` or silently losing outputs. The existing S3 backend can be selected only when the
deployment already supplies and governs the external object-store integration; the Chart does
not install one.

## Runtime Profiles and accelerator preflight

The Chart packages the repository's complete `runtime_profiles/` directory byte-for-byte in a
read-only ConfigMap. It mounts the files under
`/etc/mini-ai-cloud/runtime_profiles` and sets
`RUNTIME_PROFILE_MANIFEST_PATH=/etc/mini-ai-cloud/runtime_profiles/manifest.json`. Profile ID,
version, semantic digest, image digest, and Kubernetes placement contract enter the workload
spec hash. A missing profile or digest mismatch fails closed.

Before accelerator serving, run the bounded preflight against the exact release profile. It
checks:

- Kubernetes API readiness and an Active workload namespace;
- the named RuntimeClass and, for Ascend, its exact handler;
- Ready nodes with every required selector, label, and affinity condition;
- a positive allocatable count for the profile's extended resource;
- the required scheduler when `schedulerName` is set;
- Ascend Device Plugin DaemonSet readiness and chip-label prefix;
- the release-pinned vendor acceptance contract and image digest.

The preflight returns counts and contract identities, not kubeconfig content, Secret values,
node names, or raw cluster objects.

### NVIDIA

The NVIDIA profile uses its digest-pinned vLLM image, NVIDIA extended resource, RuntimeClass,
affinity, selector, and tolerations. The cluster administrator installs and operates the NVIDIA
Device Plugin and runtime. The repository's fake Device Plugin proves only that Kind can
allocate an extended resource; it does not execute CUDA, NCCL, vLLM, or a physical GPU.

### Huawei Ascend

The Ascend A2 profile uses its digest-pinned vLLM-Ascend image, full-card extended resource,
Ascend RuntimeClass/handler, chip labels, device visibility annotation, and
`schedulerName: volcano`. The preflight requires an observable Volcano scheduler and a ready
Ascend Device Plugin DaemonSet before workload creation. Mini AI Cloud does not install or own
Volcano, MindCluster, CANN, the Ascend runtime, or the Device Plugin.

`schedulerName: volcano` selects an administrator-provided scheduler. v0.6 does not claim to
replace Volcano or to support multi-node gang scheduling or distributed training.

## Serving and OpenAI-compatible access

The serving controller reuses the M6 service and replica state machine. It creates only exact
Runtime-Profile-derived serving Pods, waits for readiness, persists the resolved image digest,
replaces failed executions with a new fenced identity, drains active requests before scale
down, and adopts a healthy matching Pod after a controller restart. A mismatch in profile,
placement, image, ServiceAccount, pull Secrets, labels, or spec hash fails closed.

Clients use the existing OpenAI-compatible endpoints:

```text
GET  /v1/models
POST /v1/chat/completions
POST /v1/completions
```

Both JSON and SSE responses pass through the existing Gateway routing, request accounting,
fallback, and circuit contracts. `config.servingFakeEnabled` is a deterministic Kind/test path
only and is rejected twice in production, by Chart validation and application settings.

## Reproducing the Kind contract

The final P4 entry point is:

```bash
make test-kind-kubernetes-adaptation
```

It creates a fresh run ID, Kind cluster, system namespace, workload namespace, release name,
and private kubeconfig; builds the current checkout; manages isolated PostgreSQL and Redis;
runs Helm install, migration, readiness, batch, serving, accelerator-contract, restart,
upgrade, uninstall, and scoped cleanup checks; then deletes the Kind cluster. The default
evidence root is `build/kind-evidence`. A successful run prints exactly one machine-readable
location line:

```text
EVIDENCE_BUNDLE=<absolute-bundle-directory>
```

Until that final bundle is generated for the release SHA, the repository status remains
`PENDING_FINAL_P4_EVIDENCE`. `NOT_RUN` is never accepted as `KIND_K8S_PASS`.

Component-level Chart rendering remains available without creating a cluster:

```bash
make test-helm-render
```

## Upgrade and uninstall

Use the same release name, system namespace, reviewed values, and pinned image when upgrading:

```bash
helm upgrade --install mini-ai-cloud deploy/helm/mini-ai-cloud \
  --namespace mini-ai-cloud-system \
  --values values-production.yaml \
  --wait --timeout 10m
```

The migration hook runs before the new Deployments become ready. The control-plane Deployment
uses `Recreate`; expect an API interruption. v0.6 supplies an upgrade smoke contract, not a
zero-downtime or rollback guarantee. Back up PostgreSQL and the external artifact store before
upgrading.

Uninstall only the release-owned resources:

```bash
helm uninstall mini-ai-cloud --namespace mini-ai-cloud-system --wait
```

Afterward, verify that Helm-owned Deployments, Services, ConfigMaps, Roles, RoleBindings,
ServiceAccounts, and hook Jobs are gone. The external Secret, namespaces, PostgreSQL, Redis,
Device Plugins, RuntimeClasses, Volcano, and object storage must remain. Delete those resources
only through their administrator-owned lifecycle.

## Security and support limitations

- Every rendered container is non-root, drops all Linux capabilities, disables privilege
  escalation, uses a read-only root filesystem, and runs with `RuntimeDefault` seccomp.
- Writable Chart volumes are bounded `emptyDir`; the Chart renders no Docker socket,
  `hostPath`, privileged container, host namespace, Secret, or cluster-scoped RBAC.
- ClusterIP is the default Service type. NodePort is restricted to explicit test mode. The
  Chart does not install an ingress or TLS endpoint.
- External database, cache, registry, Secret, object-store, Device Plugin, RuntimeClass,
  scheduler, node security, network policy enforcement, backup, and monitoring remain operator
  responsibilities.
- The verified target is a single-node Kind contract. There is no production HA,
  multi-physical-node, SLA, DR, universal distribution, universal model, KServe/Kubeflow/Kueue
  replacement, or production artifact-pipeline claim.
- Real NVIDIA and Huawei Ascend execution remains `REAL_HW_NOT_RUN`.

## Optional E1-E3 evidence

E1 is a non-Kind single-cluster smoke covering install, external dependencies, a CPU Job,
serving, restart adoption, upgrade, uninstall, and cleanup. E2 runs the exact NVIDIA profile on
physical NVIDIA hardware. E3 runs the exact Ascend profile with the administrator's Ascend and
Volcano stack. These evidence tracks are optional for v0.6 release preparation and remain
unclaimed until their own commit-bound bundles pass.
