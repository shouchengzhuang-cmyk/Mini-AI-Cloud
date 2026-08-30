# Mini AI Cloud Helm Chart

This Chart installs the Mini AI Cloud v0.6 application as a single-cluster,
single-control-plane Kubernetes deployment. It packages the v0.6 fenced batch Job runtime and
Runtime-Profile-driven NVIDIA/Ascend serving integration. HA and a production Kubernetes
artifact transport remain outside this release.

## What the Chart owns

- one single-replica control-plane Deployment and ClusterIP Service;
- one configurable Worker Deployment;
- a pre-install/pre-upgrade migration Job;
- two ServiceAccounts plus namespaced Roles and RoleBindings;
- a headless Service for managed serving Pods;
- one ConfigMap with non-secret settings;
- one read-only ConfigMap containing the repository's complete immutable Runtime Profile set.

The Chart does **not** create namespaces, Secrets, PostgreSQL, Redis, MinIO, persistent
volumes, Device Plugins, Volcano, Ingress, Gateway API resources, or third-party CRDs. Helm
uninstall therefore cannot delete those administrator-owned resources.

## Prerequisites

- Kubernetes 1.27 or newer;
- Helm 3;
- two pre-existing namespaces: the Helm release namespace and one statically configured
  workload namespace;
- reachable external PostgreSQL and Redis services;
- an existing Secret in the system namespace;
- an explicitly versioned application image. Production operators should set
  `image.digest` to a verified `sha256:` digest.

The existing Secret must contain these keys by default:

| Key | Purpose |
| --- | --- |
| `database-url` | SQLAlchemy async PostgreSQL URL |
| `redis-url` | Redis URL |
| `api-key-pepper` | production API-key pepper, at least 32 bytes |
| `worker-auth-token` | production Worker token, at least 32 bytes |
| `secret-master-key` | application key ring in `key-id:base64-key` format |
| `bootstrap-token` | required only when `config.bootstrapEnabled=true` |

Key names are configurable under `existingSecret.keys`. The Chart uses individual
`secretKeyRef` entries and never imports a complete Secret with `envFrom`.

## Install

Create the namespaces and Secret outside Helm. Keep credential values in a secret manager or
root-owned `0600` files; do not commit them to a values file or put them directly in process
arguments.

```bash
kubectl create namespace mini-ai-cloud-system
kubectl create namespace mini-ai-cloud-workloads
kubectl --namespace mini-ai-cloud-system create secret generic mini-ai-cloud \
  --from-file=database-url=/secure/mini-ai-cloud/database-url \
  --from-file=redis-url=/secure/mini-ai-cloud/redis-url \
  --from-file=api-key-pepper=/secure/mini-ai-cloud/api-key-pepper \
  --from-file=worker-auth-token=/secure/mini-ai-cloud/worker-auth-token \
  --from-file=secret-master-key=/secure/mini-ai-cloud/secret-master-key

helm upgrade --install mini-ai-cloud deploy/helm/mini-ai-cloud \
  --namespace mini-ai-cloud-system \
  --set namespaces.workload=mini-ai-cloud-workloads \
  --set existingSecret.name=mini-ai-cloud \
  --set image.repository=registry.example/mini-ai-cloud \
  --set image.digest=sha256:<verified-digest> \
  --wait --timeout 10m
```

`namespaces.system` defaults to the Helm release namespace. If explicitly set, it must match
`--namespace`; this avoids a release silently writing system resources elsewhere.

The migration is a Helm pre-install/pre-upgrade hook with `backoffLimit: 0`. A failed or
timed-out migration fails the install or upgrade. The command is fixed to `alembic upgrade
head` and does not print the database URL.

## Services

`service.type` defaults to `ClusterIP`. `LoadBalancer` is available only by explicit operator
configuration. `NodePort` fails schema validation unless `global.testMode=true`; it is meant
only for isolated Kind/test values. This Chart intentionally contains no Ingress or Gateway
API template.

## Namespace and RBAC boundary

The workload namespace is one static allowlist entry. Both the control plane and Worker use
Roles in that namespace; there are no ClusterRoles, wildcard resources, wildcard verbs, or
permissions to discover arbitrary namespaces.

The Worker can manage Jobs, Pods, Pod logs/status, and the per-execution NetworkPolicies used
by the v0.6 Kubernetes runtime. The control plane can manage serving Pods. Managed task
and serving Pods created by the application set `automountServiceAccountToken=false`.

Because P1 deliberately does not split the Web API from the controller, the externally
reachable control-plane Pod also holds namespaced workload write permissions. Operators must
restrict network access to this Pod, pin its image, and dedicate the workload namespace. This
is a documented simplified single-replica boundary, not production HA.

The immutable Runtime Profile files are packaged byte-for-byte from the repository and
mounted read-only at `/etc/mini-ai-cloud/runtime_profiles`. The application receives
`RUNTIME_PROFILE_MANIFEST_PATH=/etc/mini-ai-cloud/runtime_profiles/manifest.json`; a render
test rejects any drift between the source directory, packaged files, and ConfigMap data.

P3 can set `workload.serviceAccountName` and `workload.imagePullSecrets` for application-
managed task/serving Pods. These are names only: the Chart neither creates those workload
credentials nor grants them API permissions. The corresponding non-secret controller inputs
are exposed as `KUBERNETES_SERVING_SERVICE_ACCOUNT_NAME` and
`KUBERNETES_SERVING_IMAGE_PULL_SECRETS`.

## Replica and storage boundaries

`controlPlane.replicas` is fixed to `1` by both JSON schema and template validation. The
Deployment uses the `Recreate` strategy so upgrades do not overlap two control-plane Pods.
There is no leader election, HPA, or PDB.

Worker replicas are configurable. Existing PostgreSQL claim/session fencing remains the
authority preventing duplicate claims; P1 does not replace it.

Writable paths use bounded `emptyDir` volumes. No production `hostPath`, Docker socket,
privileged container, or host namespace is rendered. The default local artifact backend is
ephemeral and is not a cross-Pod production artifact pipeline. Select the existing S3 backend
only when the deployment environment already supplies and governs that external integration.

## Security defaults

Every rendered Pod and container uses non-root UID/GID 10001, `RuntimeDefault` seccomp,
`allowPrivilegeEscalation=false`, a read-only root filesystem, and dropped Linux
capabilities. Service-account tokens are mounted only into the control-plane and Worker Pods
that call the namespaced Kubernetes API. Migration has token automount disabled.

`config.servingFakeEnabled` defaults to `false`. Both JSON schema and template validation
reject it unless serving is enabled and `config.appEnvironment` is `development` or `test`;
production values therefore fail closed even if multiple overrides are combined.

## Validate

```bash
helm lint deploy/helm/mini-ai-cloud
helm template mini-ai-cloud deploy/helm/mini-ai-cloud \
  --namespace mini-ai-cloud-system \
  --values deploy/helm/mini-ai-cloud/ci/values-kind.yaml
make test-helm-render
```

`make test-helm-render` covers random release/namespace rendering, a digest-pinned positive
fixture, the no-owned-Secret snapshot, single-replica rejection, test-only NodePort, unknown
hostPath/privileged values, RBAC wildcards, bounded writable volumes, and container security
contexts.

The Kind values assume the harness has already loaded `mini-ai-cloud:kind-m7-p1`, created both
namespaces and the `mini-ai-cloud-kind` Secret, and provided external PostgreSQL/Redis. Those
test dependencies are deliberately not Chart dependencies.

## Uninstall

```bash
helm uninstall mini-ai-cloud --namespace mini-ai-cloud-system
```

After uninstall, separately verify and manage the external Secret, namespaces, PostgreSQL,
Redis, and any object storage. Helm must not remove them.
