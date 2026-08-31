#!/usr/bin/env bash
set -euo pipefail
umask 077

RUN_ID=${RUN_ID:-}
HELM_BIN=${HELM_BIN:-helm}
KIND_BIN=${KIND_BIN:-kind}
KUBECTL_BIN=${KUBECTL_BIN:-kubectl}
DOCKER_BIN=${DOCKER_BIN:-docker}
PYTHON_BIN=${PYTHON_BIN:-python3}
IMAGE=${KIND_HELM_IMAGE:-mini-ai-cloud:kind-m7-p1}
KIND_NODE_IMAGE=kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f

if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9-]{2,24}$ ]]; then
  echo "RUN_ID must match ^[a-z0-9][a-z0-9-]{2,24}$" >&2
  exit 2
fi

for binary in "$HELM_BIN" "$KIND_BIN" "$KUBECTL_BIN" "$DOCKER_BIN" "$PYTHON_BIN"; do
  if ! command -v "$binary" >/dev/null 2>&1; then
    echo "required binary not found: $binary" >&2
    exit 2
  fi
done

KIND_VERSION=$("$KIND_BIN" version 2>/dev/null | awk '{print $2}')
if [[ "$KIND_VERSION" != "v0.27.0" ]]; then
  echo "kind v0.27.0 is required, found: ${KIND_VERSION:-unknown}" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CHART="$ROOT/deploy/helm/mini-ai-cloud"
KIND_VALUES="$CHART/ci/values-kind.yaml"
DATA_STORES="$ROOT/deploy/kind-serving/10-data-stores.yaml"
CLUSTER="mac-$RUN_ID"
CONTEXT="kind-$CLUSTER"
RELEASE="mac-$RUN_ID"
SYSTEM_NAMESPACE="mac-$RUN_ID-system"
WORKLOAD_NAMESPACE="mac-$RUN_ID-workloads"
APP_SECRET="mac-$RUN_ID-app"
DATA_SECRET="mac-$RUN_ID-db"
TEMP_DIR=$(mktemp -d "/tmp/mini-ai-cloud-$RUN_ID.XXXXXX")
KUBECONFIG_PATH="$TEMP_DIR/kubeconfig"
APP_SECRET_FILE="$TEMP_DIR/app-secret.env"
DATA_SECRET_FILE="$TEMP_DIR/data-secret.env"

cleanup() {
  local status=$?
  trap - EXIT
  if "$KIND_BIN" get clusters 2>/dev/null | grep -Fx "$CLUSTER" >/dev/null; then
    if ! "$KIND_BIN" delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_PATH"; then
      status=1
    fi
  fi
  rm -f -- "$APP_SECRET_FILE" "$DATA_SECRET_FILE" "$KUBECONFIG_PATH"
  if ! rmdir -- "$TEMP_DIR"; then
    status=1
  fi
  if [[ $status -eq 0 ]]; then
    echo "KIND_HELM_CLEANUP_PASS run_id=$RUN_ID"
  fi
  exit "$status"
}
trap cleanup EXIT

wait_for_release_cleanup() {
  local namespace=$1
  local deadline=$((SECONDS + 60))
  local remaining
  while true; do
    remaining=$("${KUBECTL[@]}" --namespace "$namespace" get \
      all,configmaps,roles,rolebindings,serviceaccounts \
      --selector="app.kubernetes.io/instance=$RELEASE" --output=name)
    if [[ -z "$remaining" ]]; then
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "Helm-owned resources remain in $namespace after uninstall:" >&2
      echo "$remaining" >&2
      return 1
    fi
    sleep 2
  done
}

if "$KIND_BIN" get clusters | grep -Fx "$CLUSTER" >/dev/null; then
  echo "refusing to reuse existing Kind cluster: $CLUSTER" >&2
  exit 2
fi
if ! "$DOCKER_BIN" image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "image is not present locally; build it before the smoke: $IMAGE" >&2
  exit 2
fi

"$KIND_BIN" create cluster \
  --name "$CLUSTER" \
  --kubeconfig "$KUBECONFIG_PATH" \
  --image "$KIND_NODE_IMAGE" \
  --wait 120s
"$KIND_BIN" load docker-image "$IMAGE" --name "$CLUSTER"

KUBECTL=("$KUBECTL_BIN" --kubeconfig "$KUBECONFIG_PATH" --context "$CONTEXT")
HELM=("$HELM_BIN" --kubeconfig "$KUBECONFIG_PATH" --kube-context "$CONTEXT")

"${KUBECTL[@]}" create namespace "$SYSTEM_NAMESPACE"
"${KUBECTL[@]}" create namespace "$WORKLOAD_NAMESPACE"
"${KUBECTL[@]}" --namespace "$WORKLOAD_NAMESPACE" \
  create serviceaccount mini-ai-cloud-data

"$PYTHON_BIN" - "$DATA_SECRET_FILE" "$APP_SECRET_FILE" "$WORKLOAD_NAMESPACE" <<'PY'
import base64
import os
import secrets
import sys
from pathlib import Path

data_secret_path = Path(sys.argv[1])
app_secret_path = Path(sys.argv[2])
workload_namespace = sys.argv[3]
postgres_password = secrets.token_urlsafe(32)
database_url = (
    "postgresql+asyncpg://task:"
    f"{postgres_password}@postgres.{workload_namespace}.svc.cluster.local:5432/task_platform"
)
app_values = {
    "DATABASE_URL": database_url,
    "REDIS_URL": f"redis://redis.{workload_namespace}.svc.cluster.local:6379/0",
    "API_KEY_PEPPER": secrets.token_urlsafe(48),
    "WORKER_AUTH_TOKEN": secrets.token_urlsafe(48),
    "SECRET_MASTER_KEY": "kind:"
    + base64.b64encode(secrets.token_bytes(32)).decode("ascii"),
}
data_secret_path.write_text(f"POSTGRES_PASSWORD={postgres_password}\n", encoding="utf-8")
app_secret_path.write_text(
    "".join(f"{key}={value}\n" for key, value in app_values.items()),
    encoding="utf-8",
)
os.chmod(data_secret_path, 0o600)
os.chmod(app_secret_path, 0o600)
PY

"${KUBECTL[@]}" --namespace "$WORKLOAD_NAMESPACE" \
  create secret generic "$DATA_SECRET" --from-env-file="$DATA_SECRET_FILE"

sed \
  -e "s/mini-ai-cloud-serving/$WORKLOAD_NAMESPACE/g" \
  -e "s/mini-ai-cloud-kind-secrets/$DATA_SECRET/g" \
  -e "s/postgres-password/POSTGRES_PASSWORD/g" \
  "$DATA_STORES" | "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" --namespace "$WORKLOAD_NAMESPACE" \
  rollout status deployment/postgres --timeout=180s
"${KUBECTL[@]}" --namespace "$WORKLOAD_NAMESPACE" \
  rollout status deployment/redis --timeout=180s

"${KUBECTL[@]}" --namespace "$SYSTEM_NAMESPACE" \
  create secret generic "$APP_SECRET" --from-env-file="$APP_SECRET_FILE"
rm -f -- "$APP_SECRET_FILE" "$DATA_SECRET_FILE"

"${HELM[@]}" upgrade --install "$RELEASE" "$CHART" \
  --namespace "$SYSTEM_NAMESPACE" \
  --values "$KIND_VALUES" \
  --set-string "namespaces.workload=$WORKLOAD_NAMESPACE" \
  --set-string "existingSecret.name=$APP_SECRET" \
  --set-string existingSecret.keys.databaseUrl=DATABASE_URL \
  --set-string existingSecret.keys.redisUrl=REDIS_URL \
  --set-string existingSecret.keys.apiKeyPepper=API_KEY_PEPPER \
  --set-string existingSecret.keys.workerAuthToken=WORKER_AUTH_TOKEN \
  --set-string existingSecret.keys.secretMasterKey=SECRET_MASTER_KEY \
  --wait \
  --timeout 5m

"${KUBECTL[@]}" --namespace "$SYSTEM_NAMESPACE" \
  rollout status "deployment/$RELEASE-mini-ai-cloud-control-plane" --timeout=120s
"${KUBECTL[@]}" --namespace "$SYSTEM_NAMESPACE" \
  rollout status "statefulset/$RELEASE-mini-ai-cloud-worker" --timeout=120s

"${KUBECTL[@]}" auth can-i create services \
  --namespace "$WORKLOAD_NAMESPACE" \
  --as="system:serviceaccount:$SYSTEM_NAMESPACE:$RELEASE-mini-ai-cloud-control-plane" \
  >/dev/null
"${KUBECTL[@]}" auth can-i create jobs.batch \
  --namespace "$WORKLOAD_NAMESPACE" \
  --as="system:serviceaccount:$SYSTEM_NAMESPACE:$RELEASE-mini-ai-cloud-worker" \
  >/dev/null
if "${KUBECTL[@]}" auth can-i get secrets \
  --namespace "$WORKLOAD_NAMESPACE" \
  --as="system:serviceaccount:$SYSTEM_NAMESPACE:$RELEASE-mini-ai-cloud-worker" \
  >/dev/null; then
  echo "worker ServiceAccount unexpectedly has Secret read permission" >&2
  exit 1
fi

"${HELM[@]}" uninstall "$RELEASE" \
  --namespace "$SYSTEM_NAMESPACE" \
  --wait \
  --timeout 3m

"${KUBECTL[@]}" get namespace "$SYSTEM_NAMESPACE" "$WORKLOAD_NAMESPACE" >/dev/null
"${KUBECTL[@]}" --namespace "$SYSTEM_NAMESPACE" get secret "$APP_SECRET" >/dev/null
"${KUBECTL[@]}" --namespace "$WORKLOAD_NAMESPACE" get secret "$DATA_SECRET" >/dev/null

wait_for_release_cleanup "$SYSTEM_NAMESPACE"
wait_for_release_cleanup "$WORKLOAD_NAMESPACE"

echo "KIND_HELM_SMOKE_PASS run_id=$RUN_ID external_resources_preserved=true"
