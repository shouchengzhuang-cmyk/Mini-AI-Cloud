#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build/kind-serving"
KUBECONFIG_FILE="$BUILD_DIR/kubeconfig"
CREDENTIALS_FILE="$BUILD_DIR/credentials.env"
API_KEY_FILE="$BUILD_DIR/api-key"
CLUSTER_NAME="mini-ai-cloud-serving-v4a"
NAMESPACE="mini-ai-cloud-serving"
APP_IMAGE="mini-ai-cloud:kind-serving-v4a"
POSTGRES_IMAGE="postgres:16-alpine"
REDIS_IMAGE="redis:7.4-alpine"
BASE_URL="http://127.0.0.1:18080"
UV_BIN="${UV:-uv}"

not_run() {
  printf 'NOT RUN: %s\n' "$*" >&2
  exit 2
}

preflight() {
  local tool
  local -a missing=()
  for tool in docker kind kubectl "$UV_BIN"; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  if ((${#missing[@]})); then
    not_run "required commands are unavailable: ${missing[*]}"
  fi
  docker info >/dev/null 2>&1 || not_run "Docker Engine is not reachable"
  kind version >/dev/null 2>&1 || not_run "kind is installed but not executable"
  kubectl version --client=true >/dev/null 2>&1 \
    || not_run "kubectl client is installed but not executable"
  case "$APP_IMAGE" in
    *:latest|latest) not_run "the Kind serving image must use a fixed non-latest tag" ;;
  esac
}

cluster_exists() {
  kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"
}

kubectl_kind() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"
}

ensure_kubeconfig() {
  mkdir -p "$BUILD_DIR"
  if [[ ! -s "$KUBECONFIG_FILE" ]]; then
    kind get kubeconfig --name "$CLUSTER_NAME" >"$KUBECONFIG_FILE"
    chmod 600 "$KUBECONFIG_FILE"
  fi
}

ensure_credentials() {
  if [[ ! -s "$CREDENTIALS_FILE" ]]; then
    "$UV_BIN" run python scripts/kind_serving_e2e.py generate-credentials \
      --output "$CREDENTIALS_FILE"
  fi
  chmod 600 "$CREDENTIALS_FILE"
  # Generated values contain only URL-safe characters and are never echoed.
  set -a
  # shellcheck disable=SC1090
  source "$CREDENTIALS_FILE"
  set +a
  : "${KIND_SERVING_POSTGRES_PASSWORD:?missing generated PostgreSQL password}"
  : "${KIND_SERVING_BOOTSTRAP_TOKEN:?missing generated bootstrap token}"
  : "${KIND_SERVING_API_KEY_PEPPER:?missing generated API key pepper}"
  : "${KIND_SERVING_WORKER_AUTH_TOKEN:?missing generated worker token}"
  : "${KIND_SERVING_USER_PASSWORD:?missing generated test user password}"
}

apply_generated_secret() {
  local database_url
  database_url="postgresql+asyncpg://task:${KIND_SERVING_POSTGRES_PASSWORD}@postgres:5432/task_platform"
  kubectl_kind -n "$NAMESPACE" create secret generic mini-ai-cloud-kind-secrets \
    --from-literal="postgres-password=$KIND_SERVING_POSTGRES_PASSWORD" \
    --from-literal="database-url=$database_url" \
    --from-literal="bootstrap-token=$KIND_SERVING_BOOTSTRAP_TOKEN" \
    --from-literal="api-key-pepper=$KIND_SERVING_API_KEY_PEPPER" \
    --from-literal="worker-auth-token=$KIND_SERVING_WORKER_AUTH_TOKEN" \
    --dry-run=client -o yaml \
    | kubectl_kind apply -f - >/dev/null
}

up() {
  preflight
  cd "$ROOT_DIR"
  mkdir -p "$BUILD_DIR"

  local created=false
  if ! cluster_exists; then
    printf 'Creating isolated Kind cluster %s...\n' "$CLUSTER_NAME"
    kind create cluster \
      --name "$CLUSTER_NAME" \
      --config deploy/kind-serving/kind-config.yaml \
      --kubeconfig "$KUBECONFIG_FILE" \
      --wait 120s
    chmod 600 "$KUBECONFIG_FILE"
    created=true
    rm -f -- "$API_KEY_FILE"
  else
    printf 'Reusing isolated Kind cluster %s.\n' "$CLUSTER_NAME"
    ensure_kubeconfig
  fi

  if [[ "$created" == false && ! -s "$CREDENTIALS_FILE" ]]; then
    not_run "the cluster exists but its local credentials are missing; run make kind-serving-down first"
  fi
  ensure_credentials

  printf 'Building fixed Kind application image %s...\n' "$APP_IMAGE"
  docker build --pull -f docker/Dockerfile -t "$APP_IMAGE" .
  docker pull "$POSTGRES_IMAGE"
  docker pull "$REDIS_IMAGE"
  kind load docker-image --name "$CLUSTER_NAME" \
    "$APP_IMAGE" "$POSTGRES_IMAGE" "$REDIS_IMAGE"

  kubectl_kind apply -f deploy/kind-serving/00-namespace-rbac.yaml >/dev/null
  apply_generated_secret
  kubectl_kind apply -f deploy/kind-serving/10-data-stores.yaml >/dev/null
  kubectl_kind -n "$NAMESPACE" rollout status deployment/postgres --timeout=120s
  kubectl_kind -n "$NAMESPACE" rollout status deployment/redis --timeout=120s

  kubectl_kind -n "$NAMESPACE" delete job mini-ai-cloud-migrate \
    --ignore-not-found=true --wait=true >/dev/null
  kubectl_kind apply -f deploy/kind-serving/20-migrate.yaml >/dev/null
  if ! kubectl_kind -n "$NAMESPACE" wait \
    --for=condition=complete job/mini-ai-cloud-migrate --timeout=120s; then
    kubectl_kind -n "$NAMESPACE" logs job/mini-ai-cloud-migrate --tail=200 >&2 || true
    exit 1
  fi

  kubectl_kind apply -f deploy/kind-serving/30-api.yaml >/dev/null
  # The fixed E2E tag is intentionally stable; restart so a reused cluster runs
  # the freshly loaded image instead of retaining an older container instance.
  kubectl_kind -n "$NAMESPACE" rollout restart deployment/mini-ai-cloud-api >/dev/null
  kubectl_kind -n "$NAMESPACE" rollout status deployment/mini-ai-cloud-api --timeout=180s
  "$UV_BIN" run python scripts/kind_serving_e2e.py wait-ready \
    --base-url "$BASE_URL" --timeout 120
  printf 'Kind serving stack is ready at %s.\n' "$BASE_URL"
}

test_e2e() {
  preflight
  cd "$ROOT_DIR"
  cluster_exists || not_run "Kind cluster '$CLUSTER_NAME' does not exist; run make kind-serving-up"
  [[ -s "$KUBECONFIG_FILE" ]] \
    || not_run "isolated kubeconfig is missing; run make kind-serving-up"
  [[ -s "$CREDENTIALS_FILE" ]] \
    || not_run "Kind serving credentials are missing; run make kind-serving-up"
  ensure_credentials

  export KIND_SERVING_E2E=1
  export KIND_SERVING_BASE_URL="$BASE_URL"
  export KIND_SERVING_KUBECONFIG="$KUBECONFIG_FILE"
  export KIND_SERVING_NAMESPACE="$NAMESPACE"
  export KIND_SERVING_CLUSTER_NAME="$CLUSTER_NAME"
  export KIND_SERVING_APP_IMAGE="$APP_IMAGE"
  export KIND_SERVING_API_KEY_FILE="$API_KEY_FILE"
  "$UV_BIN" run pytest -q -rs tests/e2e/test_kind_serving.py
}

down() {
  preflight
  cd "$ROOT_DIR"
  if cluster_exists; then
    kind delete cluster --name "$CLUSTER_NAME"
    printf 'Deleted isolated Kind cluster %s.\n' "$CLUSTER_NAME"
  else
    printf 'Isolated Kind cluster %s is already absent.\n' "$CLUSTER_NAME"
  fi
  rm -f -- "$API_KEY_FILE" "$CREDENTIALS_FILE" "$KUBECONFIG_FILE"
  rmdir "$BUILD_DIR" 2>/dev/null || true
  printf 'Removed local Kind serving credentials and kubeconfig.\n'
}

usage() {
  printf 'Usage: %s {up|test|down}\n' "$0" >&2
  exit 2
}

case "${1:-}" in
  up) up ;;
  test) test_e2e ;;
  down) down ;;
  *) usage ;;
esac
