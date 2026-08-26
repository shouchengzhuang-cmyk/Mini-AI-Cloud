#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_ROOT="${XDG_RUNTIME_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}"
BUILD_DIR="${KIND_SERVING_STATE_DIR:-$STATE_ROOT/mini-ai-cloud-kind-serving-$UID}"
LEGACY_BUILD_DIR="$ROOT_DIR/build/kind-serving"
KUBECONFIG_FILE="$BUILD_DIR/kubeconfig"
CREDENTIALS_FILE="$BUILD_DIR/credentials.env"
API_KEY_FILE="$BUILD_DIR/api-key"
IMAGE_ARCHIVE="$BUILD_DIR/images.tar"
CLUSTER_NAME="mini-ai-cloud-serving-v4a"
NAMESPACE="mini-ai-cloud-serving"
APP_IMAGE="mini-ai-cloud:kind-serving-v4a"
POSTGRES_IMAGE="postgres:16-alpine"
REDIS_IMAGE="redis:7.4-alpine"
PULL_IMAGES="${KIND_SERVING_PULL:-true}"
BASE_URL="http://127.0.0.1:18080"
UV_BIN="${UV:-uv}"
SERVING_POD_SELECTOR="mini-ai-cloud/managed=true,mini-ai-cloud/resource-kind=serving-pod"

not_run() {
  printf 'NOT RUN: %s\n' "$*" >&2
  exit 2
}

ensure_private_state_dir() {
  local mode owner
  case "$BUILD_DIR" in
    /*) ;;
    *) not_run "KIND_SERVING_STATE_DIR must resolve to an absolute path" ;;
  esac
  [[ ! -L "$BUILD_DIR" ]] || not_run "Kind serving state directory must not be a symlink"
  mkdir -p -- "$BUILD_DIR"
  chmod 700 -- "$BUILD_DIR" \
    || not_run "Kind serving state directory permissions could not be restricted"
  mode="$(stat -c '%a' -- "$BUILD_DIR")" \
    || not_run "Kind serving state directory permissions could not be inspected"
  owner="$(stat -c '%u' -- "$BUILD_DIR")" \
    || not_run "Kind serving state directory owner could not be inspected"
  if [[ "$owner" != "$EUID" || "$mode" != 700 ]]; then
    not_run "Kind serving state directory must be owned by the current user with mode 700"
  fi
}

ensure_private_file() {
  local path="$1"
  local description="$2"
  local mode owner
  [[ -f "$path" && ! -L "$path" ]] || not_run "$description is not a regular private file"
  chmod 600 -- "$path" || not_run "$description permissions could not be restricted"
  mode="$(stat -c '%a' -- "$path")" \
    || not_run "$description permissions could not be inspected"
  owner="$(stat -c '%u' -- "$path")" \
    || not_run "$description owner could not be inspected"
  if [[ "$owner" != "$EUID" || "$mode" != 600 ]]; then
    not_run "$description must be owned by the current user with mode 600"
  fi
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
  docker buildx version >/dev/null 2>&1 \
    || not_run "Docker Buildx is required for a loadable single-platform image"
  kind version >/dev/null 2>&1 || not_run "kind is installed but not executable"
  kubectl version --client=true >/dev/null 2>&1 \
    || not_run "kubectl client is installed but not executable"
  case "$APP_IMAGE" in
    *:latest|latest) not_run "the Kind serving image must use a fixed non-latest tag" ;;
  esac
  case "$PULL_IMAGES" in
    true|false) ;;
    *) not_run "KIND_SERVING_PULL must be true or false" ;;
  esac
}

cluster_exists() {
  kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"
}

kubectl_kind() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"
}

ensure_kubeconfig() {
  ensure_private_state_dir
  if [[ ! -s "$KUBECONFIG_FILE" ]]; then
    kind get kubeconfig --name "$CLUSTER_NAME" >"$KUBECONFIG_FILE"
  fi
  ensure_private_file "$KUBECONFIG_FILE" "isolated kubeconfig"
}

ensure_credentials() {
  ensure_private_state_dir
  if [[ ! -s "$CREDENTIALS_FILE" ]]; then
    "$UV_BIN" run python scripts/kind_serving_e2e.py generate-credentials \
      --output "$CREDENTIALS_FILE"
  fi
  ensure_private_file "$CREDENTIALS_FILE" "Kind serving credentials"
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
  ensure_private_state_dir

  local created=false
  if ! cluster_exists; then
    printf 'Creating isolated Kind cluster %s...\n' "$CLUSTER_NAME"
    kind create cluster \
      --name "$CLUSTER_NAME" \
      --config deploy/kind-serving/kind-config.yaml \
      --kubeconfig "$KUBECONFIG_FILE" \
      --wait 120s
    ensure_private_file "$KUBECONFIG_FILE" "isolated kubeconfig"
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

  local platform
  platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
  case "$platform" in
    linux/*) ;;
    *) not_run "Kind serving requires a Linux Docker server platform" ;;
  esac

  local -a pull_flag=()
  if [[ "$PULL_IMAGES" == true ]]; then
    pull_flag=(--pull)
  else
    docker image inspect "$POSTGRES_IMAGE" "$REDIS_IMAGE" >/dev/null \
      || not_run "KIND_SERVING_PULL=false requires cached PostgreSQL and Redis images"
    printf 'Using explicitly requested local image cache; remote pulls are disabled.\n'
  fi

  printf 'Building fixed Kind application image %s...\n' "$APP_IMAGE"
  docker buildx build "${pull_flag[@]}" --load --platform "$platform" \
    --provenance=false --sbom=false \
    -f docker/Dockerfile -t "$APP_IMAGE" .
  if [[ "$PULL_IMAGES" == true ]]; then
    docker pull --platform "$platform" "$POSTGRES_IMAGE"
    docker pull --platform "$platform" "$REDIS_IMAGE"
  fi

  # Docker's containerd image store keeps local tags as OCI indexes. A normal
  # `kind load docker-image` asks containerd to import every descriptor in those
  # indexes, including attestations or remote platforms whose content is absent.
  # Export only the Docker server platform so the Kind archive is self-contained.
  rm -f -- "$IMAGE_ARCHIVE"
  if ! docker image save --platform "$platform" --output "$IMAGE_ARCHIVE" \
    "$APP_IMAGE" "$POSTGRES_IMAGE" "$REDIS_IMAGE"; then
    rm -f -- "$IMAGE_ARCHIVE"
    return 1
  fi
  if ! kind load image-archive --name "$CLUSTER_NAME" "$IMAGE_ARCHIVE"; then
    rm -f -- "$IMAGE_ARCHIVE"
    return 1
  fi
  rm -f -- "$IMAGE_ARCHIVE"

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
    diagnostic_kubectl -n "$NAMESPACE" logs job/mini-ai-cloud-migrate --tail=200 >&2
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
  ensure_private_file "$KUBECONFIG_FILE" "isolated kubeconfig"

  export KIND_SERVING_E2E=1
  export KIND_SERVING_BASE_URL="$BASE_URL"
  export KIND_SERVING_KUBECONFIG="$KUBECONFIG_FILE"
  export KIND_SERVING_NAMESPACE="$NAMESPACE"
  export KIND_SERVING_CLUSTER_NAME="$CLUSTER_NAME"
  export KIND_SERVING_APP_IMAGE="$APP_IMAGE"
  export KIND_SERVING_API_KEY_FILE="$API_KEY_FILE"
  "$UV_BIN" run pytest -q -rs tests/e2e/test_kind_serving.py
}

redact_diagnostic_output() {
  local line name value
  local -a sensitive_values=()
  if [[ -s "$CREDENTIALS_FILE" ]]; then
    while IFS='=' read -r name value; do
      case "$name" in
        KIND_SERVING_*)
          [[ -n "$value" ]] && sensitive_values+=("$value")
          ;;
      esac
    done <"$CREDENTIALS_FILE"
  fi
  if [[ -s "$API_KEY_FILE" ]]; then
    value="$(<"$API_KEY_FILE")"
    [[ -n "$value" ]] && sensitive_values+=("$value")
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
    for value in "${sensitive_values[@]}"; do
      line="${line//"$value"/[REDACTED]}"
    done
    printf '%s\n' "$line"
  done
}

diagnostic_kubectl() {
  kubectl_kind --request-timeout=15s "$@" 2>&1 | redact_diagnostic_output || true
}

diagnostics() {
  set +e
  cd "$ROOT_DIR"

  printf '%s\n' '=== Kind serving tool versions ==='
  if command -v docker >/dev/null 2>&1; then
    docker version || true
  else
    printf '%s\n' 'docker: unavailable'
  fi
  if command -v kind >/dev/null 2>&1; then
    kind version || true
  else
    printf '%s\n' 'kind: unavailable'
  fi
  if command -v kubectl >/dev/null 2>&1; then
    kubectl version --client=true || true
  else
    printf '%s\n' 'kubectl: unavailable'
  fi

  if ! command -v kind >/dev/null 2>&1 || ! cluster_exists; then
    printf 'Kind cluster %s is unavailable; cluster diagnostics were not collected.\n' \
      "$CLUSTER_NAME"
    return 0
  fi
  if ! command -v kubectl >/dev/null 2>&1; then
    printf '%s\n' 'kubectl is unavailable; cluster diagnostics were not collected.'
    return 0
  fi
  if [[ ! -s "$KUBECONFIG_FILE" ]] && ! ensure_kubeconfig; then
    printf '%s\n' 'The isolated kubeconfig could not be recovered.'
    return 0
  fi

  printf '%s\n' '=== Kubernetes server version ==='
  diagnostic_kubectl version
  printf '%s\n' '=== Pods in all namespaces ==='
  diagnostic_kubectl get pods -A -o wide
  printf '%s\n' '=== Services in all namespaces ==='
  diagnostic_kubectl get services -A -o wide
  printf '%s\n' '=== Events in all namespaces ==='
  diagnostic_kubectl get events -A --sort-by=.lastTimestamp
  printf '%s\n' '=== Pod descriptions in the serving namespace ==='
  diagnostic_kubectl -n "$NAMESPACE" describe pods
  printf '%s\n' '=== API and embedded controller logs ==='
  diagnostic_kubectl -n "$NAMESPACE" logs deployment/mini-ai-cloud-api \
    --all-containers=true --prefix=true --tail=500

  printf '%s\n' '=== Non-ready or failed serving Pod logs ==='
  local pod_ref phase ready waiting_reason
  local found_problem_pod=false
  local -a serving_pods=()
  mapfile -t serving_pods < <(
    kubectl_kind --request-timeout=15s -n "$NAMESPACE" get pods \
      -l "$SERVING_POD_SELECTOR" -o name 2>/dev/null
  )
  for pod_ref in "${serving_pods[@]}"; do
    phase="$(
      kubectl_kind --request-timeout=15s -n "$NAMESPACE" get "$pod_ref" \
        -o jsonpath='{.status.phase}' 2>/dev/null
    )"
    ready="$(
      kubectl_kind --request-timeout=15s -n "$NAMESPACE" get "$pod_ref" \
        -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}' \
        2>/dev/null
    )"
    if [[ "$phase" == "Running" && "$ready" == "True" ]]; then
      continue
    fi
    found_problem_pod=true
    waiting_reason="$(
      kubectl_kind --request-timeout=15s -n "$NAMESPACE" get "$pod_ref" \
        -o jsonpath='{range .status.containerStatuses[*]}{.state.waiting.reason}{" "}{end}' \
        2>/dev/null
    )"
    printf '%s phase=%s ready=%s waiting=%s\n' \
      "$pod_ref" "${phase:-unknown}" "${ready:-unknown}" "${waiting_reason:-none}"
    diagnostic_kubectl -n "$NAMESPACE" logs "$pod_ref" \
      --all-containers=true --prefix=true --tail=200
    diagnostic_kubectl -n "$NAMESPACE" logs "$pod_ref" \
      --all-containers=true --prefix=true --previous --tail=200
  done
  if [[ "$found_problem_pod" == false ]]; then
    printf '%s\n' 'No non-ready or failed serving Pods are present.'
  fi
}

down() {
  cd "$ROOT_DIR"
  local delete_status=0
  if ! command -v kind >/dev/null 2>&1; then
    printf '%s\n' 'kind is unavailable; the cluster could not be deleted.' >&2
    delete_status=1
  elif kind delete cluster --name "$CLUSTER_NAME"; then
    printf 'Ensured isolated Kind cluster %s is absent.\n' "$CLUSTER_NAME"
  else
    printf 'Failed to delete isolated Kind cluster %s.\n' "$CLUSTER_NAME" >&2
    delete_status=1
  fi
  rm -f -- "$API_KEY_FILE" "$CREDENTIALS_FILE" "$KUBECONFIG_FILE" "$IMAGE_ARCHIVE"
  rmdir "$BUILD_DIR" 2>/dev/null || true
  rm -f -- \
    "$LEGACY_BUILD_DIR/api-key" \
    "$LEGACY_BUILD_DIR/credentials.env" \
    "$LEGACY_BUILD_DIR/kubeconfig" \
    "$LEGACY_BUILD_DIR/images.tar"
  rmdir "$LEGACY_BUILD_DIR" 2>/dev/null || true
  printf 'Removed local Kind serving credentials and kubeconfig.\n'
  return "$delete_status"
}

usage() {
  printf 'Usage: %s {up|test|diagnostics|down}\n' "$0" >&2
  exit 2
}

case "${1:-}" in
  up) up ;;
  test) test_e2e ;;
  diagnostics) diagnostics ;;
  down) down ;;
  *) usage ;;
esac
