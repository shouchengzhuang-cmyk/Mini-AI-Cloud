#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_ROOT="${XDG_RUNTIME_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}"
BUILD_DIR="${KIND_SERVING_STATE_DIR:-$STATE_ROOT/mini-ai-cloud-kind-serving-$UID}"
KUBECONFIG_FILE="$BUILD_DIR/kubeconfig"
NAMESPACE="mini-ai-cloud-serving"
PLUGIN_NAME="mini-ai-cloud-sample-device-plugin"
POD_NAME="mini-ai-cloud-fake-device-allocation"

not_run() {
  printf 'NOT RUN: %s\n' "$*" >&2
  exit 2
}

kubectl_kind() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"
}

cleanup() {
  kubectl_kind -n "$NAMESPACE" delete pod "$POD_NAME" \
    --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
  kubectl_kind -n kube-system delete daemonset "$PLUGIN_NAME" \
    --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
}

run_test() {
  command -v kubectl >/dev/null 2>&1 || not_run "kubectl is unavailable"
  [[ -s "$KUBECONFIG_FILE" ]] || not_run "isolated Kind kubeconfig is unavailable"
  cd "$ROOT_DIR"
  trap cleanup EXIT
  cleanup

  kubectl_kind apply -f deploy/nvidia-runtime/00-fake-device-plugin.yaml >/dev/null
  kubectl_kind -n kube-system rollout status daemonset/"$PLUGIN_NAME" --timeout=120s

  local advertised=""
  for _attempt in $(seq 1 60); do
    advertised="$(
      kubectl_kind get nodes \
        -o go-template='{{range .items}}{{index .status.allocatable "example.com/resource"}}{{"\n"}}{{end}}'
    )"
    if grep -Eq '^[1-9][0-9]*$' <<<"$advertised"; then
      break
    fi
    sleep 1
  done
  grep -Eq '^[1-9][0-9]*$' <<<"$advertised" \
    || not_run "sample device plugin did not advertise example.com/resource"

  kubectl_kind apply -f deploy/nvidia-runtime/10-fake-device-allocation.yaml >/dev/null
  kubectl_kind -n "$NAMESPACE" wait --for=condition=Ready pod/"$POD_NAME" --timeout=120s
  local node_name
  node_name="$(
    kubectl_kind -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}'
  )"
  [[ -n "$node_name" ]] || not_run "fake device allocation Pod was not bound to a node"

  printf '%s\n' \
    'MANIFEST_VALIDATED: Kubernetes allocated a pinned fake extended resource.' \
    'REAL_HW_NOT_RUN: this test is not NVIDIA hardware or vLLM evidence.'
}

case "${1:-}" in
  test) run_test ;;
  *) printf 'Usage: %s test\n' "$0" >&2; exit 2 ;;
esac
