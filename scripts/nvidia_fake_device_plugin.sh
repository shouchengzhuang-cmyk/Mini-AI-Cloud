#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_ROOT="${XDG_RUNTIME_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}"
BUILD_DIR="${KIND_SERVING_STATE_DIR:-$STATE_ROOT/mini-ai-cloud-kind-serving-$UID}"
KUBECONFIG_FILE="${KIND_SERVING_KUBECONFIG:-$BUILD_DIR/kubeconfig}"
NAMESPACE="${KIND_SERVING_WORKLOAD_NAMESPACE:-${KIND_SERVING_NAMESPACE:-mini-ai-cloud-serving}}"
PLUGIN_NAME="${KIND_SERVING_FAKE_PLUGIN_NAME:-mini-ai-cloud-sample-device-plugin}"
POD_NAME="${KIND_SERVING_FAKE_ALLOCATION_NAME:-mini-ai-cloud-fake-device-allocation}"

not_run() {
  printf 'NOT RUN: %s\n' "$*" >&2
  exit 2
}

kubectl_kind() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"
}

require_dns_label() {
  local value="$1"
  local description="$2"
  if ((${#value} > 63)) || [[ ! "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    not_run "$description must be a DNS-1123 label"
  fi
}

render_plugin_manifest() {
  sed "s/mini-ai-cloud-sample-device-plugin/$PLUGIN_NAME/g" \
    deploy/nvidia-runtime/00-fake-device-plugin.yaml
}

render_allocation_manifest() {
  sed \
    -e "s/mini-ai-cloud-fake-device-allocation/$POD_NAME/g" \
    -e "s/namespace: mini-ai-cloud-serving/namespace: $NAMESPACE/" \
    deploy/nvidia-runtime/10-fake-device-allocation.yaml
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
  require_dns_label "$NAMESPACE" "workload namespace"
  require_dns_label "$PLUGIN_NAME" "fake Device Plugin name"
  require_dns_label "$POD_NAME" "fake allocation Pod name"
  cd "$ROOT_DIR"
  trap cleanup EXIT
  cleanup

  render_plugin_manifest | kubectl_kind apply -f - >/dev/null
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

  render_allocation_manifest | kubectl_kind apply -f - >/dev/null
  kubectl_kind -n "$NAMESPACE" wait --for=condition=Ready pod/"$POD_NAME" --timeout=120s
  local node_name
  node_name="$(
    kubectl_kind -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}'
  )"
  [[ -n "$node_name" ]] || not_run "fake device allocation Pod was not bound to a node"

  printf '%s\n' \
    'KIND_CONTRACT_VALIDATED: Kind allocated a pinned fake extended resource.' \
    'REAL_HW_NOT_RUN: this test is not NVIDIA hardware or vLLM evidence.'
}

case "${1:-}" in
  test) run_test ;;
  *) printf 'Usage: %s test\n' "$0" >&2; exit 2 ;;
esac
