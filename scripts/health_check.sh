#!/bin/bash
set -euo pipefail

# Health check script for the osquery-datawarehouse-demo cluster.
# Runs through each bootstrap phase, waiting up to PHASE_TIMEOUT for each.
# If the cluster is already up, it completes quickly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$REPO_DIR/terraform"

PHASE_TIMEOUT=600  # 10 minutes per phase
POLL_INTERVAL=5    # seconds between retries
START_TIME=$(date +%s)

# ── helpers ──────────────────────────────────────────────────────────────────

ok()   { echo -e "  \033[32m✓\033[0m $*"; }
fail() { echo -e "  \033[31m✗\033[0m $*"; exit 1; }
info() { echo -e "  \033[90m  $*\033[0m"; }

# Wait for a command to succeed, polling every POLL_INTERVAL seconds.
# Usage: phase_wait "description" cmd [args...]
phase_wait() {
  local desc="$1"; shift
  local elapsed=0
  info "Waiting for: $desc"
  while ! "$@" >/dev/null 2>&1; do
    if (( elapsed >= PHASE_TIMEOUT )); then
      fail "Timed out after ${PHASE_TIMEOUT}s waiting for: $desc"
    fi
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
    if (( elapsed % 30 == 0 )); then
      info "  (${elapsed}s) still waiting"
    fi
  done
  ok "$desc (${elapsed}s)"
}

# ── resolve hostname ────────────────────────────────────────────────────────

phase() {
  echo ""
  echo -e "\033[1m▶ $*\033[0m"
}

# Refresh all Argo CD apps by adding the refresh annotation.
# Used to unstick apps that report Progressing despite being healthy.
argo_refresh_all() {
  kubectl annotate applications -n argocd \
    --all argocd.argoproj.io/refresh=hard --overwrite 2>/dev/null || true
}

# wait_on_ns <namespace>
#   Discover and wait for every Deployment and StatefulSet in the namespace.
wait_on_ns() {
  local ns="$1"

  # Wait for Argo CD to finish creating resources in this namespace.
  # First wait for the app itself to exist, then wait for it to be Synced.
  # Every 2 minutes, refresh all Argo apps to unstick any that are stuck Progressing.
  local app_name=""
  local app_elapsed=0
  info "Waiting for Argo app targeting namespace $ns"
  while [[ -z "$app_name" ]]; do
    if (( app_elapsed >= PHASE_TIMEOUT )); then
      fail "Timed out after ${PHASE_TIMEOUT}s waiting for Argo app targeting namespace $ns"
    fi
    app_name=$(kubectl get applications -n argocd -o json 2>/dev/null \
      | jq -r --arg ns "$ns" '.items[] | select(.spec.destination.namespace == $ns and .spec.type != "Directory") | .metadata.name' | head -1 || true)
    if [[ -z "$app_name" ]]; then
      if (( app_elapsed > 0 && app_elapsed % 120 == 0 )); then
        info "  (${app_elapsed}s) refreshing all Argo apps to unstick Progressing state"
        argo_refresh_all
      fi
      sleep "$POLL_INTERVAL"
      app_elapsed=$((app_elapsed + POLL_INTERVAL))
    fi
  done
  ok "Found Argo app $app_name for namespace $ns (${app_elapsed}s)"
  phase_wait "Argo app $app_name synced" kubectl \
    wait --for=jsonpath={.status.sync.status}=Synced --timeout=5s "application/$app_name" -n argocd

  # Deployments
  local deps
  deps=$(kubectl get deployments -n "$ns" -o name 2>/dev/null | sed 's|deployment.apps/||' || true)
  for dep in $deps; do
    [[ -z "$dep" ]] && continue
    phase_wait "deployment/$dep in $ns ready" kubectl \
      wait --for=condition=available --timeout=5s "deployment/$dep" -n "$ns"
  done

  # StatefulSets
  local sts_list
  sts_list=$(kubectl get statefulsets -n "$ns" -o name 2>/dev/null | sed 's|statefulset.apps/||' || true)
  for sts in $sts_list; do
    [[ -z "$sts" ]] && continue
    phase_wait "statefulset/$sts in $ns ready" bash -c "kubectl get -n $ns statefulset $sts -o json | jq -e '.status.readyReplicas == .spec.replicas and .status.readyReplicas > 0'"
  done
}

# ── jobs allowlist ───────────────────────────────────────────────────────────
# Static list of jobs to check (namespace/name). Recurring Flink jobs
# (iceberg-optimize, iceberg-cleanup) are excluded - they are deleted
# after completion and recreated by CronJobs.
JOBS_ALLOWLIST=(
  "build-images/build-data-warehouse-mcp"
  "build-images/build-dwh-chat"
  "build-images/build-flink-osquery-job"
  "build-images/build-superset"
  "garage/garage-init"
  "polaris/polaris-bootstrap"
  "polaris/polaris-init"
  "superset/superset-init-db"
)

# sync_wave <wave> <namespace1> [namespace2 ...]
sync_wave() {
  local wave="$1"; shift
  echo ""
  info "── sync-wave $wave: $* ──"
  for ns in "$@"; do
    wait_on_ns "$ns"
  done

  # Check allowed jobs whose namespace is in this wave
  for entry in "${JOBS_ALLOWLIST[@]}"; do
    local j_ns="${entry%%/*}"
    local j_name="${entry#*/}"
    local match=false
    for wns in "$@"; do
      if [[ "$j_ns" == "$wns" ]]; then match=true; break; fi
    done
    $match || continue
    phase_wait "job/$j_name in $j_ns completed" kubectl \
      wait --for=condition=complete --timeout=5s "job/$j_name" -n "$j_ns"
  done
}

phase "1 / 10  DNS propagation"
sleep 10
ok "Waited 10s for DNS propagation"

phase "2 / 10  Resolve cluster hostname"
pushd "$TF_DIR" >/dev/null
HOSTNAME=$(tofu output -raw cloudflare_root_record 2>/dev/null) || \
  fail "Cannot read 'cloudflare_root_record' from terraform output. Run 'tofu apply' first."
popd >/dev/null
[[ -n "$HOSTNAME" ]] || fail "cloudflare_root_record is empty"

phase_wait "DNS resolves $HOSTNAME via 1.1.1.1" bash -c '
  dig @1.1.1.1 +short +time=5 +tries=2 '"$HOSTNAME"' | grep -qE "^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$"
'
IP=$(dig @1.1.1.1 +short +time=5 +tries=2 "$HOSTNAME" | head -1)
ok "Hostname = $HOSTNAME -> $IP"

# ── SSH ──────────────────────────────────────────────────────────────────────

SSH="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

phase "3 / 10  SSH access"
phase_wait "SSH to $HOSTNAME" $SSH "ubuntu@$HOSTNAME" true

# ── k3s ──────────────────────────────────────────────────────────────────────

phase "4 / 10  k3s installed"
phase_wait "k3s kubectl get nodes" $SSH "ubuntu@$HOSTNAME" sudo k3s kubectl get nodes --no-headers

# ── kubeconfig ───────────────────────────────────────────────────────────────
KUBECONFIG_DIR=$(mktemp -d)
trap 'rm -rf "$KUBECONFIG_DIR"' EXIT
KUBECONFIG="$KUBECONFIG_DIR/config"

$SSH "ubuntu@$HOSTNAME" "sudo cat /etc/rancher/k3s/k3s.yaml" > "$KUBECONFIG"

# Update the server URL to use the cloudflare domain (port 6443)
sed -i -e "s|https://127.0.0.1:6443|https://$HOSTNAME:6443|" "$KUBECONFIG"
export KUBECONFIG

ok "kubeconfig ready → $KUBECONFIG"

# ── Argo CD ──────────────────────────────────────────────────────────────────

phase "5 / 10  Argo CD ready"
phase_wait "argocd-server available" kubectl \
  wait --for=condition=available --timeout=5s deployment/argocd-server -n argocd
phase_wait "argocd-repo-server available" kubectl \
  wait --for=condition=available --timeout=5s deployment/argocd-repo-server -n argocd

# ── Wave -15: foundational cluster services ──

phase "6 / 10  Foundational services (sync-wave -15)"
sync_wave -15 local-path-storage registry replicator cert-manager

# ── build-images jobs ───────────────────────────────────────────────────────

phase "7 / 10  Build-images jobs completed"
for job in build-superset build-dwh-chat build-flink-osquery-job build-data-warehouse-mcp; do
  phase_wait "Job $job completed" kubectl \
    wait --for=condition=complete --timeout=5s "job/$job" -n build-images
done

# ── TLS certs ────────────────────────────────────────────────────────────────

phase "8 / 10  TLS certificates ready"
phase_wait "ingress-tls certificate ready" kubectl \
  wait --for=condition=Ready --timeout=5s certificate/ingress-tls -n cert-manager
phase_wait "ingress-tls-pkcs8 certificate ready" kubectl \
  wait --for=condition=Ready --timeout=5s certificate/ingress-tls-pkcs8 -n cert-manager

phase "9 / 10  Services by sync-wave"

# ── Wave -5: shared infra ──
sync_wave -5 cloudnative-pg password-generator traefik observability

# ── Wave 0: osquery-datawarehouse (meta app, no own pods) ──
echo ""
info "── sync-wave 0: osquery-datawarehouse (meta app) ──"

# ── Wave 1: data ingestion layer ──
sync_wave 1 garage strimzi-operator kafka

# ── Wave 2: data lake layer ──
sync_wave 2 trino polaris

# ── Wave 3: application layer ──
sync_wave 3 data-warehouse-mcp dwh-chat flink superset

# ── Phase 10: verify all ingresses ──────────────────────────────────────────
# Static list of expected ingresses (namespace/name).
# Derived from the charts deployed above.

phase "10 / 10  Ingresses"
for ing in \
    "argocd/argo-server" \
    "data-warehouse-mcp/data-warehouse-mcp-ingress" \
    "dwh-chat/dwh-chat-ingress" \
    "flink/flink-ingress" \
    "observability/grafana-server" \
    "superset/superset-ingress"; do
  ns="${ing%%/*}"
  name="${ing#*/}"
  phase_wait "ingress $name in $ns has ADDRESS" bash -c "
    kubectl get ingress $name -n $ns -o json | jq -re '.status.loadBalancer.ingress[0].ip // empty' | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$'
  "
done

# Also check the TCP IngressRoute for Kafka
phase_wait "IngressRouteTCP kafka in kafka has endpoints" kubectl \
  get ingressroutetcp kafka -n kafka -o name

# ── done ─────────────────────────────────────────────────────────────────────

echo ""
echo -e "\033[1;32m✓ All phases passed - cluster is fully healthy.\033[0m"
echo -e "  Total time: $(( $(date +%s) - START_TIME ))s"
echo ""
