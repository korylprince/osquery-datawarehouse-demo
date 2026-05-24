#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get upgrade -y
apt-get install -y curl git
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server ${cluster_hostname_flag} --disable-helm-controller --disable traefik,local-storage" sh -s -

manifest_path="/opt/cluster_init.yaml"
argocd_install_path="/opt/argocd-install"
argocd_namespace="argocd"

until k3s kubectl get nodes >/dev/null 2>&1; do
  sleep 5
done

# pre-apply some CRDs. CRDs applied with helm by argo tend to get stuck.
git clone --depth 1 --branch v1.28.1 https://github.com/cloudnative-pg/cloudnative-pg.git /tmp/cloudnative-pg
k3s kubectl apply --server-side --force-conflicts -k /tmp/cloudnative-pg/config/crd

git clone --depth 1 --branch v1.31.0 https://github.com/traefik/hub-crds.git /tmp/hub-crds
k3s kubectl apply --server-side --force-conflicts -f /tmp/hub-crds/pkg/apis/hub/v1alpha1/crd/

k3s kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/traefik/traefik/v3.7/docs/content/reference/dynamic-configuration/kubernetes-crd-definition-v1.yml
k3s kubectl apply --server-side --force-conflicts -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.1/cert-manager.crds.yaml
k3s kubectl apply --server-side --force-conflicts -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.5.0/standard-install.yaml
k3s kubectl apply --server-side --force-conflicts -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml

k3s kubectl create namespace "$argocd_namespace" --dry-run=client -o yaml | k3s kubectl apply -f -

install -d "$argocd_install_path"
cat <<'EOF_ARGOCD_KUSTOMIZATION' > "$argocd_install_path/kustomization.yaml"
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - github.com/argoproj/argo-cd//manifests/cluster-install?ref=v3.4.3
patches:
  - target:
      kind: ConfigMap
      name: argocd-cm
    patch: |-
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: argocd-cm
      data:
        timeout.reconciliation: "1h"
        users.anonymous.enabled: "true"
        # Custom health check: treat apps as "Progressing" (not "Missing")
        # while they are still syncing, so sync-wave ordering between Argo
        # apps is respected and later waves don't start prematurely.
        resource.customizations.health.argoproj.io_Application: |
          hs = {}
          if obj.status == nil then
            hs.status = "Progressing"
            hs.message = "Waiting for application to initialize"
            return hs
          end
          if obj.status.health ~= nil and obj.status.health.status == "Degraded" then
            hs.status = "Degraded"
            hs.message = obj.status.health.message or "Application is degraded"
            return hs
          end
          if obj.status.sync == nil or obj.status.sync.status ~= "Synced" then
            hs.status = "Progressing"
            hs.message = "Waiting for application to sync"
            return hs
          end
          if obj.status.health ~= nil then
            hs.status = obj.status.health.status
            hs.message = obj.status.health.message or ""
          else
            hs.status = "Progressing"
            hs.message = "Waiting for application health"
          end
          return hs
        # Custom health check: keep CRDs as "Progressing" until the
        # "Established" condition is true, so Argo keeps polling instead
        # of marking them synced while the underlying controllers are
        # not yet ready.
        resource.customizations.health.apiextensions.k8s.io_CustomResourceDefinition: |
          hs = {}
          if obj.status == nil or obj.status.conditions == nil then
            hs.status = "Progressing"
            hs.message = "Waiting for CRD conditions"
            return hs
          end
          for i, condition in pairs(obj.status.conditions) do
            if condition.type == "Established" and condition.status == "True" then
              hs.status = "Healthy"
              hs.message = "CRD is established"
              return hs
            end
          end
          hs.status = "Progressing"
          hs.message = "CRD is not yet established"
          return hs
  - target:
      kind: ConfigMap
      name: argocd-rbac-cm
    patch: |-
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: argocd-rbac-cm
      data:
        policy.default: role:admin
  - target:
      kind: ConfigMap
      name: argocd-cmd-params-cm
    patch: |-
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: argocd-cmd-params-cm
      data:
        server.insecure: "true"
        repo.server.timeout: "300s"
  - target:
      kind: Deployment
    patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: ignored
      spec:
        revisionHistoryLimit: 0
  - target:
      kind: StatefulSet
    patch: |-
      apiVersion: apps/v1
      kind: StatefulSet
      metadata:
        name: ignored
      spec:
        revisionHistoryLimit: 0
  - target:
      kind: Deployment
      name: argocd-repo-server
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/env/-
        value:
          name: ARGOCD_EXEC_TIMEOUT
          value: "5m"
EOF_ARGOCD_KUSTOMIZATION

k3s kubectl apply --server-side --force-conflicts -n "$argocd_namespace" -k "$argocd_install_path"

until k3s kubectl get crd applications.argoproj.io >/dev/null 2>&1; do
  sleep 2
done

k3s kubectl wait --for=condition=Established --timeout=120s crd/applications.argoproj.io

k3s kubectl wait --for=condition=available --timeout=120s deployment/argocd-server -n "$argocd_namespace"
k3s kubectl wait --for=condition=available --timeout=120s deployment/argocd-repo-server -n "$argocd_namespace"

install -d /opt /opt/local-storage
cat <<'EOF_HELM_CLUSTER' > "$manifest_path"
${helm_cluster_manifest}
EOF_HELM_CLUSTER

k3s kubectl apply -f "$manifest_path"
