#!/usr/bin/env bash
set -euo pipefail

CHART_DIR="$(cd "$(dirname "$0")" && pwd)"
ERRORS=0
PASSED=0
FAILED=0

echo "=== Helm Chart Template Verification ==="
echo "Chart: ${CHART_DIR}"
echo ""

# Check if helm is installed
if ! command -v helm &>/dev/null; then
  echo "SKIP: helm not found in PATH — install helm to run template verification"
  exit 0
fi

HELM_VERSION="$(helm version --short 2>/dev/null || true)"
echo "Helm: ${HELM_VERSION}"
echo ""

# run_test <description> [--set flags...]
run_test() {
  local description="$1"
  shift
  local set_flags=("$@")

  if helm template test-release "${CHART_DIR}" "${set_flags[@]}" \
      --generate-name=false \
      --validate=false \
      >/dev/null 2>/tmp/helm_test_err; then
    echo "  PASS  ${description}"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL  ${description}"
    sed 's/^/         /' /tmp/helm_test_err
    FAILED=$((FAILED + 1))
    ERRORS=$((ERRORS + 1))
  fi
}

# ---------------------------------------------------------------------------
# Default values (logs enabled, traces disabled)
# ---------------------------------------------------------------------------
echo "--- Default rendering ---"
run_test "default values (logs only)"

# ---------------------------------------------------------------------------
# Signal mode combinations
# ---------------------------------------------------------------------------
echo ""
echo "--- Signal modes ---"

run_test "logs-only mode (explicit)" \
  --set "logs.enabled=true" \
  --set "traces.enabled=false"

run_test "traces-only mode" \
  --set "logs.enabled=false" \
  --set "traces.enabled=true"

run_test "both signals enabled" \
  --set "logs.enabled=true" \
  --set "traces.enabled=true"

# ---------------------------------------------------------------------------
# vmauth
# ---------------------------------------------------------------------------
echo ""
echo "--- vmauth ---"

run_test "vmauth enabled" \
  --set "vmauth.enabled=true" \
  --set "vmauth.config=someconfig"

run_test "vmauth with ingress" \
  --set "vmauth.enabled=true" \
  --set "vmauth.config=test" \
  --set "vmauth.ingress.enabled=true" \
  --set "vmauth.ingress.hosts[0].host=vmauth.example.com" \
  --set "vmauth.ingress.hosts[0].paths[0].path=/" \
  --set "vmauth.ingress.hosts[0].paths[0].pathType=Prefix"

run_test "vmauth with ServiceMonitor" \
  --set "vmauth.enabled=true" \
  --set "vmauth.config=test" \
  --set "vmauth.serviceMonitor.enabled=true"

# ---------------------------------------------------------------------------
# HPA
# ---------------------------------------------------------------------------
echo ""
echo "--- HorizontalPodAutoscaler ---"

run_test "logs-select HPA enabled" \
  --set "logs.select.horizontalPodAutoscaler.enabled=true"

run_test "logs-insert HPA enabled" \
  --set "logs.insert.horizontalPodAutoscaler.enabled=true"

run_test "traces-select HPA enabled" \
  --set "traces.enabled=true" \
  --set "traces.select.horizontalPodAutoscaler.enabled=true"

run_test "traces-insert HPA enabled" \
  --set "traces.enabled=true" \
  --set "traces.insert.horizontalPodAutoscaler.enabled=true"

run_test "all HPAs enabled" \
  --set "logs.select.horizontalPodAutoscaler.enabled=true" \
  --set "logs.insert.horizontalPodAutoscaler.enabled=true" \
  --set "traces.enabled=true" \
  --set "traces.select.horizontalPodAutoscaler.enabled=true" \
  --set "traces.insert.horizontalPodAutoscaler.enabled=true"

# ---------------------------------------------------------------------------
# PodDisruptionBudget
# ---------------------------------------------------------------------------
echo ""
echo "--- PodDisruptionBudget ---"

run_test "logs-select PDB enabled" \
  --set "logs.select.podDisruptionBudget.enabled=true"

run_test "logs-insert PDB enabled" \
  --set "logs.insert.podDisruptionBudget.enabled=true"

run_test "all PDBs enabled (logs + traces)" \
  --set "logs.select.podDisruptionBudget.enabled=true" \
  --set "logs.insert.podDisruptionBudget.enabled=true" \
  --set "traces.enabled=true" \
  --set "traces.select.podDisruptionBudget.enabled=true" \
  --set "traces.insert.podDisruptionBudget.enabled=true"

# ---------------------------------------------------------------------------
# Ingress
# ---------------------------------------------------------------------------
echo ""
echo "--- Ingress ---"

run_test "logs-select ingress enabled" \
  --set "logs.select.ingress.enabled=true" \
  --set "logs.select.ingress.hosts[0].host=logs.example.com" \
  --set "logs.select.ingress.hosts[0].paths[0].path=/" \
  --set "logs.select.ingress.hosts[0].paths[0].pathType=Prefix"

run_test "logs-insert ingress enabled" \
  --set "logs.insert.ingress.enabled=true" \
  --set "logs.insert.ingress.hosts[0].host=logs-insert.example.com" \
  --set "logs.insert.ingress.hosts[0].paths[0].path=/" \
  --set "logs.insert.ingress.hosts[0].paths[0].pathType=Prefix"

run_test "traces-select ingress enabled" \
  --set "traces.enabled=true" \
  --set "traces.select.ingress.enabled=true" \
  --set "traces.select.ingress.hosts[0].host=traces.example.com" \
  --set "traces.select.ingress.hosts[0].paths[0].path=/" \
  --set "traces.select.ingress.hosts[0].paths[0].pathType=Prefix"

run_test "ingress with TLS" \
  --set "logs.select.ingress.enabled=true" \
  --set "logs.select.ingress.className=nginx" \
  --set "logs.select.ingress.hosts[0].host=logs.example.com" \
  --set "logs.select.ingress.hosts[0].paths[0].path=/" \
  --set "logs.select.ingress.hosts[0].paths[0].pathType=Prefix" \
  --set "logs.select.ingress.tls[0].secretName=logs-tls" \
  --set "logs.select.ingress.tls[0].hosts[0]=logs.example.com"

# ---------------------------------------------------------------------------
# ServiceMonitor
# ---------------------------------------------------------------------------
echo ""
echo "--- ServiceMonitor ---"

run_test "logs-select ServiceMonitor enabled" \
  --set "logs.select.serviceMonitor.enabled=true"

run_test "logs-insert ServiceMonitor enabled" \
  --set "logs.insert.serviceMonitor.enabled=true"

run_test "traces ServiceMonitor enabled" \
  --set "traces.enabled=true" \
  --set "traces.select.serviceMonitor.enabled=true" \
  --set "traces.insert.serviceMonitor.enabled=true"

run_test "all ServiceMonitors enabled" \
  --set "logs.select.serviceMonitor.enabled=true" \
  --set "logs.insert.serviceMonitor.enabled=true" \
  --set "traces.enabled=true" \
  --set "traces.select.serviceMonitor.enabled=true" \
  --set "traces.insert.serviceMonitor.enabled=true"

# ---------------------------------------------------------------------------
# VPA
# ---------------------------------------------------------------------------
echo ""
echo "--- VerticalPodAutoscaler ---"

run_test "logs-select VPA enabled" \
  --set "logs.select.verticalPodAutoscaler.enabled=true"

run_test "logs-insert VPA enabled (updateMode Auto)" \
  --set "logs.insert.verticalPodAutoscaler.enabled=true" \
  --set "logs.insert.verticalPodAutoscaler.updateMode=Auto"

# ---------------------------------------------------------------------------
# NetworkPolicy
# ---------------------------------------------------------------------------
echo ""
echo "--- NetworkPolicy ---"

run_test "networkPolicy enabled" \
  --set "networkPolicy.enabled=true"

# ---------------------------------------------------------------------------
# ServiceAccount
# ---------------------------------------------------------------------------
echo ""
echo "--- ServiceAccount ---"

run_test "serviceAccount create disabled (logs-select)" \
  --set "logs.select.serviceAccount.create=false"

run_test "serviceAccount custom name" \
  --set "logs.select.serviceAccount.create=true" \
  --set "logs.select.serviceAccount.name=custom-sa"

# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------
echo ""
echo "--- Compaction ---"

run_test "compaction enabled (logs)" \
  --set "lakehouseConfig.compaction.enabled=true"

run_test "compaction enabled (both signals)" \
  --set "traces.enabled=true" \
  --set "lakehouseConfig.compaction.enabled=true"

# ---------------------------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------------------------
echo ""
echo "--- S3 configuration ---"

run_test "S3 bucket and credentials set" \
  --set "lakehouseConfig.s3.bucket=my-bucket" \
  --set "lakehouseConfig.s3.region=eu-west-1" \
  --set "lakehouseConfig.s3.access_key=AKIAIOSFODNN7EXAMPLE" \
  --set "lakehouseConfig.s3.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

run_test "MinIO endpoint (force path style)" \
  --set "lakehouseConfig.s3.bucket=my-bucket" \
  --set "lakehouseConfig.s3.endpoint=http://minio:9000" \
  --set "lakehouseConfig.s3.force_path_style=true"

# ---------------------------------------------------------------------------
# nameOverride / fullnameOverride
# ---------------------------------------------------------------------------
echo ""
echo "--- Name overrides ---"

run_test "nameOverride set" \
  --set "nameOverride=custom-name"

run_test "fullnameOverride set" \
  --set "fullnameOverride=my-lakehouse"

# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------
echo ""
echo "--- Global options ---"

run_test "global imagePullSecrets" \
  --set "global.imagePullSecrets[0]=myregistrysecret"

run_test "global commonLabels and annotations" \
  --set "global.commonLabels.env=production" \
  --set "global.commonAnnotations.team=platform"

run_test "custom image tag" \
  --set "image.tag=v0.5.0"

# ---------------------------------------------------------------------------
# Full kitchen-sink
# ---------------------------------------------------------------------------
echo ""
echo "--- Kitchen sink ---"

run_test "all major features enabled" \
  --set "logs.enabled=true" \
  --set "traces.enabled=true" \
  --set "vmauth.enabled=true" \
  --set "vmauth.config=test" \
  --set "networkPolicy.enabled=true" \
  --set "lakehouseConfig.compaction.enabled=true" \
  --set "lakehouseConfig.s3.bucket=my-bucket" \
  --set "logs.select.horizontalPodAutoscaler.enabled=true" \
  --set "logs.insert.horizontalPodAutoscaler.enabled=true" \
  --set "logs.select.podDisruptionBudget.enabled=true" \
  --set "logs.insert.podDisruptionBudget.enabled=true" \
  --set "logs.select.serviceMonitor.enabled=true" \
  --set "logs.insert.serviceMonitor.enabled=true" \
  --set "traces.select.horizontalPodAutoscaler.enabled=true" \
  --set "traces.insert.horizontalPodAutoscaler.enabled=true" \
  --set "traces.select.podDisruptionBudget.enabled=true" \
  --set "traces.insert.podDisruptionBudget.enabled=true" \
  --set "traces.select.serviceMonitor.enabled=true" \
  --set "traces.insert.serviceMonitor.enabled=true" \
  --set "vmauth.serviceMonitor.enabled=true" \
  --set "vmauth.ingress.enabled=true" \
  --set "vmauth.ingress.hosts[0].host=vmauth.example.com" \
  --set "vmauth.ingress.hosts[0].paths[0].path=/" \
  --set "vmauth.ingress.hosts[0].paths[0].pathType=Prefix"

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
echo ""
echo "=== Results ==="
echo "  Passed: ${PASSED}"
echo "  Failed: ${FAILED}"
echo "  Total:  $((PASSED + FAILED))"
echo ""

if [[ ${ERRORS} -gt 0 ]]; then
  echo "FAIL: ${ERRORS} test(s) failed"
  exit 1
else
  echo "PASS: All tests passed"
fi
