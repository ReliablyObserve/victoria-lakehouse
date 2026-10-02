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
  echo "FAIL: helm not found in PATH — install helm to run template verification" >&2
  exit 1
fi

ERROR_FILE="$(mktemp)"
trap 'rm -f "${ERROR_FILE}"' EXIT

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
      >/dev/null 2>"${ERROR_FILE}"; then
    echo "  PASS  ${description}"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL  ${description}"
    sed 's/^/         /' "${ERROR_FILE}"
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
# Opt-in ingest listeners (syslog, OTLP gRPC): off by default, wired when enabled
# ---------------------------------------------------------------------------
echo ""
echo "--- Ingest listeners (opt-in) ---"

# render_has <description> <expected: yes|no> <regex> [--set flags...]
# Renders the chart and asserts the regex does (yes) or does not (no) appear.
render_has() {
  local description="$1" expect="$2" regex="$3"
  shift 3
  local out
  if ! out="$(helm template test-release "${CHART_DIR}" "$@" --generate-name=false --validate=false 2>/tmp/helm_test_err)"; then
    echo "  FAIL  ${description} (helm template failed)"
    sed 's/^/         /' /tmp/helm_test_err
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
    return
  fi
  local found=no
  if grep -Eq -- "${regex}" <<<"${out}"; then found=yes; fi
  if [[ "${found}" == "${expect}" ]]; then
    echo "  PASS  ${description}"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL  ${description} (pattern '${regex}': found=${found}, want ${expect})"
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
  fi
}

render_has "default: no syslog listener, flag, container port or Service port" no \
  'syslog'
render_has "default: no OTLP gRPC listener" no \
  'otlpGRPC|otlp-grpc' \
  --set "traces.enabled=true"
render_has "syslog tcp: flag" yes \
  '"-syslog.listenAddr.tcp=:5140"' \
  --set "logs.insert.syslog.tcp.enabled=true"
render_has "syslog tcp: Service port" yes \
  'name: syslog-tcp' \
  --set "logs.insert.syslog.tcp.enabled=true"
render_has "syslog udp: UDP protocol on the container port" yes \
  'containerPort: 5141' \
  --set "logs.insert.syslog.udp.enabled=true"
render_has "syslog udp only: no tcp flag" no \
  'syslog.listenAddr.tcp' \
  --set "logs.insert.syslog.udp.enabled=true"
render_has "syslog UDP null port: listen flag uses UDP fallback" yes \
  '"-syslog.listenAddr.udp=:5141"' \
  --set "logs.insert.syslog.udp.enabled=true" \
  --set "logs.insert.syslog.udp.port=null"
render_has "syslog TCP null port: listen flag uses TCP fallback" yes \
  '"-syslog.listenAddr.tcp=:5140"' \
  --set "logs.insert.syslog.tcp.enabled=true" \
  --set "logs.insert.syslog.tcp.port=null"
render_has "syslog tenantID flag" yes \
  '"-syslog.tenantID.tcp=7:1"' \
  --set "logs.insert.syslog.tcp.enabled=true" \
  --set "logs.insert.syslog.tcp.tenantID=7:1"
render_has "syslog custom port flows to flag" yes \
  '"-syslog.listenAddr.tcp=:1514"' \
  --set "logs.insert.syslog.tcp.enabled=true" \
  --set "logs.insert.syslog.tcp.port=1514"
# The flag must appear once: on the insert StatefulSet, never on select.
n="$(helm template test-release "${CHART_DIR}" --set "logs.insert.syslog.tcp.enabled=true" --generate-name=false --validate=false 2>/dev/null | grep -c 'syslog.listenAddr.tcp' || true)"
if [[ "${n}" == "1" ]]; then
  echo "  PASS  syslog listener flag is on the insert pods only"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL  syslog listener flag appears ${n} times, want 1 (insert StatefulSet only)"
  FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
fi
render_has "syslog ports reach the NetworkPolicy" yes \
  'port: 5140' \
  --set "logs.insert.syslog.tcp.enabled=true" \
  --set "networkPolicy.enabled=true"
render_has "OTLP gRPC plaintext: flags" yes \
  '"-otlpGRPC.tls=false"' \
  --set "traces.enabled=true" \
  --set "traces.insert.otlpGrpc.enabled=true" \
  --set "traces.insert.otlpGrpc.tls.enabled=false"
render_has "OTLP gRPC: Service port" yes \
  'name: otlp-grpc' \
  --set "traces.enabled=true" \
  --set "traces.insert.otlpGrpc.enabled=true" \
  --set "traces.insert.otlpGrpc.tls.enabled=false"
render_has "OTLP gRPC with TLS: cert and key flags" yes \
  '"-otlpGRPC.tlsCertFile=/tls/tls.crt"' \
  --set "traces.enabled=true" \
  --set "traces.insert.otlpGrpc.enabled=true" \
  --set "traces.insert.otlpGrpc.tls.certFile=/tls/tls.crt" \
  --set "traces.insert.otlpGrpc.tls.keyFile=/tls/tls.key"

# TLS on (the upstream default) without a certificate must fail at render time,
# not crash the pod at startup.
if helm template test-release "${CHART_DIR}" --set "traces.enabled=true" --set "traces.insert.otlpGrpc.enabled=true" \
    --generate-name=false --validate=false >/dev/null 2>/tmp/helm_test_err; then
  echo "  FAIL  OTLP gRPC with TLS and no certificate must be rejected"
  FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
elif grep -q "tls.certFile and tls.keyFile" /tmp/helm_test_err; then
  echo "  PASS  OTLP gRPC with TLS and no certificate is rejected with a clear message"
  PASSED=$((PASSED + 1))
else
  echo "  FAIL  OTLP gRPC with TLS and no certificate failed with an unexpected message"
  sed 's/^/         /' /tmp/helm_test_err
  FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
fi

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
# Insert buffer: durable by default
# ---------------------------------------------------------------------------
echo ""
echo "--- Insert buffer ---"

# check_render <description> <helm args...> -- <grep -E pattern that must match> [<pattern that must not>]
check_render() {
  local description="$1" must="$2" mustnot="$3"
  shift 3
  local out
  if ! out="$(helm template test-release "${CHART_DIR}" "$@" 2>/tmp/helm_test_err)"; then
    echo "  FAIL  ${description}"; sed 's/^/         /' /tmp/helm_test_err
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1)); return
  fi
  if ! grep -Eq "${must}" <<<"${out}"; then
    echo "  FAIL  ${description}: no match for ${must}"
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1)); return
  fi
  if [[ -n "${mustnot}" ]] && grep -Eq "${mustnot}" <<<"${out}"; then
    echo "  FAIL  ${description}: unexpected match for ${mustnot}"
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1)); return
  fi
  echo "  PASS  ${description}"
  PASSED=$((PASSED + 1))
}

check_render "the rendered config sets the segment buffer and none of the removed keys" \
  'buffer_dir: /data/lakehouse/buffer' \
  '(buffer_engine|buffer_flush_enabled|buffer_retention|ack_mode|max_buffer_rows|max_buffer_bytes|flush_linger|flush_max_rows|peer_replicate)'
check_render "insert and select pods get a persistent volume claim by default" \
  'volumeClaimTemplates:' ''
check_render "a pod without persistence gets an emptyDir (select pods need no PVC for the buffer)" \
  'emptyDir: \{\}' '' \
  --set "logs.select.persistence.enabled=false"

# ---------------------------------------------------------------------------
# Rendered content
# ---------------------------------------------------------------------------
# expect_render <description> <yes|no> <pattern> [--set flags...]: the rendered
# manifests contain (yes) or do not contain (no) the fixed string pattern.
expect_render() {
  local description="$1" want="$2" pattern="$3"
  shift 3
  local out
  if ! out="$(helm template test-release "${CHART_DIR}" "$@" --validate=false 2>/tmp/helm_test_err)"; then
    echo "  FAIL  ${description} (render)"
    sed 's/^/         /' /tmp/helm_test_err
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1)); return
  fi
  if grep -qF -- "${pattern}" <<<"${out}"; then local has=yes; else local has=no; fi
  if [[ "${has}" == "${want}" ]]; then
    echo "  PASS  ${description}"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL  ${description}: expected '${pattern}' present=${want}"
    FAILED=$((FAILED + 1)); ERRORS=$((ERRORS + 1))
  fi
}

# Select pods read the insert pods' unflushed rows through the insert headless
# service of their own signal; nothing read it before, so the buffer bridge of a
# split deployment had no insert pods to ask.
expect_render "logs select pods find the logs insert pods" yes \
  "insert_headless_service: test-release-victoria-lakehouse-logs-insert-headless:9428"
expect_render "traces select pods find the traces insert pods" yes \
  "insert_headless_service: test-release-victoria-lakehouse-traces-insert-headless:10428" \
  --set "traces.enabled=true"
expect_render "an explicit insert_headless_service wins" yes \
  "insert_headless_service: custom-insert:9428" \
  --set "lakehouseConfig.select.insert_headless_service=custom-insert:9428"
expect_render "no insert service without insert pods" no \
  "insert-headless:9428" \
  --set "logs.insert.enabled=false"
expect_render "no insert service without its headless service" no \
  "insert_headless_service: test-release" \
  --set "logs.insert.headlessService.enabled=false"

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
