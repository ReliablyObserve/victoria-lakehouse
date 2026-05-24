#!/usr/bin/env bash
set -euo pipefail

# Benchmark preflight validation — runs BEFORE comparative benchmarks.
# Fails fast on: unhealthy services, missing data, parity mismatch, query errors.
#
# Usage:
#   ./scripts/benchmark-preflight.sh [options]
#
# Options:
#   --lh URL         Lakehouse endpoint (default: http://localhost:39428)
#   --vl URL         VictoriaLogs endpoint (default: http://localhost:39401)
#   --loki URL       Loki endpoint (default: http://localhost:33100)
#   --min-logs N     Minimum required log rows (default: 500000)
#   --parity-pct N   Max allowed parity deviation percent (default: 5)
#   --skip-loki      Skip Loki checks

LH_URL="http://localhost:39428"
VL_URL="http://localhost:39401"
LOKI_URL="http://localhost:33100"
MIN_LOGS=500000
PARITY_PCT=5
SKIP_LOKI=false
ERRORS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lh)         LH_URL="$2"; shift 2 ;;
    --vl)         VL_URL="$2"; shift 2 ;;
    --loki)       LOKI_URL="$2"; shift 2 ;;
    --min-logs)   MIN_LOGS="$2"; shift 2 ;;
    --parity-pct) PARITY_PCT="$2"; shift 2 ;;
    --skip-loki)  SKIP_LOKI=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

NOW_S=$(date +%s)
START_NS=$(( (NOW_S - 604800) ))000000000
END_NS=$(( NOW_S + 3600 ))000000000

pass() { echo "  ✓ $1"; }
fail() { echo "  ✗ $1"; ERRORS=$((ERRORS + 1)); }

echo "=== Benchmark Preflight Validation ==="
echo ""

# ─── 1. Health checks ───────────────────────────────────────────────
echo "1. Health Checks"

check_health() {
  local name="$1" url="$2"
  local status
  status=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 5 "${url}/health" 2>/dev/null || echo "000")
  if [[ "$status" == "200" ]]; then
    pass "$name is healthy (${url})"
  else
    fail "$name is NOT healthy (status=$status, url=${url}/health)"
  fi
}

check_health "Lakehouse" "$LH_URL"
check_health "VictoriaLogs" "$VL_URL"
if [[ "$SKIP_LOKI" != "true" ]]; then
  local_loki_status=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 5 "${LOKI_URL}/ready" 2>/dev/null || echo "000")
  if [[ "$local_loki_status" == "200" ]]; then
    pass "Loki is healthy (${LOKI_URL})"
  else
    fail "Loki is NOT healthy (status=$local_loki_status, url=${LOKI_URL}/ready)"
  fi
fi
echo ""

if [[ $ERRORS -gt 0 ]]; then
  echo "ABORT: $ERRORS health check(s) failed. Fix services before benchmarking."
  exit 1
fi

# ─── 2. Data count and parity ───────────────────────────────────────
echo "2. Data Volume & Parity"

count_logs() {
  local name="$1" url="$2"
  local result
  result=$(curl -s "${url}/select/logsql/query?query=*%20%7C%20stats%20count()&start=${START_NS}&end=${END_NS}" 2>/dev/null)
  echo "$result" | grep -oE '[0-9]+' | head -1
}

LH_COUNT=$(count_logs "Lakehouse" "$LH_URL")
VL_COUNT=$(count_logs "VictoriaLogs" "$VL_URL")

if [[ -z "$LH_COUNT" || "$LH_COUNT" == "0" ]]; then
  fail "Lakehouse has 0 logs (check flush interval / S3 write)"
  LH_COUNT=0
else
  pass "Lakehouse has $LH_COUNT logs"
fi

if [[ -z "$VL_COUNT" || "$VL_COUNT" == "0" ]]; then
  fail "VictoriaLogs has 0 logs"
  VL_COUNT=0
else
  pass "VictoriaLogs has $VL_COUNT logs"
fi

# Min threshold
if [[ "$LH_COUNT" -lt "$MIN_LOGS" ]]; then
  fail "Lakehouse has $LH_COUNT logs (minimum required: $MIN_LOGS)"
fi
if [[ "$VL_COUNT" -lt "$MIN_LOGS" ]]; then
  fail "VictoriaLogs has $VL_COUNT logs (minimum required: $MIN_LOGS)"
fi

# Parity check
if [[ "$VL_COUNT" -gt 0 && "$LH_COUNT" -gt 0 ]]; then
  if [[ "$VL_COUNT" -gt "$LH_COUNT" ]]; then
    DIFF=$(( VL_COUNT - LH_COUNT ))
    PCT=$(( DIFF * 100 / VL_COUNT ))
  else
    DIFF=$(( LH_COUNT - VL_COUNT ))
    PCT=$(( DIFF * 100 / LH_COUNT ))
  fi
  if [[ "$PCT" -le "$PARITY_PCT" ]]; then
    pass "Data parity OK: LH=$LH_COUNT VL=$VL_COUNT (${PCT}% deviation, max=${PARITY_PCT}%)"
  else
    fail "Data parity MISMATCH: LH=$LH_COUNT VL=$VL_COUNT (${PCT}% deviation, max=${PARITY_PCT}%)"
  fi
fi
echo ""

if [[ $ERRORS -gt 0 ]]; then
  echo "ABORT: $ERRORS data check(s) failed. Seed more data or check ingest."
  exit 1
fi

# ─── 3. Query validation ────────────────────────────────────────────
echo "3. Query Response Validation"

validate_query() {
  local name="$1" url="$2" query="$3" desc="$4"
  local http_code body
  body=$(curl -s -w '\n%{http_code}' "${url}/select/logsql/query?query=${query}&start=${START_NS}&end=${END_NS}&limit=5" 2>/dev/null)
  http_code=$(echo "$body" | tail -1)
  body_content=$(echo "$body" | head -n -1)

  if [[ "$http_code" != "200" ]]; then
    fail "$name: $desc returned HTTP $http_code"
    return
  fi
  if [[ -z "$body_content" ]]; then
    fail "$name: $desc returned empty body"
    return
  fi
  pass "$name: $desc → HTTP 200, non-empty response"
}

# Wildcard
validate_query "LH"  "$LH_URL" "*" "wildcard query"
validate_query "VL"  "$VL_URL" "*" "wildcard query"

# Service filter
validate_query "LH"  "$LH_URL" "service.name%3A%3D%22api-gateway%22" "service filter"
validate_query "VL"  "$VL_URL" "service.name%3A%3D%22api-gateway%22" "service filter"

# Field names
for name_url in "LH:$LH_URL" "VL:$VL_URL"; do
  sname="${name_url%%:*}"
  surl="${name_url#*:}"
  fn_status=$(curl -s -o /dev/null -w '%{http_code}' "${surl}/select/logsql/field_names?query=*&start=${START_NS}&end=${END_NS}" 2>/dev/null)
  if [[ "$fn_status" == "200" ]]; then
    pass "$sname: field_names → HTTP 200"
  else
    fail "$sname: field_names → HTTP $fn_status"
  fi
done

# Stats query
validate_query "LH"  "$LH_URL" "*%20%7C%20stats%20count()%20by(service.name)" "stats by service"
validate_query "VL"  "$VL_URL" "*%20%7C%20stats%20count()%20by(service.name)" "stats by service"

echo ""

# ─── 4. Service-level data presence ─────────────────────────────────
echo "4. Per-Service Data Presence"

for svc in api-gateway user-service order-service payment-service notification-service; do
  lh_svc=$(curl -s "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22${svc}%22%20%7C%20stats%20count()&start=${START_NS}&end=${END_NS}" 2>/dev/null | grep -oE '[0-9]+' | head -1)
  if [[ -n "$lh_svc" && "$lh_svc" -gt 0 ]]; then
    pass "LH has $lh_svc logs for $svc"
  else
    fail "LH has 0 logs for $svc"
  fi
done
echo ""

# ─── Summary ────────────────────────────────────────────────────────
if [[ $ERRORS -gt 0 ]]; then
  echo "PREFLIGHT FAILED: $ERRORS check(s) failed. Do NOT run benchmarks."
  exit 1
fi

echo "=== PREFLIGHT PASSED ==="
echo "  LH: $LH_COUNT logs"
echo "  VL: $VL_COUNT logs"
echo "  All health checks OK, data parity OK, queries validated."
echo "  Ready for comparative benchmarks."
exit 0
