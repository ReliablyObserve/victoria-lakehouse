#!/usr/bin/env bash
#
# Matrix sweep probe — locks every UNVERIFIED row from
# tests/verification/matrix.md that was verified during the
# `verify/matrix-completion` sweep.
#
# The ingest rows (LI2-LI8, TI3) are retired from this script: they run in CI as the
# ingest parity matrix (tests/e2e/ingest_matrix_test.go), see docs/ingest-parity.md.
#
# Each section corresponds to a single matrix row and asserts the
# minimum contract that the row's `last_state` captures. Failures
# are loud and exit non-zero so this script can be wired into CI
# or run manually after any LH change.
#
# Usage:
#   tests/verification/probe_matrix_sweep.sh            # run all
#   ROW=LA8 tests/verification/probe_matrix_sweep.sh    # run a single row
#
# Exit codes:
#   0 — all selected probes passed
#   1 — at least one probe failed
#   2 — backend unreachable or missing tooling
#
# Endpoints (from docker-compose-e2e.yml):
#   lakehouse-logs:    http://localhost:29428
#   lakehouse-traces:  http://localhost:20428
#   grafana:           http://localhost:3003

set -uo pipefail

LH_LOGS_URL="${LH_LOGS_URL:-http://localhost:29428}"
LH_TRACES_URL="${LH_TRACES_URL:-http://localhost:20428}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3003}"
DOCKER_NET="${DOCKER_NET:-victoria-lakehouse_lakehouse-net}"
ROW="${ROW:-}"

FAILED=()
PASSED=()

skip_row() { [[ -n "$ROW" && "$ROW" != "$1" ]]; }

ok()   { echo "  PASS: $*"; PASSED+=("$1"); }
fail() { echo "  FAIL: $*" >&2; FAILED+=("$1"); }

curl_get() { curl -sS --max-time 30 "$@"; }

dnet_curl() {
  docker run --rm --network "$DOCKER_NET" curlimages/curl:latest -sS --max-time 30 "$@"
}

# -------------------- LA8 — /internal/cache/clear ------------------------
if ! skip_row LA8; then
  echo "=== LA8 — /internal/cache/clear (POST + before/after stats) ==="
  before=$(curl_get "$LH_LOGS_URL/internal/cache/stats" || echo "")
  http=$(curl -sS --max-time 30 -o /tmp/cache_clear_resp -w "%{http_code}" \
    -X POST "$LH_LOGS_URL/internal/cache/clear" || echo "000")
  after=$(curl_get "$LH_LOGS_URL/internal/cache/stats" || echo "")
  if [[ "$http" =~ ^(200|202|204)$ ]] && [[ -n "$after" ]]; then
    ok "LA8 (HTTP $http; before/after stats reachable)"
  else
    fail "LA8 (HTTP $http; before=$before after=$after)"
  fi
fi

# -------------------- T10 — Jaeger operations/{svc} ----------------------
if ! skip_row T10; then
  echo "=== T10 — /select/jaeger/api/services/{svc}/operations ==="
  resp=$(curl_get "$LH_TRACES_URL/select/jaeger/api/services/api-gateway/operations" || echo "")
  n=$(printf '%s' "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo "0")
  if [[ "$n" -ge 1 ]]; then
    ok "T10 (operations=$n for api-gateway)"
  else
    fail "T10 (operations=$n; resp=$(printf '%s' "$resp" | head -c200))"
  fi
fi

# -------------------- T11 — Jaeger dependencies --------------------------
if ! skip_row T11; then
  echo "=== T11 — /select/jaeger/api/dependencies ==="
  end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
  resp=$(curl_get -G "$LH_TRACES_URL/select/jaeger/api/dependencies" \
    --data-urlencode "endTs=$end_ms" --data-urlencode "lookback=86400000" || echo "")
  status=$(printf '%s' "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print('data' in d)" 2>/dev/null || echo "False")
  if [[ "$status" == "True" ]]; then
    ok "T11 (response has 'data' key; len=$(printf '%s' "$resp" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('data',[])))" 2>/dev/null || echo 0))"
  else
    fail "T11 (resp=$(printf '%s' "$resp" | head -c200))"
  fi
fi

# -------------------- T14 — Tempo tag/{key}/values -----------------------
# VT v0.9.0 exposes /select/tempo/api/v2/search/tag/{key}/values only;
# matrix path was missing the /v2 prefix. LH inherits the same route via
# upstream handler integration (PR #93).
if ! skip_row T14; then
  echo "=== T14 — /select/tempo/api/v2/search/tag/{key}/values ==="
  resp=$(curl_get "$LH_TRACES_URL/select/tempo/api/v2/search/tag/service.name/values" || echo "")
  status=$(printf '%s' "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print('tagValues' in d)" 2>/dev/null || echo "False")
  if [[ "$status" == "True" ]]; then
    ok "T14 (response has 'tagValues' key)"
  else
    fail "T14 (resp=$(printf '%s' "$resp" | head -c200))"
  fi
fi

# -------------------- T17 — Tempo metrics/instant ------------------------
# VT v0.9.0 does NOT expose /select/tempo/api/metrics/instant. VT only
# implements /select/tempo/api/metrics/query_range. This row is documented
# as DIFFER: endpoint does not exist upstream → LH should not implement it.
# The probe asserts LH's response matches VT's (both return failure for
# the missing endpoint).
if ! skip_row T17; then
  echo "=== T17 — /select/tempo/api/metrics/instant (parity vs VT) ==="
  lh_code=$(curl -sS --max-time 15 -o /tmp/t17_lh -w "%{http_code}" \
    -G "$LH_TRACES_URL/select/tempo/api/metrics/instant" \
    --data-urlencode 'q={} | count_over_time()' || echo "000")
  vt_code=$(docker run --rm --network "$DOCKER_NET" curlimages/curl:latest \
    -sS --max-time 15 -o /tmp/t17_vt -w "%{http_code}" \
    -G "http://victoriatraces:10428/select/tempo/api/metrics/instant" \
    --data-urlencode 'q={} | count_over_time()' || echo "000")
  # VT returns 400 "unsupported path". Acceptable LH behaviors:
  # (a) LH also 400 (best parity)
  # (b) LH 200 with empty body (LH-internal stub) — must be documented.
  if [[ "$vt_code" == "400" && "$lh_code" == "400" ]]; then
    ok "T17 (LH=400 VT=400 — full parity on unsupported endpoint)"
  elif [[ "$vt_code" == "400" && "$lh_code" == "200" ]]; then
    ok "T17 (DIFFER: VT=400 unsupported, LH=200 stub — documented divergence)"
  else
    fail "T17 (LH=$lh_code VT=$vt_code)"
  fi
fi

# -------------------- TI2 — Zipkin spans push ----------------------------
# VT v0.9.0 does NOT implement /insert/zipkin/* — only /insert/jsonline
# and /insert/opentelemetry/v1/traces exist in vtinsert/main.go.
# Per feedback_vl_vt_upstream, LH should not invent an endpoint that VT
# does not expose. This row is documented as DIFFER (no upstream).
if ! skip_row TI2; then
  echo "=== TI2 — /insert/zipkin/api/v2/spans (parity vs VT) ==="
  lh=$(curl -sS --max-time 15 -o /tmp/ti2_lh -w "%{http_code}" \
    -H 'Content-Type: application/json' -X POST --data '[]' \
    "$LH_TRACES_URL/insert/zipkin/api/v2/spans" || echo "000")
  vt=$(docker run --rm --network "$DOCKER_NET" curlimages/curl:latest \
    -sS --max-time 15 -o /tmp/ti2_vt -w "%{http_code}" \
    -H 'Content-Type: application/json' -X POST --data '[]' \
    "http://victoriatraces:10428/insert/zipkin/api/v2/spans" || echo "000")
  # VT returns 400 "unsupported path", LH returns 404; both reject the
  # request without ingesting. Semantically equivalent: endpoint missing.
  if [[ "$lh" =~ ^(400|404)$ && "$vt" =~ ^(400|404)$ ]]; then
    ok "TI2 (DIFFER: LH=$lh VT=$vt — both reject; endpoint not in VT v0.9.0)"
  else
    fail "TI2 (LH=$lh VT=$vt — unexpected)"
  fi
fi

# -------------------- T13 deep — Tempo /v2/search/tags -------------------
# VT v0.9.0's actual path is /select/tempo/api/v2/search/tags. The matrix
# row had the v1-shape path; the v2 path returns a populated 'scopes'
# array with resource/span/event/link/instrumentation buckets.
if ! skip_row T13; then
  echo "=== T13 — /select/tempo/api/v2/search/tags (parity vs VT) ==="
  lh=$(curl_get "$LH_TRACES_URL/select/tempo/api/v2/search/tags" || echo "")
  vt=$(dnet_curl "http://victoriatraces:10428/select/tempo/api/v2/search/tags" || echo "")
  ok_test() { python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    print('scopes' in d)
except Exception:
    print(False)
" 2>/dev/null; }
  lh_ok=$(printf '%s' "$lh" | ok_test || echo "False")
  vt_ok=$(printf '%s' "$vt" | ok_test || echo "False")
  if [[ "$lh_ok" == "True" && "$vt_ok" == "True" ]]; then
    ok "T13 (LH and VT both return 'scopes'; v2 path)"
  else
    fail "T13 (lh_ok=$lh_ok vt_ok=$vt_ok lh=$(printf '%s' "$lh" | head -c100) vt=$(printf '%s' "$vt" | head -c100))"
  fi
fi

# -------------------- G12-G16 — Grafana datasources ----------------------
gf_check_ds() {
  local uid="$1" name="$2"
  local resp
  resp=$(curl -sS --max-time 15 -u admin:admin "$GRAFANA_URL/api/datasources/uid/$uid" || echo "")
  local id type
  id=$(printf '%s' "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null || echo "")
  type=$(printf '%s' "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('type',''))" 2>/dev/null || echo "")
  if [[ -z "$id" ]]; then
    fail "$name datasource lookup failed (resp=$(printf '%s' "$resp" | head -c150))"
    return 1
  fi
  # Use Grafana's health proxy endpoint
  local health
  health=$(curl -sS --max-time 30 -u admin:admin -X POST "$GRAFANA_URL/api/datasources/uid/$uid/health" || echo "")
  local hstatus
  hstatus=$(printf '%s' "$health" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || echo "")
  if [[ "$hstatus" == "OK" || "$hstatus" == "Success" ]]; then
    ok "$name (type=$type, health=$hstatus)"
  else
    # ClickHouse plugin may not implement health endpoint; do shallow probe instead
    fail "$name (type=$type, health=$hstatus, resp=$(printf '%s' "$health" | head -c150))"
  fi
}

if ! skip_row G12; then
  echo "=== G12 — clickhouse-logs ==="
  gf_check_ds clickhouse-logs G12
fi
if ! skip_row G13; then
  echo "=== G13 — clickhouse-traces ==="
  gf_check_ds clickhouse-traces G13
fi
if ! skip_row G14; then
  echo "=== G14 — clickhouse-otel ==="
  gf_check_ds clickhouse-otel G14
fi
if ! skip_row G15; then
  echo "=== G15 — clickhouse-analytics ==="
  gf_check_ds clickhouse-analytics G15
fi
if ! skip_row G16; then
  echo "=== G16 — victoriametrics-metrics ==="
  gf_check_ds victoriametrics-metrics G16
fi

# -------------------- U4 — Logs Drilldown smoke --------------------------
# Smoke contract: the Lakehouse cold logs datasource backing the
# Grafana Logs Drilldown app returns facets (the drilldown's primary
# data source). Tested via Grafana's HTTP proxy so the path matches
# what the browser app uses end-to-end.
if ! skip_row U4; then
  echo "=== U4 — Logs Drilldown (Grafana facets via cold LH proxy) ==="
  resp=$(curl -sS --max-time 30 -u admin:admin -G \
    "$GRAFANA_URL/api/datasources/proxy/uid/victoria-lakehouse-cold/select/logsql/facets" \
    --data-urlencode 'query=*' --data-urlencode 'limit=5' || echo "")
  has=$(printf '%s' "$resp" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    print('facets' in d and len(d['facets']) > 0)
except Exception:
    print(False)
" 2>/dev/null || echo "False")
  if [[ "$has" == "True" ]]; then
    ok "U4 (cold-LH facets returns non-empty 'facets' array via Grafana proxy)"
  else
    fail "U4 (resp=$(printf '%s' "$resp" | head -c150))"
  fi
fi

# -------------------- U5 — Traces Drilldown re-verify --------------------
# The Tempo metrics_query_range endpoint is what the Traces Drilldown
# app fetches for its rate/latency panels. Tested via Grafana's HTTP
# proxy so the path matches the browser app. step must be a Go duration
# string ("60s"), not bare seconds.
if ! skip_row U5; then
  echo "=== U5 — Traces Drilldown (Tempo metrics_query_range via Grafana) ==="
  end_ns=$(python3 -c "import time; print(int(time.time()*1e9))")
  start_ns=$(python3 -c "import time; print(int((time.time()-3600)*1e9))")
  resp=$(curl -sS --max-time 30 -u admin:admin -G \
    "$GRAFANA_URL/api/datasources/proxy/uid/tempo-lh-cold/api/metrics/query_range" \
    --data-urlencode 'q={} | rate()' \
    --data-urlencode "start=$start_ns" --data-urlencode "end=$end_ns" \
    --data-urlencode 'step=60s' || echo "")
  has=$(printf '%s' "$resp" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    print(any(k in d for k in ('series','data','status','metrics','exemplars')))
except Exception:
    print(False)
" 2>/dev/null || echo "False")
  if [[ "$has" == "True" ]]; then
    ok "U5 (metrics_query_range returns valid Tempo shape)"
  else
    fail "U5 (resp=$(printf '%s' "$resp" | head -c200))"
  fi
fi

# ----------------------------- Summary ----------------------------------
echo ""
echo "============================================================"
echo "MATRIX SWEEP SUMMARY"
echo "  PASSED: ${#PASSED[@]} (${PASSED[*]:-})"
echo "  FAILED: ${#FAILED[@]} (${FAILED[*]:-})"
echo "============================================================"

if [[ "${#FAILED[@]}" -gt 0 ]]; then
  exit 1
fi
exit 0
