#!/usr/bin/env bash
#
# Regression smoke-probe for the Tempo /api/search empty-`q` fix
# (selectapi/handler.go: normalizeTempoSearchParams).
#
# Locks the user-visible contract: a Tempo search submitted with only
# `tags=service.name=...` (the legacy Grafana / Tempo HTTP search shape)
# must return >=1 trace for a service known to exist in the cold tier.
#
# Failure mode this probe catches: 0 traces returned in ~5 ms with no
# storage activity — upstream VT's parseTempoAPIParam unconditionally
# overwrites the default `q="{}"` with the URL `q` value (even when
# empty), then `traceql.ParseQuery("")` fails and `searchTraces` returns
# nil. See deps/VictoriaTraces/app/vtselect/traces/tempo/tempo.go ~L514
# and ~L557 for the upstream quirk, and selectapi/handler.go for the
# Lakehouse-side workaround that injects the default `{}` (or converts
# `tags` into a TraceQL filter) before forwarding to the upstream
# Tempo handler.
#
# The Grafana UX symptom this guards against: Explore > Tempo > Search
# (or any older Tempo HTTP client that uses `tags=…` instead of TraceQL
# `q=…`) returns "no results" on every search through the
# "Tempo LH Cold (S3 Parquet)" datasource.
#
# Usage:
#   tests/verification/probe_tempo_search_24h.sh                       # localhost defaults
#   LH_TRACES_URL=http://lh-traces:10428 ./probe_tempo_search_24h.sh   # custom backend
#
# Exit codes:
#   0 — probe passed (>=1 trace returned)
#   1 — probe failed (0 traces returned; regression)
#   2 — probe could not run (backend unreachable, jq/python missing, ...)
#
# To verify this probe is a true regression lock: temporarily revert
# normalizeTempoSearchParams from lakehouse-traces/internal/selectapi/
# handler.go, rebuild + recreate the container, and re-run this script.
# It MUST exit 1. If it passes without the workaround, the probe isn't
# pinning the contract.

set -euo pipefail

URL="${LH_TRACES_URL:-http://localhost:20428}"
SERVICE="${LH_PROBE_SERVICE:-api-gateway}"
LIMIT="${LH_PROBE_LIMIT:-3}"
WINDOW_HOURS="${LH_PROBE_WINDOW_HOURS:-24}"

now_s() {
  python3 -c "import time; print(int(time.time()))"
}
shift_s() {
  python3 -c "import time; print(int(time.time() - $1))"
}

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not available" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not available (used for unix timestamps)" >&2
  exit 2
fi

start_s=$(shift_s $((WINDOW_HOURS * 3600)))
end_s=$(now_s)

echo "probing ${URL}/select/tempo/api/search"
echo "  tags=service.name=${SERVICE} limit=${LIMIT} window=${WINDOW_HOURS}h"
echo "  start_s=${start_s}  end_s=${end_s}"

# NOTE: deliberately use the legacy `tags=` shape (NO `q=` parameter).
# That's the request shape that triggered the regression we are locking.
response=$(curl -fsS --max-time 30 -G \
  "${URL}/select/tempo/api/search" \
  --data-urlencode "tags=service.name=${SERVICE}" \
  --data-urlencode "start=${start_s}" \
  --data-urlencode "end=${end_s}" \
  --data-urlencode "limit=${LIMIT}" 2>&1) || {
    echo "FAIL: curl could not reach backend at ${URL}" >&2
    echo "${response}" >&2
    exit 2
  }

count=$(printf '%s' "${response}" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception as e:
    print(0)
    sys.stderr.write(f"json parse error: {e}\n")
    sys.exit(0)
print(len(d.get("traces", [])))
')

echo "  → traces returned: ${count}"

if [[ "${count}" -lt 1 ]]; then
  cat <<EOF >&2
FAIL: Tempo 24h search regressed
  endpoint: ${URL}/select/tempo/api/search
  request:  tags=service.name=${SERVICE}, start=${start_s}, end=${end_s}
  expected: >=1 trace (data known to exist in cold tier)
  got: ${count}
  likely cause: normalizeTempoSearchParams in
                lakehouse-traces/internal/selectapi/handler.go was
                removed, or the upstream VT parseTempoAPIParam quirk
                changed shape. Without the workaround, the empty 'q'
                URL parameter clobbers the upstream default of '{}' and
                traceql.ParseQuery("") returns an error, so
                searchTraces returns nil/empty for any client that
                doesn't explicitly send 'q='. Run the unit tests in
                lakehouse-traces/internal/selectapi/handler_test.go
                (TestNormalizeTempoSearchParams) for the per-case lock.
                The companion is probe_jaeger_search_24h.sh
                (verifies the same data exists in the cold tier).
EOF
  exit 1
fi

echo "PASS: Tempo 24h search returned ${count} trace(s) for tags=service.name=${SERVICE}"
exit 0
