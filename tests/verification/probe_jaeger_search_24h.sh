#!/usr/bin/env bash
#
# Regression smoke-probe for the Jaeger 24 h search fix
# (commit c511ed6 — "-search.latencyOffset=10m" on lakehouse-traces).
#
# Locks the user-visible contract: a Jaeger search with explicit
# start/end microseconds (the exact shape the Grafana traces datasource
# sends for a "Last 24 hours" range) must return ≥1 trace for a service
# known to exist in the cold tier.
#
# Failure mode this probe catches: 0 traces returned in ~25 ms with no
# storage activity — see commit message for c511ed6 for the root-cause
# write-up. The Grafana UX symptom is "0 series returned" on every
# trace search through the Lakehouse Traces Cold (S3 Jaeger) datasource.
#
# Usage:
#   tests/verification/probe_jaeger_search_24h.sh                    # localhost defaults
#   LH_TRACES_URL=http://lh-traces:10428 ./probe_jaeger_search_24h.sh  # custom backend
#
# Exit codes:
#   0 — probe passed (data returned)
#   1 — probe failed (0 traces returned; regression)
#   2 — probe could not run (backend unreachable, jq missing, etc.)
#
# To verify this probe is a true regression lock: temporarily revert
# the -search.latencyOffset=10m flag from lakehouse-traces in the
# compose, recreate the container, and re-run this script. It MUST
# fail with exit 1. If it passes without the flag, the probe isn't
# pinning the contract.

set -euo pipefail

URL="${LH_TRACES_URL:-http://localhost:20428}"
SERVICE="${LH_PROBE_SERVICE:-api-gateway}"
LIMIT="${LH_PROBE_LIMIT:-3}"
WINDOW_HOURS="${LH_PROBE_WINDOW_HOURS:-24}"

now_us() {
  python3 -c "import time; print(int(time.time() * 1_000_000))"
}
shift_us() {
  python3 -c "import time; print(int((time.time() - $1) * 1_000_000))"
}

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not available" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not available (used for usec timestamps)" >&2
  exit 2
fi

start_us=$(shift_us $((WINDOW_HOURS * 3600)))
end_us=$(now_us)

echo "probing ${URL}/select/jaeger/api/traces"
echo "  service=${SERVICE} limit=${LIMIT} window=${WINDOW_HOURS}h"
echo "  start_us=${start_us}  end_us=${end_us}"

response=$(curl -fsS --max-time 30 -G \
  "${URL}/select/jaeger/api/traces" \
  --data-urlencode "service=${SERVICE}" \
  --data-urlencode "start=${start_us}" \
  --data-urlencode "end=${end_us}" \
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
print(len(d.get("data", [])))
')

echo "  → traces returned: ${count}"

if [[ "${count}" -lt 1 ]]; then
  cat <<EOF >&2
FAIL: Jaeger 24h search regressed
  endpoint: ${URL}/select/jaeger/api/traces
  service: ${SERVICE}
  expected: ≥1 trace (data known to exist in cold tier)
  got: ${count}
  likely cause: -search.latencyOffset on lakehouse-traces was reverted
                or lowered below the cold-tier flush lag, so the upstream
                Jaeger search expansion loop (1m → 6m → 31m windows
                from end-LatencyOffset) misses every flushed span.
                See commit c511ed6 and feedback_post_work_resource_caps.
EOF
  exit 1
fi

echo "PASS: Jaeger 24h search returned ${count} trace(s) for ${SERVICE}"
exit 0
