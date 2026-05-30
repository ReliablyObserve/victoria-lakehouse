#!/usr/bin/env bash
#
# Regression smoke-probe for the Jaeger search tag-filter fix
# (lakehouse-traces/internal/vtstorage_adapter/adapter.go — stop stripping
#  pipes before passing the query to storage.RunQuery).
#
# Locks the user-visible contract: a Jaeger search with explicit
# start/end microseconds AND a tag filter (the exact shape Grafana sends
# when the user adds a key=value tag filter in the trace search UI) must
# return >=1 trace for a service known to exist in the cold tier.
#
# Failure mode this probe catches: the no-tag query works, but adding
# any tag (e.g. tags={"http.status_code":"200"}) returns 0 traces while
# vtselect upstream returns >=1 with the same query and the same time
# window. Root cause was the adapter passing a pipe-stripped query to
# storage.RunQuery, which made the storage's column-projection planner
# drop pipe-referenced fields (trace_id, _time) from the parquet
# projection, so the downstream `partition by (trace_id)` pipe operated
# on DataBlocks missing the trace_id column and yielded zero rows.
#
# The Grafana UX symptom this guards against: Explore > Traces (Jaeger
# datasource) > add a tag filter (e.g. status_code=200) > "no results"
# even though the same service search without the tag returns traces.
#
# Usage:
#   tests/verification/probe_jaeger_search_24h_with_tag.sh                    # localhost defaults
#   LH_TRACES_URL=http://lh-traces:10428 ./probe_jaeger_search_24h_with_tag.sh # custom backend
#
# Exit codes:
#   0 - probe passed (data returned)
#   1 - probe failed (0 traces returned; regression)
#   2 - probe could not run (backend unreachable, jq/python missing, ...)
#
# To verify this probe is a true regression lock: in
# lakehouse-traces/internal/vtstorage_adapter/adapter.go, change all three
# RunQuery call sites in the rewriteTraceIndexQuery / stripTraceIndexStream
# / QueryHasPipes branches back to use
#   filterOnly := logstorage.CloneWithoutPipes(...)
# and pass filterOnly. Rebuild + recreate the container and re-run this
# script. It MUST fail with exit 1. If it passes without the fix, the
# probe isn't pinning the contract.
#
# Companion: probe_jaeger_search_24h.sh (verifies the no-tag query still
# works — both probes together guarantee the fix did not break the
# previously-passing path).

set -euo pipefail

URL="${LH_TRACES_URL:-http://localhost:20428}"
SERVICE="${LH_PROBE_SERVICE:-api-gateway}"
TAG_KEY="${LH_PROBE_TAG_KEY:-http.status_code}"
TAG_VALUE="${LH_PROBE_TAG_VALUE:-200}"
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

# Jaeger tags parameter uses JSON object format: {"key":"value"}
tags_json="{\"${TAG_KEY}\":\"${TAG_VALUE}\"}"

echo "probing ${URL}/select/jaeger/api/traces"
echo "  service=${SERVICE} tags=${tags_json} limit=${LIMIT} window=${WINDOW_HOURS}h"
echo "  start_us=${start_us}  end_us=${end_us}"

response=$(curl -fsS --max-time 30 -G \
  "${URL}/select/jaeger/api/traces" \
  --data-urlencode "service=${SERVICE}" \
  --data-urlencode "tags=${tags_json}" \
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

echo "  -> traces returned: ${count}"

if [[ "${count}" -lt 1 ]]; then
  cat <<EOF >&2
FAIL: Jaeger 24h search WITH tag filter regressed
  endpoint: ${URL}/select/jaeger/api/traces
  service:  ${SERVICE}
  tags:     ${tags_json}
  expected: >=1 trace (data known to exist in cold tier)
  got:      ${count}
  likely cause: lakehouse-traces/internal/vtstorage_adapter/adapter.go
                started stripping pipes before passing the query to
                storage.RunQuery (CloneWithoutPipes reintroduced).
                Without pipes, the storage column-projection planner
                cannot see fields referenced only by pipes (trace_id,
                _time from \`partition by (trace_id) | fields _time,
                trace_id\`) and emits DataBlocks missing those columns,
                so \`partition by (trace_id)\` yields zero rows.
                Run the regression unit tests:
                  cd lakehouse-traces && GOWORK=off go test -count=1 \\
                    -run TestRunQuery_PreservesPipesToStorage \\
                    ./internal/vtstorage_adapter/
                The companion is probe_jaeger_search_24h.sh
                (verifies the no-tag query still works).
EOF
  exit 1
fi

echo "PASS: Jaeger 24h search with tag returned ${count} trace(s) for ${SERVICE} (${TAG_KEY}=${TAG_VALUE})"
exit 0
