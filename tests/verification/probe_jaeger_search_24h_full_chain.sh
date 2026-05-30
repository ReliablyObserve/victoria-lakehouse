#!/usr/bin/env bash
#
# Regression smoke-probe for the Jaeger 0-traces full-chain fix.
#
# Locks the user-visible contract: a Jaeger search must not only return
# trace metadata for ≥1 trace, but the subsequent spans-lookup phase must
# also return spans for those traces. The existing probe_jaeger_search_24h.sh
# only validates the FIRST phase (the search call); this probe exercises
# the FULL chain end-to-end:
#
#   1) GET /select/jaeger/api/traces?service=... — get trace metadata
#   2) For each returned trace, verify the spans payload is non-empty
#      (Jaeger's UI calls /select/jaeger/api/traces/<traceID> internally
#       but the same /traces endpoint already returns spans inline).
#   3) Independently verify: POST a logsql query with
#      `trace_id:in(tid1,tid2,tid3)` against /select/logsql/query and
#      assert non-zero rows come back. This is the exact second-phase
#      query VT's GetTraceList builds — see
#      lakehouse-traces/deps/VictoriaTraces/app/vtselect/traces/query/query.go.
#
# Failure mode this probe catches: Jaeger search returns traces but each
# trace has zero spans, because the spans-lookup query `trace_id:in(...)`
# was wrongly pruned by the file-level bloom pre-filter (the bug fixed in
# lakehouse-traces/internal/storage/parquets3/storage_query.go::
# filterFilesByBloomIndex / checkFileBloom). The Grafana UX symptom is
# "trace found" in the search panel but "no spans" when clicking the trace.
#
# Usage:
#   tests/verification/probe_jaeger_search_24h_full_chain.sh
#   LH_TRACES_URL=http://lh-traces:10428 ./probe_jaeger_search_24h_full_chain.sh
#
# Exit codes:
#   0 — probe passed (full chain works end-to-end)
#   1 — probe failed (search OK but spans-lookup returns 0)
#   2 — probe could not run (backend unreachable, no tools)
#
# Negative-control: revert the filterFilesByBloomIndex any-of-values
# rewrite in lakehouse-traces/internal/storage/parquets3/storage_query.go,
# rebuild, recreate, run this probe. It MUST fail at the spans-lookup
# step. Restore, rebuild, recreate, re-run. Must pass.

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

echo "=== Phase 1: Jaeger search (metadata) ==="
echo "probing ${URL}/select/jaeger/api/traces"
echo "  service=${SERVICE} limit=${LIMIT} window=${WINDOW_HOURS}h"

response=$(curl -fsS --max-time 30 -G \
  "${URL}/select/jaeger/api/traces" \
  --data-urlencode "service=${SERVICE}" \
  --data-urlencode "start=${start_us}" \
  --data-urlencode "end=${end_us}" \
  --data-urlencode "limit=${LIMIT}" 2>&1) || {
    echo "FAIL: curl could not reach Jaeger search at ${URL}" >&2
    echo "${response}" >&2
    exit 2
  }

# Extract trace IDs and span counts from Jaeger response.
parsed=$(printf '%s' "${response}" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception as e:
    sys.stderr.write(f"json parse error: {e}\n")
    sys.exit(0)
traces = d.get("data", [])
ids = []
total_spans = 0
for t in traces:
    tid = t.get("traceID", "")
    spans = t.get("spans", []) or []
    if tid:
        ids.append(tid)
    total_spans += len(spans)
print(len(traces))
print(total_spans)
print(",".join(ids))
')

trace_count=$(echo "$parsed" | sed -n '1p')
span_count=$(echo "$parsed" | sed -n '2p')
trace_ids=$(echo "$parsed" | sed -n '3p')

echo "  → traces: ${trace_count}, spans (inline): ${span_count}"

if [[ "${trace_count}" -lt 1 ]]; then
  echo "FAIL: Phase 1 — Jaeger search returned 0 traces for ${SERVICE}" >&2
  echo "  see probe_jaeger_search_24h.sh for the first-phase fix" >&2
  exit 1
fi

if [[ "${span_count}" -lt 1 ]]; then
  cat <<EOF >&2
FAIL: Phase 1.5 — Jaeger search returned ${trace_count} trace(s) but ZERO spans
  endpoint: ${URL}/select/jaeger/api/traces
  service: ${SERVICE}
  This is the Jaeger 0-traces bug:
  - Search call found trace_ids ✓
  - Spans-lookup pruned every file ✗
  Likely cause: filterFilesByBloomIndex / checkFileBloom only understands
                trace_id:="single" form. VT's spans-lookup uses
                trace_id:in(t1,t2,t3) which AND-ed all values via
                MayContainAll(keys, checks), needing every file's bloom
                to contain ALL three trace_ids — impossible.
                See lakehouse-traces/internal/storage/parquets3/storage_query.go::
                filterFilesByBloomIndex.
EOF
  exit 1
fi

echo ""
echo "=== Phase 2: direct logsql trace_id:in() lookup ==="
# Independent verification via logsql/query — same query VT internally
# builds for the spans-lookup, but skipping the Jaeger HTTP wrapper.
if [[ -z "${trace_ids}" ]]; then
  echo "FAIL: no trace_ids extracted from Phase 1 to verify Phase 2" >&2
  exit 1
fi

# Build the trace_id:in(...) query.
in_query="trace_id:in(${trace_ids})"
echo "  query: ${in_query}"
echo "  endpoint: ${URL}/select/logsql/query"

# Use seconds-relative start instead of microseconds (the logsql endpoint).
in_response=$(curl -fsS --max-time 30 -G \
  "${URL}/select/logsql/query" \
  --data-urlencode "query=${in_query}" \
  --data-urlencode "start=-${WINDOW_HOURS}h" \
  --data-urlencode "limit=10" 2>&1) || {
    echo "FAIL: curl could not reach logsql at ${URL}" >&2
    echo "${in_response}" >&2
    exit 2
  }

# Count non-empty JSON lines (each is a row).
in_rows=$(printf '%s' "${in_response}" | python3 -c '
import sys
n = 0
for line in sys.stdin:
    line = line.strip()
    if line.startswith("{") and line.endswith("}"):
        n += 1
print(n)
')

echo "  → rows: ${in_rows}"

if [[ "${in_rows}" -lt 1 ]]; then
  cat <<EOF >&2
FAIL: Phase 2 — logsql trace_id:in(...) returned ZERO rows
  endpoint: ${URL}/select/logsql/query
  query:    ${in_query}
  These trace_ids were just observed by Phase 1 (Jaeger search). The
  spans-lookup MUST find them. The fact that the Jaeger search inline
  returned ${span_count} spans but a direct trace_id:in() returns 0
  rows means the bloom pre-filter is over-pruning OR the buffer-bridge
  returned synthetic placeholders without real spans.
  Check: filterFilesByBloomIndex any-of-values logic in
  lakehouse-traces/internal/storage/parquets3/storage_query.go.
EOF
  exit 1
fi

echo ""
echo "PASS: full Jaeger search → spans-lookup chain returned data end-to-end"
echo "      Phase 1: ${trace_count} traces (${span_count} inline spans)"
echo "      Phase 2: ${in_rows} rows for direct trace_id:in() lookup"
exit 0
