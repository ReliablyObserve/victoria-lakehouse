#!/usr/bin/env bash
#
# Production-shape regression smoke-probe for the lakehouse-logs N-day
# wildcard OOM bug. This is the lock for the underlying bound — not
# just the 24h reproducer that the prior probe covered.
#
# Locks the user-visible contract: a wildcard `*` log query spanning
# the last N days (2-day and 7-day by default, matching the exact
# shape Grafana sends from the "Last 2 days" / "Last 7 days" range
# selector through `loki-vl-proxy-cold`) MUST NOT OOM-kill the
# lakehouse-logs container.
#
# Sized to the live cluster's actual file count: ~600 parquet files
# across ~6 days of data. A 2-day wildcard touches ~200 files; a 7-day
# wildcard touches the full manifest (subject to
# `-lakehouse.query.max-files-per-query=300`).
#
# Why this exists when probe_logs_24h_wildcard.sh already runs the 24h
# case: the user reported that AFTER the 24h fix landed, the 2-day
# wildcard still OOMs. The 24h probe is too small to catch the
# scalability regression; the row-group decoder fanout doesn't blow up
# until the workload crosses ~30-60 files. This probe must reproduce
# the user's actual failure shape and survive.
#
# Failure mode this probe catches: container OOM-killed by the cgroup
# memory limit (mem_limit=2g) while decoding row groups across 100+
# files concurrently in 16 file workers. Symptom: curl exit 52/56
# (connection reset mid-stream), RestartCount increments by 1, Grafana
# shows "Lakehouse Logs Cold (S3) datasource returned 0 series" with
# a transient connection drop on every refresh.
#
# Usage:
#   tests/verification/probe_logs_Nday_wildcard.sh               # 2-day and 7-day
#   LH_PROBE_WINDOWS_DAYS="2 7" ./probe_logs_Nday_wildcard.sh    # custom windows
#   LH_PROBE_WINDOWS_DAYS=14 ./probe_logs_Nday_wildcard.sh       # 14-day only
#   LH_LOGS_URL=http://lh-logs:9428 ./probe_logs_Nday_wildcard.sh
#
# Exit codes:
#   0 — all windows probed survived (HTTP 4xx/5xx or 200 acceptable;
#       container did not restart)
#   1 — at least one window crashed the container (RestartCount up)
#   2 — probe could not run (backend unreachable, docker missing)
#
# To verify this probe is a true regression lock:
#   1. Revert the row-group decoder semaphore (set rgDecodeSem cap to
#      1000) AND remove the splitAndEmitDataBlock call from
#      readRowGroupWithProjection (both modules).
#   2. Rebuild: docker compose -f deployment/docker/docker-compose-e2e.yml \
#        build lakehouse-logs lakehouse-traces
#   3. Recreate: docker compose -f deployment/docker/docker-compose-e2e.yml \
#        up -d --force-recreate lakehouse-logs lakehouse-traces
#   4. Re-run this script — it MUST fail with exit 1 on the 2-day window.

set -uo pipefail

URL="${LH_LOGS_URL:-http://localhost:29428}"
CONTAINER="${LH_LOGS_CONTAINER:-victoria-lakehouse-lakehouse-logs-1}"
WINDOWS_DAYS="${LH_PROBE_WINDOWS_DAYS:-2 7}"
SETTLE_SECONDS="${LH_PROBE_SETTLE_SECONDS:-15}"
LIMIT="${LH_PROBE_LIMIT:-10}"
QUERY_TIMEOUT="${LH_PROBE_QUERY_TIMEOUT:-120}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not available" >&2
  exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available (used to read container state)" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not available (used for ns timestamps)" >&2
  exit 2
fi

restart_count() {
  docker inspect "$CONTAINER" --format '{{.RestartCount}}' 2>/dev/null || echo "-1"
}

container_state() {
  docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null || echo "missing"
}

health_state() {
  docker inspect "$CONTAINER" --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown"
}

if [[ "$(container_state)" != "running" ]]; then
  echo "FAIL: container ${CONTAINER} not running before probe" >&2
  exit 2
fi

overall_rc=0
for days in $WINDOWS_DAYS; do
  echo
  echo "============================================================"
  echo "PROBE: ${days}-day wildcard against ${CONTAINER} (mem_limit=2g)"
  echo "============================================================"

  end_ns=$(python3 -c "import time; print(int(time.time()*1000000000))")
  start_ns=$((end_ns - days * 86400 * 1000000000))
  before_rc=$(restart_count)
  before_state=$(container_state)
  before_health=$(health_state)
  echo "  pre-probe: restarts=${before_rc} state=${before_state} health=${before_health}"
  echo "  range:     ${days} day(s)  start=${start_ns} end=${end_ns}"

  out_file="/tmp/probe_logs_${days}day_wildcard.out"
  http_code=$(curl -s -o "$out_file" \
    --max-time "$QUERY_TIMEOUT" \
    -w '%{http_code}' \
    -G "${URL}/select/logsql/query" \
    --data-urlencode "query=*" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "limit=${LIMIT}" || echo "000")

  resp_bytes=$(stat -f %z "$out_file" 2>/dev/null || stat -c %s "$out_file" 2>/dev/null || echo 0)
  echo "  query:     http=${http_code} bytes=${resp_bytes}"

  # Wait for any deferred OOM/restart to materialise.
  sleep "$SETTLE_SECONDS"

  after_rc=$(restart_count)
  after_state=$(container_state)
  after_health=$(health_state)
  echo "  post-probe: restarts=${after_rc} state=${after_state} health=${after_health}"

  if [[ "${after_rc}" -gt "${before_rc}" ]]; then
    cat <<EOF >&2
FAIL (${days}-day): container restart-count increased during the probe
  container: ${CONTAINER}
  restarts:  before=${before_rc} after=${after_rc}
  endpoint:  ${URL}/select/logsql/query (${days}-day wildcard)
  expected:  container survives the query — either returns a result
             OR a clean HTTP 4xx/5xx without a transport-layer drop
  got:       container restarted = OOM-killed by cgroup mem_limit=2g
  likely cause: the row-group decoder semaphore (rgDecodeSem in
                internal/storage/parquets3/query_memory_budget.go) or
                the chunked DataBlock emission (splitAndEmitDataBlock
                wired into readRowGroupWithProjection) was reverted.
                See the commit message and feedback_prove_on_large_data.
EOF
    overall_rc=1
    continue
  fi

  if [[ "${after_state}" != "running" ]]; then
    echo "FAIL (${days}-day): container not running after probe (state=${after_state})" >&2
    overall_rc=1
    continue
  fi

  if [[ "${http_code}" == "000" ]]; then
    echo "  WARN (${days}-day): curl returned no HTTP response (likely timeout); container survived → PASS"
  fi

  echo "  PASS (${days}-day): container survived; restarts stable at ${after_rc}"
done

echo
if [[ "$overall_rc" -eq 0 ]]; then
  echo "ALL ${WINDOWS_DAYS}-day wildcard probes PASSED"
fi
exit "$overall_rc"
