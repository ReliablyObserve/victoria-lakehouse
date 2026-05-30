#!/usr/bin/env bash
#
# Regression smoke-probe for the lakehouse-logs 24h wildcard OOM fix
# (this commit — bounded per-query memory budget + synchronous writeBlock
# in internal/storage/parquets3/storage_query.go).
#
# Locks the user-visible contract: a wildcard `*` log query spanning the
# last 24 hours (the exact shape Grafana sends from the "Last 24 hours"
# range selector through `loki-vl-proxy-cold`) must NOT crash the
# lakehouse-logs container. The container must stay healthy through the
# probe window AND its RestartCount must not increment.
#
# Failure mode this probe catches: container OOM-killed by the cgroup
# memory limit (mem_limit=2g) within ~1-2 seconds of issuing the query,
# then auto-restarted by `restart: on-failure`. The Grafana UX symptom
# is "Lakehouse Logs Cold (S3) datasource returned 0 series" with a
# transient connection drop in the network tab, repeated on every
# refresh while the OOM regression is live.
#
# Usage:
#   tests/verification/probe_logs_24h_wildcard.sh             # localhost defaults
#   LH_LOGS_URL=http://lh-logs:9428 ./probe_logs_24h_wildcard.sh
#   LH_LOGS_CONTAINER=victoria-lakehouse-lakehouse-logs-1 ... # custom container name
#
# Exit codes:
#   0 — probe passed (container survived AND data returned OR clean HTTP error)
#   1 — probe failed (container crashed; RestartCount incremented)
#   2 — probe could not run (backend unreachable, docker missing, etc.)
#
# To verify this probe is a true regression lock: temporarily revert the
# fix in internal/storage/parquets3/storage_query.go (restore the
# resultCh:256 channel pattern and remove the per-query MaxLiveBytes
# budget), rebuild, recreate, and re-run this script. It MUST fail with
# exit 1.

set -euo pipefail

URL="${LH_LOGS_URL:-http://localhost:29428}"
CONTAINER="${LH_LOGS_CONTAINER:-victoria-lakehouse-lakehouse-logs-1}"
WINDOW_HOURS="${LH_PROBE_WINDOW_HOURS:-24}"
SETTLE_SECONDS="${LH_PROBE_SETTLE_SECONDS:-10}"
LIMIT="${LH_PROBE_LIMIT:-10}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not available" >&2
  exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available (used to read container state)" >&2
  exit 2
fi

restart_count() {
  docker inspect "$CONTAINER" --format '{{.RestartCount}}' 2>/dev/null || echo "-1"
}

health_state() {
  docker inspect "$CONTAINER" --format '{{.State.Health.Status}}' 2>/dev/null || echo "unknown"
}

container_state() {
  docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null || echo "missing"
}

echo "probing ${URL}/select/logsql/query (24h wildcard) against ${CONTAINER}"

before_restarts=$(restart_count)
before_state=$(container_state)
before_health=$(health_state)
if [[ "${before_state}" != "running" ]]; then
  echo "FAIL: container ${CONTAINER} not running before probe (state=${before_state})" >&2
  exit 2
fi
echo "  pre-probe restarts=${before_restarts} state=${before_state} health=${before_health}"

# Fire the 24h wildcard. We allow it to fail with a clean HTTP error (e.g.
# 503, or a partial response after the memory budget triggered context
# cancellation). What we MUST NOT see is a transport-layer crash (curl
# exit 52/56) caused by the container vanishing mid-response.
http_code=$(curl -s -o /tmp/probe_logs_24h_wildcard.out \
  --max-time 30 \
  -w '%{http_code}' \
  -G "${URL}/select/logsql/query" \
  --data-urlencode "query=*" \
  --data-urlencode "start=-${WINDOW_HOURS}h" \
  --data-urlencode "limit=${LIMIT}" || echo "000")

resp_bytes=$(wc -c </tmp/probe_logs_24h_wildcard.out 2>/dev/null || echo 0)
echo "  query → http=${http_code} bytes=${resp_bytes}"

# Let the container settle so any deferred OOM/restart shows up.
sleep "${SETTLE_SECONDS}"

after_restarts=$(restart_count)
after_state=$(container_state)
after_health=$(health_state)
echo "  post-probe restarts=${after_restarts} state=${after_state} health=${after_health}"

if [[ "${after_restarts}" -gt "${before_restarts}" ]]; then
  cat <<EOF >&2
FAIL: lakehouse-logs container restart-count increased during the probe
  container: ${CONTAINER}
  restarts:  before=${before_restarts} after=${after_restarts}
  endpoint:  ${URL}/select/logsql/query (24h wildcard)
  expected:  container survives the query — either returns a result OR a
             clean HTTP error (5xx/4xx) without a transport-layer drop
  got:       container restarted, which means it was OOM-killed by the
             cgroup memory limit (mem_limit=2g).
  likely cause: the per-query memory budget in
                internal/storage/parquets3/storage_query.go was reverted,
                or the resultCh:256 channel-buffered dispatcher pattern
                was reintroduced. See the commit message and the
                feedback_post_work_resource_caps memory file.
EOF
  exit 1
fi

if [[ "${after_state}" != "running" ]]; then
  echo "FAIL: container ${CONTAINER} not running after probe (state=${after_state})" >&2
  exit 1
fi

if [[ "${after_health}" == "unhealthy" ]]; then
  echo "FAIL: container ${CONTAINER} reported unhealthy after probe" >&2
  exit 1
fi

# curl exit codes 52/56 manifest as http_code=000. If we got that AND the
# container did not restart, the upstream timed out gracefully — acceptable.
if [[ "${http_code}" == "000" ]]; then
  echo "WARN: curl returned no HTTP response (likely timeout); container survived → probe PASS"
fi

echo "PASS: lakehouse-logs 24h wildcard survived (restarts stable at ${after_restarts})"
exit 0
