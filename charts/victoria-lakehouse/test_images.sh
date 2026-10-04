#!/usr/bin/env bash
set -euo pipefail

CHART_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_VERSION="$(helm show chart "${CHART_DIR}" | awk '/^appVersion:/ {gsub(/"/, "", $2); print $2}')"
if [[ ! "${APP_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[[:alnum:].-]+)?$ ]]; then
  echo "FAIL: expected a semantic chart app version, got ${APP_VERSION}" >&2
  exit 1
fi

# These references follow the auto-release publisher's repository namespace
# and v-prefixed version tags, rather than accepting any renderable image.
LOG_REPO="ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-logs"
TRACE_REPO="ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-traces"

assert_images() {
  local description="$1" expected="$2" actual
  shift 2
  actual="$(helm template image-tests "${CHART_DIR}" "$@" | awk '$1 == "image:" {gsub(/"/, "", $2); print $2}' | LC_ALL=C sort)"
  expected="$(printf '%s\n' "${expected}" | LC_ALL=C sort)"
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'FAIL: %s\nExpected:\n%s\nActual:\n%s\n' "${description}" "${expected}" "${actual}" >&2
    exit 1
  fi
  echo "PASS: ${description}"
}

logs="${LOG_REPO}:v${APP_VERSION}"
traces="${TRACE_REPO}:v${APP_VERSION}"
assert_images "default logs-only release" "${logs}"$'\n'"${logs}"
assert_images "traces-only release" "${traces}"$'\n'"${traces}" --set logs.enabled=false --set traces.enabled=true
assert_images "both signals" "${logs}"$'\n'"${logs}"$'\n'"${traces}"$'\n'"${traces}" --set traces.enabled=true

logs="${LOG_REPO}:build-123"
traces="${TRACE_REPO}:build-123"
assert_images "explicit tag is preserved" "${logs}"$'\n'"${logs}"$'\n'"${traces}"$'\n'"${traces}" --set traces.enabled=true --set image.tag=build-123

logs="registry.example.test/custom/logs:build-123"
traces="registry.example.test/custom/traces:build-123"
assert_images "explicit repositories and tag are preserved" "${logs}"$'\n'"${logs}"$'\n'"${traces}"$'\n'"${traces}" --set traces.enabled=true --set image.tag=build-123 --set image.logs.repository=registry.example.test/custom/logs --set image.traces.repository=registry.example.test/custom/traces

logs="${LOG_REPO}:v${APP_VERSION}-fips"
traces="${TRACE_REPO}:v${APP_VERSION}-fips"
assert_images "explicit FIPS release tag is preserved" "${logs}"$'\n'"${logs}"$'\n'"${traces}"$'\n'"${traces}" --set traces.enabled=true --set "image.tag=v${APP_VERSION}-fips"
