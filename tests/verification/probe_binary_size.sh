#!/usr/bin/env bash
#
# probe_binary_size.sh
#
# Hard upper bound on the lakehouse-logs and lakehouse-traces stripped binary
# size. Locks the binary-size win delivered by PR #96 (Option B: hand-rolled
# rest+meta/v1 K8s elector vs. the heavy k8s.io/client-go closure). The
# pre-PR baseline was 55 MB; the always-on K8s Option B baseline is ~37 MB.
# 40 MB gives a 3 MB cushion for future churn.
#
# Usage:
#   tests/verification/probe_binary_size.sh
#
# Exit codes:
#   0 — both binaries within bounds
#   1 — at least one binary over bound
#   2 — toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found in PATH" >&2
  exit 2
fi

LIMIT_MB="${LIMIT_MB:-40}"
LIMIT_BYTES=$(( LIMIT_MB * 1024 * 1024 ))

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

check_binary() {
  local name="$1"
  local pkg="$2"
  local workdir="$3"
  local out="$TMP/$name"
  ( cd "$workdir" && \
    GOWORK=off CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "$out" "$pkg" )
  local size
  size=$(stat -f%z "$out" 2>/dev/null || stat -c%s "$out")
  local mb
  mb=$(awk "BEGIN{printf \"%.1f\", $size/1024/1024}")
  if (( size > LIMIT_BYTES )); then
    echo "  FAIL: $name = ${size} bytes (${mb} MB); limit = ${LIMIT_MB} MB" >&2
    return 1
  fi
  echo "  PASS: $name = ${mb} MB (limit ${LIMIT_MB} MB)"
}

echo "=== probe_binary_size (limit ${LIMIT_MB} MB) ==="
status=0
check_binary lakehouse-logs   ./cmd/lakehouse-logs "$REPO_ROOT"                    || status=1
check_binary lakehouse-traces .                    "$REPO_ROOT/lakehouse-traces"   || status=1

if [[ $status -eq 0 ]]; then
  echo "PASS: both binaries fit within ${LIMIT_MB} MB ceiling"
else
  echo "FAIL: at least one binary exceeds the size ceiling" >&2
fi
exit $status
