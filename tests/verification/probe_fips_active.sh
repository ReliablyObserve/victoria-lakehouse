#!/usr/bin/env bash
#
# probe_fips_active.sh
#
# Asserts that the FIPS-tagged variant of the LH binary has Go's native
# FIPS 140-3 mode active when GOFIPS140=v1.0.0 is set at build time and
# GODEBUG=fips140=on is set at runtime. The probe runs the `fips-status`
# subcommand and inspects the exit code and stdout.
#
# Two scenarios are exercised:
#
#   1. Default build (no GOFIPS140), no GODEBUG=fips140=on at runtime:
#      fips-status MUST report "disabled" and exit 1. This locks the
#      "non-FIPS image, default operator runtime" contract.
#
#   2. FIPS build (GOFIPS140=v1.0.0 set at build), with the runtime
#      GODEBUG=fips140=on, fips-status MUST report "enabled" and exit 0.
#      This locks the "FIPS image, FIPS-mode operator runtime" contract.
#
# (A third combo, default build + GODEBUG=fips140=on, also reports enabled
# in Go 1.26 because the certified module is always present; the
# distinction GOFIPS140=v1.0.0 makes is enforcing-by-default in the build,
# not gating availability at runtime. That combo is intentionally not
# locked here — operators have flexibility, and the lock that matters is
# "FIPS build + FIPS runtime works end to end".)
#
# Usage:
#   tests/verification/probe_fips_active.sh
#
# Exit codes:
#   0 — both scenarios behaved as expected
#   1 — at least one scenario diverged
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found in PATH" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

build_logs() {
  local out="$1"
  local fips="${2:-0}"
  local env_args=("GOWORK=off" "CGO_ENABLED=0")
  if [[ "$fips" == "1" ]]; then
    env_args+=("GOFIPS140=v1.0.0")
  fi
  ( cd "$REPO_ROOT" && \
    env "${env_args[@]}" go build -trimpath -ldflags='-s -w' -o "$out" ./cmd/lakehouse-logs )
}

echo "=== probe_fips_active ==="

echo "scenario 1: default build, no GODEBUG=fips140=on at runtime..."
build_logs "$TMP/lh-default"
set +e
"$TMP/lh-default" fips-status > "$TMP/out.default" 2>&1
rc=$?
set -e
got=$(cat "$TMP/out.default")
if [[ "$rc" -eq 1 && "$got" == *"fips140: disabled"* ]]; then
  echo "  PASS: default build (no GODEBUG) reports fips140: disabled (rc=1)"
else
  echo "  FAIL: default build expected 'fips140: disabled' rc=1; got rc=$rc, output='$got'" >&2
  exit 1
fi

echo "scenario 2: FIPS build (GOFIPS140=v1.0.0)..."
build_logs "$TMP/lh-fips" 1
set +e
GODEBUG=fips140=on "$TMP/lh-fips" fips-status > "$TMP/out.fips" 2>&1
rc=$?
set -e
got=$(cat "$TMP/out.fips")
if [[ "$rc" -eq 0 && "$got" == *"fips140: enabled"* ]]; then
  echo "  PASS: FIPS build with GODEBUG=fips140=on reports fips140: enabled (rc=0)"
else
  echo "  FAIL: FIPS build expected 'fips140: enabled' rc=0; got rc=$rc, output='$got'" >&2
  exit 1
fi

echo "PASS: FIPS contract holds for both default and FIPS-tagged builds"
