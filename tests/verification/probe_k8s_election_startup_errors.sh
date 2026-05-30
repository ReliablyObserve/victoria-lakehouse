#!/usr/bin/env bash
#
# probe_k8s_election_startup_errors.sh
#
# PR #98 Item 4 — process-local smoke probe that bootstrap() surfaces
# startup failures via StartupError() instead of silently returning.
#
# Exit codes:
#   0 — startup error paths work
#   1 — one or more tests failed
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "=== probe_k8s_election_startup_errors ==="
if GOWORK=off go test \
      -count=1 \
      -timeout=30s \
      -run "TestK8sElector_Bootstrap|TestK8sElector_NoServiceAccountToken|TestK8sElector_StartupError" \
      ./internal/election/; then
  echo "PASS: bootstrap startup-error surfacing verified"
  exit 0
else
  echo "FAIL: startup error path broken; see go test output" >&2
  exit 1
fi
