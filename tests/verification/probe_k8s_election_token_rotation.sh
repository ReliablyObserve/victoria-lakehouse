#!/usr/bin/env bash
#
# probe_k8s_election_token_rotation.sh
#
# PR #98 Item 1 — process-local smoke probe that the SA token re-read
# path works. Runs the unit tests that drive the rotation contract
# without a real cluster.
#
# Exit codes:
#   0 — token re-read works as expected
#   1 — one or more tests failed
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "=== probe_k8s_election_token_rotation ==="
if GOWORK=off go test \
      -count=1 \
      -timeout=30s \
      -run "TestK8sElector_TokenRotation" \
      ./internal/election/; then
  echo "PASS: SA token re-read path verified"
  exit 0
else
  echo "FAIL: token rotation re-read broken; see go test output" >&2
  exit 1
fi
