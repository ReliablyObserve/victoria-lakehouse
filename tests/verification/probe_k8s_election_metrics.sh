#!/usr/bin/env bash
#
# probe_k8s_election_metrics.sh
#
# PR #98 Item 8 — fast smoke probe that the lakehouse_leader_election_*
# metric families are wired correctly. Runs the unit test that drives a
# K8sElector through acquire → renew → release against a recording hook
# and asserts every metric was emitted.
#
# This is the "process-local" version of the kind e2e section 8 metrics
# assertion. It runs in ~1 s and gives CI fast feedback without spinning
# a kind cluster.
#
# Exit codes:
#   0 — metrics emitted as expected
#   1 — at least one metric family did not fire
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "=== probe_k8s_election_metrics ==="
if GOWORK=off go test \
      -count=1 \
      -timeout=30s \
      -run "TestK8sElector_EmitsExpectedMetrics|TestK8sElector_RenewFailure_EmitsResultLabel|TestK8sElector_RenewConflict_EmitsResultLabel" \
      ./internal/election/; then
  echo "PASS: lakehouse_leader_election_* metric families emitted correctly"
  exit 0
else
  echo "FAIL: at least one election metric family did not emit; see go test output" >&2
  exit 1
fi
