#!/usr/bin/env bash
#
# probe_k8s_election_failure_modes.sh
#
# PR #98 Items 2/3/5/6/7 — process-local smoke probe for the 5 critical
# K8s elector failure modes (lease deleted, lease edited, apiserver 5xx,
# apiserver timeout, same-identity reclaim, same-identity collision).
#
# Each one of these was discovered to either need a production code fix
# (item 2: NotFound on renew → return false, nil to trigger re-acquire)
# or already had correct behavior that we now lock with a regression
# test.
#
# Exit codes:
#   0 — all 6 failure-mode tests pass
#   1 — at least one failed
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

echo "=== probe_k8s_election_failure_modes ==="
if GOWORK=off go test \
      -count=1 \
      -timeout=60s \
      -run "TestK8sElector_LeaseDeletedExternally|TestK8sElector_LeaseEditedExternally|TestK8sElector_Apiserver5xx|TestK8sElector_ApiserverTimeout|TestK8sElector_ReclaimsOwnLease|TestK8sElector_SameIdentityTwoCandidates" \
      ./internal/election/; then
  echo "PASS: K8s elector failure-mode regressions hold"
  exit 0
else
  echo "FAIL: at least one failure-mode regression broke; see go test output" >&2
  exit 1
fi
