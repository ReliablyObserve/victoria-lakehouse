#!/usr/bin/env bash
#
# probe_k8s_election_failover.sh
#
# httptest-based smoke probe for the K8sElector failover semantics. Spins up
# a tiny local Go program that mounts the elector against an httptest server
# implementing coordination.k8s.io/v1 Lease CAS semantics, and asserts:
#
#   1. The first candidate to call Start() becomes leader.
#   2. After we Stop() that candidate, a different candidate takes over
#      within LeaseDuration + RenewDeadline (with margin).
#   3. Lease.holderIdentity reflects the new holder.
#
# This is the "process-local" version of the kind-based e2e test at
# tests/e2e-k8s/test_leader_election.sh. It runs in 2-3 seconds and gives
# CI fast coverage of the failover state machine without spinning a real
# Kubernetes cluster.
#
# Usage:
#   tests/verification/probe_k8s_election_failover.sh
#
# Exit codes:
#   0 — failover happened as expected
#   1 — failover did not happen within the deadline
#   2 — go toolchain missing
set -euo pipefail

if ! command -v go >/dev/null 2>&1; then
  echo "FAIL: go toolchain not found in PATH" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# We exercise the integration test in -run mode. It's the canonical
# multi-candidate failover assertion (acquire -> stop -> successor takes over).
echo "=== probe_k8s_election_failover ==="
echo "running TestK8sElector_Integration_MultiCandidate (full failover state machine)..."
cd "$REPO_ROOT"
if GOWORK=off go test \
      -count=1 \
      -timeout=60s \
      -run TestK8sElector_Integration_MultiCandidate \
      ./internal/election/; then
  echo "PASS: K8sElector failover (acquire -> stop -> successor) within bounds"
  exit 0
else
  echo "FAIL: K8sElector failover probe failed; see go test output above" >&2
  exit 1
fi
