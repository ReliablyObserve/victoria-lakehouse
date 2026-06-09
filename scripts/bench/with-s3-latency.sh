#!/usr/bin/env bash
# Run a command with S3 latency injected ONLY for its duration.
#
# This is the safe way to use the s3-latency toxiproxy in benchmarks: it injects
# before the command and ALWAYS removes the toxic afterward via a trap — on normal
# exit, on error, AND on Ctrl-C / SIGTERM. So an interrupted or failed benchmark
# never leaves the toxic active polluting the normal/e2e compose (which is exactly
# what happened: a manual `inject-s3-latency.sh 100 30` was left injected and made
# every cold-LH query 50x slower).
#
# Usage:
#   scripts/bench/with-s3-latency.sh <mean_ms> <jitter_ms> -- <command...>
#   scripts/bench/with-s3-latency.sh 100 30 -- scripts/bench/full-scope-s3-bench.sh 20
#
# mean_ms=0 runs the command with pass-through (no added latency) but still
# guarantees a clean toxic state before and after.
set -uo pipefail

MEAN=${1:?usage: with-s3-latency.sh <mean_ms> <jitter_ms> -- <command...>}
JITTER=${2:?usage: with-s3-latency.sh <mean_ms> <jitter_ms> -- <command...>}
shift 2
[ "${1:-}" = "--" ] && shift

SCRIPTS="$(cd "$(dirname "$0")/.." && pwd)"
INJECT="$SCRIPTS/inject-s3-latency.sh"

cleanup() {
  "$INJECT" clear >/dev/null 2>&1 || true
  echo "[with-s3-latency] toxic cleared — pass-through restored on s3-latency proxy"
}
trap cleanup EXIT INT TERM

# Start from a known-clean state, then inject for the run (unless mean=0).
"$INJECT" clear >/dev/null 2>&1 || true
if [ "$MEAN" -gt 0 ] 2>/dev/null; then
  "$INJECT" "$MEAN" "$JITTER"
  echo "[with-s3-latency] injected ${MEAN}ms ± ${JITTER}ms for the duration of: $*"
else
  echo "[with-s3-latency] mean=0 → pass-through (no added latency) for: $*"
fi

"$@"
