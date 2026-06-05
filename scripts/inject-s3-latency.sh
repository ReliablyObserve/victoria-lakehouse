#!/usr/bin/env bash
# Inject (or clear) S3 latency on the lakehouse-net via the toxiproxy
# instance sitting between lakehouse-{logs,traces} and minio.
#
# Usage:
#   scripts/inject-s3-latency.sh <mean_ms> [jitter_ms]
#   scripts/inject-s3-latency.sh clear
#
# Examples:
#   scripts/inject-s3-latency.sh 100 30   # ~100ms ± 30ms per request (typical p50)
#   scripts/inject-s3-latency.sh 300 100  # degraded region behavior
#   scripts/inject-s3-latency.sh 2000 500 # bad day in us-east-1
#   scripts/inject-s3-latency.sh clear    # remove all toxics, back to pass-through
#
# The added latency is applied to every byte of every S3 request, so the
# perceived impact is "delay until first byte = mean_ms ± jitter". Read
# patterns (GetObject, ListObjects, manifest refresh) and write patterns
# (PutObject during flush, multi-part during compaction) all feel it.

set -euo pipefail

PROXY=${TOXIPROXY:-victoria-lakehouse-s3-latency-1}
TOXIC=s3-latency

if [[ "${1:-}" == "clear" ]]; then
  docker exec "$PROXY" /toxiproxy-cli toxic remove --toxicName "$TOXIC" s3 2>/dev/null || true
  echo "S3 latency injection cleared (pass-through restored)"
  exit 0
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <mean_ms> [jitter_ms]   |   $0 clear" >&2
  exit 1
fi

MEAN=$1
JITTER=${2:-0}

# Idempotent: remove any existing latency toxic before adding a new one.
docker exec "$PROXY" /toxiproxy-cli toxic remove --toxicName "$TOXIC" s3 2>/dev/null || true
docker exec "$PROXY" /toxiproxy-cli toxic add \
  --toxicName "$TOXIC" \
  --type latency \
  --attribute latency="$MEAN" \
  --attribute jitter="$JITTER" \
  s3

echo "S3 latency injected: ${MEAN}ms mean, ${JITTER}ms jitter"
echo "All lakehouse-{logs,traces} S3 calls now see this delay."
echo "Watch tail latencies in Grafana or via direct query timing."
