#!/usr/bin/env bash
set -euo pipefail

# Config sweep: test different MaxConcurrent and FileWorkers values.
# Requires: lakehouse binary built, MinIO running, data already seeded.
#
# Usage:
#   ./scripts/config-sweep.sh http://localhost:9428 ./lakehouse-logs config.yaml results/

TARGET="${1:-http://localhost:9428}"
BINARY="${2:-./lakehouse-logs}"
BASE_CONFIG="${3:-config.yaml}"
RESULTS_DIR="${4:-results/config-sweep}"

LOADTEST_DURATION="${LOADTEST_DURATION:-30s}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1,10,50,100}"

SWEEP_PAIRS=(
  "16,4"
  "32,8"
  "64,16"
)

mkdir -p "$RESULTS_DIR"

echo "=== Config Sweep ==="
echo "Target: $TARGET"
echo "Binary: $BINARY"
echo "Base config: $BASE_CONFIG"
echo "Results: $RESULTS_DIR"
echo "Pairs: ${SWEEP_PAIRS[*]}"
echo ""

stop_lakehouse() {
  pkill -f "$BINARY" 2>/dev/null || true
  sleep 1
}

for pair in "${SWEEP_PAIRS[@]}"; do
  IFS=',' read -r max_conc file_workers <<< "$pair"
  label="mc${max_conc}_fw${file_workers}"
  echo "--- Testing MaxConcurrent=$max_conc FileWorkers=$file_workers ---"

  stop_lakehouse

  sweep_config="${RESULTS_DIR}/config_${label}.yaml"
  sed -e "s/max_concurrent:.*/max_concurrent: ${max_conc}/" \
      -e "s/file_workers:.*/file_workers: ${file_workers}/" \
      "$BASE_CONFIG" > "$sweep_config"

  "$BINARY" -config="$sweep_config" &
  LAKEHOUSE_PID=$!
  echo "  Started lakehouse (PID=$LAKEHOUSE_PID)"

  for i in $(seq 1 30); do
    if curl -sf "${TARGET}/health" > /dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if ! curl -sf "${TARGET}/health" > /dev/null 2>&1; then
    echo "  ERROR: lakehouse failed to start"
    stop_lakehouse
    continue
  fi

  output="${RESULTS_DIR}/result_${label}.json"
  GOWORK=off go run ./cmd/loadtest \
    -mode=concurrent \
    -target="$TARGET" \
    -duration="$LOADTEST_DURATION" \
    -concurrency="$CONCURRENCY_LEVELS" \
    -output="$output" || true

  echo "  Results: $output"
  stop_lakehouse
  echo ""
done

echo "=== Comparison Summary ==="
printf "%-20s %8s %8s %8s %8s\n" "Config" "C=1 p95" "C=50 p95" "C=100 p95" "QPS@100"
printf '%0.s-' {1..56}
echo ""

for pair in "${SWEEP_PAIRS[@]}"; do
  IFS=',' read -r max_conc file_workers <<< "$pair"
  label="mc${max_conc}_fw${file_workers}"
  result="${RESULTS_DIR}/result_${label}.json"

  if [ ! -f "$result" ]; then
    printf "%-20s %8s %8s %8s %8s\n" "$label" "N/A" "N/A" "N/A" "N/A"
    continue
  fi

  extract() {
    local conc=$1 field=$2
    python3 -c "
import json, sys
d = json.load(open('$result'))
for r in d.get('concurrent_results', []):
    if r['concurrency'] == $conc:
        print(f\"{r['$field']:.1f}\")
        sys.exit()
print('N/A')
" 2>/dev/null || echo "N/A"
  }

  printf "%-20s %8s %8s %8s %8s\n" "$label" \
    "$(extract 1 p95_ms)" "$(extract 50 p95_ms)" "$(extract 100 p95_ms)" "$(extract 100 qps)"
done

echo ""
echo "Done. Detailed results in $RESULTS_DIR/"
