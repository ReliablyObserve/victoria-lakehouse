#!/usr/bin/env bash
set -euo pipefail

# Run E2E smoke tests against a running docker-compose stack.
# Usage: ./scripts/smoke-test.sh [--up] [--down]
#
# Options:
#   --up     Bring up the compose stack before testing (includes build)
#   --down   Tear down the compose stack after testing
#
# Environment variables (override service URLs if not using default ports):
#   LOGS_BASE_URL    (default: http://localhost:29428)
#   TRACES_BASE_URL  (default: http://localhost:20428)
#   LOKI_PROXY_URL   (default: http://localhost:23100)
#   VLSELECT_URL     (default: http://localhost:29471)
#   VTSELECT_URL     (default: http://localhost:20471)

COMPOSE_FILE="deployment/docker/docker-compose-e2e.yml"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

DO_UP=false
DO_DOWN=false

for arg in "$@"; do
  case "$arg" in
    --up)   DO_UP=true ;;
    --down) DO_DOWN=true ;;
    *)      echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

if $DO_UP; then
  echo "==> Building and starting compose stack..."
  docker compose -f "$COMPOSE_FILE" build --parallel
  docker compose -f "$COMPOSE_FILE" up -d
  echo "==> Waiting for services to become healthy..."
  sleep 10
fi

echo "==> Running smoke tests..."
GOWORK=off go test -tags e2e -v -count=1 ./tests/e2e/ -run "TestSmoke" -timeout 180s
result=$?

if $DO_DOWN; then
  echo "==> Tearing down compose stack..."
  docker compose -f "$COMPOSE_FILE" down -v
fi

exit $result
