#!/usr/bin/env bash
# Ingest the reader-matrix fixture into the running lhreaders stack: logs AND traces,
# for every tenant form, in two rounds so each partition ends up with several small
# files (what compaction later merges).
#
#   numeric   AccountID 4401  ProjectID 1        (header form)
#   alias     X-Scope-OrgID: acme-corp -> 1001:0 (string form)
#   big       AccountID 3000000000 ProjectID 0   (>= 2^31: the unsigned 32-bit column)
#
# Rounds are separated by one flush interval so every round lands in its own file.
set -euo pipefail
cd "$(dirname "$0")"
P=${COMPOSE_PROJECT_NAME:-lhreaders}
FLUSH_WAIT=${FLUSH_WAIT:-20}
ENDPOINTS=(--lh-logs-endpoint=http://lakehouse-logs:9428 --lh-traces-endpoint=http://lakehouse-traces:10428)

gen() { docker compose -p "$P" --profile datagen run --rm -T datagen "$@" "${ENDPOINTS[@]}" 2>&1 | tail -1; }

for round in 1 2; do
  echo "== round $round"
  gen --logs=1500 --traces=300 --hours-back=48 --account-id=4401 --project-id=1
  gen --logs=300 --traces=60 --hours-back=48 --account-id=3000000000 --project-id=0
  gen --logs=300 --traces=60 --hours-back=48 --org-id=acme-corp
  [ "$round" = 1 ] && sleep "$FLUSH_WAIT"
done
echo "ingest done"
