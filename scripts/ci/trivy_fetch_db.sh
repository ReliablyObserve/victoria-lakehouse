#!/usr/bin/env bash
# Download the Trivy vulnerability database before a scan, so that a registry or network
# problem is reported as such and is never mistaken for a scan finding.
#
# Trivy exits 1 both when it finds a vulnerability and when it cannot download its database,
# and it does not try its other mirrors on a failed download. So the database is fetched
# here, in its own step: every mirror in turn, a few rounds with a growing pause. If every
# attempt fails and an earlier database is still in the cache directory, the scan continues
# with that one and a warning says so; with no database at all the step fails with a message
# that names the cause. The scan steps then run with TRIVY_SKIP_DB_UPDATE=true, so a failure
# there is a finding (or a scan error), not a download.
#
# Environment:
#   TRIVY_CACHE_DIR      cache directory shared with the scan steps (required)
#   TRIVY_DB_ATTEMPTS    rounds over all mirrors (default 3)
#   TRIVY_DB_SLEEP_STEP  seconds added to the pause after each round (default 15; 0 in tests)
#   TRIVY_DB_REPOS       space separated mirrors in order (default below)
set -uo pipefail

cache_dir="${TRIVY_CACHE_DIR:?TRIVY_CACHE_DIR is required}"
attempts="${TRIVY_DB_ATTEMPTS:-3}"
sleep_step="${TRIVY_DB_SLEEP_STEP:-15}"
read -r -a repos <<<"${TRIVY_DB_REPOS:-mirror.gcr.io/aquasec/trivy-db:2 ghcr.io/aquasecurity/trivy-db:2 public.ecr.aws/aquasecurity/trivy-db:2}"

for ((round = 1; round <= attempts; round++)); do
  for repo in "${repos[@]}"; do
    echo "vulnerability database: round ${round}/${attempts}, ${repo}"
    if trivy fs --download-db-only --no-progress --cache-dir "$cache_dir" --db-repository "$repo" "$cache_dir"; then
      echo "vulnerability database ready (${repo})"
      exit 0
    fi
  done
  if ((round < attempts)); then
    sleep $((round * sleep_step))
  fi
done

if [ -s "$cache_dir/db/trivy.db" ]; then
  echo "::warning::the vulnerability database could not be refreshed from any mirror; scanning with the cached copy"
  exit 0
fi

echo "::error::the vulnerability database could not be downloaded from any mirror and none is cached: a network or registry problem, not a scan finding"
exit 1
