#!/usr/bin/env bash
set -euo pipefail

# Replace [Unreleased] header with a versioned header and add a fresh [Unreleased] section.
# Usage: materialize_unreleased.sh VERSION
#   VERSION  e.g. "0.13.0"

VERSION="${1:?Usage: materialize_unreleased.sh VERSION}"
CHANGELOG="${CHANGELOG:-CHANGELOG.md}"
DATE=$(date +%Y-%m-%d)

if [[ ! -f "$CHANGELOG" ]]; then
  echo "error: $CHANGELOG not found" >&2
  exit 1
fi

if ! grep -q '^\#\# \[Unreleased\]' "$CHANGELOG"; then
  echo "error: no [Unreleased] section found in $CHANGELOG" >&2
  exit 1
fi

sed -i.bak "s/^## \[Unreleased\]/## [Unreleased]\n\n## [$VERSION] - $DATE/" "$CHANGELOG"
rm -f "$CHANGELOG.bak"

echo "Materialized [Unreleased] as [$VERSION] - $DATE in $CHANGELOG"
