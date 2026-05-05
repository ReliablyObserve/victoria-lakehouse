#!/usr/bin/env bash
set -euo pipefail

# Extract a specific version section from CHANGELOG.md.
# Usage: extract_changelog_section.sh [VERSION]
#   VERSION  e.g. "0.12.0" or "Unreleased" (default: Unreleased)

VERSION="${1:-Unreleased}"
CHANGELOG="${CHANGELOG:-CHANGELOG.md}"

if [[ ! -f "$CHANGELOG" ]]; then
  echo "error: $CHANGELOG not found" >&2
  exit 1
fi

awk -v ver="$VERSION" '
  /^## \[/ {
    if (found) exit
    if (index($0, "[" ver "]")) found=1
    next
  }
  found { print }
' "$CHANGELOG" | sed -e '/./,$!d' -e :a -e '/^\n*$/{$d;N;ba' -e '}'
