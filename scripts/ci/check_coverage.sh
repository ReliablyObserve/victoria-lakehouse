#!/usr/bin/env bash
# The per-package coverage floor of the CI test jobs.
#
# Reads `go test -cover` lines (already filtered to those containing
# "coverage:") on stdin and fails when a package under internal/ is below the
# threshold. A package is identified by its FULL path below internal/
# (parquets3 lives in internal/storage/parquets3, which is not "storage"), so a
# nested package is held to the floor like any other.
#
# Usage: check_coverage.sh [-t THRESHOLD] [-x EXCLUDED_PATTERN]... [-r REQUIRED_PATH]...
#   -t  minimum percent (default 90)
#   -x  a path below internal/ that is NOT held to the floor; a shell pattern,
#       matched against the whole path (testutil/* excludes the packages below
#       testutil, testutil does not). Name every exclusion and say why at the
#       call site.
#   -r  a path below internal/ that must appear in the input: fails when the
#       package was not measured at all, so a gate that silently stops seeing a
#       package is caught.
# Exit: 0 all measured packages at or above the floor, 1 a violation, 2 bad invocation.
set -uo pipefail

threshold=90
excluded=()
required=()
while getopts "t:x:r:" opt; do
  case "$opt" in
    t) threshold="$OPTARG" ;;
    x) excluded+=("$OPTARG") ;;
    r) required+=("$OPTARG") ;;
    *) echo "usage: check_coverage.sh [-t N] [-x PATTERN]... [-r PATH]..." >&2; exit 2 ;;
  esac
done
if ! [[ "$threshold" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "check_coverage: threshold must be a number, got '$threshold'" >&2
  exit 2
fi

failed=0
checked=0
seen=" "
while IFS= read -r line || [[ -n "$line" ]]; do
  # The package path below /internal/ (up to the first blank or tab).
  pkg=$(printf '%s\n' "$line" | sed -n 's#.*/internal/\([^[:space:]]*\).*#\1#p' | head -1)
  [[ -z "$pkg" ]] && continue
  skip=0
  for pat in ${excluded[@]+"${excluded[@]}"}; do
    # shellcheck disable=SC2053
    [[ "$pkg" == $pat ]] && skip=1
  done
  [[ "$skip" -eq 1 ]] && continue
  pct=$(printf '%s\n' "$line" | sed -n 's#.*coverage: \([0-9][0-9.]*\)%.*#\1#p' | head -1)
  [[ -z "$pct" ]] && continue
  seen="$seen$pkg "
  checked=$((checked + 1))
  if [[ "$(awk -v p="$pct" -v t="$threshold" 'BEGIN{print (p < t)}')" -eq 1 ]]; then
    echo "::error::Coverage for $pkg is ${pct}% (minimum: ${threshold}%)"
    failed=1
  fi
done

for want in ${required[@]+"${required[@]}"}; do
  if [[ "$seen" != *" $want "* ]]; then
    echo "::error::Coverage for $want was not measured (the coverage run produced no line for it)"
    failed=1
  fi
done

if [[ "$failed" -eq 1 ]]; then
  exit 1
fi
echo "All $checked packages at or above ${threshold}% coverage"
