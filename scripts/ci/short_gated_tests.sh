#!/usr/bin/env bash
# Print a `go test -run` regexp matching every Test function in a package
# directory that consults testing.Short() (and so is skipped under -short).
#
# CI runs a package's unit job with -short and runs exactly these tests in a
# separate heavy job, so the split can never drift: a test that gains a
# testing.Short() gate lands in the heavy job automatically, and one that
# loses it is simply part of the unit run. Benchmarks and helpers are ignored.
#
# usage: short_gated_tests.sh <package-dir>
# output: ^(TestA|TestB|...)$   (exit 1 with a message when none are found)
set -euo pipefail

dir="${1:?usage: short_gated_tests.sh <package-dir>}"

names=$(
  find "$dir" -maxdepth 1 -name '*_test.go' -print0 |
    xargs -0 awk '
      /^func /            { cur = "" }
      /^func Test[A-Za-z0-9_]*\(t \*testing\.T\)/ {
        n = $2; sub(/\(.*/, "", n); cur = n
      }
      cur != "" && /testing\.Short\(\)/ { seen[cur] = 1 }
      END { for (n in seen) print n }
    ' | sort -u
)

if [ -z "$names" ]; then
  echo "short_gated_tests.sh: no testing.Short()-gated tests under $dir" >&2
  exit 1
fi

printf '^(%s)$\n' "$(printf '%s\n' "$names" | paste -sd'|' -)"
