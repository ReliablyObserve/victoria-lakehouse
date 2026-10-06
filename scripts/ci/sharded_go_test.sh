#!/usr/bin/env bash
# Run one Go package's tests as N deterministic shards (see shard_tests.py).
#
# usage: sharded_go_test.sh race  <pkg> <shards> <label> <budget-seconds> <json-prefix>
#        sharded_go_test.sh cover <pkg> <shards>
#
# race : `go test -short -race -timeout=<budget>s -json` once per shard, each
#        piped into gotest_report.py (own headroom gate and summary table).
#        Every shard runs even when an earlier one fails; then the shard guard
#        checks that every listed test ran in exactly its own shard.
# cover: `go test -short -cover` once per shard with a coverprofile, merged
#        into one `ok <pkg> coverage: X% of statements` line.
# Run from the module directory (repo root, or lakehouse-traces).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mode="${1:?mode}"; pkg="${2:?pkg}"; shards="${3:?shards}"

case "$mode" in
race)
  label="${4:?label}"; budget="${5:?budget-seconds}"; prefix="${6:?json-prefix}"
  rc=0
  files=()
  for ((i = 0; i < shards; i++)); do
    run=$(python3 "$here/shard_tests.py" regex --pkg "$pkg" --shards "$shards" --index "$i")
    out="${prefix}-shard$((i + 1))of${shards}.json"
    files+=("$out")
    # pipefail makes a go test failure (or a headroom-gate failure) fail this shard.
    { go test "$pkg" -short -race -count=1 -timeout="${budget}s" -run "$run" -json | tee "$out" |
        python3 "$here/gotest_report.py" --budget-seconds "$budget" \
          --title "$label shard $((i + 1))/$shards (-short)"; } || rc=1
  done
  python3 "$here/shard_tests.py" verify --pkg "$pkg" --shards "$shards" "${files[@]}" || rc=1
  exit "$rc"
  ;;
cover)
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  profiles=()
  for ((i = 0; i < shards; i++)); do
    run=$(python3 "$here/shard_tests.py" regex --pkg "$pkg" --shards "$shards" --index "$i")
    # Output is held back: a per-shard `coverage:` line would be mistaken for
    # the package total by the caller's grep. Shown only on failure.
    go test "$pkg" -short -count=1 -timeout=8m -run "$run" -coverprofile="$tmp/$i.out" >"$tmp/$i.log" 2>&1 ||
      { cat "$tmp/$i.log" >&2; echo "::error::cover shard $((i + 1))/$shards failed" >&2; exit 1; }
    profiles+=("$tmp/$i.out")
  done
  pct=$(python3 "$here/shard_tests.py" cover-merge "$tmp/all.out" "${profiles[@]}")
  printf 'ok  \t%s\t%s\n' "$(go list "$pkg")" "$pct"
  ;;
*) echo "unknown mode $mode" >&2; exit 2 ;;
esac
