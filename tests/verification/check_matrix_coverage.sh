#!/usr/bin/env bash
#
# Asserts every file path cited inside `tests/verification/matrix.md`
# actually exists on disk relative to the repo root.
#
# The verification matrix is the project's per-component contract.
# Each row cites the tests, probes, and docs that lock its behavior
# down.  Without this check the matrix can silently drift:
#   - a probe is renamed or deleted, matrix.md still references the
#     old path, and a future reader believes the row is regression-
#     locked when in fact nothing is enforcing it.
#   - a docs link in the row goes stale after a docs reorg.
#
# This script catches both classes of drift by re-resolving every
# cited path against the working tree.
#
# What counts as an enforceable cited path:
#   - Backtick-quoted token matching `<dir>/<file>.<ext>`
#   - First path segment is one of the project's top-level
#     directories that lives in the working tree (tests/,
#     internal/, cmd/, charts/, deployment/, lakehouse-traces/,
#     docs/, scripts/, .github/).
#   - Extensions checked: .sh .go .md .yaml .yml .py .json
#
# Out-of-scope (intentionally skipped):
#   - Paths under deps/ — VL/VT upstream source is cloned at CI
#     build time, not committed (deps/ is gitignored).  Matrix
#     rows cite these as informational pointers to the upstream
#     truth; we cannot enforce their existence from this repo.
#   - Bare basenames like `insert.go`, `test.go` — too ambiguous
#     to match against the tree without false positives.
#
# Exit codes:
#   0 — every in-tree cited path resolves to a real file on disk
#   1 — one or more in-tree cited paths are missing (matrix drift)
#   2 — script could not run (matrix.md missing, awk missing, etc.)
#
# Usage:
#   tests/verification/check_matrix_coverage.sh
#
# To verify this script is a real drift detector: temporarily delete
# or rename any probe under `tests/verification/probe_*.sh`, re-run
# this script.  It MUST exit 1 and list the now-missing probe.  If
# it passes after the deletion, the detector is undersized.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MATRIX="${REPO_ROOT}/tests/verification/matrix.md"

if [[ ! -f "$MATRIX" ]]; then
  echo "FAIL: $MATRIX not found" >&2
  exit 2
fi
if ! command -v awk >/dev/null 2>&1; then
  echo "FAIL: awk not available" >&2
  exit 2
fi

# Extract every backtick-quoted token that looks like dir/file.ext,
# then keep only the ones whose first segment is one of our actual
# top-level directories — anything else (deps/, vtinsert/, bare
# basenames) is informational and not enforceable.  grep -oP would
# be cleaner but BSD grep on macOS lacks -P, so we stick to BRE +
# awk for portability.
cited_paths=$(
  grep -oE '`[a-zA-Z][a-zA-Z0-9._/-]+\.(sh|go|md|yaml|yml|py|json)`' "$MATRIX" \
    | tr -d '`' \
    | awk -F/ 'NF>=2 && $1 ~ /^(tests|internal|cmd|charts|deployment|lakehouse-traces|docs|scripts|\.github)$/' \
    | sort -u
)

missing=()
checked=0
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  checked=$((checked + 1))
  if [[ ! -e "${REPO_ROOT}/${path}" ]]; then
    missing+=("$path")
  fi
done <<< "$cited_paths"

if [[ ${#missing[@]} -eq 0 ]]; then
  echo "PASS: matrix-coverage check — ${checked} cited paths all resolve to real files"
  exit 0
fi

cat <<EOF >&2
FAIL: matrix-coverage drift — ${#missing[@]} of ${checked} cited paths are missing
       from the working tree.  Either the file was renamed/deleted
       without updating tests/verification/matrix.md, or matrix.md
       cites a path that was never created.

Missing paths:
EOF
for p in "${missing[@]}"; do
  echo "  - $p" >&2
done

exit 1
