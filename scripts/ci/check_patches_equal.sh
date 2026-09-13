#!/usr/bin/env bash
# Guard: patches/vl-logs/ and patches/vl-traces/ must stay byte-equal, file
# for file, EXCEPT where the two VictoriaLogs pins genuinely force different
# diff context.
#
# Why both directories exist: the logs binary embeds VictoriaLogs at
# VL_VERSION_LOGS, the traces binary embeds its own copy at VL_COMMIT_TRACES
# (the commit VictoriaTraces itself pins). Both copies get the same LH
# extensions, so the patch *content* — the lines we add, the symbols we
# export — must be identical. What may differ is the surrounding upstream
# context a diff carries, when the two pins sit on either side of an upstream
# change to a nearby line.
#
# Every such divergence must be declared in patches/vl-traces/DIVERGENCE.md,
# one row per file, with the reason. The check fails on:
#
#   * a file present in one directory and missing from the other;
#   * files that differ without a DIVERGENCE.md entry;
#   * a stale DIVERGENCE.md entry naming a file that is in fact byte-equal
#     (so the list shrinks back to nothing once the pins converge again);
#   * a DIVERGENCE.md entry naming a file that does not exist.
#
# Usage: scripts/ci/check_patches_equal.sh [logs_dir traces_dir]
# Exit:  0 all good, 1 a violation was found, 2 bad invocation.
set -uo pipefail

LOGS_DIR="${1:-patches/vl-logs}"
TRACES_DIR="${2:-patches/vl-traces}"
DIVERGENCE_FILE="$TRACES_DIR/DIVERGENCE.md"

if [[ ! -d "$LOGS_DIR" || ! -d "$TRACES_DIR" ]]; then
  printf 'check_patches_equal: not a directory: %s or %s\n' "$LOGS_DIR" "$TRACES_DIR" >&2
  exit 2
fi

# Declared divergences: every line of DIVERGENCE.md that starts with a
# backticked file name, e.g.
#   - `vlstorage-dispatch.patch` — reason …
declared=""
if [[ -f "$DIVERGENCE_FILE" ]]; then
  declared="$(sed -n 's/^[[:space:]]*[-*][[:space:]]*`\([^`]*\)`.*/\1/p' "$DIVERGENCE_FILE")"
fi
is_declared() {
  local name="$1" d
  while IFS= read -r d; do
    [[ -n "$d" && "$d" == "$name" ]] && return 0
  done <<< "$declared"
  return 1
}

fail=0
note() { printf '%s\n' "$1" >&2; fail=1; }

# Union of both directories' file names, so a file missing on either side is
# reported once. DIVERGENCE.md is the declaration itself and lives only on the
# traces side, so it is never compared.
names="$( { ls -1 "$LOGS_DIR"; ls -1 "$TRACES_DIR"; } 2>/dev/null \
  | grep -v -x "$(basename "$DIVERGENCE_FILE")" | sort -u )"
if [[ -z "$names" ]]; then
  note "check_patches_equal: both patch directories are empty — the guard would pass vacuously"
  exit 1
fi

equal_but_declared=""
while IFS= read -r name; do
  [[ -z "$name" ]] && continue
  a="$LOGS_DIR/$name"
  b="$TRACES_DIR/$name"
  if [[ ! -f "$a" ]]; then
    note "missing: $a exists in $TRACES_DIR but not in $LOGS_DIR — the two VictoriaLogs copies must carry the same patch set"
    continue
  fi
  if [[ ! -f "$b" ]]; then
    note "missing: $b exists in $LOGS_DIR but not in $TRACES_DIR — the two VictoriaLogs copies must carry the same patch set"
    continue
  fi
  if cmp -s "$a" "$b"; then
    if is_declared "$name"; then
      equal_but_declared="$equal_but_declared $name"
    fi
    continue
  fi
  if is_declared "$name"; then
    continue
  fi
  note "diverged: $a and $b differ but $name is not listed in $DIVERGENCE_FILE"
  note "          add a row with the reason (which upstream change moved the context), or make the two identical"
done <<< "$names"

for name in $equal_but_declared; do
  note "stale: $DIVERGENCE_FILE lists $name, but $LOGS_DIR/$name and $TRACES_DIR/$name are byte-equal — remove the row"
done

while IFS= read -r d; do
  [[ -z "$d" ]] && continue
  if [[ ! -f "$LOGS_DIR/$d" && ! -f "$TRACES_DIR/$d" ]]; then
    note "unknown: $DIVERGENCE_FILE lists $d, which exists in neither $LOGS_DIR nor $TRACES_DIR"
  fi
done <<< "$declared"

if [[ $fail -ne 0 ]]; then
  exit 1
fi
printf 'check_patches_equal: %s and %s consistent\n' "$LOGS_DIR" "$TRACES_DIR"
exit 0
