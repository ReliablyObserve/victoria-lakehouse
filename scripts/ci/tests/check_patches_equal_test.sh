#!/usr/bin/env bash
# Self-test for scripts/ci/check_patches_equal.sh. Builds synthetic
# vl-logs/vl-traces directory pairs (no real patches, no upstream checkout)
# and asserts the guard's verdict for each case: identical sets, an
# undeclared divergence, a declared one, a stale declaration, a file missing
# on either side, an unknown declaration, and the vacuous-pass guard.
#
# Usage: scripts/ci/tests/check_patches_equal_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

GUARD="scripts/ci/check_patches_equal.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0 fail=0

# run <case> — populates $out (combined output) and $rc for the pair under
# "$TMP/<case>/{vl-logs,vl-traces}".
run() {
  out="$("$GUARD" "$TMP/$1/vl-logs" "$TMP/$1/vl-traces" 2>&1)"
  rc=$?
}

check_rc() {
  local desc="$1" actual="$2" expected="$3"
  if [[ "$actual" == "$expected" ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1))
  printf 'FAIL: %s\n  expected exit: %s\n  actual exit:   %s\n  output: %s\n' \
    "$desc" "$expected" "$actual" "$out" >&2
}

check_contains() {
  local desc="$1" haystack="$2" needle="$3"
  if [[ "$haystack" == *"$needle"* ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1))
  printf 'FAIL: %s\n  expected substring: %s\n  actual: %s\n' "$desc" "$needle" "$haystack" >&2
}

check_not_contains() {
  local desc="$1" haystack="$2" needle="$3"
  if [[ "$haystack" != *"$needle"* ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1))
  printf 'FAIL: %s\n  unexpected substring: %s\n  actual: %s\n' "$desc" "$needle" "$haystack" >&2
}

# mk <case> — creates the pair with one identical patch in both dirs.
mk() {
  mkdir -p "$TMP/$1/vl-logs" "$TMP/$1/vl-traces"
  printf 'same content\n' > "$TMP/$1/vl-logs/a.patch"
  printf 'same content\n' > "$TMP/$1/vl-traces/a.patch"
}

# --- 1. identical directories pass -------------------------------------
mk identical
printf 'x\n' > "$TMP/identical/vl-logs/b.go.src"
printf 'x\n' > "$TMP/identical/vl-traces/b.go.src"
run identical
check_rc "identical directories pass" "$rc" 0
check_contains "identical directories report consistency" "$out" "consistent"

# --- 2. undeclared divergence fails ------------------------------------
mk undeclared
printf 'different\n' > "$TMP/undeclared/vl-traces/a.patch"
run undeclared
check_rc "undeclared divergence fails" "$rc" 1
check_contains "undeclared divergence names the file" "$out" "diverged:"
check_contains "undeclared divergence names DIVERGENCE.md" "$out" "DIVERGENCE.md"

# --- 3. declared divergence passes -------------------------------------
mk declared
printf 'different\n' > "$TMP/declared/vl-traces/a.patch"
cat > "$TMP/declared/vl-traces/DIVERGENCE.md" <<'EOF'
# Declared divergences

- `a.patch` — upstream moved the context line above our hunk.
EOF
run declared
check_rc "declared divergence passes" "$rc" 0
check_not_contains "declared divergence is not reported" "$out" "diverged:"

# --- 4. stale declaration fails (files are in fact equal) ---------------
mk stale
cat > "$TMP/stale/vl-traces/DIVERGENCE.md" <<'EOF'
- `a.patch` — the pins converged, this row should have been removed.
EOF
run stale
check_rc "stale declaration fails" "$rc" 1
check_contains "stale declaration is named" "$out" "stale:"

# --- 5. file missing from vl-traces fails ------------------------------
mk missing-traces
printf 'only here\n' > "$TMP/missing-traces/vl-logs/c.patch"
run missing-traces
check_rc "file missing from vl-traces fails" "$rc" 1
check_contains "missing file is named" "$out" "c.patch"
check_contains "missing file is reported as missing" "$out" "missing:"

# --- 6. file missing from vl-logs fails --------------------------------
mk missing-logs
printf 'only here\n' > "$TMP/missing-logs/vl-traces/d.patch"
run missing-logs
check_rc "file missing from vl-logs fails" "$rc" 1
check_contains "missing file is named" "$out" "d.patch"

# A DIVERGENCE.md row must NOT excuse a one-sided file: a patch that exists
# on only one side is a broken patch set, not a context difference.
mk missing-declared
printf 'only here\n' > "$TMP/missing-declared/vl-logs/e.patch"
cat > "$TMP/missing-declared/vl-traces/DIVERGENCE.md" <<'EOF'
- `e.patch` — trying (and failing) to excuse a one-sided patch.
EOF
run missing-declared
check_rc "declaring a one-sided file does not excuse it" "$rc" 1
check_contains "one-sided file still reported" "$out" "missing:"

# --- 7. declaration naming a nonexistent file fails --------------------
mk unknown
cat > "$TMP/unknown/vl-traces/DIVERGENCE.md" <<'EOF'
- `ghost.patch` — names a file that does not exist in either directory.
EOF
run unknown
check_rc "unknown declaration fails" "$rc" 1
check_contains "unknown declaration is named" "$out" "unknown:"

# --- 8. DIVERGENCE.md itself is never compared -------------------------
# It lives only on the traces side; a naive union would report it missing.
mk divfile
cat > "$TMP/divfile/vl-traces/DIVERGENCE.md" <<'EOF'
# Declared divergences

(no rows)
EOF
run divfile
check_rc "DIVERGENCE.md alone does not fail the guard" "$rc" 0
check_not_contains "DIVERGENCE.md is not reported missing" "$out" "DIVERGENCE.md exists"

# --- 9. two empty directories fail rather than passing vacuously -------
mkdir -p "$TMP/empty/vl-logs" "$TMP/empty/vl-traces"
run empty
check_rc "empty directories fail" "$rc" 1
check_contains "empty directories explain the vacuous pass" "$out" "vacuously"

# --- 10. a missing directory is an invocation error (exit 2) ----------
out="$("$GUARD" "$TMP/does-not-exist" "$TMP/identical/vl-traces" 2>&1)"
rc=$?
check_rc "nonexistent directory exits 2" "$rc" 2

# --- 11. the real repository patch directories pass -------------------
out="$("$GUARD" 2>&1)"
rc=$?
check_rc "repository patch directories pass with default arguments" "$rc" 0

printf '%d checks passed, %d failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]]
