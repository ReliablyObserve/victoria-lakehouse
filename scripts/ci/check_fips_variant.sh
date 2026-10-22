#!/usr/bin/env bash
# Asserts which crypto module a built lakehouse binary was compiled with, from the build
# settings `go version -m` prints.
#
#   check_fips_variant.sh <binary> <0|1>
#
#   0  the default image: no GOFIPS140 setting, no fips140 build tag, no fips140=on default GODEBUG
#   1  the -fips image:   GOFIPS140=v1.0.0-<hash>, -tags=fips140v1.0, DefaultGODEBUG with fips140=on
#
# Exits 0 when the binary matches the expected variant, 1 when it does not, 2 on bad usage.
# Run by the trivy-image jobs (.github/workflows/security.yaml) on the binary of each built image.
set -euo pipefail

if [[ $# -ne 2 || ! "$2" =~ ^[01]$ ]]; then
  echo "usage: $0 <binary> <0|1>" >&2
  exit 2
fi
binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
expected=$2

# The Go toolchain on the runner only reads the build information: it must not switch toolchains,
# and it runs outside the repository so no go.mod is consulted.
info=$(cd / && GOTOOLCHAIN=local go version -m "$binary")
echo "$info" | grep -E '^\s+build\s+(-tags=|GOFIPS140=|DefaultGODEBUG=)' || true

has_module=0 has_tag=0 has_godebug=0
grep -qE '^\s+build\s+GOFIPS140=' <<<"$info" && has_module=1
grep -qE '^\s+build\s+-tags=.*fips140' <<<"$info" && has_tag=1
grep -qE '^\s+build\s+DefaultGODEBUG=.*fips140=on' <<<"$info" && has_godebug=1

status=0
if [[ $expected == 1 ]]; then
  grep -qE '^\s+build\s+GOFIPS140=v1\.0\.0' <<<"$info" || { echo "FIPS variant: GOFIPS140=v1.0.0 build setting is missing" >&2; status=1; }
  [[ $has_tag == 1 ]] || { echo "FIPS variant: fips140 build tag is missing" >&2; status=1; }
  [[ $has_godebug == 1 ]] || { echo "FIPS variant: DefaultGODEBUG does not turn fips140 on" >&2; status=1; }
else
  [[ $has_module == 0 ]] || { echo "default variant: built with a GOFIPS140 module" >&2; status=1; }
  [[ $has_tag == 0 ]] || { echo "default variant: carries a fips140 build tag" >&2; status=1; }
  [[ $has_godebug == 0 ]] || { echo "default variant: DefaultGODEBUG turns fips140 on" >&2; status=1; }
fi
[[ $status == 0 ]] && echo "ok: $binary matches the FIPS=$expected variant"
exit "$status"
