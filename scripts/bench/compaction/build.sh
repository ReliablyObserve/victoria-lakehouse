#!/usr/bin/env bash
# build.sh <git-ref> <label>   e.g.  build.sh origin/main main ; build.sh HEAD pr
# Static linux builds (CGO_ENABLED=0) of both binaries of <git-ref>, from a
# `git archive` of that ref (so the working tree is not touched), into
# $LH_BIN_DIR (default ./bin) as logs-<label> and traces-<label>.
# The VictoriaLogs / VictoriaTraces sources under deps/ are not tracked; they
# are taken from $DEPS_FROM (default: the repository root, prepared by
# `make deps-logs deps-traces deps-vt`).
set -euo pipefail
ref=${1:?git ref}; label=${2:?label (main|pr)}
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
deps_from=${DEPS_FROM:-$repo}
bin=${LH_BIN_DIR:-$here/bin}; mkdir -p "$bin"
arch=$(docker info --format '{{.Architecture}}')
case "$arch" in x86_64|amd64) goarch=amd64;; aarch64|arm64) goarch=arm64;; *) echo "unknown arch $arch" >&2; exit 1;; esac
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
git -C "$repo" archive "$ref" | tar -x -C "$work"
ln -s "$deps_from/deps" "$work/deps"
mkdir -p "$work/lakehouse-traces/deps"
ln -s "$deps_from/lakehouse-traces/deps/VictoriaLogs" "$work/lakehouse-traces/deps/VictoriaLogs"
ln -s "$deps_from/lakehouse-traces/deps/VictoriaTraces" "$work/lakehouse-traces/deps/VictoriaTraces"
export GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=$goarch
(cd "$work" && go build -trimpath -o "$bin/logs-$label" ./cmd/lakehouse-logs)
(cd "$work/lakehouse-traces" && go build -trimpath -o "$bin/traces-$label" .)
echo "built $ref -> $bin/logs-$label $bin/traces-$label (linux/$goarch)"
