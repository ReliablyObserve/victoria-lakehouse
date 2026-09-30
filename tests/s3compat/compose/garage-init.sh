#!/bin/sh
# Usage: garage-init.sh <compose-project> [bucket...]
# Single-node layout + import the fixed test key + create the bucket.
# Credentials (Garage requires GK + 24 hex / 64 hex):
#   access GK0123456789abcdef01234567
#   secret 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
set -eu
P=${1:-s3bake-garage}; shift; [ $# -gt 0 ] || set -- s3compat
dir=$(cd "$(dirname "$0")" && pwd)
g() { docker compose -p "$P" -f "$dir/garage.yml" exec -T s3 /garage "$@"; }
for i in $(seq 1 60); do g status >/dev/null 2>&1 && break; sleep 1; done
ID=$(g node id -q 2>/dev/null | cut -d@ -f1)
g layout assign -z dc1 -c 20G "$ID"
g layout apply --version 1
g key import --yes -n s3bake GK0123456789abcdef01234567 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
g key allow --create-bucket s3bake
for B in "$@"; do
  g bucket create "$B"
  g bucket allow --read --write --owner "$B" --key s3bake
done
