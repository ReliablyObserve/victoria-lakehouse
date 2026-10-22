#!/usr/bin/env bash
set -euo pipefail

# Push the release images that the build job exported as OCI archives.
#
# The archives hold the finished multi-arch images (zstd layers, OCI media types); this
# script only copies them, it builds nothing. Each of the four archives
# (lakehouse-{logs,traces}[-fips].tar) is copied, with every platform and attestation it
# carries, to the same tags the release has always had:
# v<version>, <version> and latest (+ the -fips suffix) in the canonical and the flat GHCR
# repository, and v<version> and latest in the Docker Hub namespace when it is enabled.
#
# Environment:
#   VERSION             required, vMAJOR.MINOR.PATCH
#   GITHUB_REPOSITORY   required, owner/name (lower-cased here)
#   IMAGES_DIR          directory with the archives (default: images)
#   GHCR_HOST           registry host for the GHCR repositories (default: ghcr.io;
#                       the dry run points it at a throwaway local registry)
#   SKOPEO_DEST_FLAGS   extra flags for every `skopeo copy` (the dry run: --dest-tls-verify=false)
#   DOCKERHUB_ENABLED   "true" to mirror to Docker Hub (needs DOCKERHUB_USERNAME too)
#   DOCKERHUB_USERNAME, DOCKERHUB_NAMESPACE   mirror owner (default namespace: reliablyobserve)
#
# Logins are done by the caller (skopeo login); this script holds no credentials.

export LC_ALL=C
VERSION="${VERSION:?VERSION is required}"
if ! [[ "$VERSION" =~ ^v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$ ]]; then
  echo "error: VERSION is not vMAJOR.MINOR.PATCH" >&2
  exit 1
fi
REPO=$(printf '%s' "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}" | tr '[:upper:]' '[:lower:]')
IMAGES_DIR="${IMAGES_DIR:-images}"
GHCR_HOST="${GHCR_HOST:-ghcr.io}"
read -r -a SKOPEO_FLAGS <<< "${SKOPEO_DEST_FLAGS:-}"

# Canonical and legacy flat GHCR repositories get v-prefixed, unprefixed and latest
# aliases from the same multiarch build. Docker Hub mirroring is conditional so forks
# without credentials do not fail.
for MODULE in logs traces; do
  for VARIANT in default fips; do
    TAG_SUFFIX=""
    if [[ "$VARIANT" == "fips" ]]; then
      TAG_SUFFIX="-fips"
    fi
    ARCHIVE="${IMAGES_DIR}/lakehouse-${MODULE}${TAG_SUFFIX}.tar"
    if [[ ! -f "$ARCHIVE" ]]; then
      echo "error: image archive $ARCHIVE not found" >&2
      exit 1
    fi
    DESTS=()
    for GHCR_REPO in "$REPO" "${REPO%%/*}"; do
      for RELEASE_TAG in "$VERSION" "${VERSION#v}" latest; do
        DESTS+=("${GHCR_HOST}/${GHCR_REPO}/lakehouse-${MODULE}:${RELEASE_TAG}${TAG_SUFFIX}")
      done
    done
    if [[ "${DOCKERHUB_ENABLED:-false}" == "true" && -n "${DOCKERHUB_USERNAME:-}" ]]; then
      # Configurable Docker Hub namespace (must match the secret's owner).
      HUB_OWNER="${DOCKERHUB_NAMESPACE:-reliablyobserve}"
      DESTS+=("docker.io/${HUB_OWNER}/lakehouse-${MODULE}:${VERSION}${TAG_SUFFIX}"
              "docker.io/${HUB_OWNER}/lakehouse-${MODULE}:latest${TAG_SUFFIX}")
    fi
    for DEST in "${DESTS[@]}"; do
      echo "Pushing lakehouse-${MODULE} variant=${VARIANT} -> ${DEST}"
      skopeo copy --all --retry-times 3 ${SKOPEO_FLAGS[@]+"${SKOPEO_FLAGS[@]}"} \
        "oci-archive:${ARCHIVE}" "docker://${DEST}"
    done
  done
done
