# MinIO server built from upstream source at a pinned tag, plus the matching
# mc client (upstream's release image ships `mc` and `curl`, and the compose
# healthchecks call `mc ready local`).
#
# Why: the quay.io/minio/* and Docker Hub minio/* images now refuse anonymous
# pulls. This is published to ghcr.io/reliablyobserve/minio by
# .github/workflows/minio-images.yaml.
#
# Build (all four args are required; the build fails if the tag object != COMMIT;
# upstream tags are annotated, so COMMIT is the tag object id as listed by
# `git ls-remote --tags`, which pins the exact tagged commit):
#   docker buildx build -f minio.Dockerfile \
#     --build-arg TAG=RELEASE.2025-04-22T22-12-26Z \
#     --build-arg COMMIT=f19c534b9f457773dcd043d977433e1a71525c3b \
#     --build-arg MC_TAG=RELEASE.2025-04-16T18-13-26Z \
#     --build-arg MC_COMMIT=1e78af443bf0443eb177f26421fe081f08d83dc5 .

# Go version = the `toolchain` line in upstream go.mod at the tag (go1.24.2).
FROM golang:1.24.2-alpine3.21 AS build

ARG TAG
ARG COMMIT
ARG MC_TAG
ARG MC_COMMIT

RUN apk add --no-cache git ca-certificates

ENV CGO_ENABLED=0

WORKDIR /src/minio
RUN test -n "${TAG}" && test -n "${COMMIT}" \
 && git clone --quiet --depth 1 --branch "${TAG}" https://github.com/minio/minio.git . \
 && test "$(git rev-parse "refs/tags/${TAG}")" = "${COMMIT}" \
 || { echo "commit verification failed: tag ${TAG} is not ${COMMIT}" >&2; exit 1; }

# Mirrors upstream `make build`: CGO off, -trimpath, kqueue tag, ldflags from
# buildscripts/gen-ldflags.go (MINIO_RELEASE=RELEASE yields ReleaseTag=RELEASE.<commit time>).
RUN LDFLAGS="$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
 && go build -trimpath -tags kqueue -ldflags "${LDFLAGS}" -o /out/minio .

# mc needs go1.23.6; the go1.24 toolchain builds it fine.
WORKDIR /src/mc
RUN test -n "${MC_TAG}" && test -n "${MC_COMMIT}" \
 && git clone --quiet --depth 1 --branch "${MC_TAG}" https://github.com/minio/mc.git . \
 && test "$(git rev-parse "refs/tags/${MC_TAG}")" = "${MC_COMMIT}" \
 || { echo "commit verification failed: tag ${MC_TAG} is not ${MC_COMMIT}" >&2; exit 1; }
RUN LDFLAGS="$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
 && go build -trimpath -tags kqueue -ldflags "${LDFLAGS}" -o /out/mc .

FROM alpine:3.21

ARG TAG
ARG COMMIT

# curl + sh + ca-certificates, as in upstream's image (healthchecks and scripts use them).
RUN apk add --no-cache ca-certificates curl \
 && mkdir -p /licenses /data
COPY --from=build /out/minio /out/mc /usr/bin/
COPY --from=build /src/minio/LICENSE /licenses/LICENSE
COPY --from=build /src/minio/CREDITS /licenses/CREDITS
COPY --from=build /src/minio/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/bin/docker-entrypoint.sh

# Same env as upstream's Dockerfile.release.
ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_UPDATE_MINISIGN_PUBKEY="RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav" \
    MINIO_CONFIG_ENV_FILE=config.env \
    MC_CONFIG_DIR=/tmp/.mc

LABEL org.opencontainers.image.title="minio" \
      org.opencontainers.image.description="MinIO server built from source at ${TAG}" \
      org.opencontainers.image.source="https://github.com/ReliablyObserve/victoria-lakehouse" \
      org.opencontainers.image.url="https://github.com/minio/minio" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.version="${TAG}" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      io.github.reliablyobserve.upstream="https://github.com/minio/minio/tree/${TAG}"

EXPOSE 9000
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]
