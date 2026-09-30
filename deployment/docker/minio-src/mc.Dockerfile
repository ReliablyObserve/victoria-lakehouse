# MinIO client (mc) built from upstream source at a pinned tag.
#
# Why: the quay.io/minio/* and Docker Hub minio/* images now refuse anonymous
# pulls. This builds the same release from source and is published to
# ghcr.io/reliablyobserve/mc by .github/workflows/minio-images.yaml.
#
# Build (both args are required; the build fails if the tag object != COMMIT;
# upstream tags are annotated, so COMMIT is the tag object id as listed by
# `git ls-remote --tags`, which pins the exact tagged commit):
#   docker buildx build -f mc.Dockerfile \
#     --build-arg TAG=RELEASE.2025-04-16T18-13-26Z \
#     --build-arg COMMIT=1e78af443bf0443eb177f26421fe081f08d83dc5 .

# Go version = the `toolchain` line in upstream go.mod at the tag (go1.23.6).
FROM golang:1.23.6-alpine3.21 AS build

ARG TAG
ARG COMMIT

RUN apk add --no-cache git ca-certificates

WORKDIR /src
RUN test -n "${TAG}" && test -n "${COMMIT}" \
 && git clone --quiet --depth 1 --branch "${TAG}" https://github.com/minio/mc.git . \
 && test "$(git rev-parse "refs/tags/${TAG}")" = "${COMMIT}" \
 || { echo "commit verification failed: tag ${TAG} is not ${COMMIT}" >&2; exit 1; }

# Mirrors upstream `make build`: CGO off, -trimpath, kqueue tag, ldflags from
# buildscripts/gen-ldflags.go (MC_RELEASE=RELEASE yields ReleaseTag=RELEASE.<commit time>).
ENV CGO_ENABLED=0
RUN LDFLAGS="$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
 && go build -trimpath -tags kqueue -ldflags "${LDFLAGS}" -o /out/mc .

FROM alpine:3.21

ARG TAG
ARG COMMIT

# sh + ca-certificates: the compose `minio-init` service runs `/bin/sh -c "mc ..."`.
RUN apk add --no-cache ca-certificates \
 && mkdir -p /licenses
COPY --from=build /out/mc /usr/bin/mc
COPY --from=build /src/LICENSE /licenses/LICENSE
COPY --from=build /src/CREDITS /licenses/CREDITS

LABEL org.opencontainers.image.title="mc" \
      org.opencontainers.image.description="MinIO client built from source at ${TAG}" \
      org.opencontainers.image.source="https://github.com/ReliablyObserve/victoria-lakehouse" \
      org.opencontainers.image.url="https://github.com/minio/mc" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.version="${TAG}" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      io.github.reliablyobserve.upstream="https://github.com/minio/mc/tree/${TAG}"

ENTRYPOINT ["mc"]
