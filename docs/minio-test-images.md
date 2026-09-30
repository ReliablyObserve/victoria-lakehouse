---
title: MinIO Test Images
sidebar_position: 8
---

# How the MinIO test images are built

The compose stacks (`deployment/docker/docker-compose-e2e.yml`,
`deployment/docker/docker-compose-benchmark.yml`, `tests/parity/docker-compose.yml`)
and the nightly load-test job use MinIO as their S3 backend. They pull:

- `ghcr.io/reliablyobserve/minio:RELEASE.2025-04-22T22-12-26Z`
- `ghcr.io/reliablyobserve/mc:RELEASE.2025-04-16T18-13-26Z`

## Why these are not the upstream images

Upstream MinIO is no longer maintained: the `minio/minio` GitHub repository is
archived, and the community edition is distributed as source only. The
`quay.io/minio/*` and Docker Hub `minio/*` images now refuse anonymous pulls
(HTTP 401), which broke every CI job that starts a compose stack.

The images above are **frozen test fixtures**, built from the upstream source at
the same release tags the stacks were already pinned to. They are not a supported
MinIO distribution, receive no security updates, and must not be used in
production.

## How they are built

`.github/workflows/minio-images.yaml` builds them from
`deployment/docker/minio-src/{minio,mc}.Dockerfile`:

- pinned Go builder (the `toolchain` version from upstream's `go.mod` at the tag);
- shallow clone at the release tag, failing the build unless the tag resolves to
  the pinned tag-object id (the `COMMIT` values in the workflow's `env` block);
- upstream-equivalent build: `CGO_ENABLED=0`, `-trimpath`, `-tags kqueue`, and
  ldflags from upstream's `buildscripts/gen-ldflags.go`;
- Alpine runtime with `sh`, `ca-certificates` and (minio image) `curl`; the minio
  image also ships `mc`, because the compose healthchecks run `mc ready local`;
- upstream's entrypoint script, `CMD`, `EXPOSE 9000`, `VOLUME /data` and
  environment defaults; the AGPL-3.0 `LICENSE` and `CREDITS` under `/licenses`;
- OCI labels for source, upstream URL, revision and version;
- `linux/amd64` and `linux/arm64`.

Tags are immutable: the workflow pushes a tag only if it does not already exist
in GHCR, otherwise it only verifies that the image still builds.

## Changing the pinned release

Edit the `env` block in `.github/workflows/minio-images.yaml`, then update every
image reference (`grep -rn 'ghcr.io/reliablyobserve/\(minio\|mc\):' .`), including
`.github/workflows/nightly-loadtest.yaml`. Get the `COMMIT` value with
`git ls-remote --tags https://github.com/minio/minio.git <tag>` (the line without
`^{}`).

## Package visibility

GitHub creates organization packages as private. After the first publish an
organization owner must make the `minio` and `mc` packages public (Package
settings, Danger Zone, Change visibility), otherwise unauthenticated pulls fail.
