---
title: Test S3 Backend
sidebar_position: 8
---

# The S3 backend used by the test and CI stacks

The compose stacks (`deployment/docker/docker-compose-e2e.yml`,
`deployment/docker/docker-compose-benchmark.yml`, `tests/parity/docker-compose.yml`,
`deployment/docker/docker-compose-cluster.override.yml`) and the nightly load-test job
run against **RustFS 1.0.0**, pinned by digest:

```
rustfs/rustfs:1.0.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff
```

The compose service is called `s3` (endpoint `http://s3:9000`); the `s3-init` sidecar creates the
buckets with the `mc` client image (`ghcr.io/reliablyobserve/mc`), which works against any S3
endpoint. Credentials for the test stacks are `minioadmin` / `minioadmin`, passed to RustFS as
`RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`. RustFS needs no other setup: one container, two
environment variables, data under `/data`, health at `GET /health`.

This is a **test fixture**, not a recommendation for production storage.

## Why RustFS

MinIO's community edition is archived and unmaintained. Five candidates were run on one host
(Apple Silicon, Docker Desktop, single container, path-style, HTTP) with the
[`tests/s3compat`](https://github.com/ReliablyObserve/victoria-lakehouse/tree/main/tests/s3compat)
harness, which drives the production `s3reader` client through every S3 operation Lakehouse issues:

| Backend | Production-derived cases | Extra cases (not issued today) | Real-binary ingest, restart, cold read | ceph/s3-tests subset | Cold start | Idle RSS | Image |
|---------|--------------------------|--------------------------------|-----------------------------------------|----------------------|-----------|----------|-------|
| MinIO 2025-04-22 | 66/67 | 16/21 | 11/11 | 166/248 | 1.2 s | 325-449 MiB | 237 MB |
| **RustFS 1.0.0** | **67/67** | 17/21 | 11/11 | 191/248 | 2.5 s | 376-399 MiB | 355 MB |
| SeaweedFS 4.48 | 67/67 | 19/21 | 11/11 | 180/248 | 5-8 s | 600-892 MiB | 687 MB |
| Versity S3 Gateway 1.8.0 | 66/67 | 17/21 | 11/11 | 147/248 | 0.4 s | 78-89 MiB | 93 MB |
| Garage 2.4.1 | 67/67 | 16/21 | 11/11 | 134/248 | 1.4-2.9 s | 11-29 MiB | 95 MB |

RustFS passed every production-derived case, supports conditional writes (including a 16-way
`If-None-Match: *` race with exactly one winner), needs no initialisation beyond two environment
variables, and scored highest on the third-party conformance subset. The only extra cases it fails
are keys containing `.`, `..` or empty path segments, which Lakehouse never generates. SeaweedFS
also passed everything Lakehouse issues but is the heaviest and defaults to behaviours that hide
misconfiguration (auto-created buckets, deleting non-empty buckets). Garage has no conditional
writes and needs six initialisation steps. Versity's filesystem key normalisation makes some keys
invisible to listing.

Numbers are from one arm64 host and will differ on CI runners; the pass/fail results should not.
RustFS 1.0.0 is a young project and only this version was measured, hence the digest pin and the
`s3-compat` job below.

## The `s3-compat` CI job

`.github/workflows/s3-compat.yaml` runs `go test ./tests/s3compat` against two backends:

| Leg | Image | Purpose |
|-----|-------|---------|
| `rustfs` | the pinned RustFS above | the backend the stacks use |
| `minio-reference` | `ghcr.io/reliablyobserve/minio:RELEASE.2025-04-22T22-12-26Z` | reference, so a failure can be attributed to the backend or to Lakehouse |

If both legs fail, suspect Lakehouse or the AWS SDK. If only the RustFS leg fails, the backend
changed. Each leg passes `-skip` for the known limitations of that backend (RustFS: dot-segment
keys; MinIO: `CopyObject` with `:` in the key, `If-Match` on a missing key, dot-segment keys), and
the job fails if fewer than all 18 `TestProd_*` tests ran and passed, so a wiring mistake cannot
turn into a silent green.

It runs on pull requests that touch `tests/s3compat/**`, `internal/s3reader/**`, the compose
files, `go.mod` or `go.sum` (AWS SDK bumps), and weekly on Monday.

To run the harness against any S3 endpoint, including your own, see
[`tests/s3compat/README.md`](https://github.com/ReliablyObserve/victoria-lakehouse/blob/main/tests/s3compat/README.md).

## Bumping RustFS

1. Pick the new tag and resolve its digest (`docker buildx imagetools inspect rustfs/rustfs:<tag>`).
2. Replace the pinned reference in every place that carries it:
   `grep -rn 'rustfs/rustfs:' .` (the four compose files, `s3-compat.yaml`,
   `tests/s3compat/compose/rustfs.yml` and this page).
3. Open the PR. The `s3-compat` job, `e2e` and `parity` are the acceptance gate; read a red
   `s3-compat` leg against the `minio-reference` leg before changing anything in Lakehouse.

Never point a stack at a floating tag: a backend change must show up as a diff in the PR that
introduces it.

## MinIO images (reference and fallback)

The MinIO server and `mc` images stay available as the reference backend and as the fallback if
RustFS ever has to be replaced. They are **frozen test fixtures** built from upstream source at
pinned release tags: upstream MinIO is unmaintained, and the `quay.io/minio/*` and Docker Hub
`minio/*` images now refuse anonymous pulls. They are not a supported MinIO distribution, receive no
security updates, and must not be used in production.

- `ghcr.io/reliablyobserve/minio:RELEASE.2025-04-22T22-12-26Z`
- `ghcr.io/reliablyobserve/mc:RELEASE.2025-04-16T18-13-26Z` (also used by the `s3-init` sidecars)

### How they are built

`.github/workflows/minio-images.yaml` builds them from
`deployment/docker/minio-src/{minio,mc}.Dockerfile`:

- pinned Go builder (the `toolchain` version from upstream's `go.mod` at the tag);
- shallow clone at the release tag, failing the build unless the tag resolves to
  the pinned tag-object id (the `COMMIT` values in the workflow's `env` block);
- upstream-equivalent build: `CGO_ENABLED=0`, `-trimpath`, `-tags kqueue`, and
  ldflags from upstream's `buildscripts/gen-ldflags.go`;
- Alpine runtime with `sh`, `ca-certificates` and (minio image) `curl`;
- upstream's entrypoint script, `CMD`, `EXPOSE 9000`, `VOLUME /data` and
  environment defaults; the AGPL-3.0 `LICENSE` and `CREDITS` under `/licenses`;
- OCI labels for source, upstream URL, revision and version;
- `linux/amd64` and `linux/arm64`.

Tags are immutable: the workflow pushes a tag only if it does not already exist
in GHCR, otherwise it only verifies that the image still builds.

### Changing the pinned release

Edit the `env` block in `.github/workflows/minio-images.yaml`, then update every
image reference (`grep -rn 'ghcr.io/reliablyobserve/\(minio\|mc\):' .`), including
`.github/workflows/s3-compat.yaml`. Get the `COMMIT` value with
`git ls-remote --tags https://github.com/minio/minio.git <tag>` (the line without
`^{}`).

### Switching a stack back to MinIO

Replace the `s3` service image with the MinIO image, set `command: server /data`, use
`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` instead of the `RUSTFS_*` variables, and use
`["CMD", "mc", "ready", "local"]` as the healthcheck. Nothing else in the stacks depends on the
backend: the service name, hostname and credentials stay the same.

### Package visibility

GitHub creates organization packages as private. After the first publish an
organization owner must make the `minio` and `mc` packages public (Package
settings, Danger Zone, Change visibility), otherwise unauthenticated pulls fail.
