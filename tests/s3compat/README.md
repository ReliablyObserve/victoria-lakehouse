# tests/s3compat - S3 backend compatibility harness

Validates that an S3-compatible endpoint is a drop-in for what Lakehouse
(logs and traces binaries) actually issues, using the **production client
constructor** (`internal/s3reader.NewClientPool`, same aws-sdk-go-v2 pin as
the binaries: default checksum behaviour, path-style, static credentials).

Three layers, cheapest first:

| Layer | Entry point | What it proves |
|-------|-------------|----------------|
| 1. Operation harness | `go test ./tests/s3compat` | every S3 call Lakehouse issues, plus edge cases and extra operations |
| 2. Lakehouse smoke | `lakehouse-smoke.sh` | real `lakehouse-logs` ingests, flushes to the backend, restarts empty and reads everything back cold, compacts |
| 3. ceph/s3-tests subset | `s3tests.sh` | third-party conformance for listing, ranges, multipart, conditional writes, checksums, delete, copy |

`bakeoff.sh` runs 1 and 2 against one docker-compose candidate and records
cold start (compose up to first successful PUT), idle RSS, and image size.
The CI job (`.github/workflows/s3-compat.yaml`) runs layer 1 against RustFS and
the MinIO reference build; see [docs/test-s3-backend.md](../../docs/test-s3-backend.md).

## 1. Operation harness

```sh
# any endpoint; the bucket is created if missing
GOWORK=off go test ./tests/s3compat -count=1 -v -args \
    -endpoint=http://127.0.0.1:9000 -key=AK -secret=SK [-bucket=s3compat] [-region=us-east-1]

# or with env vars
S3COMPAT_ENDPOINT=... S3COMPAT_KEY=... S3COMPAT_SECRET=... GOWORK=off go test ./tests/s3compat -count=1 -v
```

Without an endpoint every test is skipped, so `go test ./...` stays green.
`deps/` must exist (`make deps-logs`) because `internal/s3reader` links the
Lakehouse config package.

Flags: `-pathstyle` (default true, as in production for custom endpoints),
`-large-mib` (default 200, `0` skips the large-object test),
`-consistency-iters` (default 200 put-then-read/list iterations).

Tests are split by whether Lakehouse issues the operation today:

* `TestProd_*` - operations in `internal/s3reader`, `internal/manifest`
  (paginated `ListObjectsV2`, with and without `Delimiter`), the compaction
  orphan sweep (`HeadObject` `LastModified`) and the startup write probe:
  PutObject (SDK default CRC32 trailer), GetObject with `bytes=a-b`,
  `bytes=-N`, `bytes=N-`, HeadObject 404/403, DeleteObject (idempotent),
  CopyObject (production builds `CopySource` with `url.PathEscape`),
  ListObjectsV2 (prefix, delimiter at depth, 1100-key pagination, StartAfter,
  MaxKeys, continuation tokens), keys with `=`, `/`, `+`, `%`, unicode,
  ETag equality across PUT/HEAD/GET/LIST, read-after-write and
  list-after-write (200 iterations), overwrite-then-read, delete-then-list,
  0-byte and 200 MiB objects.
* `TestExtra_*` - not issued by Lakehouse today, but relevant to user
  backends and near-term features: `DeleteObjects` (1000 keys), multipart
  (5 MiB parts, abort, ListParts), conditional PUT (`If-None-Match: *`,
  `If-Match`, 16-way create race), explicit CRC32/CRC32C/SHA256/SHA1/CRC64NVME,
  `Content-MD5`, keys containing `.`/`..`/empty path segments.

A backend is a Lakehouse-compatible drop-in when every `TestProd_*` passes.
`TestExtra_*` failures are informational.

### Validating your own backend

Point the harness at your endpoint (see the flags above) and run it as a user
would run any test: the exit code is the verdict for the `TestProd_*` cases.
To run only what Lakehouse needs today, add `-run 'TestProd_'`. Use a
throwaway bucket (the harness creates `-bucket` if missing and namespaces each
run under `run-<nanos>/`; it does not delete what it writes). Use Go's `-skip`
flag to record a known limitation of a backend by test name, for example
`-skip 'TestExtra_DotSegmentKeys'`; the CI job does this per backend.

Known limitations of the two backends in CI:

| Backend | Failing case | Effect on Lakehouse |
|---------|--------------|---------------------|
| RustFS 1.0.0 | `TestExtra_DotSegmentKeys` (keys with `.`, `..` or empty path segments are rejected with 400) | none, such keys are never generated |
| MinIO 2025-04-22 | `TestProd_CopyObject/col:on...` (`CopySource` containing `:`) | tenant bucket-migration tool only |
| MinIO 2025-04-22 | `TestExtra_ConditionalPut/if-match-missing-key-404`, `TestExtra_DotSegmentKeys` | none |

## 2. Lakehouse smoke

```sh
GOWORK=off go build -o /tmp/lakehouse-logs ./cmd/lakehouse-logs
tests/s3compat/lakehouse-smoke.sh http://127.0.0.1:9000 AK SK <fresh-bucket> [workdir]
```

Use a **fresh, empty bucket per run**: Lakehouse writes tenant data at the
bucket root (`0/0/...`) regardless of `-lakehouse.s3.prefix`, so a reused
bucket inflates counts. The script ingests 20000 records with
`ack_mode: flush-sync`, checks the manifest sees the flushed Parquet,
restarts the binary with empty local storage/cache (everything must come
back through LIST + ranged GETs), runs count/filter/point-lookup/word-search/
field_names/hits queries, then runs a short compaction pass and re-verifies
no row was lost. Ingest must send `Content-Type: application/x-ndjson`;
curl's default form content-type is silently dropped by the insert handler.

## 3. ceph/s3-tests subset

```sh
git clone --depth 1 https://github.com/ceph/s3-tests /tmp/s3bake-work/s3-tests
python3 -m venv /tmp/s3bake-work/venv
/tmp/s3bake-work/venv/bin/pip install -r /tmp/s3bake-work/s3-tests/requirements.txt pytest-timeout
tests/s3compat/s3tests.sh 127.0.0.1:9000 AK SK /tmp/s3t-out
```

The key must be allowed to create buckets. The script generates
`s3tests.conf`, runs the groups listing, ranges, multipart, conditional
writes, checksums, delete_objects, copy and basic_rw (deselecting ACL,
policy, SSE, versioning, object-lock, lifecycle, website, CORS, tagging,
logging), and writes `summary.tsv` plus one pytest log per group.

## Compose candidates

`compose/<candidate>.yml`, one container each, project name `s3bake-<name>`,
host ports 39000-39199:

| Candidate | Image (pinned) | Port | Init steps |
|-----------|----------------|------|------------|
| minio | `ghcr.io/reliablyobserve/minio:RELEASE.2025-04-22T22-12-26Z` (frozen reference build) | 39000 | none |
| rustfs | `rustfs/rustfs:1.0.0@sha256:8cc9...d1ff` (the test backend) | 39010 | none |
| seaweedfs | `chrislusf/seaweedfs:4.48` | 39020 | mount an identities JSON (`seaweedfs-s3.json`) |
| versitygw | `ghcr.io/versity/versitygw:v1.8.0` | 39030 | mount a volume at `/data` |
| garage | `dxflrs/garage:v2.4.1` | 39040 | `compose/garage-init.sh` (layout assign/apply, key import, bucket create+allow) |

```sh
tests/s3compat/bakeoff.sh rustfs /tmp/s3bake-out   # up, harness, smoke, RSS, teardown
```

As a CI job validating a user's backend, run layer 1 (and optionally 2)
against the customer endpoint; layer 3 is for backend-vendor conformance
reports.
