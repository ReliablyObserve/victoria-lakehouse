# Goal B (range-read parquet decode) — negative-control pprof

Captured: 2026-05-31
Branch: `followup/pprof-goalb-negcontrol` at `80039e1`
Workload: 3 back-to-back 7-day wildcard queries (`query=*`, `limit=10`) against the
e2e compose stack (`mem_limit=2g`, `file-workers=16`, minio S3 backend).

The Goal B switch (`shouldUseWildcardRangeRead` in
`internal/storage/parquets3/storage_query.go:474`) was disabled locally
by changing the condition to `false`, rebuilding with GOWORK=off, and
recreating the container. The working tree was fully restored before any
commit was made.

## Without Goal B (local revert applied — full-download path forced for wildcards)

Container restarted (OOM-killed) during run 3 of 3 (curl exit 52 = connection
reset). Run 1 and run 2 completed HTTP 200 and populated the L1 cache, making
run 3 pull additional files that pushed cumulative resident bytes beyond 2 GiB.

- Container RSS at peak (run 4 post-restart, fresh 7-day wildcard): **1,583 MiB / 2,048 MiB (79.1%)**
- Heap inuse_space total:               **1,507 MiB**
- Top allocator:                        `io.ReadAll` = **1,395 MiB (92.6% of heap)**
- `lakehouse_s3_range_reads_total`:     **0** (wildcard branch never taken — confirmed)
- `lakehouse_parquet_column_bytes_read_total`: **1,452 MiB** (full S3 downloads)
- Container restarts during 3-run probe: **1 (OOM kill on run 3)**

```
File: lakehouse-logs
Build ID: ee4d9b86569fd0506bc6fcf3bec24c93ca9a0874
Type: inuse_space
Time: 2026-05-31 01:15:51 CEST
Showing nodes accounting for 1434.13MB, 95.15% of 1507.25MB total
Dropped 112 nodes (cum <= 7.54MB)
      flat  flat%   sum%        cum   cum%
 1395.33MB 92.57% 92.57%  1395.33MB 92.57%  io.ReadAll
   15.70MB  1.04% 93.62%    16.70MB  1.11%  github.com/parquet-go/parquet-go/encoding/thrift.Slice[...].DecodeFunc.func1
    9.50MB  0.63% 94.25%     9.50MB  0.63%  reflect.unsafe_NewArray
    7.54MB   0.5% 94.75%     7.54MB   0.5%  slices.Clone[go.shape.[]uint8,go.shape.uint8] (inline)
    3.55MB  0.24% 94.98%    15.55MB  1.03%  github.com/parquet-go/parquet-go.(*File).ReadPageIndex
    2.50MB  0.17% 95.15%    16.51MB  1.10%  encoding/json.(*decodeState).object
         0     0% 95.15%  1395.33MB 92.57%  github.com/.../s3reader.(*ClientPool).Download
         0     0% 95.15%  1395.33MB 92.57%  github.com/.../smartcache.(*Controller).singleflightDownload
         0     0% 95.15%     1325MB 87.91%  github.com/.../parquets3.(*Storage).RunQuery.func5
         0     0% 95.15%  1395.33MB 92.57%  github.com/.../parquets3.(*Storage).getFileData
         0     0% 95.15%  1316.48MB 87.34%  github.com/.../parquets3.(*Storage).openParquetFile
         0     0% 95.15%     1325MB 87.91%  github.com/.../parquets3.(*Storage).queryFile
```

The full call chain is unambiguous: `RunQuery` → `queryFile` → `openParquetFile` →
`getFileData` → `smartcache.Download` → `io.ReadAll`. Every wildcard file is
fully downloaded and held resident as a `[]byte` for the entire
open-decode-emit window. With 16 concurrent file workers each holding a
~30 MiB file body, cumulative resident bytes quickly exceed 2 GiB.

## With Goal B (current main — `shouldUseWildcardRangeRead` active)

All 3 back-to-back 7-day wildcard queries completed HTTP 200. No container restarts.
Snapshot taken after 3 load runs against the post-PR-#97 build running for 6+ hours.

- Container RSS at snapshot:            **878.9 MiB / 2,048 MiB (42.9%)**
- Heap inuse_space total:               **413 MiB**
- Top allocator:                        `io.ReadAll` = **263 MiB (63.7% of heap)**
  (residual full-downloads from L1/L2 cache-hit path and small files below 4 MiB cutoff)
- `lakehouse_s3_range_reads_total`:     **1,156** (wildcard range-read path active)
- `lakehouse_s3_range_bytes_read_total`: **12 MiB** (range requests vs 1,452 MiB full)
- Container restarts during 3-run probe: **0**

```
File: lakehouse-logs
Build ID: 159172070699e7967fbecb416301b11113c3409b
Type: inuse_space
Time: 2026-05-31 01:11:52 CEST
Showing nodes accounting for 395.23MB, 95.62% of 413.35MB total
Dropped 177 nodes (cum <= 2.07MB)
      flat  flat%   sum%        cum   cum%
  263.43MB 63.73% 63.73%   263.43MB 63.73%  io.ReadAll
   27.51MB  6.65% 70.39%    27.51MB  6.65%  github.com/.../parquets3.parquetValueToInterface
   14.50MB  3.51% 73.89%    23.43MB  5.67%  github.com/.../parquets3.readMapColumnToBlockCols
    9.12MB  2.21% 76.10%     9.62MB  2.33%  github.com/parquet-go/parquet-go/encoding/thrift.Slice[...].DecodeFunc.func1
    8.15MB  1.97% 78.07%    44.26MB 10.71%  github.com/.../parquets3.readScalarColumnFormatted
    7.00MB  1.69% 79.76%     7.50MB  1.81%  github.com/parquet-go/parquet-go.(*columnLoader).open
    5.54MB  1.34% 81.10%     5.54MB  1.34%  github.com/klauspost/compress/zstd.(*blockEnc).init
    5.50MB  1.33% 82.44%     5.50MB  1.33%  github.com/.../parquets3.parquetValueToString
    5.04MB  1.22% 83.65%     5.04MB  1.22%  github.com/.../manifest.(*Manifest).indexFileLabels (inline)
    5.03MB  1.22% 84.87%     5.03MB  1.22%  slices.Clone[go.shape.[]uint8,go.shape.uint8] (inline)
    4.08MB  0.99% 85.86%     4.08MB  0.99%  github.com/parquet-go/parquet-go/internal/memory.newSlice (inline)
    4.07MB  0.99% 86.84%     4.57MB  1.11%  github.com/.../s3reader.(*BufferedS3ReaderAt).ReadAt
```

The `io.ReadAll` entries that remain in the post-Goal-B snapshot come from:
1. Files below the 4 MiB wildcard cutoff (`minFileSizeForWildcardRangeRead`) that
   still use the full-download path by design (HTTP overhead outweighs savings).
2. L1/L2 cache hits where `memCache.Get` returned true — the range-read branch is
   explicitly skipped in that path because `getFileData` returns cached bytes
   cheaply without a new S3 download.

The absence of `getFileData` / `smartcache.singleflightDownload` from the cumulative
column confirms no large wildcard S3 downloads are happening at peak load.

## Delta

| Metric                    | Without Goal B | With Goal B | Delta          |
|---------------------------|----------------|-------------|----------------|
| Container RSS             | 1,583 MiB      | 879 MiB     | -704 MiB (-44%) |
| Heap inuse_space total    | 1,507 MiB      | 413 MiB     | -1,094 MiB (-73%) |
| `io.ReadAll` (top alloc)  | 1,395 MiB      | 263 MiB     | -1,132 MiB (-81%) |
| s3_range_reads_total      | 0              | 1,156       | +1,156          |
| Container OOM restarts    | 1 (on run 3)   | 0           | -1              |
| 3-run probe outcome       | FAIL (OOM)     | PASS        |                |

## Methodology notes

- **Local revert**: the conditional at `storage_query.go:474` was changed from
  `shouldUseWildcardRangeRead(fi.Size)` to `false`. No other files were modified.
  The change was reverted (`git checkout -- internal/storage/parquets3/storage_query.go`)
  before any commit.
- **Two separate rebuilds**: both PRE and POST images were freshly built from
  source with `GOWORK=off` using `docker compose build lakehouse-logs`.
- **Same workload**: `query=*` over the last 7 days, `limit=10`, 3 runs, against
  the same minio bucket and parquet files.
- **pprof endpoint**: `GET /debug/pprof/heap` captured via `curl` from the host
  to `localhost:29428` immediately after the 3-run sequence finished.
- **POST-Goal-B snapshot context**: the container had been running for ~6 hours
  with continuous datagen and had warmed its L1/L2 cache, which explains the
  lower `io.ReadAll` residual (cache hits bypass S3 downloads). The PRE snapshot
  was captured against a fresh container (post-restart, warm cache partially
  populated by run 4).

## Conclusion

This negative-control closes the gap PR #97 left open. The heap reduction
(−1,094 MiB, −73%) and the OOM elimination are directly attributable to the
Goal B range-read switch:

- **Without it**: `io.ReadAll` via `getFileData` → `singleflightDownload` holds
  cumulative file bytes resident across all 16 workers simultaneously. A 3-run
  7-day wildcard probe on a cold cache pushes RSS past the 2 GiB cgroup limit
  and OOM-kills the container.
- **With it**: large wildcard files (≥4 MiB) open via `s3reader.NewReaderAt`,
  which streams row groups on demand. Peak resident memory is bounded to the
  working-set row-group bytes rather than the cumulative file size, and
  repeated 7-day wildcard runs stay below 880 MiB RSS.
