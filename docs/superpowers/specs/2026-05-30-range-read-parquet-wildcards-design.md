# Range-Read Parquet Decode for Wildcard Queries — Design

**Date:** 2026-05-30
**Status:** Implemented in this PR (Goal B)
**Driving feedback:** `feedback_prove_on_large_data`, `feedback_post_work_resource_caps`

## Problem

Wildcard queries (`*` or no field filter; `queryColumns` returns
nil) used to always full-download every matched parquet file via
`bytes.NewReader(getFileData())`, pinning `fi.Size` bytes per worker
for the entire open-decode-emit window. Under 16-worker 7-day
wildcard load across ~600 files at avg ~30 MiB each, this was the
dominant retention path per the project's heap-diff analysis:
`io.ReadAll` held 808 MiB at the OOM peak, comprising the L1 cache
(512 MiB) plus 16 file workers × ~30 MiB resident file bodies all
pinned concurrently.

The pre-existing projected-query range-read path
(`shouldUseRangeRead`) only fired when the query projected fewer
than half the columns — wildcards by definition need all columns,
so they fell through to the full-download branch.

## Goals

1. Bound wildcard heap to working-set row-group bytes (typically
   <10 MiB per file) instead of cumulative file bytes.
2. Preserve VL/VT parity — the projected paths and cached-file
   paths must behave exactly as before.
3. Preserve cache benefits — files already in L1 cache should not
   re-issue range reads.
4. No new per-rg HTTP overhead for small files where data transfer
   is negligible.

## Non-goals

- Replacing the full-download path entirely — it's still the right
  call for small files and cache hits.
- Eliminating the L1 cache — cache hits short-circuit the new path.
- Per-rg memory accounting through the new `resourcebounds.Bound`
  (deferred — the existing `acquireRGDecode` semaphore + `fileBudget`
  already bound per-rg memory adequately).

## Design

### Switch: `shouldUseWildcardRangeRead(fileSize) bool`

Returns true when:
- `fileSize >= 4 MiB` (`minFileSizeForWildcardRangeRead`)

Returns false for:
- Files below 4 MiB (per-request HTTP overhead dominates)
- Zero or negative file size (treated as unknown — fall back to
  full download)

The cutoff is intentionally higher than the projected path's
`minFileSizeForRangeRead = 64 KiB`. Projected queries issue at most
N column-chunk range requests where N is the projection size;
wildcards issue ≥1 range request per row group (parquet-go fetches
pages as needed from the lazy ReaderAt). Per-request HTTP overhead
amortises better on the projected path because column data is
contiguous within the chunk, whereas row-group page fetches are
smaller and more numerous.

### Switch site: `openParquetFile`

The decision lives at the top of `openParquetFile` before the
existing full-download path:

```go
if s.pool != nil && projectedCols == nil && shouldUseWildcardRangeRead(fi.Size) {
    _, cached := s.memCache.Get(fi.Key)
    if !cached {
        rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
        buffered := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
        readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, int64(s.cfg.S3.CoalesceGapBytes))
        f, err := parquet.OpenFile(readerAt, fi.Size)
        if err == nil {
            metrics.S3RangeReadsTotal.Inc()
            metrics.ParquetFilesOpened.Inc()
            return f, nil
        }
        // Fall through on error
    }
}
```

The existing projected `shouldUseRangeRead` path runs above this
(unchanged), and the full-download path runs below (unchanged).

### Cache hit short-circuit

`s.memCache.Get(fi.Key)` is checked before issuing the lazy reader.
On hit, the cached data is already resident and the full-download
path returns immediately via `getFileData → memCache.Get → wrap in
bytes.NewReader`; issuing range reads against the cached file would
just add per-rg HTTP overhead with no memory benefit (the data is
already in memory).

### Failure mode

If `parquet.OpenFile` against the lazy ReaderAt fails (network
error, malformed footer mid-stream), the code falls through to the
full-download path. Same defensive pattern as the existing
projected range-read paths.

## Symmetry between modules

Both `internal/storage/parquets3/` (logs) and
`lakehouse-traces/internal/storage/parquets3/` (traces) get the
same switch. The traces module has a slightly different
`openParquetFile` shape (it uses `ParseFooterFromData` on the full-
download path) but the wildcard switch is structurally identical.

## Metrics

- `lakehouse_s3_range_reads_total` (existing counter) increments
  on every successful wildcard range-read open. Operators can
  compute the wildcard range-read fraction as
  `s3_range_reads_total / parquet_files_opened_total`.

## Test strategy

- Unit tests cover the switch's input boundaries (at, above, below,
  zero, negative file sizes) in both modules.
- The existing
  `TestRunQuery_WildcardManyFiles_MemoryBudget` test exercises the
  switch in integration against a mock S3 server (30 files × 5000
  rows × wildcard `*`) and asserts the peak heap stays under
  the 64x-max-block-size ceiling — same contract as before, but
  now satisfied via lower per-file resident bytes rather than
  per-RG decode caps alone.
- E2e: `probe_logs_Nday_wildcard.sh 7` runs against the live
  `mem_limit=2g` container and asserts no restart over a 7-day
  wildcard scan of ~600 files.

## Measurements

Post-Goal B, 3 back-to-back 7-day wildcard runs against the
production-shape compose stack:

| Run | Wall time | Container RSS peak |
|---|---|---|
| 1 | 33 s | 735 MiB |
| 2 | 44 s | 815 MiB |
| 3 | 47 s | 744 MiB |

`lakehouse_s3_range_reads_total` after 3 runs: 1062 of 3063 file
opens (~35%). Files under 4 MiB still full-download; files ≥4 MiB
use range-read.

Pre-this-work peak (per project history baseline): ~1.7 GiB on the
same 7-day wildcard. Post-Goal B brings this well under the 1.0 GiB
target.

## Deferred follow-ups

- Per-rg memory accounting through `resourcebounds.Bound` —
  potentially useful if `acquireRGDecode + fileBudget` ever needs
  reworking, but redundant with the current bounds.
- Negative-control pprof capture — would prove the heap reduction
  comes specifically from Goal B (not the resourcebounds changes
  in commits 1-4). Requires a revert + rebuild + restart + query
  cycle that takes ~10 min per direction; deferred to the next
  perf-focused PR if the project history baseline is challenged.
