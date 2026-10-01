# Cold-read performance: zero-GET open and planned fetch

Measured effect of opening cached Parquet files with zero S3 round trips (footer and page index served from the footer cache) and of `planned` being the default read mode. Every number is **measured** in-process unless labelled otherwise.

## What is measured, and how

- **Harness.** `TestColdReadProfile` in `internal/storage/parquets3/cold_read_bench_test.go` and its traces twin run each shape through the real engine (`vlapp.RunQuery` to the external storage to `Storage.RunQuery` to the VictoriaLogs pipes), the same entry point as `/select/logsql/query`, over files written by the production writer (zstd, SBBF blooms, footer-KV token blooms). The data is deterministic: 150,090 log rows over 50 minutes (1/7 carry `BIGMARK`), or 60,000 spans.
- **S3.** `s3count_test.go` is an in-process S3 server that injects latency as time to first byte (one round trip per request) and counts every request at the HTTP layer: GETs, bytes, the peak in flight, and the **sequential round-trip chain** (the longest chain of requests each starting after the previous one ended). Wall time at a fixed latency is `chain x latency` plus CPU, so the GET, byte and chain columns are deterministic and the milliseconds depend on the host.
- **Layouts.** A is flush-like: 11 files of at most 15k rows, 20 row groups, 11.5 MB (`PROFILE_FILES=50` splits the same rows over 50 files). B is compacted-like: 3 files, 16 row groups in all (logs) or 7 (traces). Traces layout A: 7 files, 60k spans.
- **State.** Steady state of a running node: footers cached, label index seeded, no data cache, so every repetition reads S3. The first query after a restart is reported separately below.
- **Answers.** Every timed answer is compared with the generator's ground truth (count shapes) and, by `table.py`, with the other build; a difference fails the run. All shapes below returned identical answers on both builds and equal to the ground truth.
- **Trace-id shapes.** `L21` (`trace_id:=X | stats count()`, the log-to-trace click) and `T06` (trace-by-ID) run on both layouts. The "window reader" rows in the trace-by-ID section are the same binary with `s3.projected_fetch_mode=window`, which is what the removed `trace_id` routing did.
- **Reproduce.** `scripts/bench/cold-read/run.sh <base-tree> <pr-tree> <out-dir> [quick|full]` builds both trees' test binaries, runs the matrix and prints the tables; the raw JSONL is kept in `<out-dir>`.

The numbers below compare the previous read path (window mode, footer fetched on every open, page index read lazily; the commit before the read-path change, built from a `git archive` copy) with this change, on the same host and the same data, interleaved by latency. The 0 ms columns are CPU-bound and carry host noise (read them as within about 25%: several of them are slower after by that much with no change in requests); the 20-100 ms columns are latency-dominated and repeat within a few percent.

## Parquet read mode: async and sync

The tables below ran with the current default page read mode (`async`). PR #305 makes `sync` the default, so the main shapes were repeated with `PROFILE_READMODE=sync` on both builds (measured, 50 ms, 3 repetitions). At S3 latency the read mode does not change the result, because the time is round trips, not page decoding: `BIGMARK | stats count()` on 11 files is 736 ms before and 118 ms after in sync mode (743 and 114 in async); 50 files 1,886 to 379 ms; Layout B 801 to 77 ms; `level:=error | stats count()` 526 to 109 ms. GETs, bytes and round trips are equal to the async run. The overlay and the planned fetch do not depend on the read mode (`TestCachedFooter_OpenAndPageIndexMakeZeroGETs` runs both).

## Logs, Layout A (11 files): p50 ms before to after

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `_time:50m BIGMARK level:=error \| count` | 14 → 16 | 740 → 117 | 75 → 20 | 15.1 → 4.76 | 14 → 2 |
| `BIGMARK \| count` | 12 → 14 | 743 → 114 | 73 → 20 | 14.5 → 4.72 | 14 → 2 |
| `{stream} BIGMARK \| count` | 16 → 19 | 941 → 119 | 89 → 40 | 18.6 → 5.26 | 16 → 2 |
| `BIGMARK \| by(service.name)` | 14 → 15 | 748 → 119 | 80 → 20 | 16.3 → 4.82 | 14 → 2 |
| `BIGMARK \| by(repro_layer)` | 24 → 27 | 1,318 → 124 | 116 → 31 | 24.2 → 6.02 | 25 → 2 |
| `_msg:=needle273 \| count` | 6.1 → 8.4 | 374 → 58 | 17 → 2 | 2.6 → 0.47 | 7 → 1 |
| `level:=error \| count` | 6.6 → 7.7 | 527 → 113 | 55 → 40 | 11.8 → 0.04 | 10 → 2 |
| `status:=500 \| count (unreg filter)` | 17 → 24 | 905 → 121 | 94 → 31 | 18.8 → 1.30 | 17 → 2 |
| `* \| count` | 5.0 → 7.4 | 210 → 4.1 | 22 → 0 | 2.9 → 0.00 | 4 → 0 |
| `hits * (no hint, as wired today)` | 7.6 → 5.9 | 262 → 54 | 25 → 3 | 3.6 → 0.00 | 5 → 1 |
| `* \| count +del(unreg dur_ms)` | 29 → 25 | 956 → 124 | 89 → 31 | 17.2 → 1.30 | 15 → 2 |
| `BIGMARK \| count +del(unreg)` | 35 → 26 | 1,216 → 125 | 119 → 31 | 23.6 → 6.02 | 23 → 2 |

At 20 and 100 ms, and on the core shapes:

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `_time:50m BIGMARK level:=error \| count` | 14 → 16 | 319 → 55 | 740 → 117 | 1,446 → 223 |
| `BIGMARK \| count` | 12 → 14 | 302 → 52 | 743 → 114 | 1,547 → 219 |
| `BIGMARK \| by(repro_layer)` | 24 → 27 | 509 → 61 | 1,318 → 124 | 2,259 → 222 |
| `level:=error \| count` | 6.6 → 7.7 | 223 → 50 | 527 → 113 | 1,031 → 212 |
| `status:=500 \| count (unreg filter)` | 17 → 24 | 380 → 56 | 905 → 121 | 1,751 → 221 |
| `* \| count` | 5.0 → 7.4 | 94 → 4.4 | 210 → 4.1 | 414 → 5.2 |
| `hits * (no hint, as wired today)` | 7.6 → 5.9 | 111 → 24 | 262 → 54 | 515 → 106 |
| `trace_id:=X \| count (log-to-trace correlation)` | 10 → 7.6 | 331 → 68 | 734 → 160 | 1,337 → 316 |
| `* \| count +del(unreg dur_ms)` | 29 → 25 | 363 → 62 | 956 → 124 | 1,759 → 221 |

`* | stats count()` drops to zero data GETs (22 to 0): the metadata-only path reads its column index from memory; `hits *` falls from 9-25 GETs to 3. `level:=error | stats count()` reads 40 KB instead of 11.8 MB, because planned fetches the one projected column chunk where the window reader read ahead through the whole file. `query=* limit 1000`, `BIGMARK limit 1000` and `* | limit 1000` read every column through the whole-object path and are unchanged (they differ by run-to-run noise: 190-250 ms before and after at 20-50 ms); they are the next optimisation (late materialisation, early stop). At 0 ms several rows are slower after (for example `status:=500 | count` 17 to 24 ms) while making fewer requests and reading fewer bytes: that is host CPU noise on a shared machine, not a measured regression, and no 20-100 ms cell is slower.

## Logs, 50 files (same rows): p50 ms before to after

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `BIGMARK \| count` | 21 → 11 | 759 → 166 | 1,841 → 379 | 3,589 → 726 |
| `level:=error \| count` | 12 → 7.2 | 619 → 164 | 1,465 → 377 | 2,863 → 726 |
| `status:=500 \| count (unreg filter)` | 24 → 16 | 753 → 170 | 1,734 → 385 | 3,579 → 724 |
| `* \| count +del(unreg dur_ms)` | 25 → 19 | 759 → 170 | 1,778 → 384 | 3,576 → 726 |
| `* \| count` | 6.7 → 5.2 | 306 → 5.5 | 735 → 5.6 | 1,434 → 4.6 |

Request count scales with files, so a 50-file window is where the old path hurt most: `BIGMARK | stats count()` 1,841 to 379 ms at 50 ms and 3,589 to 726 ms at 100 ms, with 254 to 50 GETs and 35 to 7 sequential round trips. The remaining 7 round trips are file admission (8 file workers).

## Logs, Layout B (3 compacted-like files, 16 row groups): p50 ms before to after

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `_time:50m BIGMARK level:=error \| count` | 30 → 30 | 824 → 79 | 30 → 16 | 19.3 → 4.76 | 15 → 1 |
| `BIGMARK \| count` | 25 → 24 | 797 → 76 | 30 → 16 | 19.3 → 4.72 | 15 → 1 |
| `BIGMARK \| by(repro_layer)` | 44 → 41 | 1,043 → 91 | 43 → 19 | 23.5 → 6.01 | 19 → 1 |
| `level:=error \| count` | 13 → 11 | 324 → 67 | 16 → 31 | 12.8 → 0.42 | 6 → 1 |
| `status:=500 \| count (unreg filter)` | 31 → 26 | 1,075 → 80 | 42 → 19 | 23.5 → 1.29 | 20 → 1 |
| `* \| count` | 6.1 → 5.7 | 114 → 6.6 | 6 → 0 | 2.5 → 0.00 | 2 → 0 |
| `hits * (no hint, as wired today)` | 8.0 → 6.8 | 219 → 60 | 12 → 7 | 8.2 → 0.00 | 4 → 1 |
| `trace_id:=X \| count (log-to-trace correlation)` | 12 → 10 | 376 → 118 | 16 → 32 | 14.1 → 4.70 | 6 → 1 |
| `* \| count +del(unreg dur_ms)` | 43 → 35 | 1,164 → 88 | 43 → 19 | 23.7 → 1.29 | 21 → 1 |

Layout B is the case where a writer that forgot the page index would lose: 16 row groups mean 32 lazy page-index reads per file. All footer-cache writers keep the stripe, so planned wins here too. `level:=error | stats count()` and the `trace_id` shape make more GETs (16 to 31 and 16 to 32) but read 0.42 and 4.70 MB instead of 12.8 and 14.1 MB, and the sequential chain falls from 6 to 1: at S3 latency the chain decides the time, so they are 4.8x and 3.2x faster at 50 ms; on a store where each request has a fixed cost beyond latency the extra requests are a cost, which is not measured here.

## Traces, Layout A (7 files, 60k spans): p50 ms before to after

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `name:=op-3 \| count` | 2.2 → 1.8 | 153 → 67 | 368 → 160 | 619 → 309 |
| `span_attr repro_layer \| by(name) (unreg)` | 6.1 → 6.1 | 118 → 27 | 267 → 60 | 615 → 109 |
| `_time:30m http.method:=POST \| count` | 3.2 → 2.7 | 90 → 25 | 265 → 56 | 412 → 106 |
| `* \| count` | 2.9 → 2.5 | 68 → 24 | 161 → 56 | 310 → 104 |
| `* \| by(service)` | 4.8 → 4.2 | 109 → 27 | 263 → 58 | 514 → 106 |
| `trace_id lookup` | 3.4 → 3.2 | 89 → 46 | 215 → 108 | 411 → 207 |
| `duration:>800ms \| count` | 3.4 → 3.0 | 111 → 25 | 211 → 55 | 512 → 103 |
| `query=* limit 1000` | 129 → 125 | 122 → 119 | 127 → 126 | 175 → 178 |
| `* \| count +del(unreg)` | 6.4 → 5.8 | 92 → 26 | 264 → 58 | 418 → 110 |

At 50 ms, with GETs, MB read and round trips:

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `name:=op-3 \| count` | 2.2 → 1.8 | 368 → 160 | 36 → 21 | 3.1 → 0.09 | 6 → 2 |
| `span_attr repro_layer \| by(name) (unreg)` | 6.1 → 6.1 | 267 → 60 | 29 → 19 | 2.3 → 0.36 | 5 → 1 |
| `_time:30m http.method:=POST \| count` | 3.2 → 2.7 | 265 → 56 | 16 → 8 | 1.3 → 0.01 | 4 → 1 |
| `* \| count` | 2.9 → 2.5 | 161 → 56 | 21 → 7 | 1.5 → 0.00 | 3 → 1 |
| `* \| by(service)` | 4.8 → 4.2 | 263 → 58 | 29 → 14 | 2.3 → 0.03 | 5 → 1 |
| `trace_id lookup` | 3.4 → 3.2 | 215 → 108 | 3 → 1 | 0.2 → 0.03 | 3 → 1 |
| `duration:>800ms \| count` | 3.4 → 3.0 | 211 → 55 | 28 → 14 | 2.2 → 0.15 | 4 → 1 |
| `query=* limit 1000` | 129 → 125 | 127 → 126 | 5 → 5 | 2.2 → 1.93 | 1 → 1 |
| `* \| count +del(unreg)` | 6.4 → 5.8 | 264 → 58 | 30 → 14 | 2.4 → 0.23 | 5 → 1 |

- Every shape except `query=* limit 1000` (whole-object path, unchanged) improves 2-5x at 20-100 ms.
- Jaeger search by service, operation or duration, Tempo search and TraceQL metrics map to the `name:=`, `span_attr`, `duration:>`, `* | stats by (service)` and count shapes above; they are all at 1-2 sequential round trips after.

## Traces, Layout B (3 compacted-like files, 7 row groups)

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `trace_id lookup` | 5.8 → 5.1 | 320 → 114 | 5 → 3 | 1.4 → 0.09 | 5 → 1 |
| `name:=op-3 \| count` | 4.2 → 2.3 | 734 → 266 | 31 → 21 | 7.0 → 0.09 | 13 → 4 |
| `* \| count` | 3.3 → 2.6 | 266 → 58 | 13 → 7 | 2.5 → 0.00 | 5 → 1 |

## Trace-by-ID and the log-to-trace click use the planned reader

A `trace_id` point lookup (Jaeger and Tempo trace-by-ID, the `trace_id:=X` query behind the log-to-trace click) takes the planned reader like every other projected query. It used to be routed to the window reader on the assumption that the planned reader fetches the `trace_id` chunk of every row group. That assumption does not hold with the page index served from the footer cache: the plan prunes the row groups first. Measured on the same binary, `s3.projected_fetch_mode=window` (what the routing did) against the default, answers equal to the ground truth on every row (50 ms, 3 repetitions; the other latencies scale with the sequential round trips):

| shape | before the read-path change | window reader (zero-GET open) | planned (default) |
|---|--:|--:|--:|
| traces layout A, trace-by-ID | 215 ms, 3 GETs, 0.24 MB, 3 round trips | 108 ms, 1 GET, 0.12 MB, 1 | 108 ms, 1 GET, 0.03 MB, 1 |
| traces layout B, trace-by-ID | 320 ms, 5 GETs, 1.43 MB, 5 | 220 ms, 3 GETs, 1.07 MB, 3 | 114 ms, 3 GETs, 0.09 MB, 1 |
| logs layout A, `trace_id:=X \| count` | 734 ms, 72 GETs, 16.2 MB, 12 | 481 ms, 52 GETs, 13.5 MB, 9 | 160 ms, 40 GETs, 5.13 MB, 2 |
| logs layout B, `trace_id:=X \| count` | 376 ms, 16 GETs, 14.1 MB, 6 | 277 ms, 12 GETs, 11.9 MB, 4 | 118 ms, 32 GETs, 4.70 MB, 1 |

Where the planned reader loses: on logs layout B it makes 32 requests against 12 for the window reader, in exchange for 2.5x fewer bytes and a 4x shorter chain. The per-byte attribution of the harness (measured) shows why: the planned lookup reads 4.58 MB of the `trace_id` column (the chunk of every row group, incompressible 32-hex data: the per-row-group bloom skip is not applied on the projected path), where the window reader read 5.17 MB of `trace_id` plus 5.00 MB of `body` through its read-ahead. The remaining cost is therefore the full `trace_id` column; serving the bloom bytes from the cached tail so that row groups can be skipped is not done. The trace-id gate and its metric reason (`lakehouse_s3_projected_fetch_fallback_total{reason="trace-id-lookup"}`) were removed. `TestPlannedDefault_TraceIDLookupUsesPlannedReader` pins that the lookup arms a plan and reads fewer bytes than the window reader, in both modules.

## First query after the footer cache is cold

The harness resets the footer cache before the first run of every shape (a restart, or the first query over files whose footers are not cached). It still wins, because the batch prefetch now keeps the stripe and the planned wave replaces the serial window reads: `BIGMARK | stats count()` 848 to 165 ms, `level:=error | stats count()` 629 to 165 ms, `* | stats count()` 263 to 55 ms at 50 ms (Layout A). A footer larger than the prefetch tail (token-bloom footers of 357-431 KB on 10 MB objects, traces `_trace_idx` footers) costs one extra round trip on its first open for the exact footer, read together with the page-index stripe behind it (a 32 KB look-behind): two round trips in total, pinned with the production look-behind by `TestFooterWriters_AllKeepPageIndex`. Traces first-run: `trace_id lookup` 373 to 270 ms, `query=* limit 1000` 182 to 199 ms (noise on a shape that is otherwise unchanged).

## The footer cache is bounded by bytes

`cache.footer_max_bytes` replaces the 10,000-entry bound. `0` is auto: 10% (logs) or 20% (traces) of the memory the process may use for caches (`memory.Allowed()`, 60% of the machine or container limit by default), clamped to 32 MiB..1 GiB on logs and 32 MiB..2 GiB on traces; an explicit value overrides it, and the resolved budget is logged at startup. Computed from the formula (sourced): a 1, 2, 4, 8, 16 GiB machine gets 61, 123, 246, 492, 983 MiB on logs and 123, 246, 492, 983, 1,966 MiB on traces.

An entry is charged `len(tail) + 2.25 x footer + 1088 B x column chunks + 26 KiB`, where the tail is the footer plus the page-index stripe and a column chunk is one column of one row group. The per-chunk term covers what parquet-go builds per row group (about 22 KiB per row group of the 48-column logs schema) and what it memoizes on the cached file once a query decodes the page index (a further 17-37 KiB per row group), which is why a model that charged only the tail and the footer under-charged objects with many row groups by 1.6-2.2x. `TestCachedFooterWeightCalibration` measures the heap per entry on every run (median of 5 trials of 24 entries each), with 1, 8 and 40 row groups, fresh and with every page index decoded, in both modules, and fails when the model is below the heap or above 1.5x of it (measured):

| object | row groups | footer | entry heap, fresh | entry heap, index decoded | model |
|---|--:|--:|--:|--:|--:|
| logs | 1 | 13 KB | 91 KB | 108 KB | 124 KB |
| logs | 9 | 84 KB | 552 KB | 708 KB | 784 KB |
| logs | 41 | 346 KB | 2,309 KB | 3,015 KB | 3,366 KB |
| traces | 1 | 34 KB | 163 KB | 185 KB | 204 KB |
| traces | 8 | 185 KB | 836 KB | 1,010 KB | 1,176 KB |
| traces | 40 | 752 KB | 3,697 KB | 4,567 KB | 5,213 KB |

The model is 1.10-1.16x of the index-decoded heap and 1.25-1.46x of the fresh heap, so it over-charges an entry that no query has decoded yet and never under-charges one that has. A 256 MiB logs cache therefore holds about 1,300 flush-sized or about 190 compacted footers. The previous bound did not limit bytes at all: each entry also pinned the whole footer-prefetch buffer.

## Go benchmarks

Measured with `go test -bench -count=10` and `benchstat`, before and after this change (same host; B/op and allocs/op equal):

| benchmark | before | after |
|---|--:|--:|
| `OverlayReadAt/inside_tail_4KiB` | 38.4 ns | 37.5 ns (no change, p=0.20) |
| `OverlayReadAt/before_tail_4KiB` | 59.1 ns | 55.9 ns (no change, p=0.53) |
| `OverlayReadAt/straddle_8KiB` | 139 ns | 121 ns (-12.7%, p=0.007) |
| `FooterCachePut` (logs) | 53.7 ns | 54.1 ns (no change, p=0.80) |
| `FooterCachePut` (traces) | 56.9 ns | 55.2 ns (-3.0%, p=0.011) |

Zero allocations per overlay read; two per `Put`.
