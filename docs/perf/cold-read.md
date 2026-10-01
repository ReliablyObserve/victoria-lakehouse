# Cold-read performance: zero-GET open and planned fetch

Measured effect of opening cached Parquet files with zero S3 round trips (footer and page index served from the footer cache) and of `planned` being the default read mode. Every number is **measured** in-process unless labelled otherwise.

## What is measured, and how

- **Harness.** `TestColdReadProfile` in `internal/storage/parquets3/cold_read_bench_test.go` and its traces twin run each shape through the real engine (`vlapp.RunQuery` to the external storage to `Storage.RunQuery` to the VictoriaLogs pipes), the same entry point as `/select/logsql/query`, over files written by the production writer (zstd, SBBF blooms, footer-KV token blooms). The data is deterministic: 150,090 log rows over 50 minutes (1/7 carry `BIGMARK`), or 60,000 spans.
- **S3.** `s3count_test.go` is an in-process S3 server that injects latency as time to first byte (one round trip per request) and counts every request at the HTTP layer: GETs, bytes, the peak in flight, and the **sequential round-trip chain** (the longest chain of requests each starting after the previous one ended). Wall time at a fixed latency is `chain x latency` plus CPU, so the GET, byte and chain columns are deterministic and the milliseconds depend on the host.
- **Layouts.** A is flush-like: 11 files of at most 15k rows, 20 row groups, 11.5 MB (`PROFILE_FILES=50` splits the same rows over 50 files). B is compacted-like: 3 files, 16 row groups each. Traces: 7 files, 60k spans.
- **State.** Steady state of a running node: footers cached, label index seeded, no data cache, so every repetition reads S3. The first query after a restart is reported separately below.
- **Answers.** Every timed answer is compared with the generator's ground truth (count shapes) and, by `table.py`, with the other build; a difference fails the run. All shapes below returned identical answers.
- **Reproduce.** `scripts/bench/cold-read/run.sh <base-tree> <pr-tree> <out-dir> [quick|full]` builds both trees' test binaries, runs the matrix and prints the tables; the raw JSONL is kept in `<out-dir>`.

The numbers below compare the previous read path (window mode, footer fetched on every open, page index read lazily) with this change, on the same host and the same data, interleaved by latency. The machine was shared and busy (load average 7-10): the 0 ms columns carry that noise (read them as within about 20%); the 20-100 ms columns are latency-dominated and repeat within a few percent.

## Logs, Layout A (11 files): p50 ms before to after

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `_time:50m BIGMARK level:=error \| count` | 21 → 15 | 836 → 118 | 77 → 20 | 15.5 → 4.76 | 16 → 2 |
| `BIGMARK \| count` | 18 → 13 | 738 → 115 | 75 → 20 | 14.9 → 4.72 | 14 → 2 |
| `{stream} BIGMARK \| count` | 25 → 18 | 845 → 120 | 90 → 40 | 18.8 → 5.26 | 16 → 2 |
| `BIGMARK \| by(service.name)` | 22 → 15 | 741 → 121 | 76 → 20 | 15.4 → 4.82 | 14 → 2 |
| `BIGMARK \| by(repro_layer)` | 36 → 25 | 1,254 → 132 | 120 → 31 | 24.3 → 6.02 | 21 → 2 |
| `_msg:=needle273 \| count` | 8.1 → 5.3 | 370 → 59 | 17 → 2 | 2.6 → 0.47 | 7 → 1 |
| `level:=error \| count` | 10 → 6.4 | 533 → 110 | 53 → 40 | 11.2 → 0.04 | 10 → 2 |
| `status:=500 \| count (unreg filter)` | 22 → 17 | 902 → 121 | 87 → 31 | 16.8 → 1.30 | 15 → 2 |
| `* \| count` | 6.6 → 4.6 | 213 → 5.2 | 22 → 0 | 2.9 → 0.00 | 4 → 0 |
| `hits * (no hint, as wired today)` | 6.5 → 6.9 | 262 → 55 | 25 → 3 | 3.6 → 0.00 | 5 → 1 |
| `* \| count +del(unreg dur_ms)` | 28 → 27 | 948 → 130 | 93 → 31 | 17.2 → 1.30 | 18 → 2 |
| `BIGMARK \| count +del(unreg)` | 31 → 30 | 1,232 → 131 | 128 → 31 | 26.3 → 6.02 | 24 → 2 |

At 20 and 100 ms, and on the core shapes:

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `_time:50m BIGMARK level:=error \| count` | 21 → 15 | 312 → 60 | 836 → 118 | 1,442 → 222 |
| `BIGMARK \| count` | 18 → 13 | 314 → 56 | 738 → 115 | 1,435 → 216 |
| `BIGMARK \| by(repro_layer)` | 36 → 25 | 538 → 69 | 1,254 → 132 | 2,364 → 231 |
| `level:=error \| count` | 10 → 6.4 | 224 → 50 | 533 → 110 | 1,027 → 209 |
| `status:=500 \| count (unreg filter)` | 22 → 17 | 391 → 61 | 902 → 121 | 1,842 → 219 |
| `* \| count` | 6.6 → 4.6 | 92 → 5.8 | 213 → 5.2 | 413 → 5.4 |
| `hits * (no hint, as wired today)` | 6.5 → 6.9 | 112 → 25 | 262 → 55 | 516 → 105 |
| `* \| count +del(unreg dur_ms)` | 28 → 27 | 409 → 68 | 948 → 130 | 1,744 → 228 |

`* | stats count()` drops to zero data GETs (22 to 0): the metadata-only path reads its column index from memory; `hits *` falls from 9-25 GETs to 3. `level:=error | stats count()` reads 45 KB instead of 11.6 MB, because planned fetches the one projected column chunk where the window reader read ahead through the whole file. `query=* limit 1000`, `BIGMARK limit 1000` and `* | limit 1000` read every column through the whole-object path and are unchanged (they differ by run-to-run noise: 5 interleaved repeats gave 272-368 ms before and 276-319 ms after at 50 ms); they are the next optimisation (late materialisation, early stop).

## Logs, 50 files (same rows): p50 ms before to after

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `BIGMARK \| count` | 19 → 13 | 785 → 161 | 1,829 → 375 | 3,597 → 725 |
| `level:=error \| count` | 12 → 8.7 | 617 → 160 | 1,470 → 375 | 2,962 → 727 |
| `status:=500 \| count (unreg filter)` | 25 → 20 | 745 → 172 | 1,776 → 383 | 3,377 → 750 |
| `* \| count +del(unreg dur_ms)` | 32 → 25 | 738 → 178 | 1,835 → 384 | 3,574 → 737 |
| `* \| count` | 7.3 → 5.0 | 318 → 5.6 | 737 → 5.5 | 1,435 → 5.1 |

Request count scales with files, so a 50-file window is where the old path hurt most: `BIGMARK | stats count()` 1,829 to 375 ms at 50 ms and 3,597 to 725 ms at 100 ms, with 258 to 50 GETs and 35 to 7 sequential round trips. The remaining 7 round trips are file admission (8 file workers); a query-wide wave removes them.

## Logs, Layout B (3 compacted-like files, 16 row groups): p50 ms before to after

| shape | 0 ms | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|--:|---:|---:|---:|
| `_time:50m BIGMARK level:=error \| count` | 33 → 30 | 806 → 83 | 30 → 16 | 19.3 → 4.76 | 15 → 1 |
| `BIGMARK \| count` | 28 → 24 | 801 → 76 | 30 → 16 | 19.3 → 4.72 | 15 → 1 |
| `BIGMARK \| by(repro_layer)` | 49 → 43 | 1,122 → 95 | 42 → 19 | 22.7 → 6.01 | 18 → 1 |
| `level:=error \| count` | 14 → 12 | 320 → 64 | 18 → 31 | 15.6 → 0.42 | 7 → 1 |
| `status:=500 \| count (unreg filter)` | 34 → 29 | 1,064 → 82 | 44 → 19 | 24.0 → 1.29 | 22 → 1 |
| `* \| count` | 7.3 → 6.6 | 112 → 6.8 | 6 → 0 | 2.5 → 0.00 | 2 → 0 |
| `hits * (no hint, as wired today)` | 9.3 → 8.4 | 213 → 58 | 12 → 7 | 8.2 → 0.00 | 4 → 1 |
| `* \| count +del(unreg dur_ms)` | 44 → 39 | 1,074 → 90 | 43 → 19 | 23.8 → 1.29 | 22 → 1 |

Layout B is the case where a writer that forgot the page index would lose: 16 row groups mean 32 lazy page-index reads per file. All footer-cache writers keep the stripe, so planned wins here too. `level:=error | stats count()` makes more GETs (18 to 31) but reads 0.42 MB instead of 15.6 MB, and the sequential chain falls from 7 to 1.

## Traces (7 files, 60k spans): p50 ms before to after

| shape | 0 ms | 20 ms | 50 ms | 100 ms |
|---|--:|--:|--:|--:|
| `name:=op-3 \| count` | 2.3 → 1.7 | 155 → 68 | 364 → 158 | 617 → 308 |
| `span_attr repro_layer \| by(name) (unreg)` | 5.7 → 7.3 | 120 → 27 | 264 → 57 | 617 → 107 |
| `_time:30m http.method:=POST \| count` | 3.0 → 2.6 | 96 → 23 | 263 → 55 | 511 → 105 |
| `* \| count` | 2.7 → 2.4 | 70 → 24 | 158 → 54 | 309 → 103 |
| `* \| by(service)` | 4.2 → 3.9 | 96 → 26 | 212 → 56 | 513 → 106 |
| `trace_id lookup` | 3.3 → 3.0 | 95 → 48 | 212 → 108 | 411 → 208 |
| `duration:>800ms \| count` | 3.3 → 2.7 | 90 → 25 | 260 → 56 | 513 → 105 |
| `query=* limit 1000` | 116 → 108 | 143 → 128 | 153 → 167 | 201 → 202 |
| `* \| count +del(unreg)` | 5.7 → 5.2 | 97 → 27 | 215 → 58 | 414 → 108 |

At 50 ms, with GETs, MB read and round trips:

| shape | 50 ms | GETs | MB read | seq. round trips (50 ms) |
|---|--:|---:|---:|---:|
| `name:=op-3 \| count` | 364 → 158 | 35 → 21 | 2.9 → 0.09 | 5 → 2 |
| `span_attr repro_layer \| by(name) (unreg)` | 264 → 57 | 28 → 19 | 2.2 → 0.36 | 4 → 1 |
| `_time:30m http.method:=POST \| count` | 263 → 55 | 18 → 8 | 1.5 → 0.01 | 5 → 1 |
| `* \| count` | 158 → 54 | 21 → 7 | 1.5 → 0.00 | 3 → 1 |
| `* \| by(service)` | 212 → 56 | 28 → 14 | 2.2 → 0.03 | 4 → 1 |
| `trace_id lookup` | 212 → 108 | 3 → 1 | 0.2 → 0.12 | 3 → 1 |
| `duration:>800ms \| count` | 260 → 56 | 28 → 14 | 2.2 → 0.15 | 4 → 1 |
| `query=* limit 1000` | 153 → 167 | 5 → 4 | 2.2 → 1.91 | 1 → 1 |
| `* \| count +del(unreg)` | 215 → 58 | 29 → 14 | 2.3 → 0.23 | 5 → 1 |

- Every shape except `query=* limit 1000` (whole-object path, unchanged) improves 2-5x at 20-100 ms.
- **Trace-by-ID** (`trace_id lookup`, the Jaeger and Tempo trace page) goes from 3 GETs and 212 ms to 1 GET and 108 ms at 50 ms. It stays on the window reader by design (a `trace_id` point lookup is routed to the window path, `lakehouse_s3_projected_fetch_fallback_total{reason="trace-id-lookup"}`), so the gain is the zero-GET open alone: the window reader is the same one the prototype showed to be best for this shape.
- Jaeger search by service, operation or duration, Tempo search and TraceQL metrics map to the `name:=`, `span_attr`, `duration:>`, `* | stats by (service)` and count shapes above; they are all at 1-2 sequential round trips after.

## First query after the footer cache is cold

The harness resets the footer cache before the first run of every shape (a restart, or the first query over files whose footers are not cached). It still wins, because the batch prefetch now keeps the stripe and the planned wave replaces the serial window reads: `BIGMARK | stats count()` 792 to 168 ms, `level:=error | stats count()` 584 to 163 ms, `* | stats count()` 270 to 59 ms at 50 ms (Layout A). A footer larger than the prefetch tail (token-bloom footers of 357-431 KB on 10 MB objects, traces `_trace_idx` footers) costs one extra round trip for the exact footer and one for the stripe on its first open: three in total instead of two. Traces first-run: `trace_id lookup` 366 to 315 ms, `query=* limit 1000` 218 to 249 ms (noise on a shape that is otherwise unchanged).

## The footer cache is bounded by bytes

`cache.footer_max_bytes` (default 256 MiB logs, 512 MiB traces) replaces the 10,000-entry bound. An entry is charged `len(tail) + 2.25 x footer + 56 KiB`, where the tail is the footer plus the page-index stripe; `TestCachedFooterWeightCalibration` measures the heap per entry on every run (model 1.04-1.27x of the heap on traces footers, 0.99-1.10x on logs):

| object | footer | entry (measured heap) |
|---|--:|--:|
| logs, 2,000 rows | 11 KB | 86 KB |
| logs, flush-sized (15,000 rows) | 47 KB | 204 KB |
| logs, 60,000 rows | 181 KB | 668 KB |
| traces, 15,000 spans | 143 KB | 436 KB |
| traces, 60,000 spans | 557 KB | 1,478 KB |

A 256 MiB logs cache therefore holds about 1,300 flush-sized or about 190 compacted footers. The previous bound did not limit bytes at all: each entry also pinned the whole footer-prefetch buffer.
