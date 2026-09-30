# Performance after the VictoriaTraces v0.12.0 bump (traces binary)

`main` (VictoriaTraces v0.11.0, VictoriaLogs v1.51.0 pin) against the v0.12.0 branch (VictoriaLogs
v1.52.0 pin), measured 2026-09-30 on a developer host (Apple M-series, 18 cores, darwin/arm64). Two
questions: did the cold field-metadata cells move, and what does the LogsQL latency offset cost on
the served default path.

**Rule applied: a fast wrong answer is never a win.** The field-metadata harness compares every
answer with the dataset's truth and times only exact ones; the served-handler benchmark fails on any
non-200 answer.

## 1. Field-metadata cells (traces)

The harness of [field-metadata-cells.md](field-metadata-cells.md) (`TestFieldMetadataMatrixTraces`),
built from each tree and run alternately, 5 rounds (order swapped every round), 2 iterations and 1
warm-up per cell and round, S3 first-byte latency 0 ms and 100 ms, 384 cells (192 per latency).

- Gate (`perf_rows.py check`, logs + traces matrices, the `field-metadata-perf` CI job): 384 cells
  measured, 0 failures.
- Deterministic counters (S3 GETs, S3 bytes, answering path, exactness): **identical in all 384
  cells**.
- Latency, branch p50 / main p50, per latency profile (144 cells each, load average about 3):

| S3 latency | median ratio | p90 ratio | worst ratio | cells above 1.2x | sum of p50, main / branch |
|---|---|---|---|---|---|
| 0 ms | 1.000 | 1.235 | 1.667 | 19 | 508.8 ms / 515.0 ms (+1.2%) |
| 100 ms | 1.000 | 1.010 | 1.500 | 1 | 14 751 ms / 14 684 ms (-0.5%) |

The cells above 1.2x are 1-6 ms cells where one millisecond is the whole difference (worst:
`traces.streams` compacted, narrow window, 1.2 ms to 2.0 ms; at 100 ms the worst is a 0.012 ms to
0.018 ms RAM answer). A second, busier run of the same A/B had a different set of worst cells and a
median of 0.96x, so this is timing noise, not a slower path. Nothing changed on the read path these
cells drive (the harness calls the cold storage directly).

## 2. The served LogsQL path, offset on and off

`BenchmarkServedLogsQL` (`lakehouse-traces/internal/selectapi/handler_bench_test.go`): a
`/select/logsql/*` request through the select handler (admission control, tenant scope, timeout,
LogsQL handler, storage adapter, pipes) against a tier of 100 blocks x 1000 rows, every row older
than the offset so the offset filters nothing out and only its cost shows. `main` serves it with
VictoriaLogs' handlers (no offset; the `disable_latency_offset` argument is ignored). The branch
serves it with VictoriaTraces' handlers, the offset on by default; `offset_off` sends
`disable_latency_offset=true`. Interleaved, 6 rounds of 300 iterations, median ns/op, load average
about 6 (other work on the host).

| case | main ns/op | branch ns/op | branch / main | main allocs | branch allocs |
|---|---|---|---|---|---|
| hits (5m step), offset on | 2 543 802 | 2 599 454 | 1.022 | 978 | 1000 |
| hits, offset off | 2 595 120 | 2 655 971 | 1.023 | 980 | 984 |
| query `* \| limit 100`, offset on | 2 121 012 | 2 107 550 | 0.994 | 251 | 278 |
| query limit 100, offset off | 2 076 320 | 2 193 031 | 1.056 | 252 | 262 |
| query `stats by (level) count()`, offset on | 3 336 834 | 3 395 990 | 1.018 | 375 | 408 |
| stats by level, offset off | 3 326 018 | 3 364 098 | 1.011 | 375 | 391 |
| query `stats count()`, offset on | 2 181 284 | 2 210 371 | 1.013 | 311 | 342 |
| stats count, offset off | 2 127 995 | 2 141 378 | 1.006 | 312 | 326 |
| `stats_query`, offset on | 2 114 455 | 2 138 156 | 1.011 | 309 | 340 |
| `stats_query`, offset off | 2 135 050 | 2 147 274 | 1.006 | 310 | 324 |

- Branch versus main is within about 2% for every case with the offset on (median 1.013); the spread
  between runs of the same binary on this loaded host is of the same size.
- The offset itself costs about 15-30 allocations per request (the added filter and its parse) and
  no measurable time: on the branch, offset on / offset off is 0.96-1.03 across the five cases.
- The cold Parquet scan is not part of this benchmark; it is the one thing the offset can change for
  a real query (a shorter time range prunes at least as many files), and the field-metadata cells
  above show the scan itself did not move.

Reproduce: `go test ./lakehouse-traces/internal/selectapi -run XXX -bench BenchmarkServedLogsQL`
(from `lakehouse-traces`, `GOWORK=off`); for main, copy the file into a checkout of main and swap
`vtstorageadapter.Init(st)` for `internalvlstorage.SetStorage(st, delete.NewTombstoneStore())`.
