# Compaction proof, 2026-10-03

Measured on current main `7189524487e7b1812a7b5185e97f34430c9e17b6` versus PR #347 (`032c1cbe` plus the archive-class freshness and partial-error fixes in the commit containing this report). Both Linux arm64 binaries use their separate VictoriaLogs pins; no dependency source was edited.

## Settled scan

Apple M5 Pro, Darwin arm64, 100,000 files, sequential idle runs, `-benchmem -benchtime=50x -count=10`. Main uses the same benchmark file copied into an isolated checkout. Main's interleaved log lines were removed without dropping any of the ten numeric samples. Reproduce: `GOWORK=off go test ./internal/compaction -run '^$' -bench BenchmarkScanSettled -benchmem -benchtime=50x -count=10` and compare with benchstat.

| Build | Time/file | Allocated/file |
|---|---|---|
| main | 35.3 ns ±2% | 260 B |
| PR | 45.3 ns ±2% | 5.2 B |
| PR, lifecycle freeze wired | 54.0 ns ±12% | 5.2 B |

`main.txt` and `pr.txt` contain the measured samples. PR time is 1.28x main without freeze and 1.53x with freeze. Bytes fall 98.01%. Bytes use decimal units; the earlier 248-byte figure mixed units. This measures the settled planner walk, not total resident manifest memory, full LIST refresh, or active merge cost. Fleet-scale figures in operations.md are assumptions, not measurements.

## Live stack and exact answers

Isolated Compose project `lhfinish347`, ports 39700–39708, RustFS, each main/PR instance limited to 2 CPUs and 1 GiB. The committed scripts under `scripts/bench/compaction` ingest 19 runs of 40 acknowledged rows per signal: tenant 1001 has 160 rows at 72 h, 1002 has 160 rows at 30 h, and 1003 has 440 rows at 2 h. Flush interval 5 s; compaction interval 20 s. Main and PR use separate buckets.

`exact-rows.jsonl`: each of logs-main, logs-PR, traces-main and traces-PR returns all 760 acknowledged distinct rows, with zero missing, duplicate, unacknowledged or non-200 results. `layout-1.jsonl` and `layout-2.jsonl` show unchanged settled PR layouts. The quiet closed tenant-hours converge from four L0 objects on main to one object on the PR. The newer tenant-hour retains two objects on both builds. This seed does not put different tenants in the same hour, so it proves quiet-tenant convergence and answer preservation; the multi-tenant churn and starvation assertions remain covered by the committed regression tests and multi-day simulation.

Each `query-ab-*.json` contains 72 validated main/PR query cells: count, grouping, filtering, projected rows, field enumeration, field values, hits and Jaeger services, both signals. All 72 cells pass at each of 0, 20, 50 and 100 ms injected S3 GET/HEAD latency. First response and warm p50 are separate. Empty tenant/window intersections are control cells, not throughput evidence. Only unordered enumeration value arrays are canonicalized; ordered result arrays remain ordered, and field hit counts are preserved.

The latency proxy targets the actual S3 endpoint used by all four builds. Missing-object GET probes measured 25.0, 54.0 and 104.8 ms at configured 20, 50 and 100 ms, versus 0.8 ms direct. PUT/LIST delay is zero. Warm caches can avoid S3; these measurements do not establish remote-S3 savings, p95 guarantees or production-scale throughput. Per-query S3 and process resource deltas were not captured, so no claim is made about those costs.

`hot-controls.json` compares main, PR and upstream VictoriaLogs v1.52.0 / VictoriaTraces v0.12.0, six interleaved repeats, count/grouping/projected rows, three numeric tenants and populated 1 h/6 h/24 h/7 d windows. All 72 cells return equal HTTP-200 answers. First request is recorded separately; warm p50 and the maximum of five warm samples (empirical nearest-rank p95) are reported. The logs reference was reseeded into a fresh isolated volume with event timestamps fixed to the original ingest hour after a fixture crossed an hour boundary. Comparisons start after its buffer flush. These are API response comparisons, not UI or broad reader compatibility claims.

## Review regressions and mutations

The new `TestCompactionSafety_` tests cover cached archive classes, an immutable cached-class scan snapshot, class refresh between tenant merges, current bucket membership, stale LIST observations, and forced partial errors. Scheduled, forced and Tier A paths run in logs and traces modes. Six independent guard-removal mutants were each exercised in both modules: all 12 were killed by semantic regression failures; sources were restored afterward. Existing planner/property/fault/tombstone tests remain in the storage-health job. The Python comparator's three tests prove unordered enumeration equivalence while preserving count changes and ordered-series differences.

Local short full suites pass in both modules; touched compaction, manifest and delete packages pass race tests in both modules. Conformance and offline parity harness checks pass. CI on the published commit is the authoritative full-suite, lint, e2e and live parity evidence.

Known limits: INTELLIGENT_TIERING archive access tiers are not revealed by LIST alone; mirrored lifecycle age rules and observed class changes cannot eliminate the interval before a remote transition is observed. Healthy-primary Tier A stealing (#348) and cross-hour daily rollup (#281) remain separate work.
