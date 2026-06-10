# S3 / scan optimization plan — ClickHouse-parity on the pure-S3 fallback

> **STATUS UPDATE (2026-06-10) — priorities superseded by [`s3-optimization-research.md`](s3-optimization-research.md)**
> (ClickHouse-source deep-dive + live protocol experiments). Key revisions: the bare
> `parquet.OpenFile` defaults are a serial 4–6-GET-per-open problem the original plan missed
> (zero-GET opens + `ReadModeAsync` are now Tier-1); the coalescing gap (64 KB) is mispriced
> ~16× at real S3 latency; hedged GETs added (AWS-endorsed); old deep items #3 (full
> per-column decode) and #6 (cross-file scheduler) are DEPRIORITIZED; old quick-win #3 is
> obsoleted by footer absorption into `_pmeta.bundle`; quick-win #4 had already shipped
> (PERF-2). **Correction:** this doc's "HTTP/2 + pooling" wording was wrong — AWS S3 and MinIO
> negotiate http/1.1 only (ALPN-verified); there is no multiplexing and
> `MaxIdleConnsPerHost` is the true parallelism ceiling.


Goal: (1) close the small LH-vs-hot-VL gaps on count/groupby/filtered, and (2) bring
LH's **pure-S3 Parquet scan** — the path taken when no in-memory index hits — to
ClickHouse-class performance. Grounded in a code map of `internal/s3reader/` and
`internal/storage/parquets3/` (file:line below).

## The key insight

The LH-vs-VL benchmark gaps (count_1h 4.8×, groupby 2.5×, filtered_count 2.4×) are
**NOT missing machinery** — they are an **unwired hint** and a few **unconditional
calls**:

- `WithCountOnlyHint` is **defined** (`interface.go:26`) but **never set in
  production** — `selectapi/handler.go:85` only sets `WithTimestampOnlyHint`. So a
  plain `* | stats count()` has no manifest fast-path and falls through to a full
  decode, even though `FileInfo.RowCount` is already summed elsewhere (`manifest.go:1097`).
- `preFilterFiles` (labels + **bloom sidecar GET per file**) and the 16-way
  `prefetchFooters` run **unconditionally even when `filter==nil`** — pure waste on a
  count/`*` workload (`storage_query.go:232,241`).

So the benchmark gaps close with **S-effort, low-risk** wiring — no new machinery.

The pure-S3 fallback gap vs ClickHouse *is* real machinery: `BufferedS3ReaderAt` is a
single mutex-guarded **synchronous** window (`buffered_reader.go:53-106`), the matched
row-group loop is **strictly serial** by design for OOM-safety (`storage_query.go:940`),
**per-column decode is absent**, and page-skipping is partial (stats read, all pages
materialized).

## What LH already has (strong foundation)

Range GETs, **range coalescing** (`coalescing_reader.go`), read-ahead buffer
(`buffered_reader.go`, 2 MB default), full-jitter retry/backoff (`reader.go:53-103`),
**cross-file** parallelism (8 workers), 16-way **footer prefetch**, HTTP/2 +
`MaxIdleConnsPerHost=128` pooling, column projection, row-group skipping via min/max +
bloom, cache-affinity file ordering, comprehensive S3 metrics. The gaps are within-file
pipelining and decode parallelism.

## Quick wins (close the VL gaps; days, low risk)

| # | item | area | effort | impact |
|---|---|---|--:|---|
| 1 | **Wire `WithCountOnlyHint`** + manifest RowCount-sum fast-path for unfiltered `* | stats count()` | `selectapi/handler.go:85`, `storage_query.go:220` | **S** | count_1h **183→~50ms** (4.8×→<1.5×) |
| 2 | **Skip `preFilterFiles`+bloom when `filter==nil`** | `storage_query.go:232` | S | −30–40ms + removes N bloom GETs |
| 3 | **Defer footer prefetch** to the post-fast-path `remaining` set | `storage_query.go:238-241` | S | up to −1000 footer GETs on wide windows |
| 4 | **Filtered count via `LabelAggregates`** (equality/`:in` on a bloom-indexed field) | `storage_query.go:542`, `manifest.go:931` | M | filtered_count **91→~45ms** |
| 5 | **Lift the 5-field label-aggregate cap** → config-driven (cardinality-guarded) | `labels.go:85-138` | M | groupby on new fields → metadata-only |
| 6 | Record the **empty-value group** at write time | `labels.go:113-122` | S | removes query-time reconstruction |
| 7 | **Tunable footer-prefetch threshold / tail** (128KB band, 64KB tail) | `footer_prefetch.go:25,29` | S | removes full-file fallbacks |

## Deep machinery (ClickHouse-parity pure-S3 scan; higher value, higher risk)

| # | item | area | effort | impact |
|---|---|---|--:|---|
| 1 | **Async double-buffered read-ahead** (prefetch next window while decoding current) | `buffered_reader.go:32-106` | M | hides 20–60ms S3 RTT behind decode; **2–4× per-file** on multi-page RGs |
| 2 | **Pipelined column-chunk prefetch across row groups** (fetch RG N+1 while decoding N) | `storage_query.go:933-948` | L | largest win for wide cold scans; decode-bound not RTT-bound |
| 3 | **Per-column parallel decode** within a row group | `reader_columnar.go:110-140`, `reader_projected.go:112-143` | L | wide-projection/few-file scans → decode ~1/numCols |
| 4 | **True page-level skipping** (fetch only pages whose stats overlap, via OffsetIndex) | `storage_query.go:2367-2384` | L | needle-in-haystack queries skip most page I/O |
| 5 | **Recycling buffer pool** (`sync.Pool`) for range/decode buffers | `buffered_reader.go:82` | M | lower GC pressure / steadier p99 on cold scans |
| 6 | **Cross-file range coalescing** / query-scoped parallel prefetch scheduler | `coalescing_reader.go:74-91` | L | saturates S3 (CH-style massive parallel GETs); RTT-bound → bandwidth-bound |
| 7 | **PERF-2 manifest rollup** (partition→field→value→rowcount) for boundary-tolerant aggregates | `manifest.go:931-950`, `partition_stats.go` | L | durable groupby/count fast-paths across partial windows |

## The unifying constraint: the 2 GiB container budget

**Every deep item must be subordinated to the existing memory governors** — `rgDecodeSem`
(`query_memory_budget.go:128`), `fileBudgetSem` (`:216`), `liveBytes` (`:116`) — or it
reintroduces the OOM those bounds were written to prevent. Async prefetch + pipelined
column fetch + per-column decode all *multiply* in-flight buffers; each needs a bounded
prefetch depth (1–2) and to charge prefetched bytes against the budget. Cross-file
prefetch additionally needs an **adaptive in-flight cap that backs off on
`S3ThrottleTotal`** (S3 SlowDown).

## Recommended order

1. **Ship the 7 quick wins first** (days) — closes the benchmark gaps; no new machinery.
2. **Then deep machinery in listed order** — async read-ahead (#1) and cross-RG prefetch
   (#2) give the biggest pure-S3 win per unit effort; per-column decode (#3), page
   skipping (#4), and cross-file coalescing (#6) are the higher-ceiling, higher-risk L
   items. PERF-2 (#7) is the durable rollup behind quick-wins #1/#4.

No-upstream-modification holds throughout (page skipping must drive parquet-go via
OffsetIndex ranges, not a fork); rollups live in the manifest/sidecar, never in custom
Parquet framing (pure-Parquet-on-S3).
