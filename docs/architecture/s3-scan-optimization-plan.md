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

## Implemented — Tier 1 batch 1 (PR 2a, branch feat/s3-tier1-batch1)

The research review (`s3-optimization-research.md`, branch docs/research-reviews)
superseded the priority order above; this batch lands research items **2, 3, 5, 6**
(zero-GET opens and hedged GETs follow in a later batch). Everything below is
measurable: the full-scope bench now snapshots the engine's `/metrics` before/after
every scenario and emits per-scenario S3-op deltas (`full-scope-s3-bench-s3ops.csv` +
a per-query ops table in the summary).

### 1. Read-path observability first (CH pattern)

New metrics (both modules, `internal/metrics/lakehouse.go`):

| metric | meaning |
|---|---|
| `lakehouse_s3_gets_by_phase_total{phase}` | GETs by phase: `open` (parquet.OpenFile), `page` (column/page reads incl. lazy index/bloom), `footer` (footer-cache fills), `bloom` (per-file `.bloom` sidecar) |
| `lakehouse_s3_gets_per_open` | histogram — GETs one ranged open needed (research baseline: 4–6 serial) |
| `lakehouse_s3_buffer_wasted_bytes_total` | window bytes fetched but never served before eviction (high-water-mark accounting) |
| `lakehouse_s3_coalesce_overfetch_bytes_total` | gap bytes fetched only because ranges were merged |
| `lakehouse_s3_readahead_grow_total` / `_reset_total` | adaptive window growth (scan) / reset (needle) events |
| `lakehouse_s3_head_bypass_reads_total` | tiny offset-0 magic reads served by exact-size GETs (each ≈ one window of head-waste avoided) |
| `lakehouse_s3_singleflight_dedup_total{kind}` | metadata GETs shared across concurrent queries (`footer` \| `bloom` \| `pmeta_bundle`) |

### 2. Parquet open hygiene (`openRangedParquet`, twin `parquet_open.go`)

All ranged `parquet.OpenFile` sites now pass: `SkipPageIndex(true)`,
`SkipBloomFilters(true)`, `OptimisticRead(true)`, `ReadBufferSize(1MB default)`,
`FileReadMode(ReadModeAsync)` and `FileSchema(...)` when the footer cache holds the
schema. Verified against parquet-go v0.29.0 source before relying on it:

- the per-file `.bloom` fallback (`checkFileBloom`) reads a SIDECAR object, not the
  parquet-internal bloom section — skipping internal blooms cannot affect it;
- the one parquet-internal bloom consumer (`bloomFilterSkip` via
  `ColumnChunk.BloomFilter()`) is **already lazy** in v0.29.0 (`readBloomFilter`
  CAS-caches on first use), so `SkipBloomFilters(true)` only removes the eager
  open-time header reads;
- `ColumnIndex()/OffsetIndex()` likewise fall back to lazy per-chunk reads under
  `SkipPageIndex(true)`, so row-group time pruning keeps working on demand;
- `AsyncPages` spawns one goroutine per `Pages` instance with an **unbuffered**
  channel — bounded at ~one page in flight per column reader; `Close()` drains it
  (all our page readers close via `defer`), with a GC finalizer as backstop. Safe on
  the wide-scan path, so async is NOT gated to projected reads; `s3.parquet_read_mode:
  sync` is the rollback switch.

### 3. BDP-priced coalescing + adaptive read-ahead (`internal/s3reader`)

- coalesce gap default 64KB → **1MB**, safety cap lifted 1MB → 16MB (AnyBlob: at
  ~100ms RTT the breakeven gap is megabytes; 8–16MiB ranges are cost-optimal);
- read-ahead window: 2MB base, **doubles after 2+ consecutive forward-sequential
  misses** up to `s3.read_ahead_max_bytes` (default 8MB), resets to base on a random
  seek — scan-sized I/O units without needle-query over-fetch (allocation-free:
  four scalar fields on the existing reader);
- tiny reads at offset 0 (parquet 4-byte magic) bypass the window via an exact-size
  GET — kills the ~2MB head-waste per cold open.

### 4. Singleflight on metadata GETs (`golang.org/x/sync/singleflight`)

`ClientPool.DownloadDedup/DownloadRangeDedup` (keyed by object key, + range for
ranges) wrap: footer-cache fills (`prefetchFooters`, `fetchFooterFile`,
`shouldSkipByFooter`, the inline open-path footer fetch), per-file `.bloom`
downloads, and the lazy partition `_bloom.bin` bundle load. The in-flight GET runs
on `context.WithoutCancel` so one cancelled query can't poison the shared result;
each waiter still honors its own ctx. `WarmPartitions` (startup-only) intentionally
not wrapped.

### New config keys (per-signal: each binary/deployment sets its own)

| key | flag | default |
|---|---|---|
| `s3.read_ahead_max_bytes` | `-lakehouse.s3.read-ahead-max-bytes` | 8MB |
| `s3.read_buffer_size` | `-lakehouse.s3.read-buffer-size` | 1MB |
| `s3.parquet_read_mode` | `-lakehouse.s3.parquet-read-mode` | `async` |
| `s3.coalesce_gap_bytes` (default change) | `-lakehouse.s3.coalesce-gap-bytes` | 64KB → 1MB |

HTTP/2 note (research discovery 2): ALPN against AWS S3 and MinIO negotiates
http/1.1 only — `ForceAttemptHTTP2` is a no-op and `MaxIdleConnsPerHost` is the real
parallelism ceiling. Keep that in mind when reading benchmark deltas; no code change
needed in this batch.

## Measured: Tier-1 batch 1 (2026-06-10, before/after on the live e2e stack)

Same harness, same data; before = main (post-#134), after = this branch. The new
per-scenario S3-op capture ran on the after side.

**With 100 ms ± 30 ms injected S3 latency (the regime this tier attacks):**

| scenario | before p50 | after p50 | Δ | CH p50 | LH/CH after |
|---|--:|--:|--:|--:|--:|
| count_1h | 1478 | **878** | **−41%** | 562 | 1.6× |
| count_24h | 4133 | **2475** | **−40%** | 557 | 4.4× |
| filtered_count_1h | 2240 | **1694** | −24% | 859 | 2.0× |
| fulltext_scan_1h | 1880 | 1829 | −3% | 612 | 3.0× |
| groupby_service_1h | 1711 | 1726 | ~0 | 590 | 2.9× |

**Plain (no injected latency):** improved across the board — count_24h now beats hot
in-memory VL (115 vs 132 ms, 0.9×); all scan scenarios ≤1.4× VL.

**What the new ops counters prove:**
- `gets/open = 2.0` — the open tax collapsed from the 4–6 serial GETs the research
  identified (head-bypass + OptimisticRead + Skip*); buffer hit rates 69–93%.
- count_1h still issues ~50 GETs / 8 MB per query — file opens for an answer the
  manifest already holds → the count-class endgame is the manifest fast-paths
  (batch 2), which should land ~10× UNDER CH, not at parity.
- groupby issues ~99 GETs/q despite the shipped PERF-2 aggregates — the fast-path is
  not engaging for this query shape under the benchmark window; root-cause in batch 2.
- Scans are now genuinely data-bound (~100 GETs/q serialized at RTT across 8 workers)
  → Tier-2 vectored per-RG fetch + wider in-flight + footer-in-bundle.
- `waste 7–12 MB/q` on scan paths: read-ahead over-fetch to tune via the new counters.

**Target set by review: LH ≥ CH-level under latency for every scenario** — batch 2
(count-class fast-paths + groupby root-cause) and Tier-2 (vectored fetch) carry that.

## Implemented — S3 batch 2 (branch feat/s3-batch2): waste-feedback read-ahead + compactor aggregate healing

Two data-driven fixes from the combined benchmark's waste columns: **filtered_count
fetched 46 MB/query that was never read (at a 56% buffer hit rate); fulltext 17 MB/q.**

### 2a. Waste-feedback read-ahead (`internal/s3reader/buffered_reader.go`, shared by both modules)

**Root cause.** The Tier-1 adaptive window had two signals — grow on 2+
forward-sequential misses, reset on random seek — and **no feedback from waste**.
The measured pattern (pruned filtered scans probing a few pages per file) hops
forward by *less than one window* at a time, so every hop classifies as
forward-sequential: each abandoned window was a **vote to grow** the next one. The
machine ratcheted to `read_ahead_max_bytes` and tiled never-read megabytes.

**Design (allocation-free — scalar math on the existing eviction path):**

- On every window eviction, compute the evicted window's waste ratio:
  `(bufEnd − servedEnd) / (bufEnd − bufStart)` — the same high-water-mark
  accounting that already feeds `lakehouse_s3_buffer_wasted_bytes_total`.
- ratio **> `s3.read_ahead_waste_threshold`** (default **0.5**) on a
  forward-sequential miss → **halve** the next window, floored at the
  `read_ahead_bytes` base, tick `lakehouse_s3_readahead_shrink_total`, and
  **revoke the growth credit** (`seqMisses = 0`): growth resumes only after 2+
  consecutive *efficient* windows.
- ratio below the threshold → the pre-existing grow/stay logic, unchanged.
- Random seeks keep the stronger reset-to-base behavior, unchanged.
- `>= 1` disables feedback (a window's waste ratio is strictly < 1); `<= 0`
  means "default", not "shrink on any waste". `openRangedParquet`'s file-size
  clamps stay authoritative for the base/max the feedback floors/ceils against.

Config: `s3.read_ahead_waste_threshold` / `-lakehouse.s3.read-ahead-waste-threshold`
(both mains, chart values.yaml + values.schema.json).

**Unit-measured** (deterministic sim in `waste_feedback_test.go`, page-probe shape:
one 256 KB page per 3 MB stride over a 64 MB file, production 2 MB base / 8 MB max):

| | bytes fetched | GETs |
|---|--:|--:|
| feedback OFF (pre-batch-2) | 57.7 MB | 9 |
| feedback ON (default 0.5) | **45.1 MB (−21.8%)** | 22 |

Steady-state never-read bytes per abandoned window drop from up-to-8 MB (grown max)
to the 2 MB base — 4× at defaults. **Honest trade-off:** the smaller window costs
more GETs on sparse patterns (9→22 above); under injected latency the per-GET RTT
cost competes with the bandwidth saved. The live benchmark decides; Tier-2 vectored
per-RG fetch is the real fix for GET counts.

### 2b. Compactor aggregate healing (`internal/compaction/compactor.go:387` → row extraction)

**Root cause.** The compacted output's `LabelAggregates` came from
`mergeFileLabelAggregates(g.Files)` — merging the INPUT FILES' maps. Every file
written before the #138 refresh-wipe fix carries an empty map, so merge-of-empties
stayed empty: **compaction could never heal pre-fix data**, and count/groupby
fast-paths missed compacted files forever (one driver of the count_24h gap — the
24 h window is exactly where compacted files dominate).

**Design.** The compactor already holds the merged ROWS. It now extracts aggregates
from them with the **same code the flush writer runs**:
`schema.ExtractLogLabelAggregates` / `schema.ExtractTraceLabelAggregates`, moved to
`internal/schema/label_aggregates.go` (shared `MaxLabelAggregateValues = 100` cap).
One implementation — both modules' flush writers and the compactor import it, so the
field list and the per-field cap cannot drift. The old map-merge survives only as a
test-only cross-check for the equivalence regression.

Regression tests pin: healing (nil-aggregate inputs → correct counts on output, logs
+ traces), equivalence (aggregated inputs: row extraction == map merge), and the
absent-value cap contract (an over-cap field is ABSENT from compacted output exactly
like flush).

### Post-merge measurement protocol (containers run the merged build)

Run `scripts/bench/with-s3-latency.sh 100 30 scripts/bench/full-scope-s3-bench.sh`
against the live stack and compare to the Tier-1 batch-1 table above. Expected:

- per-scenario S3-op deltas: **waste B/q on filtered_count near 0 from 46 MB/q**,
  fulltext near 0 from 17 MB/q (`lakehouse_s3_buffer_wasted_bytes_total`), shrink
  counter active on those scenarios, grow still active on wide scans;
- filtered_count_1h / fulltext_scan_1h p50: bytes-on-wire saving vs GET-count cost
  nets out (sim says −22% bytes, +2.4× GETs — watch for p50 regression; the
  rollback is `read_ahead_waste_threshold: 1`);
- count_24h over successive compaction cycles: manifest-only answers stop degrading
  as partitions compact (healed `LabelAggregates` on compacted files — verify
  `count() by (field)` agreement before/after a forced `/internal/compact`).

## Measured: post-batch-2 live state (2026-06-10, main @ 88af62a — Tier-1 + #138 + batch 2 + L2+ RGs)

**Healing confirmed live**: after one settle+compaction cycle, the count fast-path served
**29/31 files from manifest aggregates** (was 0/N before #138+healing). `gets/open` stable
at **2.0** everywhere.

**Plain (no injected latency): LH ≈ hot VL across the board** — count_24h 0.9×, filtered
1.0×, groupby 1.0×, fulltext 1.2× of an in-memory hot engine, while CH-over-S3 trails
LH by 10–40× on every scenario in-run.

**At 100 ms ± 30 ms injected: count_1h 0.7× CH, groupby 0.7× CH, fulltext 1.6× CH
(improved from 2.0×), count_24h 1.7×, filtered_count 3.0× — the one structural laggard.**

**The remaining structural finding (next implementation target):** filtered_count still
wastes ~46 MB/q. The waste-feedback shrink never fired (`readahead_shrink_total = 0`)
because the adaptive state is **per-reader-instance**: each file open starts a fresh
window, wastes ~the 2 MB base on a sparse projected read, and closes before any
eviction-driven learning can apply — the lesson from file A never reaches file B.
Conclusion: for column-projected reads the speculative window is the wrong tool entirely;
the fix is CH-style **plan-then-fetch** (fetch exact coalesced column ranges, no window —
Tier-2 item 8/9 territory), plus cross-open adaptive state per signal. The within-file
shrink logic stays (it covers long multi-window scans).

**Bench-harness bug noted**: the fulltext row of the S3-ops capture produced negative
deltas (counter snapshot race) — the capture diffing needs hardening before the next
measured round.

Run-to-run variance caveat: absolute numbers (and CH's especially) swing with data shape
and proxy state between runs; within-run ratios and the ops counters are the stable
signals. Full tables: /tmp/final-{plain,lat}.md preserved in the bench artifacts.
