# S3 read-path optimization research — PR 2 pre-implementation review

**Status: REVIEWED & APPROVED (2026-06-10)** — with three adjustments:
1. **Design philosophy is ClickHouse-first.** Loki/Tempo findings are used ONLY where they are
   neutral library facts (parquet-go options, cache-section hooks on a dependency we already
   pin) — their architectural patterns are NOT design references; CH's proven S3 machinery
   (memory-budgeted admission, plan-then-fetch, stage pipelines, adaptive timeouts) is.
2. **Per-signal tunability**: logs LH and traces LH have different traffic patterns — every
   knob introduced here (read buffer, read-ahead window, coalesce gap, hedge delay, prefetch
   budget) must be configurable per signal (the chart already renders per-signal config
   overrides), with defaults tuned per module from the benchmarks.
3. **S3 Express One Zone for the metadata tier: approved as a CONFIGURABLE choice integrated
   with the config profiles** — never hardcoded. max-durability → metadata on multi-AZ
   standard S3 (default); max-performance → Express One Zone directory bucket via
   BucketRouterFunc; balanced → standard S3. Documented trade-off (1-AZ durability acceptable
   because bundles are footer-rebuildable) surfaces in the profile docs.

 Deep research for the S3-scan track, three angles, all
source-verified (file:line / URL cites in the agents' transcripts): (A) **ClickHouse**
object-storage machinery from master source, (B) the **Go/observability neighbors** —
Tempo vParquet4 (same domain, same parquet library!), Loki, Quickwit, DataFusion/arrow-rs,
(C) a **gap-hunt** beyond the existing plan incl. live protocol experiments against AWS S3
and MinIO. This document **supersedes the priority order** in `s3-scan-optimization-plan.md`.

## The three discoveries that change everything

### 1. Our parquet opens are a serial 4–6 GET catastrophe — and the fix is mostly library options
We pin **the same parquet-go v0.29.0 Tempo uses**, but call bare `parquet.OpenFile()` with
defaults at all three ranged-open sites (`storage_query.go:695/722/755`):
- `ReadModeSync` (async page read-ahead exists: `ReadModeAsync` — Tempo uses it),
- `ReadBufferSize = 4 KB` (the option's own docs say "more like 4 MiB" for network storage),
- **eager PageIndex + bloom-section reads from S3 on every open** (we prune via manifest/pmeta
  already — these are pure extra GETs),
- a 4-byte magic read at offset 0 that our `BufferedS3ReaderAt` turns into a **wasted ~2 MB
  GET at the head of every file**.

Tempo opens blocks with the opposite of every default + a `cachedReaderAt` that serves the
footer/column-index/offset-index sections from cache via parquet-go's `Set*Section` hooks and
**synthesizes the magic/footer-length reads** — a warm open costs **zero S3 GETs for metadata**.
We already cache parsed footers; we just never wired them into `OpenFile`.

### 2. HTTP/2 on S3 is a fiction (verified live)
ALPN against AWS S3 **and** MinIO negotiates `http/1.1` only (GCS does h2). Our
`ForceAttemptHTTP2` is a no-op; there is **no multiplexing** — `MaxIdleConnsPerHost=128` is the
real parallelism ceiling. The plan doc's "HTTP/2 + pooling" framing must be corrected so
benchmark analysis doesn't misattribute wins.

### 3. Our coalescing gap is mispriced by ~16× at real S3 latency
AnyBlob (VLDB 2023, measured): 8–16 MiB ranges are cost-throughput optimal; first-byte latency
dominates small requests. At 100 ms RTT, over-fetching 1 MB costs ~10–20 ms of transfer but
saves a ~100 ms round trip — the breakeven coalescing gap is **megabytes, not our 64 KB**, and
our 2 MB read-ahead is ~5× under ClickHouse's 10 MiB default I/O unit for scans (needle queries
should keep small windows → adaptive).

## The revised plan (replaces the old 7+7 ranking)

### Tier 1 — small effort, large wins (the new quick wins)
| # | item | source | effort | expected |
|---|---|---|---|---|
| 1 | **Zero-GET file open**: wire the footer cache into `OpenFile` via `Set*Section` hooks + synthesize magic/length reads + `SkipPageIndex/SkipBloomFilters(true)` + `FileSchema` + store FooterSize in pmeta | Tempo | M | removes ~400–600 ms serial latency + ~2 MB waste per cold open at 100 ms RTT — **largest single unplanned win** |
| 2 | **`ReadModeAsync` + `ReadBufferSize(1 MiB)`** on the opens — library-native async page read-ahead | Tempo / parquet-go | **S** | replaces the plan's hand-built deep item #1 + the I/O half of #2/#3; 1.5–3× per-file on wide cold scans |
| 3 | **BDP-priced coalescing + adaptive read-ahead**: gap 64 KB → ~1 MB, window 2 MB → 8–16 MB when sequential (needle stays small) | AnyBlob / CH | S | round-trip-minimal wide scans |
| 4 | **Hedged GETs** (fire duplicate after adaptive p95 delay, ranges <~4 MB, fresh connection+DNS per AWS guidance) — our retry only fires on *errors*; a stuck GET stalls 30 s today | AWS official guidance / Quickwit | S | p99 on needle + count/groupby |
| 5 | **Singleflight on metadata GETs** (footers, blooms, pmeta bundles) — concurrent queries currently duplicate identical GETs | Quickwit | S | dedupe under concurrency |
| 6 | **Prefetch observability first**: hit/waste/cancel counters (CH pattern) | CH | S | proves/tunes everything above |

### Tier 2 — structural (the surviving deep items, reshaped)
| # | item | source | notes |
|---|---|---|---|
| 7 | **Footer absorption into `_pmeta.bundle`** (Quickwit hotcache): footer+page-index+bloom offsets ride the bundle → cold open = one exact-range GET | Quickwit | obsoletes old quick-win #3 + most of `prefetchFooters` |
| 8 | **Memory-budgeted prefetch admission** (budget decides depth, not a constant) + **vectored per-RG fetch** (batch all projected chunks of RG N in one coalesced round, then pipeline RG N+1) | CH / arrow-rs | the disciplined version of old deep #2, subordinated to the 2 GiB governors |
| 9 | **Late materialization + page-level skipping as ONE feature** (decode filter column first, fetch only surviving pages/columns via OffsetIndex `splitRange`-style) | CH parquet-v3 / DataFusion | subsumes old deep #4; gate on predicate selectivity (Tempo skips page index for scan-heavy work) |
| 10 | **Fetch/decode decoupling**: in-flight GETs toward 32–128 adaptive (token-bucket on `S3ThrottleTotal`), decode stays ~NumCPU | Loki (150 fetch / 16 decode) / CH | refines old deep #3/#6 |
| 11 | **S3 Express One Zone for the metadata tier** (route `_pmeta.bundle`/manifest keys via the existing `BucketRouterFunc`) — single-digit-ms metadata floor; bundles are footer-rebuildable so 1-AZ durability is acceptable | AWS | S code / M ops — deployment decision for you |

### Deprioritized / rejected (with reasons)
- **Cross-file coalescing scheduler (old deep #6)** and **full per-column parallel decode (old
  deep #3)**: largely subsumed by tiers 1–2 at far lower risk; revisit only if post-async
  profiles still show gaps.
- **Hedging via duplicated requests everywhere**: CH itself hedges replica RPCs, *not* S3 GETs —
  we hedge only small ranges with adaptive delay (item 4), not blanket duplication.
- **Parallel part-GETs / multipart alignment**: throughput-bound technique; we're latency-bound. Skip.
- **Transfer Acceleration**: long-distance only, per-GB fees — reject for in-region.
- **Dictionary-level RG pruning**: redundant with our three bloom layers — revisit on evidence.
- **Old quick wins kept**: count-only hint, skip preFilter/bloom on unfiltered, LabelAggregates
  fields config — still valid, fold into Tier 1's PR.

### Cross-pollination with the compression track — PROMOTED TO SCOPE per review
The user explicitly approved both (as data-layout techniques, not architectural philosophy):
- **Sharded bloom filters** (~100 KB shards; a trace-ID lookup GETs ONE shard instead of a
  whole per-file/partition bloom) → **PR 2b item 12**: evolve the pmeta bloom facet / bundle
  layout to shard by value-hash, shard size configurable per signal.
- **Dedicated columns** (promote hot attribute keys into real Parquet columns with their own
  stats/dictionaries — configurable per deployment) → **PR 3 addition** after the approved
  compression sequence; composes with the dict-tag work and the field/value catalog (which
  already knows which keys are hot).

## Proposed PR 2 shape (post-review)
**PR 2a (Tier 1, ~days):** items 1–6 (zero-GET opens + ReadModeAsync/1 MiB buffer +
Skip* hygiene explicitly confirmed in review) + the still-valid old quick wins + the HTTP/2
doc correction → re-run the full-scope benchmark plain + with-latency. All new knobs
per-signal configurable.
**PR 2b (Tier 2):** items 7–10 in that order, each benchmark-gated; item 11 separately as a
deployment decision.
