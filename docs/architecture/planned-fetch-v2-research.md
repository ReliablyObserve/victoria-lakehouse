# Planned-fetch v2 research — fewer GETs, for review

> **STATUS UPDATE — Slices 0+1 IMPLEMENTED (2026-06-11, branch `feat/s3-slice01`).**
> Shipped per §II.6: **0a** `s3.footer_prefetch_bytes` per-signal (logs 128KB / traces
> 640KB, clamped at max(64KB, size/8)) — the §II.2 footer bug-class is closed, with a
> both-twins regression (oversize footer prefetches+caches; no-footer counter silent);
> **0b** the bench S3-ops summary now derives per-scenario `plans/q` + `spans/plan`
> (reset-validated) and captures the new gap-choice/strategy counters; **1a**
> `s3.planned_fetch_max_inflight` default 16 → min(16, spans) per file; **1b** per-SPAN
> cap `s3.planned_fetch_span_cap_bytes` 16MB (CH `bytes_per_read_task`; spans split,
> plans never cap-rejected — admission via the memory ledger, ceiling fi.Size;
> `projected_fetch_max_bytes` deprecated to a parsed no-op); **1c** gap discipline over
> {64K, 256K, 1M} with cost = ceil(spans/k)·RTT + bytes/BW (RTT=100ms, BW=50MB/s — the
> RTT term counts k-wide waves, otherwise the largest gap trivially always wins), each
> candidate proven winnable in unit tests; **1d** the S* ladder (warm-footer ⇒ plan;
> cold < S* [logs 5MB / traces 8MB] ⇒ whole-file warmup via smart cache +
> `ParseFooterFromData`; cold ≥ S* ⇒ footer fetch ⇒ plan) — planned-path only, window
> default untouched; the 3 legacy thresholds (64KB `minFileSizeForRangeRead`, 128KB
> `minFileSizeForPrefetch`, 4MB wildcard cutoff) remain for Slice 3 unification.
> **Committed-sim re-run with the levers** (`TestPlannedFetch_V2LeversSimMeasurement`,
> count_24h geometry, RTT=100ms/BW=50MB/s): v1 (k=4) 3.08s → **v2 1.58s** → **0.58s
> with 0a-warm footers** at 600→520 GETs — inside the table's predicted 0.8–1.6s band.
> The live re-entry gate below still decides planned-by-default. Slice 2 (D3 codec fix
> → FacetPlanGeom) is next.

**Status: awaiting review.** Three research angles on why planned mode lost the live bench
and how v2 wins it: (A) failure anatomy + an **offline planner simulator over real L2
footers** (5 live files, 3 workload shapes, 5 planner variants), (B) source-level span
policies from ClickHouse Prefetcher / arrow-rs / DuckDB (constants cited), (C) an RTT-aware
cost-model design. Simulator preserved at `scripts/bench/plansim-scratch/` (untracked).

## The corrected root cause (better than the first verdict)

Plans were **already per-file** — the live verdict's "per-RG plans" was wrong about arming.
The real fragmentation, from code + footer geometry:
1. **Cross-RG spans never merge**: same-column chunks in adjacent row groups are separated
   by all other columns' chunks (~1.9 MB stride on real L2 files) — above the 1 MB gap, so
   every (RG × column-run) is its own span: 13–15 spans/file.
2. **The 16 MB cap punishes coalescing**: merged gap bytes count into the *plan total*, so
   exactly the merging that would cut GETs trips `fallback{reason=cap}`.
3. **k=4 span concurrency + all-or-nothing Fetch**: 13–15 spans become ~4 serial RTT waves
   per file; decode waits for the last span.
4. **2 serial RTTs per cold open** (magic+footer) — 65% of per-file wall on metadata-light
   scans once fetch is fixed.

## The simulator's verdict (real footers, RTT=100 ms, 50 MB/s/conn, 40 files / 8 workers)

| workload | window (live) | planned-v1 (live) | **v2' = per-file + k=16 + footer prefetch** |
|---|--:|--:|--:|
| count_24h | 2.45 s / 175 GETs | 17.7 s / 1185 GETs | **~0.8–1.6 s / ~600 GETs / 32 MB** |
| fulltext | 2.25 s | 5.6 s | **~1.5–1.9 s** |
| filtered_count (scan part) | 1.67 s | 7.1 s | **~1.6 s** |

Crucial negative result from the sim: a **flat 5 MB "RTT-aware" gap is a trap** on this
geometry — anything ≥ the 1.9 MB RG stride merges to whole-file, wasting 74–98% of bytes
(and at 50 MB/s/conn, transferring 24 MB costs more than one extra RTT wave saves). The
operative levers are **span concurrency and the cap scope**, not the gap.

## What the references actually do (constants)

- **ClickHouse** (remote): gap `min_bytes_for_seek` = **4 MiB**, span cap
  `bytes_per_read_task = 4×gap` = **16 MiB PER SPAN** (not per plan); whole-file range
  registration, sort once, coalesce across RGs; small ranges always coalescible,
  big ones only when `likely_to_be_used`; tasks dispatched immediately to a shared IO pool
  (no fixed per-file k), decode overlaps via stage scheduler with memory watermarks;
  waste tracked as `memory_amplification`.
- **arrow-rs/object_store**: coalesce parallelism **10**; per-RG fetch batching.
- **DuckDB**: BDP-based cost model for the gap (EWMA-refined per query) + the
  **dense fast path**: ≥95% of a row group's span needed → fetch the contiguous span whole.

## Proposed v2 (the composite the evidence picks)

| # | change | source | risk |
|---|---|---|---|
| 1 | **Span in-flight k: 4 → min(16, spans)** (`s3.planned_fetch_max_inflight`, per-signal) | sim lever #1 — biggest, smallest diff | S |
| 2 | **Cap re-scope: per-plan → per-SPAN 16 MiB** + plan admission via the existing memory ledger (retires `fallback{reason=cap}`) | CH `bytes_per_read_task`; unblocks coalescing | S/M |
| 3 | **Footer-open prefetch across files** (batch-open each worker wave's footers; warm queries already covered by the cache) | sim lever #2: count_24h ~1.56 s → ~0.8 s cold | M |
| 4 | **Streaming Fetch**: spans dispatched all-at-once, decode starts when the first needed span lands (per-span ready latches) | CH dispatch-immediately; fulltext −13%, UX latency better | M |
| 5 | **EWMA RTT/BW estimator** feeding gap* = clamp(RTT×BW, **1 MiB, 4 MiB**) — NOT the naive 5 MB (the sim's geometry trap); TTFB source = existing `S3RequestDuration` point | DuckDB cost model + CH 4 MiB; clamped by A's empirics | M |
| 6 | **Dense degeneration ≥95%** of file/RG span → 1–2 contiguous big spans (DuckDB rule) — the path to eventually retiring window mode entirely (Phase 3, own bench gate) | DuckDB `WHOLE_GROUP_PREFETCH` | M |
| 7 | **Density gate recast**: planned only when spans ≤ 2k AND per-span cap holds (a bytes-% gate would wrongly demote fulltext, which planned wins) | sim | S |

**Not doing**: flat 5 MB gap (proven trap), GET-parity-at-all-costs (the V5 split variant
trades 30× bytes for it — only if a real S3 bill demands it).

## Rollout gate (the #154 re-entry condition, made precise)
Opt-in `planned` until the live bench shows, on BOTH conditions (0 ms and 100/30 ms) and
both modules: **p50 ≤ window on ALL scenarios AND GETs/q within 4× of window** with
out-of-plan ≈ 0, fallbacks ≈ 0, waste ≈ 0. Plus the instrumentation the sim asked for:
per-scenario spans-per-plan + GETs-by-phase (the 1185 GETs/q didn't fully reconcile with
footer geometry — resolve before trusting the next verdict).

## Decision points for review
1. Approve the composite (1–7) and the order (1+2 first — they alone may pass the gate)?
2. The gap estimator clamp [1, 4] MiB — or skip the estimator in v2 and keep static 1 MiB
   (the sim says gap barely matters once k=16; estimator is the smallest win, most code)?
3. Phase-3 ambition (retire window entirely once dense degeneration proves itself) — in
   scope as a goal, or keep window indefinitely as the rollback?

---

# Part II — Hybrid planner v2.5: the adaptive decision package (for review)

Four research threads (component inventory + live distributions; hybrid architecture +
bundle facet, real-data-sized; accuracy/feedback; query-shape classifier). Governing
constraint throughout: the **pmeta economy rule** — one bundle read serves every consumer,
every metadata byte justified, RAM/disk bounded at scale.

## II.1 The data the planner was ignoring (inventory verdict)

Eleven components already hold planner-relevant knowledge (full table in the research
transcript). The load-bearing items:
- **`manifest.FileInfo` is the zero-GET first decision**: Size, CompactionLevel (live fact:
  **L0 is always 1 row group** — planning can never help there), RowCount, RawBytes ratio.
- **Footer cache `Has()` gates strategy**: warm footer ⇒ plan is metadata-free and beats
  whole-file at every measured size; only cold opens consult the size threshold.
- **Buffer watermark subtracts TIME**: query windows above `LastFlushWindowEndNs` need zero
  S3 work (trace-id completeness carve-out respected).
- **Catalog as selectivity oracle**: value absent ⇒ prune the partition for free; HLL/
  IsHighCard ⇒ sparse-vs-dense prior before any GET.
- **Three ad-hoc size cutoffs already exist** (64 KB / 128 KB / 4 MB) — unified under S*.
- **SmartCache span subtraction is NOT possible today**: chunk-cache entries store DECODED
  page values, not raw byte ranges — representation mismatch. Prerequisite fix identified
  (cacheOnFlush stores raw chunk bytes); deferred unless measurement justifies.

**S\* (cold whole-file threshold)**: per-signal, from the cost model on live distributions —
**5 MiB logs / 8 MiB traces**; below it, one whole-file GET with `ParseFooterFromData`
feeding the footer cache (the download IS the warmup).

## II.2 The bundle facet — sized on real data (the economy-rule decision)

| representation | logs L2 /file | traces L2 /file | L0 /file | verdict |
|---|--:|--:|--:|---|
| R0 footer locator only (off+len, RGs) | 11 B | 11 B | 8 B | in |
| **R0+R1 varint-delta chunk table + ONE pageIndexRegion range** | **4.3 KB** | **3.3–3.8 KB** | **0.56 KB** | **RECOMMENDED** |
| R3 raw footer absorption | 46–50 KB | **467–519 KB** | 8–16 KB | **rejected — economy rule** |

Structural luck: the page-index region is **100% contiguous in all 15 measured files** —
one (start,len) per file replaces 2×nRG×nCols index entries. R0+R1 gives the planner exact
spans for a whole partition from the bundle GET already loaded — zero per-file open RTTs —
at single-digit KB/file.

**Bug-class found while sizing (immediate, independent win):** every live traces
compacted-L2 footer is **467–519 KB** (the trace index lives in footer KV) — over the
64 KiB `footerPrefetchSize`, so footer prefetch hits `too_big`, the inline fetch fails, and
**traces L2 projected reads always fall back to full download today**
(`fallback{reason=no-footer}`). Logs footers fit with only ~25% headroom. Fixes, in order:
bump `footerPrefetchSize` 64→128 KB per-signal (also covers the page-index stripe, which
ends 91–97 KB from EOF — one GET then serves footer + all indexes); the facet's footer
locator then eliminates the class entirely.

**Rollback hazard to fix BEFORE shipping facet kind 7 (D3)**: old binaries must preserve
unknown facet kinds as opaque CRC'd payloads on decode→encode — today they would churn or
drop them. Small bundle-codec change, regression-tested, ships first.

## II.3 The strategy matrix (shape × file profile)

Shapes classified from state the query path ALREADY computes (two existing stages; no new
parsing). Misclassification analysis: every wrong cell degrades to today's behavior — some
routes are impossible by construction (wildcards cannot arm plans: nil projection no-ops).

| shape \ profile | <4 MiB (L0/P1) | 4–16 MiB (L1) | ≥16 MiB (L2+) |
|---|---|---|---|
| S0 metadata-answerable | metadata-serve | metadata-serve | metadata-serve |
| S1 needle (p99) | whole-file | planned-spans | planned-spans |
| S2 sparse projection (GETs+bytes) | whole-file | **planned-spans** (the −84% cell) | planned, density-gated |
| S3 dense scan (RTT-amortized) | whole-file | window → dense-degeneration (gated) | **window** (Phase-3 candidate) |
| S4 wildcard-wide (memory first) | whole-file | window range-read | window range-read |

Density gate: plan <15% of file AND spans ≤ 2k — gate fail IS the dense detector
(demotion, not a second classifier). Hysteresis on every boundary (e.g. 0.95 enter / 0.85
exit for whole-file degeneration).

## II.4 Accuracy + feedback (with one negative result)

- **OffsetIndex page-granular planning: NOT BUILT — measured 0.0%** on real footers
  (ts/filter columns are single-page-per-RG; the 1 MB gap rejoins skipped runs). Recorded
  per the protocol; revisit only if page counts per chunk grow.
- **Gap discipline instead** (biggest accuracy win, smallest diff): price each plan at
  candidate gaps {64 K, 256 K, 1 M} — pure in-memory math over already-parsed ranges — pick
  the cheapest. No estimator needed in v2.5 (answers Part-I decision point 2: skip the EWMA
  gap estimator).
- **Boundary-RG interpolation** for dense plans (−9…−25% bytes), gated on ts-sortedness
  via ColumnIndex monotonicity.
- **Feedback controller**: two per-signal EWMAs (waste ratio W, out-of-plan O),
  widen-fast (O>2/file, 8-plan dwell) / tighten-slow (W>0.5 ∧ O<0.1, 32-plan dwell),
  kill-switch to static defaults. **One new atomic total** (served-from-span bytes) — no
  new metric families. This is the PERF-6 substrate.
- **Stability matrix as implementation checklist**: every new input (manifest size, cache
  residency, facet geometry post-compaction, estimators) has an enumerated guard; the
  hybrid degrades to today's behavior, never below.

## II.5 Execution: global span scheduler

Replace per-file k with a **process-wide span pool** (default K=64, ceiling
`MaxIdleConnsPerHost=128`): all files' spans + exact footer ranges known at t=0 from the
bundle; spans dispatched globally; per-span ready latches let decode start as spans land.
Admission via the existing memory ledger (worker's fi.Size admission subsumes plans).

## II.6 Implementation slices (each behind the live-bench gate)

1. **Slice 0 (independent quick wins)**: footerPrefetchSize 64→128 KB per-signal; the
   spans-per-plan/GETs-by-phase instrumentation the verdict asked for.
2. **Slice 1**: v2 levers — k=min(16,spans) (until the global pool), per-SPAN 16 MiB cap,
   gap discipline, S* + footer-cache-gated strategy choice (the ladder without the facet).
3. **Slice 2**: bundle-codec opaque-unknown-facets fix (D3) → FacetPlanGeom kind 7 (R0+R1)
   filled at the three lifecycle hooks (flush/compaction/expiry — zero extra S3 I/O) →
   zero-RTT plans + global span scheduler.
4. **Slice 3**: shape router formalization + feedback controller (hysteresis, kill-switch).
5. **Phase 3 (own gate)**: dense-degeneration maturity → window retirement decision.

Gate per slice: p50 ≤ window on ALL scenarios, both latency conditions, both modules;
GETs/q ≤ 4× window; out-of-plan/fallback/waste ≈ 0; **plus the economy gate**: bundle
growth + ResidentBytes measured on the live bucket in every metadata slice.

## II.7 Decision points for review (Part II)

1. **FacetPlanGeom = R0+R1** (4.3 KB/file ceiling, rejected R3 footer absorption) — approve?
2. **S\*** = 5 MiB logs / 8 MiB traces, footer-cache-gated — approve?
3. **Slice order above** (notably: D3 codec fix before the facet; gap discipline instead of
   the EWMA estimator; OffsetIndex paging not built) — approve?
4. **Global span scheduler K=64 process-wide** (vs per-query) — approve?
5. SmartCache raw-chunk representation change (enables span subtraction): defer until a
   measurement shows cached-span overlap matters, or include in Slice 2?
